// Package hyperliquid implements exchange.Venue for Hyperliquid, including
// builder-deployed HIP-3 perpetual dexs such as Entropy (dex name "io").
package hyperliquid

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
)

// agentChainID is the EIP-712 domain chain id for API-wallet (agent) L1
// action signatures. The official SDK signs the "Agent" struct over this
// fixed domain for every action type.
const agentChainID = 1337

// signature is the API {r, s, v} payload the exchange endpoint expects.
type signature struct {
	R string `json:"r"`
	S string `json:"s"`
	V int    `json:"v"`
}

// signer holds the API-wallet (agent) identity and signs L1 actions.
type signer struct {
	key     *ecdsa.PrivateKey
	address common.Address
	// mainnet selects the EIP-712 "source" field ("a" mainnet, "b" testnet),
	// matching the official SDK's base_url == MAINNET_API_URL check.
	mainnet bool
}

// newSigner parses a 32-byte hex private key (optional 0x prefix).
func newSigner(hexKey string) (*signer, error) {
	key, err := crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(hexKey), "0x"))
	if err != nil {
		return nil, fmt.Errorf("invalid agent private key: %w", err)
	}
	// Mainnet is the default; NewWithConfig overrides it when the exchange
	// URL points elsewhere.
	return &signer{key: key, address: crypto.PubkeyToAddress(key.PublicKey), mainnet: true}, nil
}

// cloid encodes the client order id as a 128-bit hex string with 0x prefix.
func cloid(clientID int64) (string, error) {
	if clientID < 0 || clientID >= 1<<48 {
		return "", fmt.Errorf("client id out of range")
	}
	return "0x" + fmt.Sprintf("%032x", uint64(clientID)), nil
}

// cloidToID parses a 128-bit hex cloid back into a client id.
func cloidToID(s string) (int64, bool) {
	s = strings.TrimPrefix(strings.TrimSpace(s), "0x")
	if len(s) != 32 {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil || v >= 1<<48 {
		return 0, false
	}
	return int64(v), true
}

// signOrder signs an order action with the standard API-wallet scheme
// (sign_l1_action). The connection id is the keccak hash of the msgpack
// encoding of the action followed by the big-endian nonce and the vault
// marker, exactly as the official Python SDK computes it. expiresAfter is
// folded into the connection id when set.
func (s *signer) signOrder(asset int, isBuy, reduceOnly bool, price, size, c string, nonce, expiresAfter uint64) (signature, error) {
	packed := packOrderAction(asset, isBuy, reduceOnly, price, size, c)
	return s.signTyped(agentPayload(connectionID(packed, nonce, expiresAfter), s.mainnet))
}

// signCancelByCloid signs a cancelByCloid action.
func (s *signer) signCancelByCloid(asset int, c string, nonce, expiresAfter uint64) (signature, error) {
	packed := packCancelByCloidAction(asset, c)
	return s.signTyped(agentPayload(connectionID(packed, nonce, expiresAfter), s.mainnet))
}

// signSendAsset signs an agentSendAsset action: the agent moves its own
// account's collateral between perp dexs (or spot).
func (s *signer) signSendAsset(account, sourceDex, destDex, token, amount string, nonce uint64) (signature, error) {
	// agentSendAsset is an L1 action: the agent signs the phantom Agent
	// struct over connectionId = keccak(msgpack(action) ‖ nonce ‖ 0x00).
	packed := packAgentSendAsset(account, sourceDex, destDex, token, amount, nonce)
	return s.signTyped(agentPayload(connectionID(packed, nonce, 0), s.mainnet))
}

// signUpdateLeverage signs an updateLeverage action. expiresAfter is supported
// on L1 actions and folded into the connection id.
func (s *signer) signUpdateLeverage(asset int, isCross bool, leverage int, nonce, expiresAfter uint64) (signature, error) {
	packed := packUpdateLeverageAction(asset, isCross, leverage)
	return s.signTyped(agentPayload(connectionID(packed, nonce, expiresAfter), s.mainnet))
}

func (s *signer) signTyped(data apitypes.TypedData) (signature, error) {
	hash, _, err := apitypes.TypedDataAndHash(data)
	if err != nil {
		return signature{}, fmt.Errorf("typed data hash: %w", err)
	}
	raw, err := crypto.Sign(hash, s.key)
	if err != nil {
		return signature{}, fmt.Errorf("sign: %w", err)
	}
	// raw is [r(32) s(32) v(0|1)]; the exchange expects EIP-155 v in {27, 28}.
	// Render r/s without leading-zero padding, matching the SDK's to_hex.
	return signature{
		R: hexutil.EncodeBig(new(big.Int).SetBytes(raw[:32])),
		S: hexutil.EncodeBig(new(big.Int).SetBytes(raw[32:64])),
		V: int(raw[64]) + 27,
	}, nil
}

// connectionID builds the bytes32 hashed over the signed action, matching the
// official SDK: keccak(msgpack(action) || nonce_be64 || vault_marker). The
// vault marker is 0x00 when no vault address is configured. When expiresAfter
// is set, its big-endian uint64 timestamp follows the vault marker.
func connectionID(packed []byte, nonce, expiresAfter uint64) []byte {
	data := make([]byte, 0, len(packed)+17)
	data = append(data, packed...)
	data = append(data,
		byte(nonce>>56), byte(nonce>>48), byte(nonce>>40), byte(nonce>>32),
		byte(nonce>>24), byte(nonce>>16), byte(nonce>>8), byte(nonce),
		0x00,
	)
	if expiresAfter != 0 {
		data = append(data,
			byte(expiresAfter>>56), byte(expiresAfter>>48), byte(expiresAfter>>40), byte(expiresAfter>>32),
			byte(expiresAfter>>24), byte(expiresAfter>>16), byte(expiresAfter>>8), byte(expiresAfter),
		)
	}
	return crypto.Keccak256(data)
}

// agentPayload is the EIP-712 document for agent-signed L1 actions: primary
// type "Agent" over the fixed "Exchange" domain with chain id 1337.
func agentPayload(connID []byte, mainnet bool) apitypes.TypedData {
	source := "b"
	if mainnet {
		source = "a"
	}
	return apitypes.TypedData{
		Types: apitypes.Types{
			"EIP712Domain": {
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
			"Agent": {
				{Name: "source", Type: "string"},
				{Name: "connectionId", Type: "bytes32"},
			},
		},
		PrimaryType: "Agent",
		Domain: apitypes.TypedDataDomain{
			Name:              "Exchange",
			Version:           "1",
			ChainId:           (*math.HexOrDecimal256)(big.NewInt(agentChainID)),
			VerifyingContract: "0x0000000000000000000000000000000000000000",
		},
		Message: apitypes.TypedDataMessage{
			"source":       source,
			"connectionId": hexutil.Encode(connID),
		},
	}
}

// msgpack is a tiny deterministic MessagePack encoder for the exact wire
// shapes Hyperliquid signs. Field order matters and mirrors the official
// Python SDK struct definitions byte for byte.
type msgpack struct{ b []byte }

func (m *msgpack) mapHeader(n int) {
	if n <= 15 {
		m.b = append(m.b, 0x80|byte(n))
		return
	}
	m.b = append(m.b, 0xde, byte(n>>8), byte(n))
}

func (m *msgpack) arrayHeader(n int) {
	if n <= 15 {
		m.b = append(m.b, 0x90|byte(n))
		return
	}
	m.b = append(m.b, 0xdc, byte(n>>8), byte(n))
}

func (m *msgpack) str(s string) {
	if len(s) <= 31 {
		m.b = append(m.b, 0xa0|byte(len(s)))
	} else {
		m.b = append(m.b, 0xd9, byte(len(s)))
	}
	m.b = append(m.b, s...)
}

func (m *msgpack) uint(v uint64) {
	switch {
	case v <= 0x7f:
		m.b = append(m.b, byte(v))
	case v <= 0xff:
		m.b = append(m.b, 0xcc, byte(v))
	case v <= 0xffff:
		m.b = append(m.b, 0xcd, byte(v>>8), byte(v))
	case v <= 0xffffffff:
		m.b = append(m.b, 0xce, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	default:
		m.b = append(m.b, 0xcf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

func (m *msgpack) boolean(v bool) {
	if v {
		m.b = append(m.b, 0xc3)
		return
	}
	m.b = append(m.b, 0xc2)
}

// packOrderAction encodes {"type":"order","orders":[{a,b,p,s,r,t,c}],
// "grouping":"na"} with the SDK's key order.
func packOrderAction(asset int, isBuy, reduceOnly bool, price, size, c string) []byte {
	var m msgpack
	m.mapHeader(3)
	m.str("type")
	m.str("order")
	m.str("orders")
	m.arrayHeader(1)
	m.mapHeader(7)
	m.str("a")
	m.uint(uint64(asset))
	m.str("b")
	m.boolean(isBuy)
	m.str("p")
	m.str(price)
	m.str("s")
	m.str(size)
	m.str("r")
	m.boolean(reduceOnly)
	m.str("t")
	m.mapHeader(1)
	m.str("limit")
	m.mapHeader(1)
	m.str("tif")
	m.str("Ioc")
	m.str("c")
	m.str(c)
	m.str("grouping")
	m.str("na")
	return m.b
}

// packCancelByCloidAction encodes {"type":"cancelByCloid",
// "cancels":[{"asset":a,"cloid":c}]}.
func packCancelByCloidAction(asset int, c string) []byte {
	var m msgpack
	m.mapHeader(2)
	m.str("type")
	m.str("cancelByCloid")
	m.str("cancels")
	m.arrayHeader(1)
	m.mapHeader(2)
	m.str("asset")
	m.uint(uint64(asset))
	m.str("cloid")
	m.str(c)
	return m.b
}

// packUpdateLeverageAction encodes {"type":"updateLeverage","asset":a,
// "isCross":b,"leverage":n}.
func packUpdateLeverageAction(asset int, isCross bool, leverage int) []byte {
	var m msgpack
	m.mapHeader(4)
	m.str("type")
	m.str("updateLeverage")
	m.str("asset")
	m.uint(uint64(asset))
	m.str("isCross")
	m.boolean(isCross)
	m.str("leverage")
	m.uint(uint64(leverage))
	return m.b
}

// packAgentSendAsset encodes {"type":"agentSendAsset","destination":account,
// "sourceDex":src,"destinationDex":dst,"token":token,"amount":amount,
// "fromSubAccount":"","nonce":nonce} in the documented field order.
func packAgentSendAsset(account, sourceDex, destDex, token, amount string, nonce uint64) []byte {
	var m msgpack
	m.mapHeader(8)
	m.str("type")
	m.str("agentSendAsset")
	m.str("destination")
	m.str(account)
	m.str("sourceDex")
	m.str(sourceDex)
	m.str("destinationDex")
	m.str(destDex)
	m.str("token")
	m.str(token)
	m.str("amount")
	m.str(amount)
	m.str("fromSubAccount")
	m.str("")
	m.str("nonce")
	m.uint(nonce)
	return m.b
}
