// Hyperliquid L1 签名：对齐官方 Python SDK hyperliquid/utils/signing.py
//
// action_hash = keccak256(msgpack(action) || nonce_be64 || vault_marker [|| expires])
// 再对 Phantom Agent EIP-712（domain Exchange / chainId 1337）签名。
package main

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/signer/core/apitypes"
	"github.com/vmihailenco/msgpack/v5"
)

type hlSig struct {
	R string `json:"r"`
	S string `json:"s"`
	V int    `json:"v"`
}

type hlLimitTif struct {
	Tif string `msgpack:"tif" json:"tif"`
}

type hlOrderTypeWire struct {
	Limit hlLimitTif `msgpack:"limit" json:"limit"`
}

// 字段顺序必须与 Python order_wire 插入顺序一致：a,b,p,s,r,t[,c]
type hlOrderWire struct {
	A int             `msgpack:"a" json:"a"`
	B bool            `msgpack:"b" json:"b"`
	P string          `msgpack:"p" json:"p"`
	S string          `msgpack:"s" json:"s"`
	R bool            `msgpack:"r" json:"r"`
	T hlOrderTypeWire `msgpack:"t" json:"t"`
	C string          `msgpack:"c,omitempty" json:"c,omitempty"`
}

type hlOrderAction struct {
	Type     string        `msgpack:"type" json:"type"`
	Orders   []hlOrderWire `msgpack:"orders" json:"orders"`
	Grouping string        `msgpack:"grouping" json:"grouping"`
}

type hlCancelItem struct {
	A int   `msgpack:"a" json:"a"`
	O int64 `msgpack:"o" json:"o"`
}

type hlCancelAction struct {
	Type    string         `msgpack:"type" json:"type"`
	Cancels []hlCancelItem `msgpack:"cancels" json:"cancels"`
}

type hlCancelCloidItem struct {
	Asset int    `msgpack:"asset" json:"asset"`
	Cloid string `msgpack:"cloid" json:"cloid"`
}

type hlCancelByCloidAction struct {
	Type    string              `msgpack:"type" json:"type"`
	Cancels []hlCancelCloidItem `msgpack:"cancels" json:"cancels"`
}

func hlActionHash(action any, nonce int64) ([]byte, error) {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	enc.UseCompactInts(true)
	if err := enc.Encode(action); err != nil {
		return nil, fmt.Errorf("msgpack action: %w", err)
	}
	data := compactMsgpackStr16(buf.Bytes())
	nonceBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(nonceBytes, uint64(nonce))
	data = append(data, nonceBytes...)
	data = append(data, 0x00) // 无 vault
	return crypto.Keccak256(data), nil
}

func hlSignL1(pk *ecdsa.PrivateKey, action any, nonce int64, mainnet bool) (hlSig, error) {
	actionHash, err := hlActionHash(action, nonce)
	if err != nil {
		return hlSig{}, err
	}
	digest, err := hlTypedDigest(actionHash, mainnet)
	if err != nil {
		return hlSig{}, fmt.Errorf("eip712 hash: %w", err)
	}
	sig, err := crypto.Sign(digest, pk)
	if err != nil {
		return hlSig{}, fmt.Errorf("sign: %w", err)
	}
	return hlSig{
		R: hexutil.Encode(sig[:32]),
		S: hexutil.Encode(sig[32:64]),
		V: int(sig[64]) + 27,
	}, nil
}

func hlTypedDigest(actionHash []byte, mainnet bool) ([]byte, error) {
	source := "b"
	if mainnet {
		source = "a"
	}
	chainID := math.HexOrDecimal256(*big.NewInt(1337))
	typed := apitypes.TypedData{
		Domain: apitypes.TypedDataDomain{
			Name:              "Exchange",
			Version:           "1",
			ChainId:           &chainID,
			VerifyingContract: "0x0000000000000000000000000000000000000000",
		},
		Types: apitypes.Types{
			"Agent": []apitypes.Type{
				{Name: "source", Type: "string"},
				{Name: "connectionId", Type: "bytes32"},
			},
			"EIP712Domain": []apitypes.Type{
				{Name: "name", Type: "string"},
				{Name: "version", Type: "string"},
				{Name: "chainId", Type: "uint256"},
				{Name: "verifyingContract", Type: "address"},
			},
		},
		PrimaryType: "Agent",
		Message: apitypes.TypedDataMessage{
			"source":       source,
			"connectionId": hexutil.Encode(actionHash),
		},
	}
	digest, _, err := apitypes.TypedDataAndHash(typed)
	return digest, err
}

func hlCloid(clientOrderID string) string {
	h := crypto.Keccak256([]byte(clientOrderID))
	return "0x" + hex.EncodeToString(h[:16])
}

func isHLCloid(s string) bool {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "0x") && !strings.HasPrefix(s, "0X") {
		return false
	}
	raw := s[2:]
	if len(raw) != 32 {
		return false
	}
	_, err := hex.DecodeString(raw)
	return err == nil
}

// compactMsgpackStr16 把 str16（0xda）且长度 <256 收成 str8（0xd9），对齐 Python msgpack。
func compactMsgpackStr16(data []byte) []byte {
	out := make([]byte, 0, len(data))
	if walkMsgpack(data, 0, &out) != len(data) {
		return data
	}
	return out
}

func walkMsgpack(data []byte, pos int, out *[]byte) int {
	if pos >= len(data) {
		return 0
	}
	b := data[pos]
	remain := len(data) - pos
	if b <= 0x7f || b >= 0xe0 || (b >= 0xc0 && b <= 0xc3) {
		*out = append(*out, b)
		return 1
	}
	if b >= 0xa0 && b <= 0xbf {
		n := int(b & 0x1f)
		total := 1 + n
		if remain < total {
			return 0
		}
		*out = append(*out, data[pos:pos+total]...)
		return total
	}
	if b >= 0x80 && b <= 0x8f {
		return walkContainer(data, pos, int(b&0x0f)*2, 1, out)
	}
	if b >= 0x90 && b <= 0x9f {
		return walkContainer(data, pos, int(b&0x0f), 1, out)
	}
	switch b {
	case 0xcc, 0xd0:
		return copyN(data, pos, 2, out)
	case 0xcd, 0xd1:
		return copyN(data, pos, 3, out)
	case 0xce, 0xd2, 0xca:
		return copyN(data, pos, 5, out)
	case 0xcf, 0xd3, 0xcb:
		return copyN(data, pos, 9, out)
	case 0xd9:
		return copyVar(data, pos, 1, out)
	case 0xda:
		if remain < 3 {
			return 0
		}
		n := (int(data[pos+1]) << 8) | int(data[pos+2])
		total := 3 + n
		if remain < total {
			return 0
		}
		if n < 256 {
			*out = append(*out, 0xd9, byte(n))
			*out = append(*out, data[pos+3:pos+total]...)
		} else {
			*out = append(*out, data[pos:pos+total]...)
		}
		return total
	case 0xdb:
		return copyVar(data, pos, 4, out)
	case 0xdc:
		if remain < 3 {
			return 0
		}
		n := (int(data[pos+1]) << 8) | int(data[pos+2])
		return walkContainer(data, pos, n, 3, out)
	case 0xde:
		if remain < 3 {
			return 0
		}
		n := (int(data[pos+1]) << 8) | int(data[pos+2])
		return walkContainer(data, pos, n*2, 3, out)
	default:
		*out = append(*out, b)
		return 1
	}
}

func walkContainer(data []byte, pos, count, header int, out *[]byte) int {
	if len(data)-pos < header {
		return 0
	}
	*out = append(*out, data[pos:pos+header]...)
	consumed := header
	for i := 0; i < count; i++ {
		c := walkMsgpack(data, pos+consumed, out)
		if c <= 0 {
			return 0
		}
		consumed += c
	}
	return consumed
}

func copyN(data []byte, pos, n int, out *[]byte) int {
	if len(data)-pos < n {
		return 0
	}
	*out = append(*out, data[pos:pos+n]...)
	return n
}

func copyVar(data []byte, pos, lenBytes int, out *[]byte) int {
	header := 1 + lenBytes
	if len(data)-pos < header {
		return 0
	}
	n := 0
	for i := 0; i < lenBytes; i++ {
		n = (n << 8) | int(data[pos+1+i])
	}
	total := header + n
	if len(data)-pos < total {
		return 0
	}
	*out = append(*out, data[pos:pos+total]...)
	return total
}
