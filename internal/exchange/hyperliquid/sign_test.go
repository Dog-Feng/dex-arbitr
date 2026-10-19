package hyperliquid

import (
	"bytes"
	"encoding/hex"
	"math"
	"strings"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// hexb decodes a hex string without a 0x prefix.
func hexb(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return b
}

// TestPackOrderActionGolden pins the MessagePack wire format to the official
// Python SDK struct encoding (field order type,orders,grouping and
// a,b,p,s,r,t,c with t:{limit:{tif}}).
func TestPackOrderActionGolden(t *testing.T) {
	c := "0x" + strings.Repeat("0", 31) + "1"
	got := packOrderAction(200002, true, false, "1356.1", "0.1", c)
	// Field order type,orders,grouping and a,b,p,s,r,t,c; t:{limit:{tif}};
	// asset 200002 = 0x30d42.
	want := hexb(t,
		"83"+
			"a4"+"74797065"+ // "type"
			"a5"+"6f72646572"+ // "order"
			"a6"+"6f7264657273"+ // "orders"
			"91"+
			"87"+
			"a1"+"61"+"ce"+"00030d42"+ // a: 200002
			"a1"+"62"+"c3"+ // b: true
			"a1"+"70"+"a6"+"313335362e31"+ // p: "1356.1"
			"a1"+"73"+"a3"+"302e31"+ // s: "0.1"
			"a1"+"72"+"c2"+ // r: false
			"a1"+"74"+"81"+"a5"+"6c696d6974"+ // t: {limit:
			"81"+"a3"+"746966"+ // {tif:
			"a3"+"496f63"+ // "Ioc"
			"a1"+"63"+"d922"+hex.EncodeToString([]byte(c))+ // c (34 bytes)
			"a8"+"67726f7570696e67"+ // "grouping"
			"a2"+"6e61", // "na"
	)
	if !bytes.Equal(got, want) {
		t.Fatalf("pack mismatch:\n got %x\nwant %x", got, want)
	}
}

// TestSignReferenceVectors pins signatures to values produced by the official
// hyperliquid-python-sdk 0.24.0 with the same key, wire action and nonce.
func TestSignReferenceVectors(t *testing.T) {
	const priv = "0x1111111111111111111111111111111111111111111111111111111111111111"
	s, err := newSigner(priv)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.EqualFold(s.address.Hex(), "0x19e7e376e7c213b7e7e7e46cc70a5dd086daff2a") {
		t.Fatalf("agent address %s", s.address.Hex())
	}
	nonce := uint64(1734398369395)
	cloid := "0x" + strings.Repeat("0", 31) + "1"
	sig, err := s.signOrder(200002, true, false, "1356.1", "0.1", cloid, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if sig.V != 27 && sig.V != 28 {
		t.Fatalf("v out of range: %d", sig.V)
	}
	if sig.R != "0x7f6bf447be31c191f13c093d4cc0cf8627fed36a18ed58de10cba1eb93fc2d0" ||
		sig.S != "0x4eba0099bf7ce6049f1b9fe4faabc00310ab0e3879529373241be5ecdc4e196e" ||
		sig.V != 28 {
		t.Fatalf("order signature mismatch: %+v", sig)
	}
	cancel, err := s.signCancelByCloid(200002, cloid, nonce+1)
	if err != nil {
		t.Fatal(err)
	}
	if cancel.R != "0x55c9ca5dd511d3c1d17ef34f485aa57a7f59591755b9c3d120e6471806457ccf" ||
		cancel.S != "0x6322e559ccadba766968de6454d0ad14962c0778115881511f3a73b18a5d4d85" ||
		cancel.V != 28 {
		t.Fatalf("cancel signature mismatch: %+v", cancel)
	}
	// The connection id the SDK hashes is keccak(packed || nonce || 0x00).
	packed := packOrderAction(200002, true, false, "1356.1", "0.1", cloid)
	conn := connectionID(packed, nonce)
	if hex.EncodeToString(conn) != "e10fac4d79c12c50d59ae8d0c478fe3d8029795c54b00e41ff5018a8b0e9b7df" {
		t.Fatalf("connection id mismatch: %x", conn)
	}
}

// TestSignSendAssetReference pins the agentSendAsset L1 signature to values
// produced with the official SDK's action_hash + l1_payload for the same
// action, key and nonce.
func TestSignSendAssetReference(t *testing.T) {
	s, err := newSigner("0x1111111111111111111111111111111111111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	sig, err := s.signSendAsset(
		"0xc01303fffd03b36666656b809808635d011a9197",
		"", "io", "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", "0.0001",
		1734398369395,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sig.R != "0x2898a4fe81e9575b31a0697072798466ed8ad3ada09fa9e262ec023f70fb6fc8" ||
		sig.S != "0x1d13fe2d4f866940b835db6503af853cf5d69fe20f9dca4ac66afac1c0cf2753" ||
		sig.V != 28 {
		t.Fatalf("sendAsset signature mismatch: %+v", sig)
	}
	packed := packAgentSendAsset(
		"0xc01303fffd03b36666656b809808635d011a9197",
		"", "io", "USDC:0x6d1e7cde53ba9467b783cb7c530ce054", "0.0001",
		1734398369395,
	)
	if got := connectionID(packed, 1734398369395); hex.EncodeToString(got) != "5201cf3f78e5ae4483b32741e6d2e2bbc3f117f2b54902ebc6876c7c40250618" {
		t.Fatalf("agentSendAsset connection id mismatch: %x", got)
	}
}

func TestCloidRoundTrip(t *testing.T) {
	for _, id := range []int64{0, 1, 42, 1<<48 - 1} {
		c, err := cloid(id)
		if err != nil {
			t.Fatalf("cloid(%d): %v", id, err)
		}
		back, ok := cloidToID(c)
		if !ok || back != id {
			t.Fatalf("cloid round trip: %d -> %q -> %d (%v)", id, c, back, ok)
		}
	}
	if _, err := cloid(-1); err == nil {
		t.Fatal("negative client id accepted")
	}
}

func TestBaseFees(t *testing.T) {
	taker, maker := baseFees(metaItem{}, false)
	if taker != 0.00045 || maker != 0.00015 {
		t.Fatalf("validator perp fees: %g/%g", taker, maker)
	}
	// io assets: growth mode + deployer fee scale 1.0 -> 2x, 0.1x.
	taker, maker = baseFees(metaItem{GrowthMode: "enabled", DeployerFeeScale: "1.0"}, true)
	if math.Abs(taker-0.00009) > 1e-12 || math.Abs(maker-0.00003) > 1e-12 {
		t.Fatalf("growth-mode HIP-3 fees: %g/%g", taker, maker)
	}
}

func TestSigFmt(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want string
	}{
		{1356.1, "1356.1"},
		{0.1, "0.1"},
		{767.84, "767.84"},
		{123456.789, "123456.789"},
	} {
		if got := sigFmt(tc.in); got != tc.want {
			t.Fatalf("sigFmt(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestMapOrderStatus(t *testing.T) {
	cases := map[string]domain.OrderStatus{
		"filled":             domain.StatusFilled,
		"canceled":           domain.StatusCanceled,
		"marginCanceled":     domain.StatusCanceled,
		"rejected":           domain.StatusRejected,
		"perpMarginRejected": domain.StatusRejected,
		"open":               domain.StatusOpen,
		"triggered":          domain.StatusOpen,
	}
	for in, want := range cases {
		if got := mapOrderStatus(in); got != want {
			t.Fatalf("mapOrderStatus(%q) = %v, want %v", in, got, want)
		}
	}
	if got := mapOrderStatus("weird"); got != domain.StatusUnknown {
		t.Fatalf("unknown status mapped to %v", got)
	}
}
