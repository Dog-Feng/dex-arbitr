package main

import (
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
)

func TestEntropyAvailableUSDCUnifiedUsesSpot(t *testing.T) {
	got := entropyAvailableUSDC(
		decimal.Zero,
		decimal.Zero,
		decimal.RequireFromString("50.869295"),
		decimal.Zero,
	)
	if !got.Equal(decimal.RequireFromString("50.869295")) {
		t.Fatalf("unified spot got %s", got)
	}
	// 手动分账：io 自己的保证金优先
	got = entropyAvailableUSDC(
		decimal.RequireFromString("20"),
		decimal.RequireFromString("100"),
		decimal.Zero,
		decimal.Zero,
	)
	if !got.Equal(decimal.RequireFromString("20")) {
		t.Fatalf("manual io got %s", got)
	}
}

func TestHip3AssetID(t *testing.T) {
	dexes := []json.RawMessage{
		[]byte("null"),
		[]byte(`{"name":"xyz"}`),
		[]byte(`{"name":"flx"}`),
		[]byte(`{"name":"vntl"}`),
		[]byte(`{"name":"hyna"}`),
		[]byte(`{"name":"km"}`),
		[]byte(`{"name":"abcd"}`),
		[]byte(`{"name":"cash"}`),
		[]byte(`{"name":"para"}`),
		[]byte(`{"name":"mkts"}`),
		[]byte(`{"name":"io"}`),
	}
	idx, err := perpDexIndex(dexes, "io")
	if err != nil {
		t.Fatal(err)
	}
	if idx != 10 {
		t.Fatalf("io index=%d want 10", idx)
	}
	if got := hip3AssetID(idx, 2); got != 200002 {
		t.Fatalf("SNDK asset=%d want 200002", got)
	}
}

func TestParseIoUniverseSkipsDelisted(t *testing.T) {
	meta := json.RawMessage(`{"universe":[
		{"name":"io:OAI","szDecimals":3,"isDelisted":true},
		{"name":"io:ANTH","szDecimals":3},
		{"name":"io:SNDK","szDecimals":4}
	]}`)
	assets, err := parseIoUniverse(meta, 10)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := assets["IO:OAI"]; ok {
		t.Fatal("delisted OAI should be skipped")
	}
	sndk, ok := assets["IO:SNDK"]
	if !ok {
		t.Fatal("missing io:SNDK")
	}
	if sndk.AssetID != 200002 {
		t.Fatalf("asset %d", sndk.AssetID)
	}
	if sndk.SzDecimals != 4 {
		t.Fatalf("szDecimals %d", sndk.SzDecimals)
	}
	if assets["SNDK"].AssetID != 200002 {
		t.Fatal("short name alias missing")
	}
}

func TestStripHip3Prefix(t *testing.T) {
	if got := stripHip3Prefix("io:SNDK"); got != "SNDK" {
		t.Fatalf("got %s", got)
	}
	if got := stripHip3Prefix("SNDK-USDG"); got != "SNDK-USDG" {
		t.Fatalf("got %s", got)
	}
}

func TestRoundHlPxDirection(t *testing.T) {
	// szDecimals=4 → pxDecimals=2（tick 0.01），但 5 位有效数字在 12.x 上更细，tick 生效。
	px := decimal.RequireFromString("12.346")
	down := roundHlPx(px, 4, false)
	up := roundHlPx(px, 4, true)
	if !down.Equal(decimal.RequireFromString("12.34")) {
		t.Fatalf("floor got %s", down)
	}
	if !up.Equal(decimal.RequireFromString("12.35")) {
		t.Fatalf("ceil got %s", up)
	}
	// 千档价 5 位有效数字是 0.1，比 tick 0.01 粗。
	hi := decimal.RequireFromString("1472.716")
	if !roundHlPx(hi, 4, false).Equal(decimal.RequireFromString("1472.7")) {
		t.Fatalf("sigfig floor got %s", roundHlPx(hi, 4, false))
	}
	sz := roundHlSz(decimal.RequireFromString("1.23456"), 4)
	if !sz.Equal(decimal.RequireFromString("1.2345")) {
		t.Fatalf("sz floor got %s", sz)
	}
}

func TestHlCloidFormat(t *testing.T) {
	c := hlCloid("arb-123")
	if !isHLCloid(c) {
		t.Fatalf("cloid %s", c)
	}
}

func TestSignL1RecoversSigner(t *testing.T) {
	pk, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	action := hlOrderAction{
		Type:     "order",
		Grouping: "na",
		Orders: []hlOrderWire{{
			A: 200002,
			B: true,
			P: "1472.8",
			S: "0.01",
			R: false,
			T: hlOrderTypeWire{Limit: hlLimitTif{Tif: "Ioc"}},
			C: hlCloid("arb-1"),
		}},
	}
	nonce := int64(1_700_000_000_000)
	sig, err := hlSignL1(pk, action, nonce, true)
	if err != nil {
		t.Fatal(err)
	}
	if sig.V != 27 && sig.V != 28 {
		t.Fatalf("v=%d", sig.V)
	}
	actionHash, err := hlActionHash(action, nonce)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := hlTypedDigest(actionHash, true)
	if err != nil {
		t.Fatal(err)
	}
	raw := make([]byte, 65)
	r, err := hexutil.Decode(sig.R)
	if err != nil {
		t.Fatal(err)
	}
	s, err := hexutil.Decode(sig.S)
	if err != nil {
		t.Fatal(err)
	}
	copy(raw[0:32], pad32(r))
	copy(raw[32:64], pad32(s))
	raw[64] = byte(sig.V - 27)
	pub, err := crypto.SigToPub(digest, raw)
	if err != nil {
		t.Fatal(err)
	}
	want := crypto.PubkeyToAddress(pk.PublicKey)
	got := crypto.PubkeyToAddress(*pub)
	if got != want {
		t.Fatalf("recovered %s want %s", got.Hex(), want.Hex())
	}
}

func TestSymbolMatch(t *testing.T) {
	if !symbolMatch("io:SNDK", "SNDK") {
		t.Fatal("hip3 coin vs bare")
	}
	if !symbolMatch("SNDK-USD", "SNDK") {
		t.Fatal("usd suffix")
	}
	if !symbolMatch("CRV", "crv") {
		t.Fatal("case")
	}
	if symbolMatch("BTC", "ETH") {
		t.Fatal("mismatch")
	}
}

func pad32(b []byte) []byte {
	if len(b) >= 32 {
		return b[len(b)-32:]
	}
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}
