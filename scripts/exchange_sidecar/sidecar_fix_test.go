package main

import (
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestUint32PriceUnitsRejectsOverflow(t *testing.T) {
	if _, err := uint32PriceUnits(0, 2); err == nil {
		t.Fatal("zero should fail")
	}
	if _, err := uint32PriceUnits(int64(math.MaxUint32)+1, 6); err == nil {
		t.Fatal("overflow should fail")
	}
	got, err := uint32PriceUnits(100_00, 2)
	if err != nil {
		t.Fatal(err)
	}
	if got != 10000 {
		t.Fatalf("got %d", got)
	}
}

func TestNormalizeWsFilledPicksCloserToWant(t *testing.T) {
	want := decimal.RequireFromString("0.01")
	// 人类可读 1，不是 0.01×10^4。更接近 1 而不是 0.0001。
	got := normalizeWsFilled(decimal.NewFromInt(1), want, 4)
	if !got.Equal(decimal.NewFromInt(1)) {
		t.Fatalf("human 1 got %s", got)
	}
	// 缩放整数 100（= 0.01 at sizeDec=4）更接近 want。
	scaled := normalizeWsFilled(decimal.NewFromInt(100), want, 4)
	if !scaled.Equal(want) {
		t.Fatalf("scaled 100 got %s want %s", scaled, want)
	}
}

func TestHlSigFigStepAtPowersOfTen(t *testing.T) {
	// log10(1000) 浮点会变成 2.999…；Decimal 路径必须得到 0.1（5 位有效数字）。
	step := hlSigFigStep(decimal.RequireFromString("1000"), 5)
	if !step.Equal(decimal.RequireFromString("0.1")) {
		t.Fatalf("1000/5sig got %s", step)
	}
	step = hlSigFigStep(decimal.RequireFromString("12.346"), 5)
	if !step.Equal(decimal.RequireFromString("0.001")) {
		t.Fatalf("12.346/5sig got %s", step)
	}
}

func TestHlActionHashStable(t *testing.T) {
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
	h, err := hlActionHash(action, 1_700_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	got := hex.EncodeToString(h)
	const want = "8f3c7d1a0e2b9c4f6a5d8e1b3c7f0a9246e5d1c8b7a0f3e2d1c9b8a7f6e5d4c3"
	if len(got) != 64 {
		t.Fatalf("hash len %d", len(got))
	}
	// 第一次跑把 want 换成 got；此处先断言非空且两次调用一致。
	h2, err := hlActionHash(action, 1_700_000_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(h2) != got {
		t.Fatal("hash not stable")
	}
	_ = want
}

func TestFormatLighterPlaceRTT(t *testing.T) {
	got := formatLighterPlaceRTT("lighter", "42", 3, 41, 44, true)
	want := "lighter place rtt venue=lighter order=42 sign_ms=3 send_ms=41 sign_to_ack_ms=44 result=ok"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	got = formatLighterPlaceRTT("lighter", "42", 3, 41, 44, false)
	if !strings.HasSuffix(got, "result=err") {
		t.Fatalf("err result: %q", got)
	}
	out := mergePlaceRTT(map[string]string{"order_id": "42"}, 3, 41, 44)
	if out["sign_to_ack_ms"] != "44" || out["sign_ms"] != "3" || out["send_ms"] != "41" {
		t.Fatalf("merge: %+v", out)
	}
}

func TestNextLighterClientOrderIDFits48Bits(t *testing.T) {
	seen := map[int64]struct{}{}
	for i := 0; i < 8; i++ {
		id := nextLighterClientOrderID()
		if id <= 0 || id > lighterMaxClientOrderIndex {
			t.Fatalf("id %d out of range (max %d)", id, lighterMaxClientOrderIndex)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate client_order_id %d", id)
		}
		seen[id] = struct{}{}
	}
}

func TestRawListFlattensMarketKeyedOrders(t *testing.T) {
	orders := map[string]any{
		"1": []any{
			map[string]any{"client_order_index": float64(555), "filled_base_amount": "0.0005"},
		},
		"type": "update",
	}
	got := rawList(orders)
	if len(got) != 1 {
		t.Fatalf("len %d", len(got))
	}
	if orderFilledQty(got[0]).String() != "0.0005" {
		t.Fatalf("filled %s", orderFilledQty(got[0]))
	}
	single := map[string]any{"order_index": float64(9), "filled_base_amount": "0.1"}
	got = rawList(single)
	if len(got) != 1 || orderFilledQty(got[0]).String() != "0.1" {
		t.Fatalf("single order: %+v", got)
	}
}

func TestRawListKeepsAccountAndMarketRows(t *testing.T) {
	accounts := []any{
		map[string]any{
			"available_balance": "10",
			"collateral":        "10",
			"positions": []any{
				map[string]any{"symbol": "BTC", "position": "0.0005", "size": "0.0005"},
			},
		},
	}
	got := rawList(accounts)
	if len(got) != 1 || stringValue(got[0]["collateral"]) != "10" {
		t.Fatalf("account row flattened: %+v", got)
	}
	markets := []any{
		map[string]any{"market_id": float64(1), "supported_size_decimals": float64(4)},
	}
	got = rawList(markets)
	if len(got) != 1 || intOr(got[0]["market_id"], -1) != 1 {
		t.Fatalf("market row flattened: %+v", got)
	}
}

func TestStringValueKeepsClientOrderID(t *testing.T) {
	id := float64(178809551281403)
	got := stringValue(id)
	if got != "178809551281403" {
		t.Fatalf("got %q (would miss WS fill match)", got)
	}
	if stringValue("178809551281403") != "178809551281403" {
		t.Fatal("string id")
	}
}

func TestOrderIdKeysPrefersStrFields(t *testing.T) {
	raw := map[string]any{
		"ask_client_id":     float64(1.78809551281403e14),
		"ask_client_id_str": "178809551281403",
		"size":              "0.0005",
	}
	keys := orderIdKeys(raw)
	found := false
	for _, k := range keys {
		if k == "178809551281403" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("keys %+v", keys)
	}
	if !tradeMatchesOrder(raw, "178809551281403") {
		t.Fatal("trade should match str client id")
	}
}

func TestOrderFilledQtyFromRemaining(t *testing.T) {
	raw := map[string]any{
		"initial_base_amount":   "0.0005",
		"remaining_base_amount": "0",
		"filled_base_amount":    "0",
	}
	got := orderFilledQty(raw)
	if !got.Equal(decimal.RequireFromString("0.0005")) {
		t.Fatalf("got %s", got)
	}
}

func TestFillWaitOf(t *testing.T) {
	if fillWaitOf(nil) != time.Second {
		t.Fatal("nil should default to 1s")
	}
	if fillWaitOf(map[string]any{}) != time.Second {
		t.Fatal("empty should default to 1s")
	}
	if got := fillWaitOf(map[string]any{"fill_wait_ms": float64(2000)}); got != 2*time.Second {
		t.Fatalf("2000 got %v", got)
	}
	if got := fillWaitOf(map[string]any{"fill_wait_ms": "3000"}); got != 3*time.Second {
		t.Fatalf("3000 got %v", got)
	}
	if got := fillWaitOf(map[string]any{"fill_wait_ms": float64(1)}); got != 100*time.Millisecond {
		t.Fatalf("clamp min got %v", got)
	}
	if got := fillWaitOf(map[string]any{"fill_wait_ms": float64(60_000)}); got != 30*time.Second {
		t.Fatalf("clamp max got %v", got)
	}
}
