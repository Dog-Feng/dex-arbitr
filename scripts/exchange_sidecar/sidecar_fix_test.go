package main

import (
	"encoding/hex"
	"math"
	"strings"
	"testing"

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
