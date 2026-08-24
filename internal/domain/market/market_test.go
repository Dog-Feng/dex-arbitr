package market

import (
	"testing"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testMarket() Market {
	return Market{
		Symbol:          "BTC",
		TickSize:        d("0.1"),
		LotSize:         d("0.001"),
		MinQty:          d("0.001"),
		MinNotional:     d("10"),
		MaxLeverage:     50,
		MakerFeeRate:    d("0.0002"),
		TakerFeeRate:    d("0.0005"),
		MaintMarginRate: d("0.005"),
	}
}

func TestRoundPrice(t *testing.T) {
	m := testMarket()
	cases := []struct {
		in   string
		mode Rounding
		want string
	}{
		{"62942.74", RoundNearest, "62942.7"},
		{"62942.75", RoundNearest, "62942.8"},
		{"62942.79", RoundDown, "62942.7"},
		{"62942.71", RoundUp, "62942.8"},
		{"62942.70", RoundNearest, "62942.7"},
	}
	for _, c := range cases {
		got := m.RoundPrice(d(c.in), c.mode)
		if !got.Equal(d(c.want)) {
			t.Errorf("RoundPrice(%s, %d) = %s, want %s", c.in, c.mode, got, c.want)
		}
	}
}

// 数量必须向下取整：向上取整会导致实际占用的保证金超出预算。
func TestRoundQtyAlwaysDown(t *testing.T) {
	m := testMarket()
	got := m.RoundQty(d("0.0019"))
	if !got.Equal(d("0.001")) {
		t.Fatalf("RoundQty(0.0019) = %s, want 0.001", got)
	}
	if got := m.RoundQty(d("0.0009")); !got.IsZero() {
		t.Fatalf("RoundQty(0.0009) = %s, want 0", got)
	}
}

func TestCheckOrder(t *testing.T) {
	m := testMarket()
	if err := m.CheckOrder(d("60000"), d("0.002")); err != nil {
		t.Fatalf("valid order rejected: %v", err)
	}
	if err := m.CheckOrder(d("60000"), decimal.Zero); err == nil {
		t.Fatal("zero quantity should be rejected")
	}
	// 0.001 * 5000 = 5 < MinNotional 10
	if err := m.CheckOrder(d("5000"), d("0.001")); err == nil {
		t.Fatal("below-minimum notional should be rejected")
	}
}

func TestRoundTripFeeRate(t *testing.T) {
	m := testMarket()
	if got := m.RoundTripFeeRate(); !got.Equal(d("0.0004")) {
		t.Fatalf("RoundTripFeeRate() = %s, want 0.0004", got)
	}
}

func TestValidateRejectsBadMetadata(t *testing.T) {
	m := testMarket()
	m.TickSize = decimal.Zero
	if err := m.Validate(); err == nil {
		t.Fatal("zero tick size should be rejected")
	}
}

func TestBookTicker(t *testing.T) {
	b := BookTicker{Bid: d("100"), Ask: d("102")}
	if !b.Valid() {
		t.Fatal("book should be valid")
	}
	if got := b.Mid(); !got.Equal(d("101")) {
		t.Fatalf("Mid() = %s, want 101", got)
	}
	crossed := BookTicker{Bid: d("103"), Ask: d("102")}
	if crossed.Valid() {
		t.Fatal("crossed book should be invalid")
	}
}
