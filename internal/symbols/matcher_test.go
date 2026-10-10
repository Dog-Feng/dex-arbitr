package symbols

import (
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestNormalize(t *testing.T) {
	m := DefaultMatcher()
	cases := map[string]string{
		"SNDK": "SNDK", "BTC-PERP": "BTC", "BTC-USDC-PERP": "BTC", "ETHUSDT": "ETH",
		"sol/usdg": "SOL", "XAU_USD": "XAU", "USDC": "USDC", "WIF-USD": "WIF",
	}
	for in, want := range cases {
		if got := m.Normalize(in); got != want {
			t.Errorf("Normalize(%q)=%q want %q", in, got, want)
		}
	}
}

func TestMatchAndAlign(t *testing.T) {
	l := []domain.Instrument{{Symbol: "SNDK", SizeDecimals: 4, MinBase: 0.0035, MinQuote: 10}, {Symbol: "BTC", SizeDecimals: 5}}
	r := []domain.Instrument{{Symbol: "SNDK-PERP", SizeDecimals: 3, MinBase: 0.01, MinQuote: 10}, {Symbol: "ETH"}}
	ps := Match(DefaultMatcher(), l, r)
	if len(ps) != 1 || ps[0].Base != "SNDK" {
		t.Fatalf("got %+v", ps)
	}
	p := ps[0]
	if p.SizeDecimals != 3 || p.MinQty != 0.01 {
		t.Fatalf("align: dec=%d min=%v", p.SizeDecimals, p.MinQty)
	}
	if ok, _ := p.ValidQty(0.01, 1590); !ok {
		t.Fatal("0.01 should be valid")
	}
	if ok, _ := p.ValidQty(0.0105, 1590); ok {
		t.Fatal("0.0105 exceeds aligned decimals")
	}
	if ok, _ := p.ValidQty(0.01, 500); ok {
		t.Fatal("notional 5 < 10 must fail")
	}
}

func TestPrefixMustBeExact(t *testing.T) {
	l := []domain.Instrument{{Symbol: "SOL"}}
	r := []domain.Instrument{{Symbol: "SOLV"}}
	if ps := Match(DefaultMatcher(), l, r); len(ps) != 0 {
		t.Fatalf("SOL must not match SOLV: %+v", ps)
	}
}
