package symbols

import (
	"math"
	"strings"
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

func TestValidQtyRejectsUntradableInputs(t *testing.T) {
	p := Pair{SizeDecimals: 3, MinQty: 0.01, MinQuote: 10}
	for _, tc := range []struct {
		name       string
		qty, price float64
		want       bool
	}{
		{name: "aligned", qty: 0.01, price: 1590, want: true},
		{name: "extra low fraction", qty: 0.0101, price: 1590},
		{name: "extra high fraction", qty: 0.0105, price: 1590},
		{name: "extra fraction just below next step", qty: 0.010999, price: 1590},
		{name: "zero", price: 1590},
		{name: "negative", qty: -0.01, price: 1590},
		{name: "nan", qty: math.NaN(), price: 1590},
		{name: "infinite qty", qty: math.Inf(1), price: 1590},
		{name: "below min base", qty: 0.009, price: 1590},
		{name: "below min quote", qty: 0.01, price: 500},
		{name: "unknown price skips notional", qty: 0.01, want: true},
		{name: "negative price", qty: 0.01, price: -1},
		{name: "nan price", qty: 0.01, price: math.NaN()},
		{name: "infinite price", qty: 0.01, price: math.Inf(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := p.ValidQty(tc.qty, tc.price)
			if got != tc.want || (!got && reason == "") {
				t.Fatalf("ValidQty(%v,%v)=(%v,%q), want valid=%v", tc.qty, tc.price, got, reason, tc.want)
			}
		})
	}
}

func TestValidQtyAllowsRepresentationErrorOnly(t *testing.T) {
	a, b := 0.1, 0.2
	for _, tc := range []struct {
		name string
		p    Pair
		qty  float64
		want bool
	}{
		{name: "arithmetic rounding", p: Pair{SizeDecimals: 1}, qty: a + b, want: true},
		{name: "fractional first step", p: Pair{SizeDecimals: 3}, qty: 0.0001},
		{name: "below tiny minimum", p: Pair{SizeDecimals: 12, MinQty: 2e-12}, qty: 1e-12},
		{name: "tiny exact quantity", p: Pair{SizeDecimals: 12, MinQty: 1e-12}, qty: 1e-12, want: true},
		{name: "negative precision", p: Pair{SizeDecimals: -1}, qty: 10},
		{name: "overflow precision", p: Pair{SizeDecimals: 309}, qty: 1},
		{name: "invalid minimum", p: Pair{SizeDecimals: 1, MinQty: math.NaN()}, qty: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if ok, reason := tc.p.ValidQty(tc.qty, 0); ok != tc.want {
				t.Fatalf("valid=%v (%s), want %v", ok, reason, tc.want)
			}
		})
	}
}

func TestNormalizeIgnoresEmptySuffixes(t *testing.T) {
	for _, suffixes := range [][]string{{""}, {"", " ", "PERP", "USDC"}} {
		t.Run(strings.Join(suffixes, ","), func(t *testing.T) {
			m := Matcher{Suffixes: suffixes, Separators: "-"}
			if got := m.Normalize("BTC"); got != "BTC" {
				t.Fatalf("Normalize(BTC)=%q", got)
			}
			if len(suffixes) > 1 && m.Normalize("BTC-USDC-PERP") != "BTC" {
				t.Fatal("empty suffix prevents nonempty suffix matching")
			}
		})
	}
}

func TestPairRoundsMinimumUp(t *testing.T) {
	p := NewPair("TEST", domain.Instrument{SizeDecimals: 4, MinBase: 0.0101}, domain.Instrument{SizeDecimals: 3, MinBase: 0.0102})
	if p.MinQty != 0.011 {
		t.Fatalf("MinQty=%v, want 0.011", p.MinQty)
	}
	if ok, _ := p.ValidQty(0.010, 0); ok {
		t.Fatal("quantity below the rounded minimum accepted")
	}
}

func TestPairMinimumDoesNotRoundDownFractionOfStep(t *testing.T) {
	for _, tc := range []struct {
		name          string
		minBase, want float64
	}{
		{name: "small extra fraction", minBase: 0.0100000000001, want: 0.011},
		{name: "exact step", minBase: 0.01, want: 0.01},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := NewPair("TEST", domain.Instrument{SizeDecimals: 3, MinBase: tc.minBase}, domain.Instrument{SizeDecimals: 3})
			if p.MinQty != tc.want {
				t.Fatalf("minimum=%v want%v", p.MinQty, tc.want)
			}
		})
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
func TestMatcherAliases(t *testing.T) {
	m := DefaultMatcher()
	m.Aliases = map[string]string{"ANTH": "ANTHROPIC"}
	if got := m.Normalize("ANTH"); got != "ANTHROPIC" {
		t.Fatalf("ANTH normalized to %q", got)
	}
	if got := m.Normalize("anth"); got != "ANTHROPIC" {
		t.Fatalf("lowercase anth normalized to %q", got)
	}
	if got := m.Normalize("ANTHROPIC"); got != "ANTHROPIC" {
		t.Fatalf("canonical side changed: %q", got)
	}
	// Aliased pairs match across venues.
	left := []domain.Instrument{{Venue: "entropy", Symbol: "ANTH", MarketID: 200001}}
	right := []domain.Instrument{{Venue: "lighter", Symbol: "ANTHROPIC", MarketID: 7}}
	pairs := Match(m, left, right)
	if len(pairs) != 1 || pairs[0].Base != "ANTHROPIC" {
		t.Fatalf("aliased pair not matched: %+v", pairs)
	}
}

func TestDefaultAliases(t *testing.T) {
	m := DefaultMatcher()
	if got := m.Normalize("OAI"); got != "OPENAI" {
		t.Fatalf("OAI normalized to %q", got)
	}
	if got := m.Normalize("ANTH"); got != "ANTHROPIC" {
		t.Fatalf("ANTH normalized to %q", got)
	}
	// Config overrides replace a built-in mapping.
	m.Aliases["OAI"] = "SOMETHING_ELSE"
	if got := m.Normalize("OAI"); got != "SOMETHING_ELSE" {
		t.Fatalf("override failed: %q", got)
	}
}

// TestNormalizeSuffixThenAlias verifies suffix stripping happens before the
// alias lookup, so "ANTH-PERP" resolves through the ANTH alias.
func TestNormalizeSuffixThenAlias(t *testing.T) {
	m := DefaultMatcher()
	if got := m.Normalize("ANTH-PERP"); got != "ANTHROPIC" {
		t.Fatalf("suffixed alias normalized to %q", got)
	}
}

// TestChainedAliasesResolveWithCycleGuard follows chained mappings and stops
// at self-references.
func TestChainedAliasesResolveWithCycleGuard(t *testing.T) {
	m := DefaultMatcher()
	m.Aliases = map[string]string{"A": "B", "B": "C"}
	if got := m.Normalize("A"); got != "C" {
		t.Fatalf("chained alias normalized to %q", got)
	}
	m.Aliases["C"] = "B"
	if got := m.Normalize("A"); got != "B" {
		t.Fatalf("cycle must terminate, got %q", got)
	}
}

// TestMergeAliases verifies config aliases override built-ins, an empty
// canonical value deletes a built-in, and builtin=false drops the table.
func TestMergeAliases(t *testing.T) {
	aliases := MergeAliases(map[string]string{"ANTH": "CUSTOM"}, true)
	if aliases["ANTH"] != "CUSTOM" || aliases["OAI"] != "OPENAI" {
		t.Fatalf("override/merge failed: %+v", aliases)
	}
	aliases = MergeAliases(map[string]string{"OAI": ""}, true)
	if _, ok := aliases["OAI"]; ok {
		t.Fatal("empty canonical must delete the built-in alias")
	}
	if aliases["ANTH"] != "ANTHROPIC" {
		t.Fatal("unrelated built-in alias lost")
	}
	aliases = MergeAliases(nil, false)
	if len(aliases) != 0 {
		t.Fatalf("builtins not disabled: %+v", aliases)
	}
}
