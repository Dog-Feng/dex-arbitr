package execution

import (
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// TestRescueLadderDistinctPrices locks the H4 ladder fix: on a 5-sig-fig grid
// each tick rung differs from the previous one, while venues without the rule
// keep the native price-decimals step.
func TestRescueLadderDistinctPrices(t *testing.T) {
	book := domain.Book{
		Bids: []domain.Level{{Price: 1963.3}},
		Asks: []domain.Level{{Price: 1963.4}},
	}
	inst := domain.Instrument{PriceDecimals: 3, SigFigs: 5}
	prices := map[float64]bool{}
	prev := 0.0
	for attempt := 0; attempt < DefaultRescue.TopTries+DefaultRescue.MaxTicks; attempt++ {
		px, typ := RescuePrice(DefaultRescue, attempt, book, domain.Buy, inst)
		if typ != domain.Limit {
			t.Fatalf("attempt %d: expected limit, got %v", attempt, typ)
		}
		if attempt < DefaultRescue.TopTries {
			continue // the top-of-book tier legitimately repeats the top price
		}
		if prices[px] {
			t.Fatalf("attempt %d collapsed onto price %v", attempt, px)
		}
		prices[px] = true
		if px <= prev {
			t.Fatalf("attempt %d price %v did not step beyond %v", attempt, px, prev)
		}
		prev = px
	}
	// Without the significant-figure rule the ladder steps at native precision.
	plain := domain.Instrument{PriceDecimals: 3}
	for attempt := 0; attempt < DefaultRescue.TopTries+3; attempt++ {
		px, _ := RescuePrice(DefaultRescue, attempt, book, domain.Buy, plain)
		if attempt >= DefaultRescue.TopTries {
			want := 1963.4 + float64(attempt-DefaultRescue.TopTries+1)*0.001
			if px != want {
				t.Fatalf("plain ladder attempt %d = %v, want %v", attempt, px, want)
			}
		}
	}
}

// TestRescueMarketPriceUsesEffectiveTick verifies the protective market price
// rounds to the venue's effective tick at the mid, not the price-decimals unit.
func TestRescueMarketPriceUsesEffectiveTick(t *testing.T) {
	book := domain.Book{
		Bids: []domain.Level{{Price: 1958.0}},
		Asks: []domain.Level{{Price: 1958.4}},
	}
	inst := domain.Instrument{PriceDecimals: 3, SigFigs: 5}
	px, typ := RescuePrice(RescueParams{Immediate: true, MarketBps: 30}, 0, book, domain.Buy, inst)
	if typ != domain.Market {
		t.Fatalf("expected market order, got %v", typ)
	}
	// mid 1958.2 * (1+30/1e4) ≈ 1964.07 -> ceil to the 0.1 grid.
	want := 1964.1
	if px != want {
		t.Fatalf("market price = %v, want %v", px, want)
	}
}
