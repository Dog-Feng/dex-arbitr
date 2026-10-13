package execution

import (
	"math"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// RescueParams control price relaxation of rescue (flatten) orders.
type RescueParams struct {
	TopTries  int     // attempts at top of book
	MaxTicks  int     // then +1 tick per attempt up to MaxTicks
	MarketBps float64 // finally protective market order bounded at mid +- MarketBps
	Immediate bool    // market from attempt 0 (emergency unwind)
}

// DefaultRescue matches the design: 3x top, then up to 5 ticks, then market @ mid±30bps.
var DefaultRescue = RescueParams{TopTries: 3, MaxTicks: 5, MarketBps: 30}

// RescuePrice returns the price and order type for a rescue attempt (0-based).
func RescuePrice(p RescueParams, attempt int, b domain.Book, side domain.Side, priceDecimals int) (float64, domain.OrderType) {
	if p.Immediate {
		return marketPrice(p.MarketBps, b, side, priceDecimals), domain.Market
	}
	tick := math.Pow10(-priceDecimals)
	top := func() float64 {
		if side == domain.Buy {
			return b.Asks[0].Price
		}
		return b.Bids[0].Price
	}
	dir := float64(side) // buy => pay up (+), sell => sell lower (-)
	switch {
	case attempt < p.TopTries:
		return domain.Round(top(), priceDecimals), domain.Limit
	case attempt < p.TopTries+p.MaxTicks:
		n := float64(attempt - p.TopTries + 1)
		return domain.Round(top()+dir*n*tick, priceDecimals), domain.Limit
	default:
		return marketPrice(p.MarketBps, b, side, priceDecimals), domain.Market
	}
}

// marketPrice returns a protective market price at mid ± bps, rounded to a tick.
func marketPrice(bps float64, b domain.Book, side domain.Side, priceDecimals int) float64 {
	tick := math.Pow10(-priceDecimals)
	px := b.Mid() * (1 + float64(side)*bps/1e4)
	if side == domain.Buy {
		px = math.Ceil(px/tick-1e-9) * tick
	} else {
		px = math.Floor(px/tick+1e-9) * tick
	}
	return domain.Round(px, priceDecimals)
}
