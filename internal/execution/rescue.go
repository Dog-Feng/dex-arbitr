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
// The ladder steps by the venue's effective tick at the top of book, so a
// significant-figure price grid (e.g. Hyperliquid's 5 sig figs) still yields a
// distinct price per attempt instead of collapsing onto the top.
func RescuePrice(p RescueParams, attempt int, b domain.Book, side domain.Side, inst domain.Instrument) (float64, domain.OrderType) {
	if p.Immediate {
		return marketPrice(p.MarketBps, b, side, inst), domain.Market
	}
	top := func() float64 {
		if side == domain.Buy {
			return b.Asks[0].Price
		}
		return b.Bids[0].Price
	}
	dir := float64(side) // buy => pay up (+), sell => sell lower (-)
	if attempt < p.TopTries {
		return domain.Round(top(), inst.PriceDecimals), domain.Limit
	}
	if attempt < p.TopTries+p.MaxTicks {
		tick := inst.TickAt(top())
		prev := domain.Round(top(), inst.PriceDecimals)
		px := prev
		for i := 0; i <= attempt-p.TopTries; i++ {
			px = domain.Round(px+dir*tick, inst.PriceDecimals)
			for px == prev {
				// Rounding on the native grid collapsed the step; advance one
				// more effective tick so every ladder rung differs.
				px = domain.Round(px+dir*tick, inst.PriceDecimals)
			}
			prev = px
		}
		return px, domain.Limit
	}
	return marketPrice(p.MarketBps, b, side, inst), domain.Market
}

// marketPrice returns a protective market price at mid ± bps, rounded to the
// venue's effective tick at the mid.
func marketPrice(bps float64, b domain.Book, side domain.Side, inst domain.Instrument) float64 {
	tick := inst.TickAt(b.Mid())
	px := b.Mid() * (1 + float64(side)*bps/1e4)
	if side == domain.Buy {
		px = math.Ceil(px/tick-1e-9) * tick
	} else {
		px = math.Floor(px/tick+1e-9) * tick
	}
	return domain.Round(px, inst.PriceDecimals)
}
