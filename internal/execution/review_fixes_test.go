package execution

import (
	"context"
	"math"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// TestFeasibleReduceTarget verifies a close that leaves an untradable dust
// residual searches to the nearest hedge both legs can still trade instead
// of flattening the whole account.
func TestFeasibleReduceTarget(t *testing.T) {
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.51}
	e := NewExecutor(DefaultConfig(), nil, nil, nil, nil, nil, nil)
	l, r := LegSpec{Inst: inst, Side: domain.Sell, Price: 100}, LegSpec{Inst: inst, Side: domain.Buy, Price: 100}
	tl, tr := e.feasibleReduceTarget(l, r, -0.01, 0.014, -0.01, 0.01)
	if !near(tl, -0.0049) || !near(tr, 0.0049) {
		t.Fatalf("nearest feasible target = %.4f/%.4f", tl, tr)
	}
	tl, tr = e.feasibleReduceTarget(l, r, -0.02, 0.02, -0.01, 0.01)
	if !near(tl, -0.01) || !near(tr, 0.01) {
		t.Fatalf("tradable target must stay unchanged: %.4f/%.4f", tl, tr)
	}
}

// forwardInst has a venue minimum of about 0.0051 at price 100.
var forwardInst = domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.51}

func forwardFakes(fillL, fillR float64) (*fakeVenue, *fakeVenue) {
	lv := newFake("L", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return fillL
		}
		return r.Qty
	})
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return fillR
		}
		return r.Qty
	})
	lv.reduceCap, rv.reduceCap = true, true
	lv.pos, rv.pos = 0.02, -0.02 // two earlier steps of 0.01
	return lv, rv
}

// An opening of 0.01 fills 0.008/0.004 with a 0.0051 minimum on top of
// earlier steps: equalizing needs a 0.004 reduction and undoing a 0.004
// reduction on R, both below the minimum and with nonzero targets. The
// executor completes forward to a tradable hedge instead of leaving dust.
func TestOpeningForwardCompletionAvoidsSubMinResidual(t *testing.T) {
	lv, rv := forwardFakes(0.008, 0.004)
	e := testExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: forwardInst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: forwardInst, Side: domain.Sell, Price: 100}, 0.01, false, 0.02, -0.02)
	if res.Err != nil {
		t.Fatalf("forward completion failed: %v", res.Err)
	}
	h := res.PosL - 0.02
	if !near(res.PosL, -res.PosR) || h < 0.008 || h > 0.008+0.0052+1e-9 {
		t.Fatalf("not hedged on a forward target: %.4f/%.4f", res.PosL, res.PosR)
	}
	if !near(res.L.Filled, h) || !near(res.R.Filled, h) || res.RescueOrders != 0 {
		t.Fatalf("forward fills not merged: L=%+v R=%+v", res.L, res.R)
	}
	if !OrdersTerminal(res) || !res.SettlementKnown {
		t.Fatalf("forward-completed execution must be settled: %+v", res)
	}
	for _, o := range append(lv.orders[1:], rv.orders[1:]...) {
		if o.ReduceOnly || o.Qty < legMinQty(forwardInst, o.Price)-1e-12 {
			t.Fatalf("forward order below the venue minimum or reduce-only: %+v", o)
		}
	}
}

// A fully filled leg also extends forward; its original order must still
// verify as terminal on its own quantity.
func TestForwardCompletionKeepsOriginalOrderTerminal(t *testing.T) {
	lv, rv := forwardFakes(0.01, 0.005)
	e := testExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: forwardInst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: forwardInst, Side: domain.Sell, Price: 100}, 0.01, false, 0.02, -0.02)
	if res.Err != nil || !near(res.PosL, -res.PosR) || res.PosL <= 0.03 {
		t.Fatalf("forward completion: err=%v pos=%.4f/%.4f", res.Err, res.PosL, res.PosR)
	}
	if res.L.Status != domain.StatusFilled || !OrdersTerminal(res) {
		t.Fatalf("original filled leg no longer verifies: %+v", res.L)
	}
}

// Forward completion is bounded by the per-leg cap plus one venue minimum.
func TestForwardCompletionRespectsBudget(t *testing.T) {
	lv, rv := forwardFakes(0.008, 0.004)
	e := testExec(t, lv, rv)
	res := e.ExecuteBounded(context.Background(),
		LegSpec{Venue: lv, Inst: forwardInst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: forwardInst, Side: domain.Sell, Price: 100}, 0.01, false, 0.02, -0.02, 0.6)
	for _, o := range append(lv.orders[1:], rv.orders[1:]...) {
		if !o.ReduceOnly {
			t.Fatalf("forward completion exceeded the budget: %+v", o)
		}
	}
	if res.Err == nil {
		t.Fatal("without forward completion the residual stays sub-minimum")
	}
}
