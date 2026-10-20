package execution

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

// maxOpenNotional is a per-leg cap. A 1.00 USD-per-leg step under a 1.05 cap
// must still be completed: only the short leg grows.
func TestStepCompletionBudgetIsPerLeg(t *testing.T) {
	lv := newFake("L", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.006
		}
		return r.Qty
	})
	rv := newFake("R", full)
	lv.reduceCap, rv.reduceCap = true, true
	e := topUpExec(t, lv, rv)
	res := e.ExecuteBounded(context.Background(),
		LegSpec{Venue: lv, Inst: inst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: inst, Side: domain.Sell, Price: 100}, 0.01, false, 0, 0, 1.05)
	if res.Err != nil || !near(res.PosL, 0.01) || !near(res.PosR, -0.01) {
		t.Fatalf("completion refused under a per-leg cap: %+v", res)
	}
	if len(lv.orders) != 2 || lv.orders[1].ReduceOnly || !near(lv.orders[1].Qty, 0.004) {
		t.Fatalf("expected one 0.004 completion order, got %+v", lv.orders)
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

// Completion fills merged into a leg survive a re-query of the original
// order, and terminal verification checks the original order alone.
func TestMergedCompletionSurvivesOriginalRequery(t *testing.T) {
	req := domain.OrderReq{ClientID: 1, Inst: inst, Side: domain.Buy, Qty: 0.01, Price: 100}
	lr := LegResult{ClientID: 1, request: req}
	mergeTopUp(&lr, LegResult{Resolved: true, Status: domain.StatusFilled, Filled: 0.004, AvgPrice: 101, Fee: 0.0002})
	lr = fill(lr, domain.OrderUpdate{ClientID: 1, Status: domain.StatusCanceled, FilledQty: 0.006, FilledQuote: 0.6, Fee: 0.0003}, time.Now(), 100)
	if !near(lr.Filled, 0.01) || !near(lr.AvgPrice, (0.6+0.404)/0.01) || !near(lr.Fee, 0.0005) || lr.FeeUnknown {
		t.Fatalf("re-query dropped completion fills: %+v", lr)
	}
	if !terminalOrder(req, lr) {
		t.Fatalf("original order must verify on its own fill: %+v", lr.original())
	}
}

// Venue-reported fees travel with the leg; a venue reporting none marks the
// leg so accounting keeps the static estimate.
func TestLegFeesFromVenueReports(t *testing.T) {
	lv, rv := newFake("L", full), newFake("R", full)
	lv.feeRate = 0.00009
	e := testExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: inst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: inst, Side: domain.Sell, Price: 100}, 0.01, false, 0, 0)
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if res.L.FeeUnknown || res.L.Fee <= 0 || !near(res.L.Fee, res.L.Filled*res.L.AvgPrice*0.00009) {
		t.Fatalf("reported fee not kept: %+v", res.L)
	}
	if !res.R.FeeUnknown || res.R.Fee != 0 {
		t.Fatalf("missing fee must be flagged unknown: %+v", res.R)
	}
}
