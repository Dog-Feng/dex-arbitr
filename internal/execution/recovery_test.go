package execution

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestResolveTerminalsRequiresExplicitOriginalOutcome(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.silent, r.silent = true, true
	e := testLifecycleExec(t, l, r)
	var terminal atomic.Bool
	query := func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		if terminal.Load() {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusFilled, FilledQty: .01, FilledQuote: 1}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}
	ls, rs := LegSpec{lifecycleVenue{fakeVenue: l, queryFn: query}, inst, domain.Buy, 100}, LegSpec{lifecycleVenue{fakeVenue: r, queryFn: query}, inst, domain.Sell, 100}
	res := e.Execute(context.Background(), ls, rs, .01, false, 0, 0)
	requireSettled(t, res)
	if OrdersTerminal(res) {
		t.Fatal("full position inference was treated as a terminal venue report")
	}
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if !errors.Is(res.Err, ErrUnresolved) || OrdersTerminal(res) {
		t.Fatalf("unknown identities were released: %+v", res)
	}
	terminal.Store(true)
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if res.Err != nil || !OrdersTerminal(res) || !res.PositionsKnown || len(l.orders) != 1 || len(r.orders) != 1 {
		t.Fatalf("explicit outcomes did not settle without resubmitting: %+v", res)
	}
}

func TestResolveTerminalsRetainsPositionInferredRescue(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", func(n int, req domain.OrderReq) float64 {
		if n == 0 {
			return 0
		}
		return req.Qty
	})
	l.pos, r.pos, r.silent = .01, -.01, true
	e := testLifecycleExec(t, l, r)
	var terminal atomic.Bool
	rv := lifecycleVenue{fakeVenue: r, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if cid == r.orders[0].ClientID {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusCanceled}, nil
		}
		if terminal.Load() {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusFilled, FilledQty: .01, FilledQuote: 1.01}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}}
	ls, rs := LegSpec{l, inst, domain.Sell, 100}, LegSpec{rv, inst, domain.Buy, 100.1}
	res := e.Execute(context.Background(), ls, rs, .01, true, .01, -.01)
	requireSettled(t, res)
	if OrdersTerminal(res) || len(res.unconfirmed) != 1 {
		t.Fatal("inferred rescue identity was discarded")
	}
	old := res
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if !errors.Is(res.Err, ErrUnresolved) || OrdersTerminal(res) {
		t.Fatal("unknown rescue batch was released")
	}
	terminal.Store(true)
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if res.Err != nil || !OrdersTerminal(res) || len(r.orders) != 2 || math.Abs(res.RescueQty-.01) > 1e-9 || math.Abs(res.R.RescuePrice-101) > 1e-9 {
		t.Fatalf("rescue terminal accounting was lost or duplicated: %+v", res)
	}
	if old.unconfirmed[0].results[1].Resolved {
		t.Fatal("terminal resolution mutated an earlier result snapshot")
	}
}

func TestExecuteBoundedFreezesPricesAndChecksBothLegs(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "within budget"
		if reject {
			name = "over budget"
		}
		t.Run(name, func(t *testing.T) {
			l, r := newFake("L", full), newFake("R", full)
			e := testLifecycleExec(t, l, r)
			e.cfg.DynamicTicks, e.cfg.TickK, e.cfg.DepthMaxScale = true, 1, 1
			e.book = nil // keep this case focused on the changing volatility allowance
			var reads atomic.Int32
			e.vol = func(string) float64 {
				if reads.Add(1) > 2 {
					return 100
				}
				return .01
			}
			cap := 1.00015
			if reject {
				cap = .0001 // below one native quantity unit
			}
			res := e.ExecuteBounded(context.Background(), LegSpec{l, inst, domain.Buy, 100}, LegSpec{r, inst, domain.Sell, 100}, .01, false, 0, 0, cap)
			if reject {
				if !errors.Is(res.Err, ErrNotionalBudget) || len(l.orders) != 0 || len(r.orders) != 0 {
					t.Fatalf("over-budget action submitted an IOC: %+v", res)
				}
			} else {
				requireSettled(t, res)
				if len(l.orders) != 1 || len(r.orders) != 1 || l.orders[0].Price != 100.01 || r.orders[0].Price != 99.99 {
					t.Fatalf("preflight prices were not the submitted bounds: L=%+v R=%+v", l.orders, r.orders)
				}
			}
			if reads.Load() != 2 {
				t.Fatalf("dynamic allowance recalculated after preflight: %d", reads.Load())
			}
		})
	}
}

func TestExecuteBoundedDoesNotBlockCloses(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.pos, r.pos = .01, -.01
	e := testLifecycleExec(t, l, r)
	res := e.ExecuteBounded(context.Background(), LegSpec{l, inst, domain.Sell, 100}, LegSpec{r, inst, domain.Buy, 100}, .01, true, .01, -.01, .0001)
	requireSettled(t, res)
	if res.PosL != 0 || res.PosR != 0 {
		t.Fatal("opening cap blocked a close")
	}
}

func TestExecuteBoundedReservesSellDecisionNotional(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	e := testLifecycleExec(t, l, r)
	res := e.ExecuteBounded(context.Background(), LegSpec{l, inst, domain.Buy, 100}, LegSpec{r, inst, domain.Sell, 101}, .01, false, 0, 0, 1.008)
	requireSettled(t, res)
	if res.RequestedQty != .0099 || l.orders[0].Qty*101 > 1.008 || r.orders[0].Qty != res.RequestedQty {
		t.Fatalf("sell decision notional did not constrain the opening quantity: %+v", res)
	}
}

func TestResolveTerminalsRemovesInvalidatedRescueInference(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", nil)
	r.fillFn = func(n int, req domain.OrderReq) float64 {
		if n == 0 {
			return 0
		}
		// fakeVenue owns its mutex while invoking fillFn. Simulate an
		// independent close racing this zero-fill rescue submission.
		r.pos = 0
		return 0
	}
	l.pos, r.pos, r.silent = .01, -.01, true
	e := testLifecycleExec(t, l, r)
	var terminal atomic.Bool
	rv := lifecycleVenue{fakeVenue: r, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		if cid == r.orders[0].ClientID || terminal.Load() {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusCanceled}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}}
	ls, rs := LegSpec{l, inst, domain.Sell, 100}, LegSpec{rv, inst, domain.Buy, 100.1}
	res := e.Execute(context.Background(), ls, rs, .01, true, .01, -.01)
	requireSettled(t, res)
	if res.R.RescueFilled != .01 || OrdersTerminal(res) {
		t.Fatal("fixture did not infer the full unknown rescue from positions")
	}
	// The zero terminal report proves that the observed flat position came
	// from an external account change, rather than this rescue order.
	terminal.Store(true)
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if res.Err != nil || !OrdersTerminal(res) || res.R.RescueFilled != 0 || res.R.RescuePrice != 0 || res.RescueQty != 0 {
		t.Fatalf("terminal zero retained inferred fees/volume: %+v", res)
	}
	res = e.ResolveTerminals(context.Background(), ls, rs, res, .01)
	if res.R.RescueFilled != 0 || res.RescueQty != 0 || math.IsNaN(res.R.RescuePrice) {
		t.Fatal("repeated terminal resolution counted the correction twice")
	}
}

func TestZeroTicksPreservesNativeDecisionPrice(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	e := testLifecycleExec(t, l, r)
	e.cfg.PriceTicks = 0
	i := inst
	i.PriceDecimals = 3
	for _, tc := range []struct {
		side  domain.Side
		price float64
	}{{domain.Buy, 33554.401}, {domain.Sell, 33554.404}} {
		if got := e.limitPrice(LegSpec{l, i, tc.side, tc.price}, .01); got != tc.price {
			t.Fatalf("native decision %g became %g", tc.price, got)
		}
	}
}
