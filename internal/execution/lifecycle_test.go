package execution

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

type lifecycleVenue struct {
	*fakeVenue
	submitFn   func(context.Context, domain.OrderReq) (domain.SubmitAck, error)
	queryFn    func(context.Context, domain.Instrument, int64) (domain.OrderUpdate, error)
	positionFn func(context.Context) ([]domain.Position, error)
}

func (v lifecycleVenue) Submit(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
	if v.submitFn != nil {
		return v.submitFn(ctx, req)
	}
	return v.fakeVenue.Submit(ctx, req)
}

func (v lifecycleVenue) QueryOrder(ctx context.Context, inst domain.Instrument, cid int64) (domain.OrderUpdate, error) {
	if v.queryFn != nil {
		return v.queryFn(ctx, inst, cid)
	}
	return v.fakeVenue.QueryOrder(ctx, inst, cid)
}

func (v lifecycleVenue) Positions(ctx context.Context) ([]domain.Position, error) {
	if v.positionFn != nil {
		return v.positionFn(ctx)
	}
	return v.fakeVenue.Positions(ctx)
}

func testLifecycleExec(t *testing.T, l, r *fakeVenue) *Executor {
	e := testExec(t, l, r)
	e.cfg.AckTimeout, e.cfg.QueryInterval = time.Millisecond, time.Millisecond
	return e
}

func requireSettled(t *testing.T, res Result) {
	t.Helper()
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		t.Fatalf("execution is not confirmed: %+v", res)
	}
}

func requireQty(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("quantity %.12f, want %.12f", got, want)
	}
}

func TestEmergencyFlattensEachLeg(t *testing.T) {
	for _, tt := range []struct {
		name   string
		pl, pr float64
	}{
		{"balanced", .01, -.01},
		{"unbalanced", .02, -.01},
		{"reverse", -.01, .01},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, r := newFake("L", full), newFake("R", full)
			l.pos, r.pos = tt.pl, tt.pr
			e := testLifecycleExec(t, l, r)
			res, err := e.EmergencyFlatten(context.Background(), LegSpec{Venue: l, Inst: inst}, LegSpec{Venue: r, Inst: inst}, 0, tt.pl, tt.pr)
			if err != nil {
				t.Fatal(err)
			}
			requireSettled(t, res)
			requireQty(t, res.PosL, 0)
			requireQty(t, res.PosR, 0)
			if res.RescueOrders != 2 || len(l.orders) != 1 || len(r.orders) != 1 {
				t.Fatalf("both legs must unwind: %+v", res)
			}
			for i, f := range []*fakeVenue{l, r} {
				o := f.orders[0]
				position := []float64{tt.pl, tt.pr}[i]
				if !o.ReduceOnly || o.Type != domain.Market || float64(o.Side)*position >= 0 {
					t.Fatalf("invalid emergency order: %+v", o)
				}
				requireQty(t, o.Qty, math.Abs(position))
			}
		})
	}
}

func TestEmergencyCannotSucceedWithoutConfirmedPositions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		canceled bool
	}{
		{"canceled", true},
		{"positions unavailable", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			l, r := newFake("L", full), newFake("R", full)
			l.pos, r.pos = .01, -.01
			e := testLifecycleExec(t, l, r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := errors.New("position endpoint unavailable")
			lv := lifecycleVenue{fakeVenue: l}
			if tt.canceled {
				cancel()
				want = context.Canceled
			} else {
				lv.positionFn = func(context.Context) ([]domain.Position, error) { return nil, want }
			}
			res, err := e.EmergencyFlatten(ctx, LegSpec{Venue: lv, Inst: inst}, LegSpec{Venue: r, Inst: inst}, 0, .01, -.01)
			if !errors.Is(err, want) || !errors.Is(res.Err, want) || res.PositionsKnown || res.SettlementKnown || res.RescueOrders != 0 {
				t.Fatalf("unconfirmed emergency reported success: %+v, err=%v", res, err)
			}
			requireQty(t, res.PosL, .01)
			requireQty(t, res.PosR, -.01)
		})
	}
}

func TestMixedPrecisionPartialReducesToCommonHedge(t *testing.T) {
	for _, side := range []domain.Side{domain.Buy, domain.Sell} {
		t.Run(side.String(), func(t *testing.T) {
			l := newFake("L", full)
			r := newFake("R", func(n int, req domain.OrderReq) float64 {
				if n == 0 {
					return .009
				}
				return req.Qty
			})
			e := testLifecycleExec(t, l, r)
			li, ri := inst, inst
			li.SizeDecimals, ri.SizeDecimals = 2, 3
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			res := e.Execute(ctx, LegSpec{l, li, side, 100}, LegSpec{r, ri, -side, 100.1}, .01, false, 0, 0)
			requireSettled(t, res)
			requireQty(t, res.PosL, 0)
			requireQty(t, res.PosR, 0)
			if res.RescueOrders != 2 || len(l.orders) != 2 || len(r.orders) != 2 {
				t.Fatalf("dust must unwind both native quantities: %+v", res)
			}
			requireQty(t, l.orders[1].Qty, .01)
			requireQty(t, r.orders[1].Qty, .009)
		})
	}
}

func TestCanceledOrFailedPositionReadsNeverInventCloseFills(t *testing.T) {
	for _, canceled := range []bool{true, false} {
		name := "failed positions"
		if canceled {
			name = "canceled before submission"
		}
		t.Run(name, func(t *testing.T) {
			zero := func(int, domain.OrderReq) float64 { return 0 }
			l, r := newFake("L", zero), newFake("R", zero)
			l.pos, r.pos = .01, -.01
			e := testLifecycleExec(t, l, r)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			lv := lifecycleVenue{fakeVenue: l}
			want := errors.New("position read failed")
			if canceled {
				cancel()
				want = context.Canceled
			} else {
				lv.positionFn = func(context.Context) ([]domain.Position, error) { return nil, want }
			}
			res := e.Execute(ctx, LegSpec{lv, inst, domain.Sell, 100}, LegSpec{r, inst, domain.Buy, 100.1}, .01, true, .01, -.01)
			if !errors.Is(res.Err, want) || res.PositionsKnown || res.SettlementKnown || res.L.Filled != 0 || res.R.Filled != 0 || res.RescueOrders != 0 {
				t.Fatalf("position failure fabricated a close: %+v", res)
			}
			requireQty(t, res.PosL, .01)
			requireQty(t, res.PosR, -.01)
			requireQty(t, l.pos, .01)
			requireQty(t, r.pos, -.01)
		})
	}
}

func TestUnknownZeroOrPartialRemainsPendingUntilTerminal(t *testing.T) {
	for _, filled := range []float64{0, .004} {
		name := "zero"
		if filled > 0 {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			f := func(int, domain.OrderReq) float64 { return filled }
			l, r := newFake("L", f), newFake("R", f)
			l.silent, r.silent = true, true
			e := testLifecycleExec(t, l, r)
			var terminal atomic.Bool
			query := func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
				u := domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}
				if terminal.Load() {
					u.Status, u.FilledQty, u.FilledQuote = domain.StatusCanceled, filled, filled*100
				}
				return u, nil
			}
			ls := LegSpec{lifecycleVenue{fakeVenue: l, queryFn: query}, inst, domain.Buy, 100}
			rs := LegSpec{lifecycleVenue{fakeVenue: r, queryFn: query}, inst, domain.Sell, 100.1}
			res := e.Execute(context.Background(), ls, rs, .01, false, 0, 0)
			for i := 0; i < 2; i++ {
				if !errors.Is(res.Err, ErrUnresolved) || res.SettlementKnown || !res.PositionsKnown || res.RescueOrders != 0 {
					t.Fatalf("unknown order was cleared: %+v", res)
				}
				requireQty(t, res.L.Filled, filled)
				requireQty(t, res.R.Filled, filled)
				res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, 0, 0)
			}
			terminal.Store(true)
			res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, 0, 0)
			requireSettled(t, res)
			if len(l.orders) != 1 || len(r.orders) != 1 || res.RescueOrders != 0 {
				t.Fatalf("recovery resubmitted original IOC: %+v", res)
			}
		})
	}
}

func TestCanceledExecutionRecoversOriginalIOCWithParentContext(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.silent, r.silent = true, true
	e := testLifecycleExec(t, l, r)
	parent := context.Background()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	var submitted atomic.Int32
	wrap := func(f *fakeVenue) lifecycleVenue {
		return lifecycleVenue{
			fakeVenue: f,
			submitFn: func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
				ack, err := f.Submit(ctx, req)
				if submitted.Add(1) == 2 {
					cancel()
				}
				return ack, err
			},
			queryFn: func(ctx context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
				if err := ctx.Err(); err != nil {
					return domain.OrderUpdate{}, err
				}
				return domain.OrderUpdate{ClientID: cid, Status: domain.StatusFilled, FilledQty: .01, FilledQuote: 1}, nil
			},
		}
	}
	ls, rs := LegSpec{wrap(l), inst, domain.Buy, 100}, LegSpec{wrap(r), inst, domain.Sell, 100.1}
	res := e.Execute(ctx, ls, rs, .01, false, 0, 0)
	if !errors.Is(res.Err, context.Canceled) || res.SettlementKnown || res.PositionsKnown {
		t.Fatalf("canceled child execution was cleared: %+v", res)
	}
	res = e.ResolveOutstanding(parent, ls, rs, res, .01, 0, 0)
	requireSettled(t, res)
	requireQty(t, res.PosL, .01)
	requireQty(t, res.PosR, -.01)
	if len(l.orders) != 1 || len(r.orders) != 1 || !res.L.Resolved || !res.R.Resolved {
		t.Fatalf("parent recovery did not resolve original orders: %+v", res)
	}
}

func TestUnknownFullRescueIsRecordedWithEstimatedPrice(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", func(n int, req domain.OrderReq) float64 {
		if n == 0 {
			return 0
		}
		return req.Qty
	})
	l.pos, r.pos, r.silent = .01, -.01, true
	e := testLifecycleExec(t, l, r)
	rv := lifecycleVenue{fakeVenue: r, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		status := domain.StatusUnknown
		if cid == r.orders[0].ClientID {
			status = domain.StatusCanceled
		}
		return domain.OrderUpdate{ClientID: cid, Status: status}, nil
	}}
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Sell, 100}, LegSpec{rv, inst, domain.Buy, 100.1}, .01, true, .01, -.01)
	requireSettled(t, res)
	requireQty(t, res.PosL, 0)
	requireQty(t, res.PosR, 0)
	requireQty(t, res.RescueQty, .01)
	requireQty(t, res.R.RescueFilled, .01)
	if res.RescueOrders != 1 || !res.R.RescuePriceEstimated || res.R.RescuePrice <= 0 {
		t.Fatalf("unknown rescue fill or estimated price was lost: %+v", res)
	}
}

func TestPartialUnknownRescueRecoveryDoesNotDuplicateOrdersOrAccounting(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", func(n int, req domain.OrderReq) float64 {
		switch n {
		case 0:
			return 0
		case 1:
			return .004
		default:
			return req.Qty
		}
	})
	l.pos, r.pos, r.silent = .01, -.01, true
	e := testLifecycleExec(t, l, r)
	var terminal atomic.Bool
	rv := lifecycleVenue{fakeVenue: r, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, req := range r.orders {
			if req.ClientID != cid {
				continue
			}
			status, q := domain.StatusCanceled, 0.0
			if i == 1 {
				status, q = domain.StatusUnknown, .004
				if terminal.Load() {
					status = domain.StatusCanceled
				}
			} else if i > 1 {
				status, q = domain.StatusFilled, req.Qty
			}
			return domain.OrderUpdate{ClientID: cid, Status: status, FilledQty: q, FilledQuote: q * req.Price}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}}
	ls, rs := LegSpec{l, inst, domain.Sell, 100}, LegSpec{rv, inst, domain.Buy, 100.1}
	res := e.Execute(context.Background(), ls, rs, .01, true, .01, -.01)
	for i := 0; i < 2; i++ {
		if !errors.Is(res.Err, ErrUnresolved) || res.SettlementKnown || res.RescueOrders != 1 || len(r.orders) != 2 {
			t.Fatalf("unknown rescue was replaced before settlement: %+v", res)
		}
		requireQty(t, res.RescueQty, .004)
		requireQty(t, res.R.RescueFilled, .004)
		res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, .01, -.01)
	}
	terminal.Store(true)
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, .01, -.01)
	requireSettled(t, res)
	requireQty(t, res.PosL, 0)
	requireQty(t, res.PosR, 0)
	requireQty(t, res.RescueQty, .01)
	requireQty(t, res.R.RescueFilled, .01)
	if res.RescueOrders != 2 || len(r.orders) != 3 {
		t.Fatalf("recovery duplicated rescue orders: %+v", res)
	}
	requireQty(t, r.orders[2].Qty, .006)
}

func TestConfirmedFillWaitsForPositionSnapshot(t *testing.T) {
	for _, delayedForever := range []bool{false, true} {
		name := "eventual snapshot"
		if delayedForever {
			name = "stale snapshot"
		}
		t.Run(name, func(t *testing.T) {
			l, r := newFake("L", full), newFake("R", full)
			e := testLifecycleExec(t, l, r)
			var reads atomic.Int32
			lv := lifecycleVenue{fakeVenue: l, positionFn: func(ctx context.Context) ([]domain.Position, error) {
				if reads.Add(1) == 1 || delayedForever {
					return []domain.Position{{MarketID: 1}}, nil
				}
				return l.Positions(ctx)
			}}
			res := e.Execute(context.Background(), LegSpec{lv, inst, domain.Buy, 100}, LegSpec{r, inst, domain.Sell, 100.1}, .01, false, 0, 0)
			if delayedForever {
				if res.Err == nil || res.SettlementKnown || res.RescueOrders != 0 {
					t.Fatalf("stale snapshot accepted confirmed fills: %+v", res)
				}
			} else {
				requireSettled(t, res)
				requireQty(t, res.PosL, .01)
				if reads.Load() < 2 {
					t.Fatal("position confirmation did not retry the stale snapshot")
				}
			}
		})
	}
}

func TestEmergencyRecoveryRetainsZeroTargetsAndMarketPricing(t *testing.T) {
	for _, partial := range []float64{0, .004} {
		name := "zero"
		if partial > 0 {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			f := func(n int, req domain.OrderReq) float64 {
				if n == 0 {
					return partial
				}
				return req.Qty
			}
			l, r := newFake("L", f), newFake("R", f)
			l.pos, r.pos, l.silent, r.silent = .01, -.01, true, true
			e := testLifecycleExec(t, l, r)
			var terminal atomic.Bool
			wrap := func(f *fakeVenue) lifecycleVenue {
				return lifecycleVenue{fakeVenue: f, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
					f.mu.Lock()
					defer f.mu.Unlock()
					for i, req := range f.orders {
						if req.ClientID != cid {
							continue
						}
						status, q := domain.StatusUnknown, partial
						if i == 0 && terminal.Load() {
							status = domain.StatusCanceled
						} else if i > 0 {
							status, q = domain.StatusFilled, req.Qty
						}
						return domain.OrderUpdate{ClientID: cid, Status: status, FilledQty: q, FilledQuote: q * req.Price}, nil
					}
					return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
				}}
			}
			ls, rs := LegSpec{Venue: wrap(l), Inst: inst}, LegSpec{Venue: wrap(r), Inst: inst}
			res, err := e.EmergencyFlatten(context.Background(), ls, rs, 0, .01, -.01)
			if !errors.Is(err, ErrUnresolved) || res.SettlementKnown || res.RescueOrders != 2 {
				t.Fatalf("unknown emergency was cleared: %+v, err=%v", res, err)
			}
			res = e.ResolveOutstanding(context.Background(), ls, rs, res, 0, .01, -.01)
			if !errors.Is(res.Err, ErrUnresolved) || len(l.orders) != 1 || len(r.orders) != 1 {
				t.Fatalf("emergency recovery submitted before settlement: %+v", res)
			}
			terminal.Store(true)
			res = e.ResolveOutstanding(context.Background(), ls, rs, res, 0, .01, -.01)
			requireSettled(t, res)
			requireQty(t, res.PosL, 0)
			requireQty(t, res.PosR, 0)
			requireQty(t, res.RescueQty, .02)
			requireQty(t, res.L.RescueFilled, .01)
			requireQty(t, res.R.RescueFilled, .01)
			if res.RescueOrders != 4 || len(l.orders) != 2 || len(r.orders) != 2 {
				t.Fatalf("emergency recovery lost its per-leg targets: %+v", res)
			}
			for _, v := range []*fakeVenue{l, r} {
				if v.orders[1].Type != domain.Market || !v.orders[1].ReduceOnly {
					t.Fatalf("emergency lost protective market parameters: %+v", v.orders[1])
				}
			}
		})
	}
}

func TestRescuePositionFailureRecoveryCountsTerminalFillOnce(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", func(n int, req domain.OrderReq) float64 {
		if n == 0 {
			return 0
		}
		return req.Qty
	})
	l.pos, r.pos = .01, -.01
	e := testLifecycleExec(t, l, r)
	var unavailable atomic.Bool
	unavailable.Store(true)
	want := errors.New("rescue position query failed")
	rv := lifecycleVenue{fakeVenue: r, positionFn: func(ctx context.Context) ([]domain.Position, error) {
		r.mu.Lock()
		rescueSubmitted := len(r.orders) > 1
		r.mu.Unlock()
		if rescueSubmitted && unavailable.Load() {
			return nil, want
		}
		return r.Positions(ctx)
	}}
	ls, rs := LegSpec{l, inst, domain.Sell, 100}, LegSpec{rv, inst, domain.Buy, 100.1}
	res := e.Execute(context.Background(), ls, rs, .01, true, .01, -.01)
	if !errors.Is(res.Err, want) || res.PositionsKnown || res.SettlementKnown || res.RescueOrders != 1 {
		t.Fatalf("failed rescue snapshot reported success: %+v", res)
	}
	requireQty(t, res.RescueQty, .01)
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, .01, -.01)
	if !errors.Is(res.Err, want) || res.RescueOrders != 1 {
		t.Fatalf("recovery resubmitted terminal rescue: %+v", res)
	}
	requireQty(t, res.RescueQty, .01)
	unavailable.Store(false)
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, .01, -.01)
	requireSettled(t, res)
	requireQty(t, res.PosL, 0)
	requireQty(t, res.PosR, 0)
	requireQty(t, res.RescueQty, .01)
	requireQty(t, res.R.RescueFilled, .01)
	if len(r.orders) != 2 || res.RescueOrders != 1 {
		t.Fatalf("recovery duplicated terminal rescue: %+v", res)
	}
}

func TestRecoveryPriceEstimateUsesOriginalOrderBound(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.silent, r.silent = true, true
	e := testLifecycleExec(t, l, r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var submitted atomic.Int32
	wrap := func(f *fakeVenue) lifecycleVenue {
		return lifecycleVenue{fakeVenue: f,
			submitFn: func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
				ack, err := f.Submit(ctx, req)
				if submitted.Add(1) == 2 {
					cancel()
				}
				return ack, err
			},
			queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
				return domain.OrderUpdate{ClientID: cid, Status: domain.StatusFilled, FilledQty: .01}, nil
			},
		}
	}
	ls, rs := LegSpec{wrap(l), inst, domain.Buy, 100}, LegSpec{wrap(r), inst, domain.Sell, 100.1}
	res := e.Execute(ctx, ls, rs, .01, false, 0, 0)
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("child cancellation missing: %+v", res)
	}
	// The volatility allowance may change before recovery. Missing venue quote
	// must use the original IOC price, whose cumulative fill is being resolved.
	e.cfg.PriceTicks += 50
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, 0, 0)
	requireSettled(t, res)
	if !res.L.PriceEstimated || !res.R.PriceEstimated {
		t.Fatalf("missing quote was labeled exact: %+v", res)
	}
	requireQty(t, res.L.AvgPrice, l.orders[0].Price)
	requireQty(t, res.R.AvgPrice, r.orders[0].Price)
}

func TestEscalatedRecoverySettlesOldBatchAndPreservesHedgeTarget(t *testing.T) {
	l := newFake("L", func(n int, req domain.OrderReq) float64 {
		switch n {
		case 0:
			return req.Qty
		case 1:
			return .004
		default:
			if req.Type == domain.Market && req.Price < 99.1 {
				return req.Qty
			}
			return 0
		}
	})
	r := newFake("R", func(int, domain.OrderReq) float64 { return .004 })
	l.pos, r.pos, l.silent = .02, -.02, true
	e := testLifecycleExec(t, l, r)
	var terminal atomic.Bool
	lv := lifecycleVenue{fakeVenue: l, queryFn: func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		for i, req := range l.orders {
			if req.ClientID != cid {
				continue
			}
			status, q := domain.StatusFilled, req.Qty
			if i == 1 {
				status, q = domain.StatusUnknown, .004
				if terminal.Load() {
					status = domain.StatusCanceled
				}
			}
			return domain.OrderUpdate{ClientID: cid, Status: status, FilledQty: q, FilledQuote: q * req.Price}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}}
	ls, rs := LegSpec{lv, inst, domain.Buy, 100}, LegSpec{r, inst, domain.Sell, 100.1}
	res := e.Execute(context.Background(), ls, rs, .01, false, .02, -.02)
	if !errors.Is(res.Err, ErrUnresolved) || res.RescueOrders != 1 {
		t.Fatalf("normal rescue did not retain old batch: %+v", res)
	}
	res = e.EscalateOutstanding(res)
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, .01, .02, -.02)
	if !errors.Is(res.Err, ErrUnresolved) || len(l.orders) != 2 || len(r.orders) != 1 {
		t.Fatalf("escalation bypassed old IOC settlement: %+v", res)
	}
	terminal.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res = e.ResolveOutstanding(ctx, ls, rs, res, .01, .02, -.02)
	requireSettled(t, res)
	requireQty(t, res.PosL, .024)
	requireQty(t, res.PosR, -.024)
	requireQty(t, res.RescueQty, .006)
	requireQty(t, res.L.RescueFilled, .006)
	if res.RescueOrders != 2 || len(l.orders) != 3 || len(r.orders) != 1 {
		t.Fatalf("escalation duplicated or erased the normal hedge: %+v", res)
	}
	if l.orders[1].Type != domain.Limit || l.orders[1].Price != 100 {
		t.Fatalf("existing rescue request was modified: %+v", l.orders[1])
	}
	last := l.orders[2]
	if last.Type != domain.Market || !last.ReduceOnly || last.Price != 99.04 {
		t.Fatalf("future rescue did not use emergency bound: %+v", last)
	}
	requireQty(t, last.Qty, .002)
}

func TestPositionInferredFillUsesConservativeOriginalIOCPrice(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.silent, r.silent = true, true
	e := testLifecycleExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Buy, 100}, LegSpec{r, inst, domain.Sell, 100.1}, .01, false, 0, 0)
	requireSettled(t, res)
	if res.L.Resolved || res.R.Resolved || !res.L.PriceEstimated || !res.R.PriceEstimated {
		t.Fatalf("position-inferred prices were labeled exact: %+v", res)
	}
	requireQty(t, res.L.AvgPrice, l.orders[0].Price)
	requireQty(t, res.R.AvgPrice, r.orders[0].Price)
	if res.L.AvgPrice <= 100 || res.R.AvgPrice >= 100.1 {
		t.Fatalf("IOC allowance was lost, allowing fabricated profit: %+v", res)
	}
}

func TestEmergencyReducesAvailableLegAndRecoversMissingPeerBook(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.pos, r.pos = .01, -.01
	e := testLifecycleExec(t, l, r)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var peerReady atomic.Bool
	book := domain.Book{Bids: []domain.Level{{Price: 100, Size: 1}}, Asks: []domain.Level{{Price: 100.1, Size: 1}}}
	e.book = func(venue string, _ int) (domain.Book, bool) {
		if venue == r.Name() && !peerReady.Load() {
			l.mu.Lock()
			leftFlat := l.pos == 0
			l.mu.Unlock()
			if leftFlat {
				return domain.Book{}, false
			}
			return domain.Book{}, false
		}
		return book, true
	}
	ls, rs := LegSpec{Venue: l, Inst: inst}, LegSpec{Venue: r, Inst: inst}
	res, err := e.EmergencyFlatten(ctx, ls, rs, 0, .01, -.01)
	if !errors.Is(err, ErrIncomplete) || !errors.Is(res.Err, ErrIncomplete) || res.RescueOrders != 1 || len(l.orders) != 1 || len(r.orders) != 0 {
		t.Fatalf("missing peer blocked the available leg or reported completion: %+v, err=%v", res, err)
	}
	requireQty(t, res.PosL, 0)
	requireQty(t, res.PosR, -.01)
	requireQty(t, res.RescueQty, .01)
	requireQty(t, res.L.RescueFilled, .01)
	leftPrice := res.L.RescuePrice
	if !res.PositionsKnown || !res.SettlementKnown || leftPrice <= 0 || !l.orders[0].ReduceOnly || l.orders[0].Type != domain.Market {
		t.Fatalf("available emergency leg was not confirmed and priced: %+v", res)
	}
	peerReady.Store(true)
	res = e.ResolveOutstanding(context.Background(), ls, rs, res, 0, .01, -.01)
	requireSettled(t, res)
	requireQty(t, res.PosL, 0)
	requireQty(t, res.PosR, 0)
	requireQty(t, res.RescueQty, .02)
	requireQty(t, res.L.RescueFilled, .01)
	requireQty(t, res.L.RescuePrice, leftPrice)
	requireQty(t, res.R.RescueFilled, .01)
	if res.RescueOrders != 2 || len(l.orders) != 1 || len(r.orders) != 1 || r.orders[0].Type != domain.Market || !r.orders[0].ReduceOnly {
		t.Fatalf("missing peer recovery lost or duplicated prior unwind: %+v", res)
	}
}

func TestImmediateRescueYieldsAfterConfirmedIncompleteBatch(t *testing.T) {
	for _, firstFill := range []float64{0, .004} {
		name := "zero fill"
		if firstFill > 0 {
			name = "partial fill"
		}
		t.Run(name, func(t *testing.T) {
			fillFn := func(n int, req domain.OrderReq) float64 {
				if n == 0 {
					return firstFill
				}
				return req.Qty
			}
			l, r := newFake("L", fillFn), newFake("R", fillFn)
			l.pos, r.pos = .01, -.01
			e := testLifecycleExec(t, l, r)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			ls, rs := LegSpec{Venue: l, Inst: inst}, LegSpec{Venue: r, Inst: inst}
			res, err := e.EmergencyFlatten(ctx, ls, rs, 0, .01, -.01)
			if !errors.Is(err, ErrIncomplete) || !errors.Is(res.Err, ErrIncomplete) || !res.PositionsKnown || !res.SettlementKnown {
				t.Fatalf("incomplete confirmed batch did not yield: %+v err=%v", res, err)
			}
			if len(l.orders) != 1 || len(r.orders) != 1 {
				t.Fatal("one attempt submitted multiple batches")
			}
			requireQty(t, res.PosL, .01-firstFill)
			requireQty(t, res.PosR, firstFill-.01)
			res = e.ResolveOutstanding(ctx, ls, rs, res, 0, .01, -.01)
			requireSettled(t, res)
			requireQty(t, res.PosL, 0)
			requireQty(t, res.PosR, 0)
			requireQty(t, res.L.RescueFilled, .01)
			requireQty(t, res.R.RescueFilled, .01)
			requireQty(t, res.RescueQty, .02)
			if res.RescueOrders != 4 || len(l.orders) != 2 || len(r.orders) != 2 {
				t.Fatal("recovery duplicated a confirmed batch")
			}
			for _, v := range []*fakeVenue{l, r} {
				for _, req := range v.orders {
					if req.Type != domain.Market || !req.ReduceOnly {
						t.Fatal("urgent retry lost its protective reduce-only order")
					}
				}
			}
		})
	}
}

func TestImmediateRescueWithoutBooksReturnsIncompleteWithoutSending(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.pos, r.pos = .01, -.01
	e := testLifecycleExec(t, l, r)
	e.book = func(string, int) (domain.Book, bool) { return domain.Book{}, false }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := e.EmergencyFlatten(ctx, LegSpec{Venue: l, Inst: inst}, LegSpec{Venue: r, Inst: inst}, 0, .01, -.01)
	if !errors.Is(err, ErrIncomplete) || !errors.Is(res.Err, ErrIncomplete) || !res.PositionsKnown || !res.SettlementKnown {
		t.Fatalf("missing books blocked or reported completion: %+v err=%v", res, err)
	}
	if len(l.orders) != 0 || len(r.orders) != 0 || res.RescueOrders != 0 {
		t.Fatal("sent urgent orders without protective prices")
	}
	requireQty(t, res.PosL, .01)
	requireQty(t, res.PosR, -.01)
}
