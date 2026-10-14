package engine

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func settlementReviewActor(t *testing.T) *PairActor {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	i := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001}
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("REVIEW", i, i),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), Store: st, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	a.volAcc = volumeAcc{day: time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02"), volumeUSD: 17, pnlUSD: -5}
	return a
}

func requireSettlementReviewBudget(t *testing.T, a *PairActor, volume, pnl float64) {
	t.Helper()
	if a.volAcc.day != time.Now().UTC().Format("2006-01-02") || math.Abs(a.volAcc.volumeUSD-volume) > 1e-9 || math.Abs(a.volAcc.pnlUSD-pnl) > 1e-9 {
		t.Fatalf("settlement retained previous-day budget: %+v, want current day volume=%v pnl=%v", a.volAcc, volume, pnl)
	}
}

func TestSettlementReviewCompletionDayBudget(t *testing.T) {
	t.Run("opening completed after midnight", func(t *testing.T) {
		a := settlementReviewActor(t)
		act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01,
			L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 100}}
		res := execution.Result{L: execution.LegResult{Filled: .01, AvgPrice: 100},
			R: execution.LegResult{Filled: .01, AvgPrice: 100}, PosL: .01, PosR: -.01, PositionsKnown: true, SettlementKnown: true}
		if err := a.accountExecution(act, spread.Quote{}, 0, 0, res); err != nil {
			t.Fatal(err)
		}
		requireSettlementReviewBudget(t, a, 2, -.002)
	})
	t.Run("unwind completed after midnight", func(t *testing.T) {
		a := settlementReviewActor(t)
		a.state.Steps = []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: .01, OpenL: 100, OpenR: 100}}
		a.next = 2
		a.posL, a.posR = .01, -.01
		res := execution.Result{L: execution.LegResult{RescueFilled: .01, RescuePrice: 100},
			R: execution.LegResult{RescueFilled: .01, RescuePrice: 100}, PositionsKnown: true, SettlementKnown: true}
		if err := a.finishUnwind(res); err != nil {
			t.Fatal(err)
		}
		requireSettlementReviewBudget(t, a, 2, -.002)
	})
}

func TestSettlementReviewLateRiskOrDrainStopsOriginalSubmission(t *testing.T) {
	for _, risk := range []bool{true, false} {
		name := "drain"
		if risk {
			name = "risk"
		}
		t.Run(name, func(t *testing.T) {
			a := settlementReviewActor(t)
			l, r := a.cfg.L.(*actorTestVenue), a.cfg.R.(*actorTestVenue)
			var orders atomic.Int32
			for _, v := range []*actorTestVenue{l, r} {
				v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
					orders.Add(1)
					return req.Qty, nil
				}
			}
			tr, err := execution.NewTracker(context.Background(), l, r)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(tr.Close)
			cfg := execution.DefaultConfig()
			cfg.SettleDelay, cfg.QueryInterval = time.Millisecond, time.Millisecond
			a.cfg.DryRun, a.cfg.ExecCfg = false, cfg
			a.exec = execution.NewExecutor(cfg, tr, execution.NewIDGen(), a.book, nil, nil, func(string) {})
			act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01,
				L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 100}}
			a.execMu.Lock()
			locked := true
			t.Cleanup(func() {
				if locked {
					a.execMu.Unlock()
				}
			})
			done := make(chan struct{})
			go func() {
				defer close(done)
				a.apply(context.Background(), act, spread.Quote{})
			}()
			// Admission has passed, but execCancel registration is blocked.
			awaitActorCondition(t, "execution context registration", a.Busy)
			var drainDone chan struct{}
			if risk {
				a.emergencyRequested.Store(true)
			} else {
				drainDone = make(chan struct{})
				go func() { defer close(drainDone); a.Drain() }()
				awaitActorCondition(t, "late drain request", a.draining.Load)
			}
			a.execMu.Unlock()
			locked = false
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("late risk/drain did not return execution to actor")
			}
			if drainDone != nil {
				select {
				case <-drainDone:
				case <-time.After(time.Second):
					t.Fatal("drain request did not finish")
				}
			}
			if orders.Load() != 0 || len(a.state.Steps) != 0 || a.Busy() {
				t.Fatalf("late request admitted an IOC: orders=%d pending=%+v", orders.Load(), a.pending)
			}
			if a.pending != nil {
				if !errors.Is(a.pending.result.Err, context.Canceled) || !a.checkpoint || a.Flat() {
					t.Fatalf("canceled IOC lost its conservative checkpoint: %+v", a.pending)
				}
				a.resolvePending(context.Background())
			}
			if a.pending != nil || a.checkpoint || orders.Load() != 0 {
				t.Fatalf("unsent IOC recovery did not settle safely: orders=%d pending=%+v checkpoint=%v", orders.Load(), a.pending, a.checkpoint)
			}
			if risk {
				a.emergency(context.Background())
				if a.emergencyRequested.Load() || !a.Flat() {
					t.Fatal("confirmed empty emergency did not complete")
				}
			} else {
				a.drain(context.Background())
				if !a.Drained() {
					t.Fatal("confirmed empty drain did not complete")
				}
			}
		})
	}
}
