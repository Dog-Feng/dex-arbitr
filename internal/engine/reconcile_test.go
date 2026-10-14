package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

type reconcileTestEstimator struct {
	actorTestEstimator
	samples chan struct{}
}

func (e reconcileTestEstimator) Update(time.Time, float64) {
	select {
	case e.samples <- struct{}{}:
	default:
	}
}

func newReconcileTestActor(t *testing.T, ctx context.Context, l, r *actorTestVenue, steps []strategy.Step, est estimator.Estimator, ec execution.Config) *PairActor {
	t.Helper()
	tr, err := execution.NewTracker(ctx, l, r)
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Save("TEST_L_R", store.PairState{NextID: 2, Steps: steps}); err != nil {
		t.Fatal(err)
	}
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
	ec.SettleDelay = time.Millisecond
	a, err := NewPairActor(PairConfig{
		Pair: symbols.NewPair("TEST", inst, inst), L: l, R: r,
		Params: strategy.Params{StepQty: 0.01, MaxSteps: 1, OpenBps: 1, CloseBps: 1, DepthMult: 2},
		Est:    est, Store: st, Tracker: tr, IDs: execution.NewIDGen(), ExecCfg: ec,
		SampleEvery: time.Millisecond, MaxBookAge: time.Second,
		IntervalMin: time.Millisecond, IntervalMax: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.state.Mode != strategy.ModePaused {
		t.Fatalf("live actor must start paused, got %s", a.state.Mode)
	}
	return a
}

func runReconcileTestActor(t *testing.T, a *PairActor, ctx context.Context, cancel context.CancelFunc) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("actor stopped: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("actor did not stop after cancellation")
		}
	})
}

func TestPairActorFailedReconciliationBlocksOpenAndClose(t *testing.T) {
	step := []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: 0.01, EntryBps: 100}}
	readErr := errors.New("positions unavailable")
	for _, tc := range []struct {
		name       string
		pl, pr     float64
		steps      []strategy.Step
		errL, errR error
	}{
		{name: "left query fails", errL: readErr},
		{name: "right query fails", pl: 0.01, pr: -0.01, steps: step, errR: readErr},
		{name: "unhedged", pl: 0.02, pr: -0.01, steps: step},
		{name: "hedged but wrong quantity", pl: 0.02, pr: -0.02, steps: step},
		{name: "positions without steps", pl: 0.01, pr: -0.01},
		{name: "steps without positions", steps: step},
		{name: "wrong direction", pl: -0.01, pr: 0.01, steps: step},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			l.pos, r.pos = tc.pl, tc.pr
			if tc.errL != nil {
				l.positions = func(context.Context) ([]domain.Position, error) { return nil, tc.errL }
			}
			if tc.errR != nil {
				r.positions = func(context.Context) ([]domain.Position, error) { return nil, tc.errR }
			}
			orders := make(chan domain.OrderReq, 4)
			for _, v := range []*actorTestVenue{l, r} {
				v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
					orders <- req
					return req.Qty, nil
				}
			}
			samples := make(chan struct{}, 1)
			a := newReconcileTestActor(t, ctx, l, r, tc.steps,
				reconcileTestEstimator{samples: samples}, execution.DefaultConfig())
			runReconcileTestActor(t, a, ctx, cancel)
			l.books <- actorTestBook("L", 100)
			r.books <- actorTestBook("R", 100.2)
			select {
			case <-samples: // The ready estimator is sampled even while trading is paused.
			case <-time.After(time.Second):
				t.Fatal("actor did not sample after failed reconciliation")
			}
			select {
			case req := <-orders:
				t.Fatalf("submitted %+v despite failed reconciliation", req)
			case <-time.After(250 * time.Millisecond): // Includes a decision tick with a profitable signal.
			}
		})
	}
}

func TestPairActorBlocksTradesUntilReconciled(t *testing.T) {
	for _, ticks := range []int{0, 7} {
		t.Run(fmt.Sprintf("%d_ticks", ticks), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			started, release := make(chan struct{}), make(chan struct{})
			r.positions = func(ctx context.Context) ([]domain.Position, error) {
				select {
				case <-started:
				default:
					close(started)
				}
				select {
				case <-release:
					r.mu.Lock()
					defer r.mu.Unlock()
					return []domain.Position{{MarketID: 1, Qty: r.pos}}, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			ordersL, ordersR := make(chan domain.OrderReq, 2), make(chan domain.OrderReq, 2)
			l.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
				ordersL <- req
				return req.Qty, nil
			}
			r.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
				ordersR <- req
				return req.Qty, nil
			}
			cfg := &config.Config{}
			cfg.Execution.PriceTicks = ticks
			a := newReconcileTestActor(t, ctx, l, r, nil, actorTestEstimator{}, ExecutionConfig(cfg))
			runReconcileTestActor(t, a, ctx, cancel)
			l.books <- actorTestBook("L", 100)
			r.books <- actorTestBook("R", 100.2)
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("reconciliation did not start")
			}
			awaitActorCondition(t, "books while reconciliation is pending", func() bool {
				_, lok := a.book("L", 1)
				_, rok := a.book("R", 1)
				return lok && rok
			})
			select {
			case req := <-ordersL:
				t.Fatalf("left submitted before reconciliation: %+v", req)
			case req := <-ordersR:
				t.Fatalf("right submitted before reconciliation: %+v", req)
			case <-time.After(250 * time.Millisecond):
			}
			close(release)
			left := awaitActorOrder(t, ordersL)
			right := awaitActorOrder(t, ordersR)
			wantL, wantR := 100.1+float64(ticks)*0.01, 100.2-float64(ticks)*0.01
			if left.Side != domain.Buy || right.Side != domain.Sell || math.Abs(left.Price-wantL) > 1e-9 || math.Abs(right.Price-wantR) > 1e-9 {
				t.Fatalf("configured %d ticks: left %+v right %+v, want %.2f/%.2f", ticks, left, right, wantL, wantR)
			}
		})
	}
}

func TestPairActorReconcileMatchesRecordedSteps(t *testing.T) {
	for _, dir := range []strategy.Dir{strategy.DirLS, strategy.DirSL} {
		t.Run(dir.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			l.pos, r.pos = -float64(dir)*0.01, float64(dir)*0.01
			a := newReconcileTestActor(t, ctx, l, r, []strategy.Step{{ID: 1, Dir: dir, Qty: 0.01, OpenL: 100, OpenR: 100.2}}, actorTestEstimator{}, execution.DefaultConfig())
			if err := a.reconcile(ctx); err != nil {
				t.Fatal(err)
			}
			if a.state.Mode != strategy.ModeWarmup || a.posL != l.pos || a.posR != r.pos {
				t.Fatalf("matched positions should enter warmup: mode %s positions %.4f/%.4f", a.state.Mode, a.posL, a.posR)
			}
		})
	}
}
