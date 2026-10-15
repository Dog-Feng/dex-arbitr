package engine

import (
	"context"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

func TestPairActorBlocksOpensUntilVolatilityReady(t *testing.T) {
	for _, tc := range []struct {
		name      string
		volEvery  time.Duration
		volMaxAge time.Duration
		wantOpen  bool
	}{
		{"volatility starved", time.Hour, time.Millisecond, false},
		{"volatility fed", 10 * time.Millisecond, time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := st.Close(); err != nil {
					t.Error(err)
				}
			})
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			orders := make(chan domain.OrderReq, 4)
			for _, v := range []*actorTestVenue{l, r} {
				v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
					orders <- req
					return req.Qty, nil
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tr, err := execution.NewTracker(ctx, l, r)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(tr.Close)
			ec := execution.DefaultConfig()
			ec.SettleDelay, ec.QueryInterval = time.Millisecond, time.Millisecond
			inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
			a, err := NewPairActor(PairConfig{
				Pair: symbols.NewPair("TEST", inst, inst), L: l, R: r, DryRun: false,
				Store: st, Est: actorTestEstimator{mean: 0}, Tracker: tr, IDs: execution.NewIDGen(), ExecCfg: ec,
				Params:   strategy.Params{StepQty: .01, MaxSteps: 3, OpenBps: 0, CloseBps: 0, DepthMult: 1},
				Vol:      volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}),
				VolEvery: tc.volEvery, VolMaxAge: tc.volMaxAge,
				SampleEvery: time.Second, MaxBookAge: time.Second,
				IntervalMin: 20 * time.Millisecond, IntervalMax: 20 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("actor stopped: %v", err)
				}
			})
			l.books <- actorTestBook("L", 100)
			r.books <- actorTestBook("R", 100.2)
			if tc.wantOpen {
				select {
				case req := <-orders:
					if req.ReduceOnly {
						t.Fatalf("expected an opening order, got %+v", req)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("warmed volatility did not permit an opening")
				}
			} else {
				select {
				case req := <-orders:
					t.Fatalf("opened without a ready volatility chain: %+v", req)
				case <-time.After(250 * time.Millisecond):
				}
			}
		})
	}
}

func TestPairActorBlocksClosesUntilVolatilityReady(t *testing.T) {
	for _, tc := range []struct {
		name      string
		volEvery  time.Duration
		volMaxAge time.Duration
		wantClose bool
	}{
		{"volatility starved", time.Hour, time.Millisecond, false},
		{"volatility fed", 10 * time.Millisecond, time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := st.Close(); err != nil {
					t.Error(err)
				}
			})
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			l.pos, r.pos = .01, -.01
			if err := st.Save("TEST_L_R", store.PairState{NextID: 2, Steps: []strategy.Step{
				{ID: 1, Dir: strategy.DirLS, Qty: .01, OpenL: 100, OpenR: 100.2},
			}}); err != nil {
				t.Fatal(err)
			}
			orders := make(chan domain.OrderReq, 4)
			for _, v := range []*actorTestVenue{l, r} {
				v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
					orders <- req
					return req.Qty, nil
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			tr, err := execution.NewTracker(ctx, l, r)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(tr.Close)
			ec := execution.DefaultConfig()
			ec.SettleDelay, ec.QueryInterval = time.Millisecond, time.Millisecond
			inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
			a, err := NewPairActor(PairConfig{
				Pair: symbols.NewPair("TEST", inst, inst), L: l, R: r, DryRun: false,
				Store: st, Est: actorTestEstimator{mean: 0}, Tracker: tr, IDs: execution.NewIDGen(), ExecCfg: ec,
				Params:   strategy.Params{StepQty: .01, MaxSteps: 3, OpenBps: 0, CloseBps: 0, DepthMult: 1},
				Vol:      volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}),
				VolEvery: tc.volEvery, VolMaxAge: tc.volMaxAge,
				SampleEvery: time.Second, MaxBookAge: time.Second,
				IntervalMin: 20 * time.Millisecond, IntervalMax: 20 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- a.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				if err := <-done; err != nil {
					t.Errorf("actor stopped: %v", err)
				}
			})
			// A profitable reverse spread: closing DirLS sells L at its bid and
			// buys R at its ask.
			l.books <- actorTestBook("L", 100.3)
			r.books <- actorTestBook("R", 100.1)
			if tc.wantClose {
				select {
				case req := <-orders:
					if !req.ReduceOnly {
						t.Fatalf("expected a reducing close, got %+v", req)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("warmed volatility did not permit a profitable close")
				}
			} else {
				select {
				case req := <-orders:
					t.Fatalf("closed without a ready volatility chain: %+v", req)
				case <-time.After(250 * time.Millisecond):
				}
			}
		})
	}
}

func TestCandidateVolReady(t *testing.T) {
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	if c.VolReady() {
		t.Fatal("unfed volatility reported ready")
	}
	now := time.Now()
	for i := 0; i <= 5; i++ {
		c.vol.AddAt(now.Add(time.Duration(i-5)*100*time.Millisecond), float64(i))
	}
	if !c.VolReady() {
		t.Fatal("warmed volatility not ready")
	}
}
