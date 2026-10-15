package engine

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func workflowRuntime(t *testing.T, kind, mode string) *runtime {
	t.Helper()
	rt := lifecycleRuntime(t)
	path := filepath.Join(t.TempDir(), "config.yaml")
	yaml := fmt.Sprintf("dry_run: true\nvenues:\n  - {name: L, kind: lighter}\n  - {name: R, kind: rhlighter}\nstrategy:\n  kind: %s\n  pair_selection: %s\n  open_bps: 20\n  close_bps: 20\n  vol_k: 0\npairs:\n  - {base: TEST, left: L, right: R, step_qty: 0.02, max_steps: 4}\n", kind, mode)
	if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rt.cfg = c
	return rt
}

func TestStrategySelectionMatrixWarmsTradesAndRetainsTwoHourWindow(t *testing.T) {
	for _, kind := range []string{"arbitrage", "volume"} {
		for _, mode := range []string{"manual", "auto"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				rt := workflowRuntime(t, kind, mode)
				est, vol, vl, vr, err := rt.newStats()
				if err != nil {
					t.Fatal(err)
				}
				inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
				pair := symbols.NewPair("TEST", inst, inst)
				l, r := newActorTestVenue("L"), newActorTestVenue("R")
				bl, br := actorTestBook("L", 99.99), actorTestBook("R", 100.3)
				bl.Asks[0].Price = 100
				br.Asks[0].Price = 100.31
				q, ok := spread.Compute(bl, br)
				if !ok {
					t.Fatal("invalid quote")
				}
				qty, maxSteps := rt.cfg.Pairs[0].StepQty, rt.cfg.Pairs[0].MaxSteps
				c := &candidate{pair: pair, l: l, r: r, est: est, vol: vol, volL: vl, volR: vr, bookL: bl, bookR: br, notional: 10, depthRatio: .25}
				if mode == "auto" {
					var ok bool
					qty, ok = c.SizedQty()
					if !ok {
						t.Fatal("scanned pair cannot be sized")
					}
					maxSteps = rt.cfg.Auto.MaxSteps
				}
				a, err := rt.buildActor(pair, qty, maxSteps, l, r, est, vol, vl, vr, nil)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "auto" {
					a.cfg.View = c
				}
				if (mode == "auto") != (a.cfg.View != nil) {
					t.Fatal("wrong statistic owner")
				}
				if a.cfg.Params.StepQty != qty || a.cfg.Params.MaxSteps != maxSteps {
					t.Fatal("pair selection lost its sizing")
				}
				wantEdge := 20.0
				if kind == "volume" {
					wantEdge = 0
				}
				if a.cfg.Params.OpenBps != wantEdge || a.cfg.Params.CloseBps != wantEdge {
					t.Fatalf("effective params=%+v", a.cfg.Params)
				}
				now := time.Now().Truncate(time.Second)
				start := now.Add(-3600 * time.Second)
				// 3,599 one-second buckets cannot meet the one-hour warmup.
				for i := 0; i < 3599; i++ {
					est.Update(start.Add(time.Duration(i)*time.Second), 0)
				}
				if estimator.StatsAt(est, start.Add(3598*time.Second)).Ready {
					t.Fatal("mean warmed before an actual hour elapsed")
				}
				mu, ready := a.mean()
				if ready {
					t.Fatal("opened mean before one-hour warmup")
				}
				if act := a.cfg.Strategy.Decide(a.cfg.Params, strategy.State{Mode: strategy.ModeRunning}, q, mu, ready, 0); act.Kind != strategy.None {
					t.Fatalf("traded unready mean: %+v", act)
				}
				est.Update(now.Add(-time.Second), 0)
				est.Update(now, 0)
				mu, ready = a.mean()
				if !ready {
					t.Fatalf("one-hour sampled mean not ready: %+v", a.statsSnapshot())
				}
				a.state.Mode = strategy.ModeRunning
				weakR := actorTestBook("R", 100.05)
				weakR.Asks[0].Price = 100.06
				weak, _ := spread.Compute(bl, weakR)
				weakAct := a.cfg.Strategy.Decide(a.cfg.Params, a.state, weak, mu, ready, 0)
				if (weakAct.Kind == strategy.Open) != (kind == "volume") {
					t.Fatalf("strategy types share the wrong threshold: %+v", weakAct)
				}
				act := a.cfg.Strategy.Decide(a.cfg.Params, a.state, q, mu, ready, 0)
				if act.Kind != strategy.Open {
					t.Fatalf("ready profitable quote did not open: %+v", act)
				}
				a.apply(context.Background(), act, q)
				if len(a.state.Steps) != 1 || a.pending != nil {
					t.Fatal("paper open did not settle")
				}
				closeL := actorTestBook("L", 101)
				closeL.Asks[0].Price = 101.01
				closeQ, _ := spread.Compute(closeL, br)
				act = a.cfg.Strategy.Decide(a.cfg.Params, a.state, closeQ, mu, ready, 0)
				if act.Kind != strategy.Close {
					t.Fatalf("profitable exit did not close: %+v", act)
				}
				a.apply(context.Background(), act, closeQ)
				if !a.Flat() || a.volAcc.pnlUSD <= 0 {
					t.Fatal("paper round trip did not settle flat with positive PnL")
				}
				// Advance virtual time, without sleeping: exactly the latest 7,200
				// buckets survive after three hours of one-second samples.
				for i := 3601; i <= 10800; i++ {
					est.Update(start.Add(time.Duration(i)*time.Second), float64(i))
				}
				st := estimator.StatsAt(est, start.Add(10800*time.Second))
				if st.Samples != 7200 || st.Span != 2*time.Hour || !st.Ready || math.Abs(st.Mean-7200.5) > 1e-9 {
					t.Fatalf("two-hour rolling window is wrong: %+v", st)
				}
			})
		}
	}
}

type workflowVenue struct {
	*actorTestVenue
	markets []domain.Instrument
	subs    chan int
}

func (v *workflowVenue) Instruments(context.Context) ([]domain.Instrument, error) {
	return v.markets, nil
}
func (v *workflowVenue) SubscribeBook(_ context.Context, inst domain.Instrument) (<-chan domain.Book, error) {
	v.subs <- inst.MarketID
	return make(chan domain.Book), nil
}

func TestManualUsesConfiguredPairsAndAutoConsumesScannedUniverse(t *testing.T) {
	for _, kind := range []string{"arbitrage", "volume"} {
		for _, mode := range []string{"manual", "auto"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				rt := workflowRuntime(t, kind, mode)
				markets := []domain.Instrument{{MarketID: 1, Symbol: "TEST", SizeDecimals: 4, PriceDecimals: 2}, {MarketID: 2, Symbol: "SCANNED", SizeDecimals: 4, PriceDecimals: 2}}
				l := &workflowVenue{actorTestVenue: newActorTestVenue("L"), markets: markets, subs: make(chan int, 4)}
				r := &workflowVenue{actorTestVenue: newActorTestVenue("R"), markets: markets, subs: make(chan int, 4)}
				rt.venues = map[string]exchange.Venue{"L": l, "R": r}
				want := 1
				if mode == "auto" {
					// This manual-only base is absent from both venues. Auto must
					// still subscribe to all candidates produced by scan.MatchAll.
					rt.cfg.Pairs[0].Base = "MANUAL_ONLY"
					matches, err := scan.MatchAll(context.Background(), rt.matcher, rt.venues)
					if err != nil || len(matches) != 2 {
						t.Fatalf("scan universe=%v err=%v", matches, err)
					}
					want = len(matches)
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() {
					if mode == "manual" {
						done <- rt.runManual(ctx)
					} else {
						done <- rt.runAuto(ctx)
					}
				}()
				t.Cleanup(func() {
					cancel()
					select {
					case err := <-done:
						if err != nil {
							t.Errorf("driver shutdown: %v", err)
						}
					case <-time.After(time.Second):
						t.Error("driver leaked after cancellation")
					}
				})
				for _, v := range []*workflowVenue{l, r} {
					seen := map[int]bool{}
					for i := 0; i < want; i++ {
						select {
						case id := <-v.subs:
							seen[id] = true
						case err := <-done:
							t.Fatalf("driver returned early: %v", err)
						case <-time.After(time.Second):
							t.Fatal("missing market subscription")
						}
					}
					if !seen[1] || len(seen) != want || (mode == "manual" && seen[2]) {
						t.Fatalf("%s subscribed to %v", mode, seen)
					}
				}
			})
		}
	}
}
