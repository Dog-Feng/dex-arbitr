package scan

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// lensQuote builds a spread quote whose mid deviation from mu is approximately
// devBps (L leg anchored at ~100, R leg offset). Sizes are ample for any
// reasonable depth gate.
func lensQuote(devBps float64) spread.Quote {
	d := devBps / 100 // price units at ~100 ref
	l := book(100, 100.01, 1, 1)
	r := book(100+d, 100+d+0.01, 1, 1)
	q, ok := spread.Compute(l, r)
	if !ok {
		panic("lensQuote: invalid books")
	}
	return q
}

func baseLens(strategyKind string) Lens {
	return Lens{Name: "lens", Strategy: strategyKind, EstimatorKind: "sma",
		Estimator: estimator.Params{Interval: time.Second},
		OpenBps:   3, VolK: 1, ZOpen: 2, StepZ: 0.5, MaxSteps: 50,
		DepthMult: 3, StepQty: 0.01}
}

func eval(t *testing.T, l Lens, q spread.Quote, mu, sigmaShort, longVol, feeBps float64, muReady, sigmaReady bool) LensResult {
	t.Helper()
	return evaluateLens(l, q, mu, muReady, sigmaShort, sigmaReady, longVol, feeBps, "TEST", "L", "R", time.Now())
}

func TestEvaluateArbitrageLens(t *testing.T) {
	l := baseLens("arbitrage")
	feeBps := 12.0
	q := lensQuote(20)
	// threshold = 3 + 12 + 1*1.5 = 16.5 -> dev 20 is open, net ~3.5.
	res := eval(t, l, q, 0, 1.5, 0, feeBps, true, true)
	if !(res.NetBps > 0) || res.Reason != "" {
		t.Fatalf("open signal misclassified: %+v", res)
	}
	if !res.DepthOK {
		t.Fatalf("ample depth rejected: %+v", res)
	}
	q = lensQuote(10)
	res = eval(t, l, q, 0, 1.5, 0, feeBps, true, true)
	if res.NetBps > 0 || res.Reason != "below threshold" {
		t.Fatalf("below-threshold misclassified: %+v", res)
	}
	// Unready gates short-circuit with reasons, never with a signal.
	res = eval(t, l, q, 0, 1.5, 0, feeBps, false, true)
	if res.Reason != "mean not ready" || res.Steps != 0 {
		t.Fatalf("mean-not-ready misclassified: %+v", res)
	}
	res = eval(t, l, q, 0, 1.5, 0, feeBps, true, false)
	if res.Reason != "sigma not ready" {
		t.Fatalf("sigma-not-ready misclassified: %+v", res)
	}
}

func TestEvaluateZScoreLens(t *testing.T) {
	l := baseLens("zscore")
	l.EstimatorKind = "zscore"
	feeBps := 12.0
	// Expectations derive from the quote itself, mirroring the core formula
	// zLS=(LS-mu)/vol, zSL=(SL+mu)/vol with the richer raw z winning.
	expect := func(devBps float64) (z, dev float64, steps int, reason string) {
		q := lensQuote(devBps)
		zLS, zSL := q.LS/8, q.SL/8
		z, dev = zLS, q.LS
		if zSL > zLS {
			z, dev = zSL, q.SL
		}
		switch {
		case dev < feeBps:
			return z, dev, 0, "dev below fees"
		case z < 2:
			return z, dev, 0, "z below z_open"
		default:
			steps = int(math.Floor((z-2)/0.5)) + 1
			if steps > l.MaxSteps {
				steps = l.MaxSteps
			}
			return z, dev, steps, ""
		}
	}
	for _, devBps := range []float64{20, 15, 8, 500} {
		res := eval(t, l, lensQuote(devBps), 0, 0, 8, feeBps, true, true)
		wantZ, wantDev, wantSteps, wantReason := expect(devBps)
		if math.Abs(res.Z-wantZ) > 1e-6 || math.Abs(res.DevBps-wantDev) > 1e-6 ||
			res.Steps != wantSteps || res.Reason != wantReason {
			t.Fatalf("dev=%v: got z=%.6f dev=%.6f steps=%d reason=%q, want z=%.6f dev=%.6f steps=%d reason=%q",
				devBps, res.Z, res.DevBps, res.Steps, res.Reason, wantZ, wantDev, wantSteps, wantReason)
		}
	}
	// LongVol unavailable blocks the lens instead of inventing a z.
	res := eval(t, l, lensQuote(20), 0, 0, 0, feeBps, true, true)
	if res.Reason != "longvol unavailable" {
		t.Fatalf("missing longvol misclassified: %+v", res)
	}
}

func TestVolumeLensForcesZeroOpenBps(t *testing.T) {
	q := lensQuote(15)
	vol := baseLens("volume")
	vol.OpenBps = 7 // must be ignored by the volume policy
	rv := eval(t, vol, q, 0, 1.5, 0, 12, true, true)
	// The volume lens result must behave exactly like open_bps=0.
	zero := baseLens("arbitrage")
	zero.OpenBps = 0
	rz := eval(t, zero, q, 0, 1.5, 0, 12, true, true)
	if rv.NetBps != rz.NetBps {
		t.Fatalf("volume threshold not forced to zero: %v vs %v", rv.NetBps, rz.NetBps)
	}
}

// TestLensMatchesCoreDecide is the boundary test: across a sweep of quote
// deviations, the lens must agree sign-for-sign with the core strategy's
// Decide on whether an open would fire, for both arbitrage and z-score. This
// pins the scanner to the trading engine's thresholds without importing the
// engine.
func TestLensMatchesCoreDecide(t *testing.T) {
	feeBps := 12.0
	sigmaShort := 1.5
	longVol := 8.0
	state := strategy.State{Mode: strategy.ModeRunning, Now: time.Now()}
	arbParams := strategy.Parameters(strategy.Arbitrage{}, strategy.Params{
		OpenBps: 3, VolK: 1, FeeBps: feeBps, StepQty: 0.01, MaxSteps: 50, DepthMult: 3,
	})
	zParams := strategy.Parameters(strategy.ZScore{}, strategy.Params{
		ZOpen: 2, StepZ: 0.5, FeeBps: feeBps, StepQty: 0.01, MaxSteps: 50, DepthMult: 3,
	})
	for _, devBps := range []float64{-40, -30, -20, -18, -16, -12, -8, 0, 8, 12, 16, 18, 20, 30, 40} {
		q := lensQuote(devBps)
		// arbitrage
		arbAct := strategy.Arbitrage{}.Decide(arbParams, state, q, 0, true, sigmaShort)
		arbLens := eval(t, baseLens("arbitrage"), q, 0, sigmaShort, 0, feeBps, true, true)
		if (arbAct.Kind == strategy.Open) != (arbLens.NetBps > 0) {
			t.Fatalf("arbitrage lens disagrees with Decide at dev=%v: decide=%v lens=%+v", devBps, arbAct.Kind, arbLens)
		}
		// zscore
		zAct := strategy.ZScore{}.Decide(zParams, state, q, 0, true, longVol)
		zLens := eval(t, baseLens("zscore"), q, 0, 0, longVol, feeBps, true, true)
		if (zAct.Kind == strategy.Open) != (zLens.Steps > 0) {
			t.Fatalf("zscore lens disagrees with Decide at dev=%v: decide=%v lens=%+v", devBps, zAct.Kind, zLens)
		}
	}
}

// TestWatchLensesEmitsLines runs the lens loop against synthetic books and
// verifies results stream with timestamps and identifiers.
func TestWatchLensesEmitsLines(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	left, right := make(chan domain.Book, 10), make(chan domain.Book, 10)
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				l, r := book(100, 100.02, 1, 1), book(100, 100.02, 1, 1)
				l.Ts, r.Ts = now, now
				l.Venue, r.Venue = "left", "right"
				l.MarketID, r.MarketID = 1, 1
				select {
				case left <- l:
				case <-ctx.Done():
					return
				}
				select {
				case right <- r:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	lens := Lens{Name: "sma_fast", Strategy: "arbitrage", EstimatorKind: "sma",
		Estimator: estimator.Params{Interval: 100 * time.Millisecond, Window: time.Second, MinWarmup: 200 * time.Millisecond, MinCoverage: 0.8},
		OpenBps:   0, VolK: 1, DepthMult: 3, StepQty: 0.01, MaxSteps: 50}
	var lines []LensResult
	done := make(chan error, 1)
	go func() {
		done <- WatchLenses(ctx, "TEST", watchVenue{name: "left", books: left}, watchVenue{name: "right", books: right},
			domain.Instrument{MarketID: 1}, domain.Instrument{MarketID: 1}, []Lens{lens}, 12,
			100*time.Millisecond, 100*time.Millisecond, 0, time.Second, 300*time.Millisecond,
			func(r LensResult) { lines = append(lines, r) }, nil)
	}()
	if err := <-done; err != nil {
		t.Fatalf("WatchLenses: %v", err)
	}
	if len(lines) == 0 {
		t.Fatal("no lens results emitted")
	}
	if lines[0].Pair != "TEST" || lines[0].Lens != "sma_fast" || lines[0].Left != "left" || lines[0].Right != "right" {
		t.Fatalf("result identifiers wrong: %+v", lines[0])
	}
	if lines[0].Ts.IsZero() {
		t.Fatalf("result timestamp missing: %+v", lines[0])
	}
}

// TestWatchLensesSpreadSnapshots locks the spread time-series stream: at the
// spread cadence, every lens snapshot (readiness and reason included) is
// delivered to onSpread for pair-level spread statistics.
func TestWatchLensesSpreadSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	left, right := make(chan domain.Book, 10), make(chan domain.Book, 10)
	go func() {
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				l, r := book(100, 100.02, 1, 1), book(100, 100.02, 1, 1)
				l.Ts, r.Ts = now, now
				l.Venue, r.Venue = "left", "right"
				l.MarketID, r.MarketID = 1, 1
				select {
				case left <- l:
				case <-ctx.Done():
					return
				}
				select {
				case right <- r:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	lens := Lens{Name: "sma_fast", Strategy: "arbitrage", EstimatorKind: "sma",
		Estimator: estimator.Params{Interval: 100 * time.Millisecond, Window: time.Second, MinWarmup: 200 * time.Millisecond, MinCoverage: 0.8},
		OpenBps:   0, VolK: 1, DepthMult: 3, StepQty: 0.01, MaxSteps: 50}
	var spreadLines []LensResult
	done := make(chan error, 1)
	go func() {
		done <- WatchLenses(ctx, "TEST", watchVenue{name: "left", books: left}, watchVenue{name: "right", books: right},
			domain.Instrument{MarketID: 1}, domain.Instrument{MarketID: 1}, []Lens{lens}, 12,
			100*time.Millisecond, 100*time.Millisecond, 150*time.Millisecond, time.Second, 300*time.Millisecond,
			func(LensResult) {}, func(r LensResult) { spreadLines = append(spreadLines, r) })
	}()
	if err := <-done; err != nil {
		t.Fatalf("WatchLenses: %v", err)
	}
	if len(spreadLines) == 0 {
		t.Fatal("no spread snapshots emitted")
	}
	for _, r := range spreadLines {
		if r.Ts.IsZero() || r.Pair != "TEST" || r.Lens != "sma_fast" {
			t.Fatalf("spread snapshot malformed: %+v", r)
		}
	}
}
