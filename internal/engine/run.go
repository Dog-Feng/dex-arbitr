package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/Dog-Feng/dex-arbitr/internal/adaptive"
	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// runtime holds the shared services an engine run wires together. Manual and
// auto pair selection share it; they differ only in which pairs are active and
// where the per-pair stats come from.
type runtime struct {
	cfg     *config.Config
	venues  map[string]exchange.Venue
	store   *store.Store
	tracker *execution.Tracker
	mon     *monitor.Monitor
	matcher symbols.Matcher
	ids     *execution.IDGen
	// Set once during startup planning, before any actors are started. An
	// old checkpoint has no request quantity to budget until it is recovered.
	recovering []*PairActor
}

func (rt *runtime) startupRecoveryReady() bool {
	for _, a := range rt.recovering {
		if !a.startupRecovered.Load() {
			return false
		}
	}
	return true
}

// pairKey is the stable identifier used for persistence and monitor grouping.
func pairKey(base, l, r string, dryRun bool) string {
	key := fmt.Sprintf("%s_%s_%s", base, l, r)
	if dryRun {
		key += "_paper"
	}
	return key
}

// newStats builds one estimator plus three volatility estimators (spread and
// per-leg). Manual mode owns a fresh set per actor; auto mode shares them.
func (rt *runtime) newStats() (estimator.Estimator, *volatility.Estimator, *volatility.Estimator, *volatility.Estimator, error) {
	c := rt.cfg
	est, err := estimator.New(c.Estimator.Kind, estimator.Params{
		Window: c.Estimator.Window, MinWarmup: c.Estimator.MinWarmup,
		MinCoverage: c.Estimator.MinCoverage, Interval: c.Estimator.Interval,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	vol := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
	volL := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
	volR := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
	return est, vol, volL, volR, nil
}

// buildActor constructs a PairActor around the supplied stats and quantity.
func (rt *runtime) buildActor(pair symbols.Pair, qty float64, maxSteps int, l, r exchange.Venue,
	est estimator.Estimator, vol, volL, volR *volatility.Estimator, samples *store.SampleSink) (*PairActor, error) {
	c := rt.cfg
	strat, err := strategy.New(c.Strategy.Kind)
	if err != nil {
		return nil, err
	}
	feeBps := 2 * (pair.Left.TakerFee + pair.Right.TakerFee) * 1e4
	minInv := 0
	volTarget, volBudget := 0.0, 0.0
	if c.Strategy.Kind == "volume" {
		minInv = c.Volume.MinInventory
		volTarget, volBudget = c.Volume.DailyVolumeTarget, c.Volume.DailyCostBudget
	}
	return NewPairActor(PairConfig{
		Pair: pair, L: l, R: r, Strategy: strat, Est: est, DryRun: c.DryRun, Store: rt.store, Tracker: rt.tracker, IDs: rt.ids,
		ExecCfg: ExecutionConfig(c), SampleEvery: c.Estimator.Interval,
		IntervalMin: c.Strategy.IntervalMin, IntervalMax: c.Strategy.IntervalMax, MaxBookAge: c.Strategy.MaxBookAge,
		Vol: vol, VolL: volL, VolR: volR, VolEvery: c.Volatility.Interval, VolMaxAge: c.Volatility.MaxAge, Samples: samples,
		Risk: marginConfig{floor: c.Risk.MarginFloorRatio, trigger: c.Risk.MarginTriggerRatio,
			hysteresis: c.Risk.MarginHysteresis, refresh: c.Risk.PositionRefresh, marketBps: c.Risk.EmergencyMarketBps,
			maxNotional: c.Risk.MaxNotional},
		VolTarget: volTarget, VolCostBudget: volBudget,
		Adaptive: adaptive.Config{Init: c.Execution.TickK, Window: c.Adaptive.Window, High: c.Adaptive.FailHigh,
			Low: c.Adaptive.FailLow, Up: c.Adaptive.UpFactor, Down: c.Adaptive.DownFactor,
			Min: c.Adaptive.MinK, Max: c.Adaptive.MaxK},
		Monitor: rt.mon,
		Params: strategy.Parameters(strat, strategy.Params{StepQty: qty, MaxSteps: maxSteps, OpenBps: c.Strategy.OpenBps, CloseBps: c.Strategy.CloseBps,
			DepthMult: c.Strategy.DepthMult, CloseOrder: c.Strategy.CloseOrder, VolK: c.Strategy.VolK, FeeBps: feeBps,
			MinInventory: minInv, TakerFeeL: pair.Left.TakerFee, TakerFeeR: pair.Right.TakerFee}),
	})
}

// runManual trades exactly the configured pairs, each warming its own stats.
func (rt *runtime) runManual(ctx context.Context) (runErr error) {
	var actors []*PairActor
	markets := map[string]string{}
	started := false
	defer func() {
		if !started {
			for _, a := range actors {
				if a.cfg.Samples != nil {
					if err := a.cfg.Samples.Close(); err != nil {
						runErr = errors.Join(runErr, fmt.Errorf("close unused samples for %s: %w", a.key, err))
					}
				}
			}
		}
	}()
	// Resolve and build every actor before starting any of them. A later setup
	// failure must not leave earlier actors trading after this function returns.
	for _, pc := range rt.cfg.Pairs {
		l, r := rt.venues[pc.Left], rt.venues[pc.Right]
		if l == nil || r == nil {
			return fmt.Errorf("pair %s references unavailable venue", pc.Base)
		}
		pair, err := ResolvePair(ctx, rt.matcher, pc.Base, l, r)
		if err != nil {
			return err
		}
		for _, key := range []string{marketKey(l.Name(), pair.Left.MarketID), marketKey(r.Name(), pair.Right.MarketID)} {
			if previous, ok := markets[key]; ok {
				return fmt.Errorf("pairs %s and %s share market %s", previous, pair.Base, key)
			}
			markets[key] = pair.Base
		}
		qty := pair.AlignQty(pc.StepQty)
		if ok, why := pair.ValidQty(qty, 0); !ok || qty != domain.Round(pc.StepQty, pair.SizeDecimals) {
			return fmt.Errorf("pair %s: step_qty %v invalid (%s; aligned decimals %d, min %v)", pc.Base, pc.StepQty, why, pair.SizeDecimals, pair.MinQty)
		}
		if !rt.cfg.DryRun {
			rt.applyLeverage(ctx, l, pair.Left)
			rt.applyLeverage(ctx, r, pair.Right)
		}
		est, vol, volL, volR, err := rt.newStats()
		if err != nil {
			return err
		}
		var samples *store.SampleSink
		if rt.cfg.SamplesDir != "" {
			samples, err = store.NewSampleSink(filepath.Join(rt.cfg.SamplesDir, fmt.Sprintf("%s_%s_%s.jsonl", pair.Base, l.Name(), r.Name())))
			if err != nil {
				return err
			}
		}
		a, err := rt.buildActor(pair, qty, pc.MaxSteps, l, r, est, vol, volL, volR, samples)
		if err != nil {
			if samples != nil {
				if closeErr := samples.Close(); closeErr != nil {
					err = errors.Join(err, closeErr)
				}
			}
			return err
		}
		actors = append(actors, a)
		slog.Info("pair resolved", "base", pair.Base, "left", fmt.Sprintf("%s#%d", l.Name(), pair.Left.MarketID),
			"right", fmt.Sprintf("%s#%d", r.Name(), pair.Right.MarketID), "size_decimals", pair.SizeDecimals, "min_qty", pair.MinQty)
	}
	if len(actors) == 0 {
		return fmt.Errorf("manual pair selection: no pairs configured")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(actors))
	var wg sync.WaitGroup
	started = true
	for _, a := range actors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Run(runCtx); err != nil {
				results <- fmt.Errorf("pair %s stopped: %w", a.key, err)
				return
			}
			results <- nil
		}()
	}
	for range actors {
		if err := <-results; err != nil {
			runErr = errors.Join(runErr, err)
			cancel()
		}
	}
	wg.Wait()
	return runErr
}
