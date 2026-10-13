package engine

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
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

// Matcher from config.
func Matcher(c *config.Config) symbols.Matcher {
	m := symbols.DefaultMatcher()
	if len(c.Matching.StripSuffixes) > 0 {
		m.Suffixes = c.Matching.StripSuffixes
	}
	if c.Matching.Separators != "" {
		m.Separators = c.Matching.Separators
	}
	return m
}

// ExecutionConfig combines executor defaults with configured normal-order pricing.
func ExecutionConfig(c *config.Config) execution.Config {
	ec := execution.DefaultConfig()
	ec.PriceTicks = c.Execution.PriceTicks
	ec.DynamicTicks = c.Execution.DynamicTicks
	ec.TickK = c.Execution.TickK
	ec.TickMin = c.Execution.TickMin
	ec.TickMax = c.Execution.TickMax
	ec.EmergencyMarketBps = c.Risk.EmergencyMarketBps
	return ec
}

// BuildVenues instantiates all configured venues.
func BuildVenues(c *config.Config) (map[string]exchange.Venue, error) {
	out := map[string]exchange.Venue{}
	for _, vc := range c.Venues {
		v, err := exchange.New(vc.Kind, vc.Name, vc.Opts)
		if err != nil {
			return nil, err
		}
		out[vc.Name] = v
	}
	return out, nil
}

// ResolvePair fetches instruments and matches the configured base.
func ResolvePair(ctx context.Context, m symbols.Matcher, base string, l, r exchange.Venue) (symbols.Pair, error) {
	li, err := l.Instruments(ctx)
	if err != nil {
		return symbols.Pair{}, fmt.Errorf("%s instruments: %w", l.Name(), err)
	}
	ri, err := r.Instruments(ctx)
	if err != nil {
		return symbols.Pair{}, fmt.Errorf("%s instruments: %w", r.Name(), err)
	}
	for _, p := range symbols.Match(m, li, ri) {
		if p.Base == m.Normalize(base) {
			return p, nil
		}
	}
	return symbols.Pair{}, fmt.Errorf("%s not listed as perp on both %s and %s", base, l.Name(), r.Name())
}

// setLeverage applies the configured leverage on venues that support it.
func setLeverage(ctx context.Context, v exchange.Venue, inst domain.Instrument, leverage int) error {
	ls, ok := v.(exchange.LeverageSetter)
	if !ok {
		return nil
	}
	if err := ls.SetLeverage(ctx, inst, leverage); err != nil {
		return fmt.Errorf("%s set leverage %dx on market %d: %w", v.Name(), leverage, inst.MarketID, err)
	}
	slog.Info("leverage set", "venue", v.Name(), "market", inst.MarketID, "leverage", leverage)
	return nil
}

// Run starts every configured pair and blocks until ctx is done.
func Run(ctx context.Context, c *config.Config) error {
	mon := monitor.New(func(msg string) { slog.Error("ALERT", "msg", msg) })
	if c.Monitor.Addr != "" {
		addr := c.Monitor.Addr
		if !strings.Contains(addr, ":") {
			addr = ":" + addr // bare port, e.g. "8080"
		}
		srv := &http.Server{Addr: addr, Handler: mon.Handler(c.Monitor.Token)}
		go func() {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("monitor server", "err", err)
			}
		}()
		defer func() { _ = srv.Shutdown(context.Background()) }()
	}
	venues, err := BuildVenues(c)
	if err != nil {
		return err
	}
	defer func() {
		for _, v := range venues {
			_ = v.Close()
		}
	}()
	st, err := store.New(c.StateDir)
	if err != nil {
		return err
	}
	var tracker *execution.Tracker
	if !c.DryRun {
		var trading []exchange.Venue
		for _, v := range venues {
			if !v.CanTrade() {
				return fmt.Errorf("venue %s has no credentials; set dry_run: true or provide keys", v.Name())
			}
			trading = append(trading, v)
		}
		if tracker, err = execution.NewTracker(ctx, trading...); err != nil {
			return err
		}
	}
	ids := execution.NewIDGen()
	m := Matcher(c)
	var wg sync.WaitGroup
	for _, pc := range c.Pairs {
		l, r := venues[pc.Left], venues[pc.Right]
		pair, err := ResolvePair(ctx, m, pc.Base, l, r)
		if err != nil {
			return err
		}
		qty := pair.AlignQty(c.StepQty)
		if ok, why := pair.ValidQty(qty, 0); !ok || qty != domain.Round(c.StepQty, pair.SizeDecimals) {
			return fmt.Errorf("pair %s: step_qty %v invalid (%s; aligned decimals %d, min %v)", pc.Base, c.StepQty, why, pair.SizeDecimals, pair.MinQty)
		}
		if !c.DryRun {
			if err := setLeverage(ctx, l, pair.Left, c.Leverage); err != nil {
				return err
			}
			if err := setLeverage(ctx, r, pair.Right, c.Leverage); err != nil {
				return err
			}
		}
		est, err := estimator.New(c.Estimator.Kind, estimator.Params{Window: c.Estimator.Window, MinWarmup: c.Estimator.MinWarmup,
			MinCoverage: c.Estimator.MinCoverage, Interval: c.Estimator.Interval})
		if err != nil {
			return err
		}
		vol := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
		volL := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
		volR := volatility.New(volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon, HalfLife: c.Volatility.HalfLife})
		strat, err := strategy.New(c.Strategy.Kind)
		if err != nil {
			return err
		}
		feeBps := 2 * (pair.Left.TakerFee + pair.Right.TakerFee) * 1e4
		openBps, closeBps, minInv := c.Strategy.OpenBps, c.Strategy.CloseBps, 0
		volTarget, volBudget := 0.0, 0.0
		if c.Strategy.Kind == "volume" {
			openBps, closeBps = -c.Volume.LossToleranceBps, 0
			minInv = c.Volume.MinInventory
			volTarget, volBudget = c.Volume.DailyVolumeTarget, c.Volume.DailyCostBudget
		}
		var samples *store.SampleSink
		if c.SamplesDir != "" {
			samples, err = store.NewSampleSink(filepath.Join(c.SamplesDir, fmt.Sprintf("%s_%s_%s.jsonl", pair.Base, l.Name(), r.Name())))
			if err != nil {
				return err
			}
		}
		a, err := NewPairActor(PairConfig{
			Pair: pair, L: l, R: r, Strategy: strat, Est: est, DryRun: c.DryRun, Store: st, Tracker: tracker, IDs: ids,
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
			Monitor: mon,
			Params: strategy.Params{StepQty: qty, MaxSteps: c.MaxSteps, OpenBps: openBps, CloseBps: closeBps,
				DepthMult: c.Strategy.DepthMult, CloseOrder: c.Strategy.CloseOrder, VolK: c.Strategy.VolK, FeeBps: feeBps,
				MinInventory: minInv},
		})
		if err != nil {
			return err
		}
		slog.Info("pair resolved", "base", pair.Base, "left", fmt.Sprintf("%s#%d", l.Name(), pair.Left.MarketID),
			"right", fmt.Sprintf("%s#%d", r.Name(), pair.Right.MarketID), "size_decimals", pair.SizeDecimals, "min_qty", pair.MinQty)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.Run(ctx); err != nil {
				slog.Error("pair actor stopped", "pair", pair.Base, "err", err)
			}
		}()
	}
	wg.Wait()
	return nil
}
