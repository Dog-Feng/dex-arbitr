package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
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

// Run starts every configured pair and blocks until ctx is done.
func Run(ctx context.Context, c *config.Config) error {
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
		qty := pair.AlignQty(pc.StepQty)
		if ok, why := pair.ValidQty(qty, 0); !ok || qty != domain.Round(pc.StepQty, pair.SizeDecimals) {
			return fmt.Errorf("pair %s: step_qty %v invalid (%s; aligned decimals %d, min %v)", pc.Base, pc.StepQty, why, pair.SizeDecimals, pair.MinQty)
		}
		est, err := estimator.New(c.Estimator.Kind, estimator.Params{Window: c.Estimator.Window, MinWarmup: c.Estimator.MinWarmup,
			MinCoverage: c.Estimator.MinCoverage, Interval: c.Estimator.Interval})
		if err != nil {
			return err
		}
		a, err := NewPairActor(PairConfig{
			Pair: pair, L: l, R: r, Est: est, DryRun: c.DryRun, Store: st, Tracker: tracker, IDs: ids,
			ExecCfg: execution.DefaultConfig(), SampleEvery: c.Estimator.Interval,
			IntervalMin: c.Strategy.IntervalMin, IntervalMax: c.Strategy.IntervalMax, MaxBookAge: c.Strategy.MaxBookAge,
			Params: strategy.Params{StepQty: qty, MaxSteps: pc.MaxSteps, OpenBps: c.Strategy.OpenBps, CloseBps: c.Strategy.CloseBps,
				DepthMult: c.Strategy.DepthMult, CloseOrder: c.Strategy.CloseOrder},
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
