package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
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

// ExecutionConfig combines executor defaults with configured normal-order pricing.
func ExecutionConfig(c *config.Config) execution.Config {
	ec := execution.DefaultConfig()
	ec.PriceTicks = c.Execution.PriceTicks
	ec.DynamicTicks = c.Execution.DynamicTicks
	ec.TickK = c.Execution.TickK
	ec.TickMin = c.Execution.TickMin
	ec.TickMax = c.Execution.TickMax
	ec.DepthMinMult = c.Execution.DepthMinMult
	ec.DepthMaxScale = c.Execution.DepthMaxScale
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

// resolveLeverage applies min(configured, market maximum) leverage. The common
// path is a single call; when the market rejects the requested leverage as too
// high it binary-searches downward for the maximum it accepts.
func resolveLeverage(ctx context.Context, ls exchange.LeverageSetter, inst domain.Instrument, configured int) (int, error) {
	if configured < 1 {
		return 0, fmt.Errorf("leverage %d out of range", configured)
	}
	if err := ls.SetLeverage(ctx, inst, configured); err == nil {
		return configured, nil
	} else if !errors.Is(err, domain.ErrLeverageRejected) {
		return 0, err
	}
	best := 0
	lo, hi := 1, configured-1
	for lo <= hi {
		mid := (lo + hi) / 2
		if err := ls.SetLeverage(ctx, inst, mid); err == nil {
			best, lo = mid, mid+1
		} else if errors.Is(err, domain.ErrLeverageRejected) {
			hi = mid - 1
		} else {
			return 0, err
		}
	}
	if best == 0 {
		return 0, fmt.Errorf("market %d accepts no leverage in [1,%d]", inst.MarketID, configured)
	}
	return best, nil
}

// applyLeverage resolves and applies leverage best-effort: a failure never
// blocks a pair, which then trades at the venue's current leverage.
func (rt *runtime) applyLeverage(ctx context.Context, v exchange.Venue, inst domain.Instrument) {
	ls, ok := v.(exchange.LeverageSetter)
	if !ok {
		return
	}
	lev, err := resolveLeverage(ctx, ls, inst, rt.cfg.Leverage)
	if err != nil {
		slog.Warn("leverage resolve failed; using venue default", "venue", v.Name(), "market", inst.MarketID, "err", err)
		return
	}
	slog.Info("leverage set", "venue", v.Name(), "market", inst.MarketID, "leverage", lev)
}

// Run performs shared setup, then runs the configured pair-selection mode.
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
	rt := &runtime{
		cfg:     c,
		venues:  venues,
		store:   st,
		tracker: tracker,
		mon:     mon,
		matcher: Matcher(c),
		ids:     execution.NewIDGen(),
	}
	slog.Info("engine starting", "pair_selection", c.Strategy.PairSelection, "strategy", c.Strategy.Kind, "dry_run", c.DryRun)
	if c.Strategy.PairSelection == "auto" {
		return rt.runAuto(ctx)
	}
	return rt.runManual(ctx)
}
