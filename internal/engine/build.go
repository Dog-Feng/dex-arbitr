package engine

import (
	"context"
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
	if c.Strategy.PairSelection == "auto" {
		return rt.runAuto(ctx)
	}
	return rt.runManual(ctx)
}
