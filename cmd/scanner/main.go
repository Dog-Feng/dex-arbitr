// Command scanner renders a live, fixed-size ranking of arbitrage opportunities
// across every matched pair, sorted by net opportunity. It is read-only and
// needs no trading credentials.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

func main() {
	path := flag.String("config", "configs/config.yaml", "config file")
	refresh := flag.Duration("refresh", 2*time.Second, "panel refresh interval")
	flag.Parse()
	if err := validateFlags(*path, *refresh, flag.Args()); err != nil {
		slog.Error("flags", "err", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	c, err := config.Load(*path)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, c, *refresh, os.Stdout, isTTY()); err != nil {
		slog.Error("scanner", "err", err)
		os.Exit(1)
	}
}

func validateFlags(path string, refresh time.Duration, args []string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("config path must not be empty")
	}
	if refresh <= 0 {
		return fmt.Errorf("refresh must be positive")
	}
	if len(args) != 0 {
		return fmt.Errorf("unexpected arguments: %v", args)
	}
	return nil
}

func run(ctx context.Context, c *config.Config, refresh time.Duration, output io.Writer, tty bool) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	venues := map[string]exchange.Venue{}
	defer func() {
		for name, v := range venues {
			if closeErr := v.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close %s: %w", name, closeErr))
			}
		}
	}()
	for _, vc := range c.Venues {
		v, err := exchange.New(vc.Kind, vc.Name, readOnlyOptions(vc.Opts))
		if err != nil {
			return fmt.Errorf("venue %s: %w", vc.Name, err)
		}
		venues[vc.Name] = v
	}
	m := symbols.DefaultMatcher()
	if len(c.Matching.StripSuffixes) > 0 {
		m.Suffixes = c.Matching.StripSuffixes
	}
	if c.Matching.Separators != "" {
		m.Separators = c.Matching.Separators
	}
	pairs, err := matchAll(ctx, m, venues)
	if err != nil {
		return err
	}
	if len(pairs) == 0 {
		return fmt.Errorf("no matched pairs")
	}
	board := newBoard(pairs)
	errs := make(chan error, len(pairs)+1)
	renderDone := make(chan struct{})

	go func() {
		defer close(renderDone)
		t := time.NewTicker(refresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := render(output, board, tty, time.Now()); err != nil {
					errs <- fmt.Errorf("render: %w", err)
					cancel()
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for _, p := range pairs {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := scan.Watch(ctx, p.base, p.l, p.r, p.li, p.ri, scan.Config{
				StrategyKind: c.Strategy.Kind, OpenBps: c.Strategy.OpenBps, VolK: c.Strategy.VolK, FeeBps: p.fee,
				TakerFeeL: p.li.TakerFee, TakerFeeR: p.ri.TakerFee,
				MaxBookAge: c.Strategy.MaxBookAge, MaxVolBookAge: c.Volatility.MaxAge,
				EstimatorKind: c.Estimator.Kind,
				Estimator: estimator.Params{Window: c.Estimator.Window, MinWarmup: c.Estimator.MinWarmup,
					MinCoverage: c.Estimator.MinCoverage, Interval: c.Estimator.Interval},
				Volatility: volatility.Params{Interval: c.Volatility.Interval, Horizon: c.Volatility.Horizon,
					HalfLife: c.Volatility.HalfLife}}, func(s scan.Score) { board.set(s) }); err != nil {
				slog.Warn("scan stopped", "pair", p.base, "err", err)
				errs <- fmt.Errorf("scan %s: %w", p.base, err)
			}
		}()
	}
	wg.Wait()
	cancel()
	<-renderDone
	close(errs)
	for scanErr := range errs {
		err = errors.Join(err, scanErr)
	}
	return err
}

type board struct {
	mu sync.Mutex
	m  map[string]scan.Score
}

func newBoard(pairs []pairSpec) *board {
	b := &board{m: map[string]scan.Score{}}
	for _, p := range pairs {
		s := scan.Score{Base: p.base}
		if p.l != nil && p.r != nil {
			s.Left, s.Right = p.l.Name(), p.r.Name()
		}
		b.m[scoreKey(s)] = s
	}
	return b
}

func (b *board) set(s scan.Score) {
	b.mu.Lock()
	b.m[scoreKey(s)] = s
	b.mu.Unlock()
}

func scoreKey(s scan.Score) string { return s.Base + "\x00" + s.Left + "\x00" + s.Right }

func (b *board) sorted(now time.Time) []scan.Score {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]scan.Score, 0, len(b.m))
	for _, s := range b.m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		validI, validJ := scoreStatus(out[i], now) == "", scoreStatus(out[j], now) == ""
		if validI != validJ {
			return validI
		}
		if validI && out[i].NetBps != out[j].NetBps {
			return out[i].NetBps > out[j].NetBps
		}
		return scoreKey(out[i]) < scoreKey(out[j])
	})
	return out
}

// Watch evaluates once per second. A missed evaluation expires its display
// even when the last callback reported a profitable opportunity.
const scoreMaxAge = 2 * time.Second

func scoreStatus(s scan.Score, now time.Time) string {
	if s.Ts.IsZero() {
		return "warming"
	}
	if s.Ts.After(now) || now.Sub(s.Ts) > scoreMaxAge {
		return "stale"
	}
	if math.IsNaN(s.NetBps) || math.IsInf(s.NetBps, 0) {
		return "invalid"
	}
	if !s.MuReady || (!s.VolReady && !s.VolOptional) {
		return "unavailable"
	}
	return ""
}

func render(output io.Writer, b *board, tty bool, now time.Time) error {
	rows := b.sorted(now)
	if tty {
		if _, err := fmt.Fprint(output, "\033[2J\033[H"); err != nil { // clear screen, home cursor
			return err
		}
	}
	if _, err := fmt.Fprintf(output, "== pair scan  %d pairs  %s ==\n", len(rows), now.Format("15:04:05")); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(output, "%-4s %-12s %-24s %-18s %s\n", "#", "PAIR", "VENUES", "DIR", "NET_BPS"); err != nil {
		return err
	}
	for i, s := range rows {
		dir, net := "—", scoreStatus(s, now)
		if net == "" {
			if s.NetBps > 0 {
				dir = s.Dir.String()
			}
			net = fmt.Sprintf("%+.2f", s.NetBps)
		}
		if _, err := fmt.Fprintf(output, "%-4d %-12s %-24s %-18s %s\n", i+1, s.Base, s.Left+"/"+s.Right, dir, net); err != nil {
			return err
		}
	}
	return nil
}

func isTTY() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

type pairSpec struct {
	base   string
	l, r   exchange.Venue
	li, ri domain.Instrument
	fee    float64
}

// Preserve the configured market-data source while keeping the scanner read-only.
func readOnlyOptions(opts map[string]string) map[string]string {
	copy := make(map[string]string, len(opts))
	for key, value := range opts {
		switch key {
		case "private_key", "account_index", "api_key_index":
			continue
		}
		copy[key] = value
	}
	return copy
}

func matchAll(ctx context.Context, m symbols.Matcher, venues map[string]exchange.Venue) ([]pairSpec, error) {
	matches, err := scan.MatchAll(ctx, m, venues)
	if err != nil {
		return nil, err
	}
	out := make([]pairSpec, 0, len(matches))
	for _, match := range matches {
		p := match.Pair
		out = append(out, pairSpec{base: p.Base, l: match.L, r: match.R,
			li: p.Left, ri: p.Right, fee: 2 * (p.Left.TakerFee + p.Right.TakerFee) * 1e4})
	}
	return out, nil
}
