// Command scanner renders a live, fixed-size ranking of arbitrage opportunities
// across every matched pair, sorted by net opportunity. It is read-only and
// needs no trading credentials.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func main() {
	path := flag.String("config", "configs/config.yaml", "config file")
	refresh := flag.Duration("refresh", 2*time.Second, "panel refresh interval")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	c, err := config.Load(*path)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	venues := map[string]exchange.Venue{}
	for _, vc := range c.Venues {
		v, err := exchange.New(vc.Kind, vc.Name, map[string]string{}) // read-only: no credentials
		if err != nil {
			slog.Error("venue", "name", vc.Name, "err", err)
			os.Exit(1)
		}
		venues[vc.Name] = v
	}
	defer func() {
		for _, v := range venues {
			_ = v.Close()
		}
	}()

	m := symbols.DefaultMatcher()
	if len(c.Matching.StripSuffixes) > 0 {
		m.Suffixes = c.Matching.StripSuffixes
	}
	if c.Matching.Separators != "" {
		m.Separators = c.Matching.Separators
	}
	pairs := matchAll(ctx, m, venues)
	board := newBoard(pairs)
	tty := isTTY()

	go func() {
		t := time.NewTicker(*refresh)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				render(board, tty)
			}
		}
	}()

	var wg sync.WaitGroup
	for _, p := range pairs {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = scan.Watch(ctx, p.base, p.l, p.r, p.li, p.ri, scan.Config{
				OpenBps: c.Strategy.OpenBps, VolK: c.Strategy.VolK, FeeBps: p.fee, MaxBookAge: c.Strategy.MaxBookAge},
				func(s scan.Score) { board.set(s) })
		}()
	}
	wg.Wait()
}

type board struct {
	mu sync.Mutex
	m  map[string]scan.Score
}

func newBoard(pairs []pairSpec) *board {
	b := &board{m: map[string]scan.Score{}}
	for _, p := range pairs {
		b.m[p.base] = scan.Score{Base: p.base}
	}
	return b
}

func (b *board) set(s scan.Score) {
	b.mu.Lock()
	b.m[s.Base] = s
	b.mu.Unlock()
}

func (b *board) sorted() []scan.Score {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]scan.Score, 0, len(b.m))
	for _, s := range b.m {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].NetBps != out[j].NetBps {
			return out[i].NetBps > out[j].NetBps
		}
		return out[i].Base < out[j].Base
	})
	return out
}

func render(b *board, tty bool) {
	rows := b.sorted()
	if tty {
		fmt.Print("\033[2J\033[H") // clear screen, home cursor
	}
	fmt.Printf("== pair scan  %d pairs  %s ==\n", len(rows), time.Now().Format("15:04:05"))
	fmt.Printf("%-4s %-12s %-18s %s\n", "#", "PAIR", "DIR", "NET_BPS")
	for i, s := range rows {
		dir, net := "—", "warming"
		if s.MuReady {
			if s.NetBps > 0 {
				dir = s.Dir.String()
			}
			net = fmt.Sprintf("%+.2f", s.NetBps)
		}
		fmt.Printf("%-4d %-12s %-18s %s\n", i+1, s.Base, dir, net)
	}
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

func matchAll(ctx context.Context, m symbols.Matcher, venues map[string]exchange.Venue) []pairSpec {
	type ven struct {
		name  string
		venue exchange.Venue
		inst  []domain.Instrument
	}
	var vs []ven
	names := make([]string, 0, len(venues))
	for name := range venues {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic left/right: lighter before rhlighter
	for _, name := range names {
		inst, err := venues[name].Instruments(ctx)
		if err != nil {
			slog.Warn("instruments", "venue", name, "err", err)
			continue
		}
		vs = append(vs, ven{name: name, venue: venues[name], inst: inst})
	}
	var out []pairSpec
	for i := 0; i < len(vs); i++ {
		for j := i + 1; j < len(vs); j++ {
			for _, p := range symbols.Match(m, vs[i].inst, vs[j].inst) {
				out = append(out, pairSpec{base: p.Base, l: vs[i].venue, r: vs[j].venue,
					li: p.Left, ri: p.Right, fee: 2 * (p.Left.TakerFee + p.Right.TakerFee) * 1e4})
			}
		}
	}
	return out
}
