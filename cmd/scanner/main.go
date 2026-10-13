// Command scanner watches matched pairs and logs arbitrage opportunities. It is
// read-only and needs no trading credentials.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
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
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))
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
	slog.Info("scanner started", "pairs", len(pairs))

	var wg sync.WaitGroup
	var mu sync.Mutex
	last := map[string]time.Time{}
	for _, p := range pairs {
		p := p
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = scan.Watch(ctx, p.base, p.l, p.r, p.li, p.ri, scan.Config{
				OpenBps: c.Strategy.OpenBps, VolK: c.Strategy.VolK, FeeBps: p.fee, MaxBookAge: c.Strategy.MaxBookAge},
				func(o scan.Opportunity) {
					k := o.Base + o.Dir.String()
					mu.Lock()
					if time.Since(last[k]) < 5*time.Second {
						mu.Unlock()
						return
					}
					last[k] = time.Now()
					mu.Unlock()
					slog.Info("opportunity", "pair", o.Base, "dir", o.Dir, "net_bps", round2(o.NetBps))
				})
		}()
	}
	wg.Wait()
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
	for name, v := range venues {
		inst, err := v.Instruments(ctx)
		if err != nil {
			slog.Warn("instruments", "venue", name, "err", err)
			continue
		}
		vs = append(vs, ven{name: name, venue: v, inst: inst})
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

func round2(v float64) float64 { return float64(int(v*100+0.5)) / 100 }
