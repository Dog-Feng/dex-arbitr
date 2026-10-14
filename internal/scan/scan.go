// Package scan detects opportunities from read-only market data.
package scan

import (
	"context"
	"fmt"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
	"time"
)

type Score struct {
	Base        string
	Left, Right string
	Ts          time.Time
	Dir         strategy.Dir
	NetBps      float64
	MuReady     bool
	VolReady    bool
}

func Eval(q spread.Quote, mu, sigma, feeBps, openBps, volK float64) (ls, sl float64) {
	need := openBps + feeBps + volK*sigma
	return q.LS - mu - need, q.SL + mu - need
}

type Config struct {
	OpenBps, VolK, FeeBps float64
	MaxBookAge            time.Duration
	MaxVolBookAge         time.Duration
	Estimator             estimator.Params
	Volatility            volatility.Params
}

// Watch samples volatility at its configured cadence and invalidates scores
// whenever either books or either statistic becomes unavailable.
func Watch(ctx context.Context, base string, l, r exchange.Venue, li, ri domain.Instrument, cfg Config, on func(Score)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	clear := func(at time.Time) { on(Score{Base: base, Left: l.Name(), Right: r.Name(), Ts: at}) }
	defer func() { clear(time.Now()) }()
	chL, err := l.SubscribeBook(ctx, li)
	if err != nil {
		return fmt.Errorf("%s subscribe: %w", l.Name(), err)
	}
	chR, err := r.SubscribeBook(ctx, ri)
	if err != nil {
		return fmt.Errorf("%s subscribe: %w", r.Name(), err)
	}
	if cfg.MaxBookAge <= 0 {
		cfg.MaxBookAge = 10 * time.Second
	}
	if cfg.MaxVolBookAge <= 0 {
		cfg.MaxVolBookAge = 50 * time.Millisecond
	}
	ep := cfg.Estimator
	if ep.Window <= 0 {
		ep.Window = 2 * time.Hour
	}
	if ep.MinWarmup <= 0 {
		ep.MinWarmup = time.Hour
	}
	if ep.MinCoverage <= 0 {
		ep.MinCoverage = 0.8
	}
	if ep.Interval <= 0 {
		ep.Interval = time.Second
	}
	vp := cfg.Volatility
	if vp.Interval <= 0 {
		vp.Interval = 100 * time.Millisecond
	}
	if vp.Horizon <= 0 {
		vp.Horizon = 500 * time.Millisecond
	}
	if vp.HalfLife <= 0 {
		vp.HalfLife = time.Minute
	}
	mu := estimator.NewSMA(ep)
	vol := volatility.New(vp)
	var bl, br domain.Book
	quote := func(now time.Time, age time.Duration) (spread.Quote, bool) {
		if now.Sub(bl.Ts) > age || now.Sub(br.Ts) > age || bl.Ts.After(now) || br.Ts.After(now) {
			return spread.Quote{}, false
		}
		return spread.Compute(bl, br)
	}
	sample := time.NewTicker(ep.Interval)
	defer sample.Stop()
	fast := time.NewTicker(vp.Interval)
	defer fast.Stop()
	eval := time.NewTicker(time.Second)
	defer eval.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-chL:
			if !ok {
				return fmt.Errorf("%s book subscription closed", l.Name())
			}
			if b.Ts.After(bl.Ts) {
				bl = b
			}
		case b, ok := <-chR:
			if !ok {
				return fmt.Errorf("%s book subscription closed", r.Name())
			}
			if b.Ts.After(br.Ts) {
				br = b
			}
		case <-fast.C:
			now := time.Now()
			if q, ok := quote(now, cfg.MaxVolBookAge); ok {
				skew := bl.Ts.Sub(br.Ts)
				if skew <= cfg.MaxVolBookAge && -skew <= cfg.MaxVolBookAge {
					vol.AddAt(now, q.Mid)
				}
			}
		case <-sample.C:
			now := time.Now()
			if q, ok := quote(now, cfg.MaxBookAge); ok {
				mu.Update(now, q.Mid)
			}
		case <-eval.C:
			now := time.Now()
			q, ok := quote(now, cfg.MaxBookAge)
			if !ok {
				clear(now)
				continue
			}
			st := mu.StatsAt(now)
			s := Score{Base: base, Left: l.Name(), Right: r.Name(), Ts: now, MuReady: st.Ready, VolReady: vol.ReadyAt(now)}
			if !s.MuReady || !s.VolReady {
				on(s)
				continue
			}
			ls, sl := Eval(q, st.Mean, vol.VolAt(now), cfg.FeeBps, cfg.OpenBps, cfg.VolK)
			if ls >= sl {
				s.Dir, s.NetBps = strategy.DirLS, ls
			} else {
				s.Dir, s.NetBps = strategy.DirSL, sl
			}
			on(s)
		}
	}
}
