// Package scan detects opportunities from read-only market data.
package scan

import (
	"context"
	"fmt"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

type Score struct {
	Base        string
	Left, Right string
	Ts          time.Time
	Dir         strategy.Dir
	NetBps      float64
	MuReady     bool
	VolReady    bool
	VolOptional bool // no volatility filter is required when vol_k is explicitly 0
}

func Eval(q spread.Quote, mu, sigma, feeBps, openBps, volK float64) (ls, sl float64) {
	return strategy.OpeningMargins(strategy.Params{OpenBps: openBps, FeeBps: feeBps, VolK: volK}, q, mu, sigma)
}

type Config struct {
	StrategyKind          string
	OpenBps, VolK, FeeBps float64
	TakerFeeL, TakerFeeR  float64
	MaxBookAge            time.Duration
	MaxVolBookAge         time.Duration
	EstimatorKind         string
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
	if cfg.StrategyKind == "" {
		cfg.StrategyKind = "arbitrage"
	}
	strat, err := strategy.New(cfg.StrategyKind)
	if err != nil {
		return err
	}
	p := strategy.Parameters(strat, strategy.Params{OpenBps: cfg.OpenBps, VolK: cfg.VolK, FeeBps: cfg.FeeBps,
		TakerFeeL: cfg.TakerFeeL, TakerFeeR: cfg.TakerFeeR})
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
	if cfg.EstimatorKind == "" {
		cfg.EstimatorKind = "sma"
	}
	mu, err := estimator.New(cfg.EstimatorKind, ep)
	if err != nil {
		return err
	}
	vol := volatility.New(vp)
	var bl, br domain.Book
	quote := func(now time.Time, age time.Duration) (spread.Quote, bool) {
		if bl.Venue != l.Name() || br.Venue != r.Name() || bl.MarketID != li.MarketID || br.MarketID != ri.MarketID ||
			bl.Ts.IsZero() || br.Ts.IsZero() || now.Sub(bl.Ts) > age || now.Sub(br.Ts) > age || bl.Ts.After(now) || br.Ts.After(now) {
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
			if !b.Valid() || !b.Ts.Before(bl.Ts) {
				bl = b
			}
		case b, ok := <-chR:
			if !ok {
				return fmt.Errorf("%s book subscription closed", r.Name())
			}
			if !b.Valid() || !b.Ts.Before(br.Ts) {
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
			st := estimator.StatsAt(mu, now)
			s := Score{Base: base, Left: l.Name(), Right: r.Name(), Ts: now, MuReady: st.Ready, VolReady: vol.ReadyAt(now), VolOptional: p.VolK == 0}
			if !s.MuReady || (!s.VolReady && !s.VolOptional) {
				on(s)
				continue
			}
			ls, sl := strategy.OpeningMargins(p, q, st.Mean, vol.VolAt(now))
			if ls >= sl {
				s.Dir, s.NetBps = strategy.DirLS, ls
			} else {
				s.Dir, s.NetBps = strategy.DirSL, sl
			}
			on(s)
		}
	}
}
