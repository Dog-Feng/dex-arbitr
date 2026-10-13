// Package scan detects arbitrage opportunities across pairs from live books.
// It is read-only: it subscribes to market data and never places orders, and
// it never imports the trading engine.
package scan

import (
	"context"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// Opportunity is one detected signal.
type Opportunity struct {
	Base   string
	Dir    strategy.Dir
	NetBps float64 // executable deviation above fee + volatility buffer + target
}

// Eval returns the net deviation (bps) of each direction; > 0 is tradeable.
func Eval(q spread.Quote, mu, sigma, feeBps, openBps, volK float64) (ls, sl float64) {
	need := openBps + feeBps + volK*sigma
	return q.LS - mu - need, q.SL + mu - need
}

// Config of a watcher.
type Config struct {
	OpenBps    float64
	VolK       float64
	FeeBps     float64
	MaxBookAge time.Duration
}

// Watch subscribes to both legs of a pair and calls on with each opportunity.
func Watch(ctx context.Context, base string, l, r exchange.Venue, li, ri domain.Instrument, cfg Config, on func(Opportunity)) error {
	chL, err := l.SubscribeBook(ctx, li)
	if err != nil {
		return err
	}
	chR, err := r.SubscribeBook(ctx, ri)
	if err != nil {
		return err
	}
	if cfg.MaxBookAge <= 0 {
		cfg.MaxBookAge = 10 * time.Second
	}
	mu := estimator.NewSMA(estimator.Params{Window: 2 * time.Hour, MinWarmup: time.Hour, MinCoverage: 0.8, Interval: time.Second})
	vol := volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	var bl, br domain.Book
	var haveL, haveR bool
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-chL:
			if !ok {
				return nil
			}
			bl, haveL = b, true
		case b, ok := <-chR:
			if !ok {
				return nil
			}
			br, haveR = b, true
		case now := <-tick.C:
			if !haveL || !haveR || now.Sub(bl.Ts) > cfg.MaxBookAge || now.Sub(br.Ts) > cfg.MaxBookAge {
				continue
			}
			q, ok := spread.Compute(bl, br)
			if !ok {
				continue
			}
			mu.Update(now, q.Mid)
			vol.Add(q.Mid)
			if ls, sl := Eval(q, mu.Mean(), vol.Vol(), cfg.FeeBps, cfg.OpenBps, cfg.VolK); ls > 0 {
				on(Opportunity{Base: base, Dir: strategy.DirLS, NetBps: ls})
			} else if sl > 0 {
				on(Opportunity{Base: base, Dir: strategy.DirSL, NetBps: sl})
			}
		}
	}
}
