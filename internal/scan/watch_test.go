package scan

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

type watchVenue struct {
	exchange.Venue
	name  string
	books <-chan domain.Book
}

func (v watchVenue) Name() string { return v.name }
func (v watchVenue) SubscribeBook(context.Context, domain.Instrument) (<-chan domain.Book, error) {
	return v.books, nil
}

func TestWatchSamplesShortHorizonAndClearsOnExit(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	left, right := make(chan domain.Book, 10), make(chan domain.Book, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-tick.C:
				l, r := book(100, 100.02, 10, 10), book(102, 102.02, 10, 10)
				l.Ts, r.Ts = now, now
				l.Venue, r.Venue = "left", "right"
				select {
				case left <- l:
				case <-ctx.Done():
					return
				}
				select {
				case right <- r:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	cfg := Config{MaxBookAge: time.Second, MaxVolBookAge: 300 * time.Millisecond, Estimator: estimator.Params{Interval: 100 * time.Millisecond, Window: time.Second, MinWarmup: 200 * time.Millisecond, MinCoverage: 0.8}, Volatility: volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}}
	var good, last Score
	err := Watch(ctx, "TEST", watchVenue{name: "left", books: left}, watchVenue{name: "right", books: right}, domain.Instrument{}, domain.Instrument{}, cfg, func(s Score) {
		last = s
		if s.MuReady && s.VolReady {
			good = s
			cancel()
		}
	})
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if !good.MuReady || !good.VolReady || good.Dir == 0 {
		t.Fatalf("100ms sampler never warmed 500ms volatility: %+v", good)
	}
	if last.MuReady || last.VolReady || last.NetBps != 0 || last.Ts.IsZero() {
		t.Fatalf("collector exit left a live score: %+v", last)
	}
}

func TestWatchClosedBooksClearScoreAndReturnError(t *testing.T) {
	left, right := make(chan domain.Book), make(chan domain.Book)
	close(left)
	var score Score
	err := Watch(context.Background(), "TEST", watchVenue{name: "left", books: left}, watchVenue{name: "right", books: right}, domain.Instrument{}, domain.Instrument{}, Config{}, func(s Score) { score = s })
	if err == nil || !strings.Contains(err.Error(), "book subscription closed") {
		t.Fatalf("err=%v", err)
	}
	if score.MuReady || score.VolReady || score.Ts.IsZero() {
		t.Fatalf("closed collector score=%+v", score)
	}
}

func TestWatchUsesStrategyThresholdsAndOptionalVolatility(t *testing.T) {
	for _, kind := range []string{"arbitrage", "volume"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			left, right := make(chan domain.Book, 1), make(chan domain.Book, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				tick := time.NewTicker(10 * time.Millisecond)
				defer tick.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case now := <-tick.C:
						l, r := book(100, 100.02, 10, 10), book(102, 102.02, 10, 10)
						l.Venue, r.Venue = "left", "right"
						l.MarketID, r.MarketID = 1, 1
						l.Ts, r.Ts = now, now
						select {
						case left <- l:
						case <-ctx.Done():
							return
						}
						select {
						case right <- r:
						case <-ctx.Done():
							return
						}
					}
				}
			}()
			cfg := Config{StrategyKind: kind, OpenBps: 7, VolK: 0, TakerFeeL: .001, TakerFeeR: .0002, MaxBookAge: time.Second,
				Estimator: estimator.Params{Window: time.Second, MinWarmup: 100 * time.Millisecond, MinCoverage: .8, Interval: 20 * time.Millisecond},
				// The vol sampler cannot fire during this test. Disabled filtering
				// must not manufacture readiness or block a warmed mean.
				Volatility: volatility.Params{Interval: time.Hour, Horizon: time.Hour, HalfLife: time.Minute}}
			var score Score
			err := Watch(ctx, "TEST", watchVenue{name: "left", books: left}, watchVenue{name: "right", books: right}, domain.Instrument{MarketID: 1}, domain.Instrument{MarketID: 1}, cfg, func(s Score) {
				if s.MuReady && s.VolOptional {
					score = s
					cancel()
				}
			})
			cancel()
			<-done
			if err != nil {
				t.Fatal(err)
			}
			if !score.MuReady || score.VolReady || !score.VolOptional || score.Dir == 0 {
				t.Fatalf("wrong mean/volatility gates: %+v", score)
			}
			open := 7.0
			if kind == "volume" {
				open = 0
			}
			// Four actual top-book notionals, matching trading's fee reserve.
			ref := (100.01 + 102.01) / 2
			want := (-.02 / ref * 1e4) - ((100+100.02)*.001+(102+102.02)*.0002)/ref*1e4 - open
			if math.Abs(score.NetBps-want) > 1e-8 {
				t.Fatalf("scanner/effective trading params disagree: got %v want %v", score.NetBps, want)
			}
		})
	}
}
