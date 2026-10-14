package scan

import (
	"context"
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
