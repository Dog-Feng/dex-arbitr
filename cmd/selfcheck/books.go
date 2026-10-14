package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

// One goroutine owns these streams and their snapshots, including during rescue.
type bookStreams struct {
	mu          sync.Mutex
	L, R        exchange.Venue
	pair        symbols.Pair
	chL, chR    <-chan domain.Book
	bl, br      domain.Book
	nL, nR, bad int
	maxAge      time.Duration
}

func subscribeBooks(ctx context.Context, L, R exchange.Venue, pair symbols.Pair, maxAge time.Duration) (*bookStreams, error) {
	if maxAge <= 0 {
		maxAge = 10 * time.Second
	}
	s := &bookStreams{L: L, R: R, pair: pair, maxAge: maxAge}
	var err error
	if s.chL, err = L.SubscribeBook(ctx, pair.Left); err != nil {
		return nil, fmt.Errorf("%s book subscription: %w", L.Name(), err)
	}
	if s.chR, err = R.SubscribeBook(ctx, pair.Right); err != nil {
		return nil, fmt.Errorf("%s book subscription: %w", R.Name(), err)
	}
	if s.chL == nil || s.chR == nil {
		return nil, fmt.Errorf("book subscription returned a nil stream")
	}
	return s, nil
}

func validBook(b domain.Book) bool {
	if !b.Valid() {
		return false
	}
	for _, l := range []domain.Level{b.Bids[0], b.Asks[0]} {
		if l.Price <= 0 || l.Size <= 0 || math.IsNaN(l.Price) || math.IsInf(l.Price, 0) || math.IsNaN(l.Size) || math.IsInf(l.Size, 0) {
			return false
		}
	}
	return true
}

func (s *bookStreams) fresh(b domain.Book, venue string, marketID int) bool {
	return validBook(b) && b.Venue == venue && b.MarketID == marketID && !b.Ts.IsZero() &&
		time.Since(b.Ts) >= 0 && time.Since(b.Ts) <= s.maxAge
}

func (s *bookStreams) receive(left bool, b domain.Book, ok bool) error {
	v, inst := s.R, s.pair.Right
	if left {
		v, inst = s.L, s.pair.Left
	}
	if !ok {
		return fmt.Errorf("%s book stream closed", v.Name())
	}
	if b.Venue != v.Name() || b.MarketID != inst.MarketID {
		return fmt.Errorf("%s book stream delivered another venue or market", v.Name())
	}
	// An empty update is the adapter's explicit invalidation signal. It clears
	// the cached book and freshness gate, but a later snapshot may recover.
	if !validBook(b) && (len(b.Bids) != 0 || len(b.Asks) != 0) {
		s.bad++
	}
	if left {
		s.bl, s.nL = b, s.nL+1
	} else {
		s.br, s.nR = b, s.nR+1
	}
	return nil
}

func (s *bookStreams) ready() bool {
	return s.fresh(s.bl, s.L.Name(), s.pair.Left.MarketID) && s.fresh(s.br, s.R.Name(), s.pair.Right.MarketID)
}

func (s *bookStreams) observe(wctx, ctx context.Context, est *estimator.SMA) error {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case b, ok := <-s.chL:
			if err := s.receive(true, b, ok); err != nil {
				return err
			}
		case b, ok := <-s.chR:
			if err := s.receive(false, b, ok); err != nil {
				return err
			}
		case <-tick.C:
		case <-wctx.Done():
			if ctx.Err() != nil {
				return fmt.Errorf("watch books: %w", ctx.Err())
			}
			if !s.ready() {
				return fmt.Errorf("watch ended without two valid fresh books")
			}
			return nil
		}
		if s.ready() {
			if q, ok := spread.Compute(s.bl, s.br); ok {
				est.Update(time.Now(), q.Mid)
			}
		}
	}
}

// refresh consumes queued updates first, then waits for valid fresh snapshots.
func (s *bookStreams) refresh(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	for {
		for _, side := range []struct {
			left bool
			ch   <-chan domain.Book
		}{{true, s.chL}, {false, s.chR}} {
		drain:
			for {
				select {
				case <-wctx.Done():
					return fmt.Errorf("refresh books: %w", wctx.Err())
				case b, ok := <-side.ch:
					if err := s.receive(side.left, b, ok); err != nil {
						return err
					}
				default:
					break drain
				}
			}
		}
		if s.ready() {
			return nil
		}
		select {
		case b, ok := <-s.chL:
			if err := s.receive(true, b, ok); err != nil {
				return err
			}
		case b, ok := <-s.chR:
			if err := s.receive(false, b, ok); err != nil {
				return err
			}
		case <-wctx.Done():
			return fmt.Errorf("refresh books: %w", wctx.Err())
		}
	}
}

func (s *bookStreams) latest(ctx context.Context, venue string, marketID int) (domain.Book, bool) {
	if err := s.refresh(ctx); err != nil {
		return domain.Book{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.br
	if venue == s.L.Name() && marketID == s.pair.Left.MarketID {
		b = s.bl
	} else if venue != s.R.Name() || marketID != s.pair.Right.MarketID {
		return domain.Book{}, false
	}
	return b, s.fresh(b, venue, marketID)
}

func validPairQty(pair symbols.Pair, qty float64, bl, br domain.Book) error {
	if !validBook(bl) || !validBook(br) {
		return fmt.Errorf("quantity validation requires two valid books")
	}
	if ok, why := pair.ValidQty(qty, 0); !ok {
		return fmt.Errorf("aligned quantity: %s", why)
	}
	for _, side := range []struct {
		inst domain.Instrument
		book domain.Book
	}{{pair.Left, bl}, {pair.Right, br}} {
		// The bid is the lower executable price in either test direction.
		p := symbols.NewPair(pair.Base, side.inst, side.inst)
		if ok, why := p.ValidQty(qty, side.book.Bids[0].Price); !ok {
			return fmt.Errorf("%s quantity: %s", side.inst.Venue, why)
		}
	}
	return nil
}
