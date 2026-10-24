// Package execution runs two-leg IOC orders with tracking and rescue.
package execution

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// IDGen produces unique client order indexes (< 2^48).
type IDGen struct{ n atomic.Int64 }

// idMask keeps ids below 2^48, matching the venues' client-order-index range.
const idMask = uint64(1<<48) - 1

// NewIDGen seeds from 48 bits of OS randomness so two processes started in the
// same millisecond, or a clock rollback, cannot reuse client order ids. The
// wall clock is only a fallback when the platform lacks an entropy source.
func NewIDGen() *IDGen {
	g := &IDGen{}
	var seed [8]byte
	if _, err := crand.Read(seed[:]); err == nil {
		g.n.Store(int64(binary.LittleEndian.Uint64(seed[:]) & idMask))
		return g
	}
	g.n.Store(time.Now().UnixMilli() * 100 % int64(idMask))
	return g
}

// Next id.
func (g *IDGen) Next() int64 { return g.n.Add(1) }

// Tracker fans venue order streams out to per-order waiters.
type Tracker struct {
	mu      sync.Mutex
	waiters map[string]chan domain.OrderUpdate
	stopped map[string]bool
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	closed  bool
}

// NewTracker subscribes to order updates of each trading venue.
func NewTracker(ctx context.Context, venues ...exchange.Venue) (*Tracker, error) {
	ctx, cancel := context.WithCancel(ctx)
	t := &Tracker{waiters: map[string]chan domain.OrderUpdate{}, stopped: map[string]bool{}, cancel: cancel}
	for _, v := range venues {
		ch, err := v.SubscribeOrders(ctx)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("subscribe orders on %s: %w", v.Name(), err)
		}
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			defer t.stopVenue(v.Name())
			for {
				select {
				case u, ok := <-ch:
					if !ok {
						return
					}
					t.dispatch(u)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	return t, nil
}

func key(venue string, cid int64) string { return venue + "#" + itoa(cid) }

func (t *Tracker) dispatch(u domain.OrderUpdate) {
	k := key(u.Venue, u.ClientID)
	t.mu.Lock()
	defer t.mu.Unlock()
	w := t.waiters[k]
	if w != nil {
		// The latest cumulative update is enough. Preserve a terminal report
		// against a later nonterminal report and never drop it behind a full queue.
		select {
		case previous := <-w:
			if previous.Status.Terminal() && !u.Status.Terminal() || previous.FilledQty > u.FilledQty {
				u = previous
			}
		default:
		}
		select {
		case w <- u:
		default:
		}
	}
}

// Watch registers interest BEFORE submitting.
func (t *Tracker) Watch(venue string, cid int64) <-chan domain.OrderUpdate {
	ch := make(chan domain.OrderUpdate, 1)
	t.mu.Lock()
	if t.closed || t.stopped[venue] {
		close(ch)
	} else {
		t.waiters[key(venue, cid)] = ch
	}
	t.mu.Unlock()
	return ch
}

// Done unregisters.
func (t *Tracker) Done(venue string, cid int64) {
	t.mu.Lock()
	delete(t.waiters, key(venue, cid))
	t.mu.Unlock()
}

func (t *Tracker) stopVenue(venue string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped[venue] = true
	for k, ch := range t.waiters {
		if strings.HasPrefix(k, venue+"#") {
			close(ch)
			delete(t.waiters, k)
		}
	}
}

// Close stops subscriptions and waits for all order-stream readers to exit.
func (t *Tracker) Close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.cancel()
	t.wg.Wait()
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
