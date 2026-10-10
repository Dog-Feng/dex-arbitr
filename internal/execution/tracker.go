// Package execution runs two-leg IOC orders with tracking and rescue.
package execution

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// IDGen produces unique client order indexes (< 2^48).
type IDGen struct{ n atomic.Int64 }

// NewIDGen seeds from wall clock (ms*100) so restarts don't collide.
func NewIDGen() *IDGen {
	g := &IDGen{}
	g.n.Store(time.Now().UnixMilli() * 100 % ((1 << 48) - 1))
	return g
}

// Next id.
func (g *IDGen) Next() int64 { return g.n.Add(1) }

// Tracker fans venue order streams out to per-order waiters.
type Tracker struct {
	mu      sync.Mutex
	waiters map[string]chan domain.OrderUpdate
	last    map[string]domain.OrderUpdate
}

// NewTracker subscribes to order updates of each trading venue.
func NewTracker(ctx context.Context, venues ...exchange.Venue) (*Tracker, error) {
	t := &Tracker{waiters: map[string]chan domain.OrderUpdate{}, last: map[string]domain.OrderUpdate{}}
	for _, v := range venues {
		ch, err := v.SubscribeOrders(ctx)
		if err != nil {
			return nil, err
		}
		go func() {
			for {
				select {
				case u := <-ch:
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
	t.last[k] = u
	w := t.waiters[k]
	t.mu.Unlock()
	if w != nil {
		select {
		case w <- u:
		default:
		}
	}
}

// Watch registers interest BEFORE submitting.
func (t *Tracker) Watch(venue string, cid int64) <-chan domain.OrderUpdate {
	ch := make(chan domain.OrderUpdate, 16)
	t.mu.Lock()
	t.waiters[key(venue, cid)] = ch
	t.mu.Unlock()
	return ch
}

// Done unregisters.
func (t *Tracker) Done(venue string, cid int64) {
	t.mu.Lock()
	delete(t.waiters, key(venue, cid))
	delete(t.last, key(venue, cid))
	t.mu.Unlock()
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
