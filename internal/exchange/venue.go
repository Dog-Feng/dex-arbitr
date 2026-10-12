// Package exchange defines the Venue port and a factory registry.
package exchange

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// Venue is the port every DEX adapter implements.
type Venue interface {
	Name() string
	// Instruments returns perpetual markets only.
	Instruments(ctx context.Context) ([]domain.Instrument, error)
	// SubscribeBook streams full top-N snapshots (latest wins; slow readers drop).
	SubscribeBook(ctx context.Context, inst domain.Instrument) (<-chan domain.Book, error)
	// Submit is phase 1: sign and send. A nil error means "accepted for processing",
	// NOT filled. domain.ErrRejected means definitively not placed. Any other error
	// means the outcome is unknown and must be resolved via events / QueryOrder.
	Submit(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error)
	// SubscribeOrders streams order updates for this account (phase 2).
	SubscribeOrders(ctx context.Context) (<-chan domain.OrderUpdate, error)
	// QueryOrder is the REST fallback for an order identified by client id.
	QueryOrder(ctx context.Context, inst domain.Instrument, clientID int64) (domain.OrderUpdate, error)
	CancelOrder(ctx context.Context, inst domain.Instrument, clientID int64) error
	Positions(ctx context.Context) ([]domain.Position, error)
	CanTrade() bool
	Close() error
}

// LeverageSetter is implemented by venues that apply account leverage per
// market (e.g. Lighter's initial-margin fraction).
type LeverageSetter interface {
	SetLeverage(ctx context.Context, inst domain.Instrument, leverage int) error
}

// Factory builds a venue from a generic option map.
type Factory func(name string, opts map[string]string) (Venue, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register a venue type (called from adapter init()).
func Register(kind string, f Factory) {
	mu.Lock()
	defer mu.Unlock()
	factories[kind] = f
}

// New builds a venue of the given registered kind.
func New(kind, name string, opts map[string]string) (Venue, error) {
	mu.RLock()
	f, ok := factories[kind]
	mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown venue kind %q (have %v)", kind, Kinds())
	}
	return f(name, opts)
}

// Kinds lists registered venue kinds.
func Kinds() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(factories))
	for k := range factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
