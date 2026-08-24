package exchange

import (
	"context"
	"dex-grid-arbitr/internal/domain"
)

type Exchange interface {
	Connect(context.Context) error
	Close() error
	PlaceOrder(context.Context, domain.OrderIntent) (domain.ExchangeOrder, error)
	PlaceOrdersBatch(context.Context, []domain.OrderIntent) ([]domain.ExchangeOrder, error)
	ModifyOrder(context.Context, string, domain.OrderIntent) (domain.ExchangeOrder, error)
	CancelOrder(context.Context, string) error
	CancelOrdersBatch(context.Context, []string) error
	OpenOrders(context.Context) ([]domain.ExchangeOrder, error)
	ProcessPrice(context.Context, string) ([]domain.Fill, error)
	AccountSnapshot(context.Context) (domain.AccountSnapshot, error)
	Markets(context.Context) ([]domain.MarketDescriptor, error)
	SubscribeFills(context.Context) (<-chan domain.Fill, <-chan error)
}

// OrderBookProvider is implemented by live adapters that can supply the
// current best bid/ask used by Maker entry orders. It remains optional on the
// base Exchange interface so paper and legacy test adapters can keep the
// existing lifecycle contract.
type OrderBookProvider interface {
	OrderBook(context.Context, int, string) (domain.OrderBook, error)
}

type NoAccountSnapshot struct{}

func (NoAccountSnapshot) Error() string { return "account snapshot is not available" }
