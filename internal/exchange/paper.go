package exchange

import (
	"context"
	"fmt"
	"sync"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

type Paper struct {
	mu         sync.Mutex
	orders     map[string]domain.ExchangeOrder
	orderBooks map[int]domain.OrderBook
}

func NewPaper() *Paper {
	return &Paper{orders: make(map[string]domain.ExchangeOrder), orderBooks: make(map[int]domain.OrderBook)}
}

func (p *Paper) SetOrderBook(marketIndex int, book domain.OrderBook) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.orderBooks[marketIndex] = book
}

func (p *Paper) OrderBook(_ context.Context, marketIndex int, _ string) (domain.OrderBook, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	book, ok := p.orderBooks[marketIndex]
	if !ok {
		return domain.OrderBook{}, fmt.Errorf("paper order book is not configured for market %d", marketIndex)
	}
	return book, nil
}

func (p *Paper) Connect(context.Context) error { return nil }
func (p *Paper) Close() error                  { return nil }

func (p *Paper) PlaceOrder(_ context.Context, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	order := domain.ExchangeOrder{
		ExchangeOrderID: "paper-" + intent.ClientOrderID,
		ClientOrderID:   intent.ClientOrderID, MarketIndex: intent.MarketIndex,
		Side: intent.Side, Role: intent.Role, Level: intent.Level,
		Price: intent.Price, Quantity: intent.Quantity, ReduceOnly: intent.ReduceOnly, PostOnly: intent.PostOnly,
		OrderType: intent.OrderType, Status: "open",
	}
	p.orders[order.ClientOrderID] = order
	return order, nil
}

func (p *Paper) PlaceOrdersBatch(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	orders := make([]domain.ExchangeOrder, 0, len(intents))
	for _, intent := range intents {
		order, err := p.PlaceOrder(ctx, intent)
		if err != nil {
			return orders, err
		}
		orders = append(orders, order)
	}
	return orders, nil
}

func (p *Paper) ModifyOrder(_ context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	current, ok := p.orders[clientOrderID]
	if !ok {
		return domain.ExchangeOrder{}, fmt.Errorf("unknown paper client order: %s", clientOrderID)
	}
	updated := current
	updated.MarketIndex, updated.Side, updated.Role, updated.Level = intent.MarketIndex, intent.Side, intent.Role, intent.Level
	updated.Price, updated.Quantity, updated.ReduceOnly, updated.PostOnly, updated.OrderType = intent.Price, intent.Quantity, intent.ReduceOnly, intent.PostOnly, intent.OrderType
	p.orders[clientOrderID] = updated
	return updated, nil
}

func (p *Paper) CancelOrder(_ context.Context, clientOrderID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.orders, clientOrderID)
	return nil
}

func (p *Paper) CancelOrdersBatch(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if err := p.CancelOrder(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (p *Paper) OpenOrders(context.Context) ([]domain.ExchangeOrder, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	orders := make([]domain.ExchangeOrder, 0, len(p.orders))
	for _, order := range p.orders {
		orders = append(orders, order)
	}
	return orders, nil
}

func (p *Paper) ProcessPrice(_ context.Context, priceText string) ([]domain.Fill, error) {
	price, err := decimal.NewFromString(priceText)
	if err != nil || !price.GreaterThan(decimal.Zero) {
		return nil, fmt.Errorf("invalid paper price: %s", priceText)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	fills := make([]domain.Fill, 0)
	for _, order := range p.orders {
		shouldFill := order.OrderType == domain.OrderTypeMarket ||
			(order.Side == domain.OrderSideBuy && !price.GreaterThan(order.Price)) ||
			(order.Side == domain.OrderSideSell && !price.LessThan(order.Price))
		if !shouldFill {
			continue
		}
		delete(p.orders, order.ClientOrderID)
		fills = append(fills, domain.Fill{
			ExchangeOrderID: order.ExchangeOrderID, ClientOrderID: order.ClientOrderID,
			MarketIndex: order.MarketIndex, Side: order.Side, Role: order.Role,
			Level: order.Level, Price: order.Price, Quantity: order.Quantity,
		})
	}
	return fills, nil
}

func (p *Paper) AccountSnapshot(context.Context) (domain.AccountSnapshot, error) {
	return domain.AccountSnapshot{Positions: []domain.PositionSnapshot{}, Assets: []domain.AssetSnapshot{}}, NoAccountSnapshot{}
}

func (p *Paper) Markets(context.Context) ([]domain.MarketDescriptor, error) {
	return []domain.MarketDescriptor{{
		DEX: "paper", MarketIndex: 0, Symbol: "BTC-USD", ExchangeSymbol: "BTC",
		BaseAsset: "BTC", QuoteAsset: "USD", MarketType: "perp",
		PriceStep: decimal.NewFromFloat(0.01), SizeStep: decimal.NewFromFloat(0.00001),
	}}, nil
}

func (p *Paper) SubscribeFills(context.Context) (<-chan domain.Fill, <-chan error) {
	return make(chan domain.Fill), make(chan error)
}
