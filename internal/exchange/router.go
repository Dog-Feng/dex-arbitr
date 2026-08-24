package exchange

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

// Router exposes multiple exchange adapters through the common Exchange
// interface. The runtime owns one strategy at a time, so trading calls are
// routed to the selected DEX while Markets returns the union for the UI.
type Router struct {
	mu        sync.RWMutex
	exchanges map[string]Exchange
	active    string
	markets   []domain.MarketDescriptor
}

func NewRouter(exchanges map[string]Exchange, defaultDEX string) *Router {
	copyExchanges := make(map[string]Exchange, len(exchanges))
	for name, ex := range exchanges {
		name = normalizeDEX(name)
		if name != "" && ex != nil {
			copyExchanges[name] = ex
		}
	}
	return &Router{exchanges: copyExchanges, active: normalizeDEX(defaultDEX)}
}

func (r *Router) Connect(ctx context.Context) error {
	names := r.exchangeNames()
	if len(names) == 0 {
		return errors.New("no exchange adapters configured")
	}
	markets := make([]domain.MarketDescriptor, 0)
	connected := make(map[string]Exchange, len(names))
	connectionFailures := make([]string, 0)
	for _, name := range names {
		ex := r.exchanges[name]
		if err := ex.Connect(ctx); err != nil {
			connectionFailures = append(connectionFailures, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		items, err := ex.Markets(ctx)
		if err != nil {
			_ = ex.Close()
			connectionFailures = append(connectionFailures, fmt.Sprintf("%s: %v", name, err))
			continue
		}
		connected[name] = ex
		for _, item := range items {
			if item.DEX == "" {
				item.DEX = name
			}
			markets = append(markets, item)
		}
	}
	if len(connected) == 0 || len(markets) == 0 {
		if len(connectionFailures) > 0 {
			return fmt.Errorf("connect exchange adapters failed: %s", strings.Join(connectionFailures, "; "))
		}
		return errors.New("no exchange markets available")
	}
	r.mu.Lock()
	r.exchanges = connected
	r.markets = markets
	if _, ok := r.exchanges[r.active]; !ok {
		for _, name := range names {
			if _, connectedOK := r.exchanges[name]; connectedOK {
				r.active = name
				break
			}
		}
	}
	r.mu.Unlock()
	return nil
}

func (r *Router) Close() error {
	names := r.exchangeNames()
	var firstErr error
	for _, name := range names {
		if err := r.exchanges[name].Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", name, err)
		}
	}
	return firstErr
}

func (r *Router) SelectDEX(name string) error {
	name = normalizeDEX(name)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.exchanges[name]; !ok {
		return fmt.Errorf("DEX is not configured: %s", name)
	}
	r.active = name
	return nil
}

func (r *Router) ActiveDEX() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.active
}

func (r *Router) Markets(context.Context) ([]domain.MarketDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]domain.MarketDescriptor(nil), r.markets...), nil
}

// RefreshMarkets reloads market metadata and tickers from every configured
// adapter. Markets is intentionally cached for normal reads; the monitor uses
// this method when it needs a fresh latest price.
func (r *Router) RefreshMarkets(ctx context.Context) ([]domain.MarketDescriptor, error) {
	names := r.exchangeNames()
	if len(names) == 0 {
		return nil, errors.New("no exchange adapters configured")
	}
	markets := make([]domain.MarketDescriptor, 0)
	for _, name := range names {
		items, err := r.exchanges[name].Markets(ctx)
		if err != nil {
			return nil, fmt.Errorf("refresh %s markets: %w", name, err)
		}
		for _, item := range items {
			if item.DEX == "" {
				item.DEX = name
			}
			markets = append(markets, item)
		}
	}
	r.mu.Lock()
	r.markets = markets
	r.mu.Unlock()
	return append([]domain.MarketDescriptor(nil), markets...), nil
}

func (r *Router) activeExchange() (Exchange, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ex, ok := r.exchanges[r.active]
	if !ok {
		return nil, fmt.Errorf("active DEX is not configured: %s", r.active)
	}
	return ex, nil
}

func (r *Router) SetMarket(market domain.MarketSpec) {
	if ex, err := r.activeExchange(); err == nil {
		if setter, ok := ex.(interface{ SetMarket(domain.MarketSpec) }); ok {
			setter.SetMarket(market)
		}
	}
}

func (r *Router) OrderBook(ctx context.Context, marketIndex int, symbol string) (domain.OrderBook, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return domain.OrderBook{}, err
	}
	provider, ok := ex.(OrderBookProvider)
	if !ok {
		return domain.OrderBook{}, fmt.Errorf("active DEX does not provide an order book")
	}
	return provider.OrderBook(ctx, marketIndex, symbol)
}

func (r *Router) SetLeverage(ctx context.Context, marketIndex int, leverage decimal.Decimal) error {
	ex, err := r.activeExchange()
	if err != nil {
		return err
	}
	setter, ok := ex.(interface {
		SetLeverage(context.Context, int, decimal.Decimal) error
	})
	if !ok {
		return nil
	}
	return setter.SetLeverage(ctx, marketIndex, leverage)
}

func (r *Router) PlaceOrder(ctx context.Context, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	return ex.PlaceOrder(ctx, intent)
}

func (r *Router) PlaceOrdersBatch(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return nil, err
	}
	return ex.PlaceOrdersBatch(ctx, intents)
}

func (r *Router) ModifyOrder(ctx context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	return ex.ModifyOrder(ctx, clientOrderID, intent)
}

func (r *Router) CancelOrder(ctx context.Context, clientOrderID string) error {
	ex, err := r.activeExchange()
	if err != nil {
		return err
	}
	return ex.CancelOrder(ctx, clientOrderID)
}

func (r *Router) CancelOrdersBatch(ctx context.Context, ids []string) error {
	ex, err := r.activeExchange()
	if err != nil {
		return err
	}
	return ex.CancelOrdersBatch(ctx, ids)
}

func (r *Router) OpenOrders(ctx context.Context) ([]domain.ExchangeOrder, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return nil, err
	}
	return ex.OpenOrders(ctx)
}

func (r *Router) ProcessPrice(ctx context.Context, price string) ([]domain.Fill, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return nil, err
	}
	return ex.ProcessPrice(ctx, price)
}

func (r *Router) AccountSnapshot(ctx context.Context) (domain.AccountSnapshot, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return domain.AccountSnapshot{}, err
	}
	return ex.AccountSnapshot(ctx)
}

func (r *Router) SubscribeFills(ctx context.Context) (<-chan domain.Fill, <-chan error) {
	ex, err := r.activeExchange()
	if err == nil {
		return ex.SubscribeFills(ctx)
	}
	errs := make(chan error, 1)
	errs <- err
	close(errs)
	fills := make(chan domain.Fill)
	close(fills)
	return fills, errs
}

func (r *Router) ReconcileFills(ctx context.Context) ([]domain.Fill, error) {
	ex, err := r.activeExchange()
	if err != nil {
		return nil, err
	}
	reconciler, ok := ex.(interface {
		ReconcileFills(context.Context) ([]domain.Fill, error)
	})
	if !ok {
		return nil, nil
	}
	return reconciler.ReconcileFills(ctx)
}

func (r *Router) exchangeNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.exchanges))
	for name := range r.exchanges {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func normalizeDEX(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}
