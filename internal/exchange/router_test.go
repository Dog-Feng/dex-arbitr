package exchange

import (
	"context"
	"errors"
	"testing"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

type routerExchangeStub struct {
	*Paper
	markets   []domain.MarketDescriptor
	placeCall int
}

func (e *routerExchangeStub) Markets(context.Context) ([]domain.MarketDescriptor, error) {
	return append([]domain.MarketDescriptor(nil), e.markets...), nil
}

func (e *routerExchangeStub) PlaceOrder(context.Context, domain.OrderIntent) (domain.ExchangeOrder, error) {
	e.placeCall++
	return domain.ExchangeOrder{ClientOrderID: "router-stub-order"}, nil
}

func TestRouterUnionsMarketsAndRoutesActiveDEX(t *testing.T) {
	lighter := &routerExchangeStub{
		Paper:   exchangePaper(),
		markets: []domain.MarketDescriptor{{DEX: "lighter", MarketIndex: 0, Symbol: "BTC-USD"}},
	}
	sodex := &routerExchangeStub{
		Paper:   exchangePaper(),
		markets: []domain.MarketDescriptor{{DEX: "sodex", MarketIndex: 7, Symbol: "SOL-USD"}},
	}
	router := NewRouter(map[string]Exchange{"lighter": lighter, "sodex": sodex}, "sodex")
	if err := router.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	markets, err := router.Markets(context.Background())
	if err != nil || len(markets) != 2 {
		t.Fatalf("router did not expose the market union: len=%d err=%v", len(markets), err)
	}
	if router.ActiveDEX() != "sodex" {
		t.Fatalf("unexpected default DEX: %s", router.ActiveDEX())
	}
	intent := domain.OrderIntent{MarketIndex: 7, Price: decimal.NewFromInt(1), Quantity: decimal.NewFromInt(1)}
	if _, err := router.PlaceOrder(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if sodex.placeCall != 1 || lighter.placeCall != 0 {
		t.Fatalf("default DEX did not receive the order: lighter=%d sodex=%d", lighter.placeCall, sodex.placeCall)
	}
	if err := router.SelectDEX("lighter"); err != nil {
		t.Fatal(err)
	}
	if _, err := router.PlaceOrder(context.Background(), intent); err != nil {
		t.Fatal(err)
	}
	if lighter.placeCall != 1 || sodex.placeCall != 1 {
		t.Fatalf("selected DEX did not receive the order: lighter=%d sodex=%d", lighter.placeCall, sodex.placeCall)
	}
}

type failingRouterExchange struct {
	*Paper
	err error
}

func (e *failingRouterExchange) Connect(context.Context) error { return e.err }

func TestRouterKeepsAvailableDEXWhenAnotherAdapterFails(t *testing.T) {
	sodex := &routerExchangeStub{
		Paper:   exchangePaper(),
		markets: []domain.MarketDescriptor{{DEX: "sodex", MarketIndex: 7, Symbol: "SOL-USD"}},
	}
	lighter := &failingRouterExchange{Paper: exchangePaper(), err: errors.New("network unavailable")}
	router := NewRouter(map[string]Exchange{"lighter": lighter, "sodex": sodex}, "sodex")

	if err := router.Connect(context.Background()); err != nil {
		t.Fatalf("router should start with one available DEX: %v", err)
	}
	markets, err := router.Markets(context.Background())
	if err != nil || len(markets) != 1 || markets[0].DEX != "sodex" {
		t.Fatalf("unexpected markets after partial connect: markets=%+v err=%v", markets, err)
	}
	if err := router.SelectDEX("lighter"); err == nil {
		t.Fatal("failed DEX should not remain selectable")
	}
}

func exchangePaper() *Paper { return NewPaper() }
