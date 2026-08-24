package exchange

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
	sodexclient "github.com/sodex-tech/sodex-go-sdk-public/client"
	"github.com/sodex-tech/sodex-go-sdk-public/ws"
)

func TestSodexMarketsMapsSymbolPrecisionAndContractSize(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/perps/markets/symbols":
			_, _ = w.Write([]byte(`{"code":0,"data":[{"id":2,"name":"SOL-USD","displayName":"SOL/USD","baseCoin":"SOL","quoteCoin":"USD","status":"TRADING","pricePrecision":3,"quantityPrecision":3,"tickSize":"0.001","stepSize":"0.001","minQuantity":"0.1","minNotional":"10","makerFee":"0.000108","takerFee":"0.00036","contractSize":"1"}]}`))
		case "/api/v1/perps/markets/tickers":
			_, _ = w.Write([]byte(`{"code":0,"data":[{"symbol":"SOL-USD","lastPx":"72.994","bidPx":"72.993","askPx":"72.995"}]}`))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	ex := NewSodex(SodexConfig{BaseURL: server.URL, AccountAddress: "0xabc", MarketIndex: 2})
	if err := ex.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	markets, err := ex.Markets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(markets) != 1 {
		t.Fatalf("expected one market, got %d", len(markets))
	}
	market := markets[0]
	if market.DEX != "sodex" || market.MarketIndex != 2 || market.Symbol != "SOL-USD" {
		t.Fatalf("unexpected market identity: %+v", market)
	}
	if !market.PriceStep.Equal(decimal.RequireFromString("0.001")) || !market.SizeStep.Equal(decimal.RequireFromString("0.001")) {
		t.Fatalf("unexpected precision: %+v", market)
	}
	if market.LastPrice == nil || !market.LastPrice.Equal(decimal.RequireFromString("72.994")) {
		t.Fatalf("unexpected last price: %+v", market.LastPrice)
	}
	if !market.MakerFeeRate.Equal(decimal.RequireFromString("0.000108")) || !market.TakerFeeRate.Equal(decimal.RequireFromString("0.00036")) || !market.FeeRateKnown {
		t.Fatalf("unexpected market fee metadata: %+v", market)
	}
}

func TestSodexDiscoversPrimaryAccountIDFromState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/perps/accounts/0xabc/state" {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"aid":12345}}`))
	}))
	defer server.Close()

	ex := NewSodex(SodexConfig{
		BaseURL: server.URL, AccountAddress: "0xabc", PrivateKey: "0000000000000000000000000000000000000000000000000000000000000001",
		APIKeyName: "grid-bot", EnableTrading: true,
	})
	if err := ex.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ex.cfg.AccountID != 12345 {
		t.Fatalf("expected account ID to be discovered from aid, got %d", ex.cfg.AccountID)
	}
}

func TestSodexTakeProfitOrderUsesStopModifierAndBothPositionSide(t *testing.T) {
	ex := NewSodex(SodexConfig{AccountAddress: "0xabc", MarketIndex: 2})
	ex.symbols[2] = sodexSymbol(2)
	ex.symbolIDs["SOL-USD"] = 2
	ex.SetMarket(domain.MarketSpec{
		MarketIndex: 2, Symbol: "SOL-USD", PriceStep: decimal.RequireFromString("0.001"),
		SizeStep: decimal.RequireFromString("0.001"), ContractMult: decimal.NewFromInt(1),
	})
	trigger := decimal.RequireFromString("73.500")
	raw, err := ex.rawOrder(context.Background(), domain.OrderIntent{
		ClientOrderID: "tp-1", MarketIndex: 2, Symbol: "SOL-USD", Side: domain.OrderSideSell,
		Role: domain.OrderRoleTakeProfit, Price: trigger, Quantity: decimal.RequireFromString("0.1"),
		ReduceOnly: true, TriggerPrice: &trigger, OrderType: domain.OrderTypeLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatal(err)
	}
	if body["modifier"] != float64(2) || body["timeInForce"] != float64(4) || body["positionSide"] != float64(1) || body["reduceOnly"] != true || body["triggerType"] != float64(2) {
		t.Fatalf("unexpected stop-order flags: %s", encoded)
	}
	if body["stopPrice"] != "73.5" || body["price"] != "73.501" || body["quantity"] != "0.1" {
		t.Fatalf("unexpected stop-order prices: %s", encoded)
	}
}

func TestSodexModifyTakeProfitUsesExchangeOrderIDAndUpdatedPrices(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/perps/trade/orders/modify" {
			http.NotFound(w, request)
			return
		}
		body, _ := io.ReadAll(request.Body)
		_ = json.Unmarshal(body, &requestBody)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{}}`))
	}))
	defer server.Close()

	ex := NewSodex(SodexConfig{
		BaseURL: server.URL, AccountAddress: "0xabc", AccountID: 7,
		PrivateKey: "0000000000000000000000000000000000000000000000000000000000000001",
		APIKeyName: "grid-bot", EnableTrading: true,
	})
	if err := ex.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	ex.symbols[2] = sodexSymbol(2)
	ex.symbolIDs["SOL-USD"] = 2
	ex.SetMarket(domain.MarketSpec{
		MarketIndex: 2, Symbol: "SOL-USD", PriceStep: decimal.RequireFromString("0.001"),
		SizeStep: decimal.RequireFromString("0.001"), ContractMult: decimal.NewFromInt(1),
	})
	ex.orders["tp-1"] = domain.ExchangeOrder{ClientOrderID: "tp-1", ExchangeOrderID: "99", MarketIndex: 2, Role: domain.OrderRoleTakeProfit}
	ex.orderIDs["tp-1"] = 99
	ex.byOrderID[99] = "tp-1"
	trigger := decimal.RequireFromString("100.500")
	updated, err := ex.ModifyOrder(context.Background(), "tp-1", domain.OrderIntent{
		ClientOrderID: "tp-1", MarketIndex: 2, Symbol: "SOL-USD", Side: domain.OrderSideSell,
		Role: domain.OrderRoleTakeProfit, Level: -1, Price: trigger, TriggerPrice: &trigger,
		Quantity: decimal.RequireFromString("0.2"), ReduceOnly: true, OrderType: domain.OrderTypeLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if requestBody["orderID"] != float64(99) || requestBody["clOrdID"] != nil {
		t.Fatalf("modify request did not target the exchange order ID: %+v", requestBody)
	}
	if requestBody["stopPrice"] != "100.5" || requestBody["price"] != "100.501" {
		t.Fatalf("modify request did not carry updated TP prices: %+v", requestBody)
	}
	if !updated.Price.Equal(decimal.RequireFromString("100.501")) || !updated.Quantity.Equal(decimal.RequireFromString("0.2")) {
		t.Fatalf("local TP state was not updated: %+v", updated)
	}
}

func TestSodexFillCarriesExchangeFeeAndRemovesFilledOrder(t *testing.T) {
	ex := NewSodex(SodexConfig{AccountAddress: "0xabc", MarketIndex: 2})
	ex.orders["order-1"] = domain.ExchangeOrder{
		ExchangeOrderID: "99", ClientOrderID: "order-1", MarketIndex: 2,
		Side: domain.OrderSideBuy, Role: domain.OrderRoleInitial, Quantity: decimal.RequireFromString("0.1"),
	}
	ex.orderIDs["order-1"] = 99
	ex.byOrderID[99] = "order-1"
	ex.remaining["order-1"] = decimal.RequireFromString("0.1")
	fill, ok := ex.fillFromTrade(ws.AccountTrade{
		TradeID: 123, Symbol: "SOL-USD", OrderID: 99, ClOrdID: "stale-order", Side: "BUY", Price: "73.000", Quantity: "0.1", Fee: "0.00365",
	})
	if !ok {
		t.Fatal("expected fill to be mapped")
	}
	if fill.ClientOrderID != "order-1" || fill.TradeID != "123" || fill.Fee == nil || !fill.Fee.Equal(decimal.RequireFromString("0.00365")) {
		t.Fatalf("unexpected fill: %+v", fill)
	}
	if _, exists := ex.orders["order-1"]; exists {
		t.Fatal("filled order metadata was not removed")
	}
}

func TestSodexIgnoresUnknownHistoricalTrade(t *testing.T) {
	ex := NewSodex(SodexConfig{AccountAddress: "0xabc", MarketIndex: 2})
	ex.orders["current-order"] = domain.ExchangeOrder{
		ClientOrderID: "current-order", MarketIndex: 2,
		Role: domain.OrderRoleTakeProfit, Quantity: decimal.RequireFromString("0.1"),
	}
	fill, ok := ex.fillFromTrade(ws.AccountTrade{
		TradeID: 456, Symbol: "SOL-USD", OrderID: 4567, ClOrdID: "old-cycle-order",
		Side: "SELL", Price: "73.000", Quantity: "0.1", Fee: "0.00365",
	})
	if ok || !fill.Quantity.IsZero() {
		t.Fatalf("unknown historical trade was accepted as a fill: %+v", fill)
	}
	if _, exists := ex.orders["old-cycle-order"]; exists {
		t.Fatal("unknown historical trade created local order metadata")
	}
}

func sodexSymbol(id uint64) sodexclient.Symbol {
	return sodexclient.Symbol{SymbolID: id, Symbol: "SOL-USD", TickSize: "0.001", StepSize: "0.001", ContractSize: stringPtr("1")}
}

func stringPtr(value string) *string { return &value }
