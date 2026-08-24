package exchange

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dex-grid-arbitr/internal/domain"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

func TestLighterRemoteOrderMergePreservesStrategyMetadata(t *testing.T) {
	lighter := NewLighter(LighterConfig{MarketIndex: 7})
	local := lighter.remember(domain.OrderIntent{
		ClientOrderID: "grid-take-profit", MarketIndex: 7, Side: domain.OrderSideSell,
		Role: domain.OrderRoleTakeProfit, Level: -1, Price: decimal.NewFromInt(101),
		Quantity: decimal.NewFromInt(2), ReduceOnly: true, OrderType: domain.OrderTypeLimit,
	}, 12345)

	merged := lighter.mergeRemoteOrders(map[string]domain.ExchangeOrder{local.ClientOrderID: local}, []map[string]any{{
		"order_index":           88,
		"client_order_index":    12345,
		"order_id":              "remote-88",
		"market_index":          7,
		"price":                 "101.00",
		"remaining_base_amount": "1.5",
		"is_ask":                true,
		"reduce_only":           true,
		"type":                  "take-profit-limit",
		"status":                "open",
	}})
	if len(merged) != 1 {
		t.Fatalf("unexpected merged orders: %+v", merged)
	}
	if merged[0].ClientOrderID != local.ClientOrderID || merged[0].Role != domain.OrderRoleTakeProfit || !merged[0].Quantity.Equal(decimal.RequireFromString("1.5")) {
		t.Fatalf("remote merge lost local strategy metadata: %+v", merged[0])
	}
	if lighter.orderIndex[local.ClientOrderID] != 88 {
		t.Fatalf("remote order index was not refreshed: %+v", lighter.orderIndex)
	}
}

func TestLighterRemoteOrderMergeUsesOrderIndexWithoutClientIndex(t *testing.T) {
	lighter := NewLighter(LighterConfig{MarketIndex: 7})
	merged := lighter.mergeRemoteOrders(nil, []map[string]any{{
		"order_index":           88,
		"market_id":             7,
		"price":                 "101.00",
		"remaining_base_amount": "1.5",
		"is_ask":                true,
		"status":                "open",
	}})
	if len(merged) != 1 || merged[0].ClientOrderID != "88" {
		t.Fatalf("remote order without client index cannot be addressed: %+v", merged)
	}
	if lighter.orderIndex["88"] != 88 {
		t.Fatalf("order index was not retained for cancellation: %+v", lighter.orderIndex)
	}
}

func TestLighterUnitsRejectUnalignedValues(t *testing.T) {
	lighter := NewLighter(LighterConfig{})
	if _, err := lighter.toUnits(decimal.RequireFromString("1.01"), decimal.RequireFromString("0.01")); err != nil {
		t.Fatalf("aligned value rejected: %v", err)
	}
	if _, err := lighter.toUnits(decimal.RequireFromString("1.5"), decimal.NewFromInt(1)); err == nil {
		t.Fatal("unaligned value was accepted")
	}
}

func TestLighterWebSocketWaitsForConnectedBeforeSubscribe(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		if err := conn.WriteJSON(map[string]any{"type": "connected"}); err != nil {
			return
		}
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var subscription map[string]any
		if json.Unmarshal(data, &subscription) != nil || subscription["type"] != "subscribe" {
			return
		}
		_ = conn.WriteJSON(map[string]any{
			"type": "update/account_all",
			"trades": map[string]any{"3": []map[string]any{{
				"trade_id":       42,
				"bid_id":         999,
				"bid_account_id": 1,
				"ask_account_id": 2,
				"price":          "100.2",
				"size":           "0.001",
			}}},
		})
	}))
	defer server.Close()

	lighter := NewLighter(LighterConfig{
		WSURL: strings.Replace(server.URL, "http://", "ws://", 1), AccountIndex: 1, MarketIndex: 3, MaxReconnect: 0,
	})
	intent := domain.OrderIntent{
		ClientOrderID: "mg-1-initial-0-ws", MarketIndex: 3, Side: domain.OrderSideBuy,
		Role: domain.OrderRoleInitial, Price: decimal.NewFromInt(100), Quantity: decimal.RequireFromString("0.001"),
		OrderType: domain.OrderTypeMarket,
	}
	lighter.remember(intent, 123)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fills, _ := lighter.SubscribeFills(ctx)
	select {
	case fill := <-fills:
		if fill.ClientOrderID != intent.ClientOrderID || !fill.Price.Equal(decimal.RequireFromString("100.2")) {
			t.Fatalf("unexpected websocket fill: %+v", fill)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("websocket fill was not received")
	}
}
