package exchange

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"dex-grid-arbitr/internal/domain"
	lighterclient "github.com/elliottech/lighter-go/client"
	lighterhttp "github.com/elliottech/lighter-go/client/http"
	"github.com/elliottech/lighter-go/types"
	"github.com/elliottech/lighter-go/types/txtypes"
	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
)

type LighterConfig struct {
	BaseURL        string
	WSURL          string
	AccountIndex   int64
	APIKeyIndex    uint8
	PrivateKey     string
	MarketIndex    int
	ChainID        uint32
	EnableTrading  bool
	RequestTimeout time.Duration
	MaxReconnect   int
	InitialDelay   time.Duration
	MaxDelay       time.Duration
}

type Lighter struct {
	cfg         LighterConfig
	httpClient  *http.Client
	txClient    *lighterclient.TxClient
	mu          sync.Mutex
	nonceMu     sync.Mutex
	nextNonce   *int64
	orders      map[string]domain.ExchangeOrder
	orderIndex  map[string]int64
	clientIndex map[string]int64
	remaining   map[string]decimal.Decimal
	ws          *websocket.Conn
	wsCancel    context.CancelFunc
	market      domain.MarketSpec
}

func NewLighter(cfg LighterConfig) *Lighter {
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 10 * time.Second
	}
	if cfg.MaxReconnect <= 0 {
		cfg.MaxReconnect = 10
	}
	if cfg.InitialDelay <= 0 {
		cfg.InitialDelay = time.Second
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 30 * time.Second
	}
	return &Lighter{
		cfg:         cfg,
		httpClient:  &http.Client{Timeout: cfg.RequestTimeout},
		orders:      make(map[string]domain.ExchangeOrder),
		orderIndex:  make(map[string]int64),
		clientIndex: make(map[string]int64),
		remaining:   make(map[string]decimal.Decimal),
	}
}

func (l *Lighter) Connect(ctx context.Context) error {
	if l.cfg.PrivateKey == "" {
		return errors.New("LIGHTER_API_PRIVATE_KEY is not configured")
	}
	if l.cfg.AccountIndex <= 0 {
		return errors.New("LIGHTER_ACCOUNT_INDEX must be positive")
	}
	httpAPI := lighterhttp.NewClient(strings.TrimRight(l.cfg.BaseURL, "/"))
	txClient, err := lighterclient.NewTxClient(httpAPI, l.cfg.PrivateKey, l.cfg.AccountIndex, l.cfg.APIKeyIndex, l.cfg.ChainID)
	if err != nil {
		return fmt.Errorf("create Lighter signer: %w", err)
	}
	if err := txClient.Check(); err != nil {
		return fmt.Errorf("validate Lighter API key: %w", err)
	}
	l.txClient = txClient
	return nil
}

func (l *Lighter) Close() error {
	l.mu.Lock()
	if l.wsCancel != nil {
		l.wsCancel()
		l.wsCancel = nil
	}
	if l.ws != nil {
		_ = l.ws.Close()
		l.ws = nil
	}
	l.mu.Unlock()
	return nil
}

func (l *Lighter) SetMarket(market domain.MarketSpec) { l.market = market }

func (l *Lighter) Markets(ctx context.Context) ([]domain.MarketDescriptor, error) {
	result, err := l.get(ctx, "/api/v1/orderBookDetails", url.Values{"filter": {"perp"}}, nil)
	if err != nil {
		return nil, err
	}
	items := rawList(result["order_book_details"])
	markets := make([]domain.MarketDescriptor, 0, len(items))
	for _, item := range items {
		symbol := stringValue(item["symbol"])
		index, ok := intValue(item["market_id"])
		if !ok || symbol == "" {
			continue
		}
		priceDecimals := intOr(item["price_decimals"], intOr(item["supported_price_decimals"], 0))
		sizeDecimals := intOr(item["size_decimals"], intOr(item["supported_size_decimals"], 0))
		priceStep := decimal.New(1, -int32(priceDecimals))
		sizeStep := decimal.New(1, -int32(sizeDecimals))
		last := decimalValue(item["last_trade_price"])
		markets = append(markets, domain.MarketDescriptor{
			DEX: "lighter", MarketIndex: index, Symbol: symbol + "-USD", ExchangeSymbol: symbol,
			BaseAsset: symbol, QuoteAsset: "USD", MarketType: stringOr(item["market_type"], "perp"),
			PriceStep: priceStep, SizeStep: sizeStep,
			MinBaseAmount: decimalValue(item["min_base_amount"]), MinQuoteAmount: decimalValue(item["min_quote_amount"]),
			MakerFeeRate: decimal.RequireFromString("0.00005"), TakerFeeRate: decimal.RequireFromString("0.00005"), FeeRateKnown: true, LastPrice: &last,
		})
	}
	return markets, nil
}

func (l *Lighter) AccountSnapshot(ctx context.Context) (domain.AccountSnapshot, error) {
	result, err := l.get(ctx, "/api/v1/account", url.Values{
		"by": {"index"}, "value": {strconv.FormatInt(l.cfg.AccountIndex, 10)}, "active_only": {"true"},
	}, nil)
	if err != nil {
		return domain.AccountSnapshot{}, err
	}
	accounts := rawList(result["accounts"])
	var account map[string]any
	for _, candidate := range accounts {
		if index, ok := intValue(firstValue(candidate, "account_index", "index")); ok && int64(index) == l.cfg.AccountIndex {
			account = candidate
			break
		}
	}
	if account == nil && len(accounts) > 0 {
		account = accounts[0]
	}
	if account == nil {
		return domain.AccountSnapshot{}, errors.New("Lighter account snapshot is unavailable")
	}
	positions := make([]domain.PositionSnapshot, 0)
	for _, raw := range rawList(account["positions"]) {
		sign := intOr(raw["sign"], 0)
		side := "flat"
		if sign > 0 {
			side = "long"
		} else if sign < 0 {
			side = "short"
		}
		positions = append(positions, domain.PositionSnapshot{
			MarketIndex: intOr(firstValue(raw, "market_id", "market_index"), 0), Symbol: stringValue(raw["symbol"]), Side: side,
			Quantity: decimalValue(raw["position"]).Abs(), AverageEntryPrice: decimalValue(raw["avg_entry_price"]),
			PositionValue: decimalValue(raw["position_value"]), UnrealizedPnl: decimalValue(raw["unrealized_pnl"]),
			RealizedPnl: decimalValue(raw["realized_pnl"]), LiquidationPrice: decimalValue(raw["liquidation_price"]),
			AllocatedMargin: decimalValue(raw["allocated_margin"]), MarginMode: intOr(raw["margin_mode"], 0),
		})
	}
	assets := make([]domain.AssetSnapshot, 0)
	for _, raw := range rawList(account["assets"]) {
		assets = append(assets, domain.AssetSnapshot{
			Symbol: stringValue(raw["symbol"]), AssetID: intOr(raw["asset_id"], 0), Balance: decimalValue(raw["balance"]),
			LockedBalance: decimalValue(raw["locked_balance"]), MarginBalance: decimalValue(raw["margin_balance"]),
			MarginMode: stringValue(raw["margin_mode"]),
		})
	}
	return domain.AccountSnapshot{
		AccountIndex: int(l.cfg.AccountIndex), Status: intOr(account["status"], 0),
		Collateral: decimalValue(account["collateral"]), AvailableBalance: decimalValue(account["available_balance"]),
		TotalAssetValue: decimalValue(account["total_asset_value"]), CrossAssetValue: decimalValue(account["cross_asset_value"]),
		CrossInitialMarginRequirement:     decimalValue(account["cross_initial_margin_requirement"]),
		CrossMaintenanceMarginRequirement: decimalValue(account["cross_maintenance_margin_requirement"]),
		Positions:                         positions, Assets: assets,
	}, nil
}

func (l *Lighter) PlaceOrder(ctx context.Context, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	if !l.cfg.EnableTrading {
		return domain.ExchangeOrder{}, errors.New("Lighter trading is disabled")
	}
	if l.txClient == nil {
		return domain.ExchangeOrder{}, errors.New("Lighter is not connected")
	}
	baseAmount, err := l.toUnits(intent.Quantity, l.market.SizeStep)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	price := intent.Price
	orderType := uint8(txtypes.LimitOrder)
	tif := uint8(txtypes.GoodTillTime)
	trigger := decimal.Zero
	expiry := time.Now().Add(28 * 24 * time.Hour).UnixMilli()
	if intent.OrderType == domain.OrderTypeMarket {
		price, err = l.marketPriceLimit(ctx, intent)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
		orderType = txtypes.MarketOrder
		tif = txtypes.ImmediateOrCancel
		expiry = txtypes.NilOrderExpiry
	} else if intent.PostOnly {
		tif = txtypes.PostOnly
	}
	if intent.Role == domain.OrderRoleTakeProfit {
		orderType = txtypes.TakeProfitLimitOrder
		if intent.TriggerPrice != nil {
			trigger = *intent.TriggerPrice
		} else {
			trigger = intent.Price
		}
		price, err = takeProfitMakerPrice(trigger, intent.Side, l.market.PriceStep)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
		tif = txtypes.PostOnly
	}
	priceUnits, err := l.toPriceUnits(price, l.market.PriceStep)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	triggerUnits := int64(0)
	if !trigger.IsZero() {
		triggerUnits, err = l.toPriceUnits(trigger, l.market.PriceStep)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
	}
	clientIndex := clientOrderIndex(intent.ClientOrderID)
	nonce, err := l.reserveNonce(ctx)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	ops := l.ops(nonce)
	tx, err := l.txClient.GetCreateOrderTransaction(&types.CreateOrderTxReq{
		MarketIndex: int16(intent.MarketIndex), ClientOrderIndex: clientIndex, BaseAmount: baseAmount,
		Price: uint32(priceUnits), IsAsk: boolByte(intent.Side == domain.OrderSideSell), Type: orderType,
		TimeInForce: tif, ReduceOnly: boolByte(intent.ReduceOnly), TriggerPrice: uint32(triggerUnits), OrderExpiry: expiry,
	}, ops)
	if err != nil {
		return domain.ExchangeOrder{}, fmt.Errorf("sign create order: %w", err)
	}
	if err := l.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); err != nil {
		return domain.ExchangeOrder{}, err
	}
	return l.remember(intent, clientIndex), nil
}

func (l *Lighter) PlaceOrdersBatch(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	if len(intents) == 0 {
		return []domain.ExchangeOrder{}, nil
	}
	if len(intents) > 10 {
		orders := make([]domain.ExchangeOrder, 0, len(intents))
		for start := 0; start < len(intents); start += 10 {
			end := start + 10
			if end > len(intents) {
				end = len(intents)
			}
			batch, err := l.PlaceOrdersBatch(ctx, intents[start:end])
			if err != nil {
				return orders, err
			}
			orders = append(orders, batch...)
		}
		return orders, nil
	}
	for _, intent := range intents {
		if intent.OrderType == domain.OrderTypeMarket {
			orders := make([]domain.ExchangeOrder, 0, len(intents))
			for _, item := range intents {
				order, err := l.PlaceOrder(ctx, item)
				if err != nil {
					return orders, err
				}
				orders = append(orders, order)
			}
			return orders, nil
		}
	}
	if !l.cfg.EnableTrading {
		return nil, errors.New("Lighter trading is disabled")
	}
	txTypes := make([]int, 0, len(intents))
	txInfos := make([]string, 0, len(intents))
	indices := make([]int64, 0, len(intents))
	for _, intent := range intents {
		baseAmount, err := l.toUnits(intent.Quantity, l.market.SizeStep)
		if err != nil {
			return nil, err
		}
		price := intent.Price
		trigger := decimal.Zero
		orderType := uint8(txtypes.LimitOrder)
		tif := uint8(txtypes.GoodTillTime)
		if intent.Role == domain.OrderRoleTakeProfit {
			orderType = txtypes.TakeProfitLimitOrder
			if intent.TriggerPrice != nil {
				trigger = *intent.TriggerPrice
			} else {
				trigger = intent.Price
			}
			price, err = takeProfitMakerPrice(trigger, intent.Side, l.market.PriceStep)
			if err != nil {
				return nil, err
			}
			tif = uint8(txtypes.PostOnly)
		} else if intent.PostOnly {
			tif = uint8(txtypes.PostOnly)
		}
		priceUnits, err := l.toPriceUnits(price, l.market.PriceStep)
		if err != nil {
			return nil, err
		}
		triggerUnits := int64(0)
		if !trigger.IsZero() {
			triggerUnits, err = l.toPriceUnits(trigger, l.market.PriceStep)
			if err != nil {
				return nil, err
			}
		}
		index := clientOrderIndex(intent.ClientOrderID)
		nonce, err := l.reserveNonce(ctx)
		if err != nil {
			return nil, err
		}
		tx, err := l.txClient.GetCreateOrderTransaction(&types.CreateOrderTxReq{
			MarketIndex: int16(intent.MarketIndex), ClientOrderIndex: index, BaseAmount: baseAmount,
			Price: uint32(priceUnits), IsAsk: boolByte(intent.Side == domain.OrderSideSell), Type: orderType,
			TimeInForce: tif, ReduceOnly: boolByte(intent.ReduceOnly), TriggerPrice: uint32(triggerUnits),
			OrderExpiry: time.Now().Add(28 * 24 * time.Hour).UnixMilli(),
		}, l.ops(nonce))
		if err != nil {
			return nil, fmt.Errorf("sign batch order: %w", err)
		}
		txTypes = append(txTypes, int(tx.GetTxType()))
		txInfos = append(txInfos, txInfo(tx))
		indices = append(indices, index)
	}
	if err := l.sendTxBatch(ctx, txTypes, txInfos); err != nil {
		return nil, err
	}
	orders := make([]domain.ExchangeOrder, 0, len(intents))
	for i, intent := range intents {
		orders = append(orders, l.remember(intent, indices[i]))
	}
	return orders, nil
}

func (l *Lighter) ModifyOrder(ctx context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	if !l.cfg.EnableTrading {
		return domain.ExchangeOrder{}, errors.New("Lighter trading is disabled")
	}
	l.mu.Lock()
	current, ok := l.orders[clientOrderID]
	index := l.orderIndex[clientOrderID]
	l.mu.Unlock()
	if !ok {
		return domain.ExchangeOrder{}, fmt.Errorf("unknown Lighter client order: %s", clientOrderID)
	}
	nonce, err := l.reserveNonce(ctx)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	trigger := decimal.Zero
	price := intent.Price
	if intent.Role == domain.OrderRoleTakeProfit {
		trigger = intent.Price
		if intent.TriggerPrice != nil {
			trigger = *intent.TriggerPrice
		}
		price, err = takeProfitMakerPrice(trigger, intent.Side, l.market.PriceStep)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
	}
	baseAmount, err := l.toUnits(intent.Quantity, l.market.SizeStep)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	priceUnits, err := l.toPriceUnits(price, l.market.PriceStep)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	triggerUnits := int64(0)
	if !trigger.IsZero() {
		triggerUnits, err = l.toPriceUnits(trigger, l.market.PriceStep)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
	}
	tx, err := l.txClient.GetModifyOrderTransaction(&types.ModifyOrderTxReq{
		MarketIndex: int16(intent.MarketIndex), Index: index, BaseAmount: baseAmount,
		Price: uint32(priceUnits), TriggerPrice: uint32(triggerUnits),
	}, l.ops(nonce))
	if err != nil {
		return domain.ExchangeOrder{}, fmt.Errorf("sign modify order: %w", err)
	}
	if err := l.sendTx(ctx, int(tx.GetTxType()), txInfo(tx)); err != nil {
		return domain.ExchangeOrder{}, err
	}
	updated := current
	updated.Price, updated.Quantity, updated.ReduceOnly, updated.PostOnly, updated.OrderType = price, intent.Quantity, intent.ReduceOnly, intent.PostOnly, intent.OrderType
	l.mu.Lock()
	l.orders[clientOrderID] = updated
	l.remaining[clientOrderID] = intent.Quantity
	l.mu.Unlock()
	return updated, nil
}

func (l *Lighter) CancelOrder(ctx context.Context, clientOrderID string) error {
	return l.CancelOrdersBatch(ctx, []string{clientOrderID})
}

func (l *Lighter) CancelOrdersBatch(ctx context.Context, ids []string) error {
	if !l.cfg.EnableTrading {
		return errors.New("Lighter trading is disabled")
	}
	unique := uniqueStrings(ids)
	for start := 0; start < len(unique); start += 10 {
		end := start + 10
		if end > len(unique) {
			end = len(unique)
		}
		if err := l.cancelChunk(ctx, unique[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (l *Lighter) OpenOrders(ctx context.Context) ([]domain.ExchangeOrder, error) {
	l.mu.Lock()
	local := make(map[string]domain.ExchangeOrder, len(l.orders))
	for clientID, order := range l.orders {
		local[clientID] = order
	}
	l.mu.Unlock()
	if l.txClient == nil {
		return mapOrders(local), nil
	}
	token, err := l.txClient.GetAuthToken(time.Now().Add(30 * time.Second))
	if err != nil {
		return nil, fmt.Errorf("create Lighter read token: %w", err)
	}
	marketID := l.market.MarketIndex
	if marketID == 0 {
		marketID = l.cfg.MarketIndex
	}
	result, err := l.get(ctx, "/api/v1/accountActiveOrders", url.Values{
		"account_index": {strconv.FormatInt(l.cfg.AccountIndex, 10)},
		"market_id":     {strconv.Itoa(marketID)},
		"market_type":   {"perp"},
	}, http.Header{"authorization": {token}})
	if err != nil {
		return nil, err
	}
	return l.mergeRemoteOrders(local, rawList(result["orders"])), nil
}

func (l *Lighter) mergeRemoteOrders(local map[string]domain.ExchangeOrder, remote []map[string]any) []domain.ExchangeOrder {
	l.mu.Lock()
	defer l.mu.Unlock()
	merged := make(map[string]domain.ExchangeOrder, len(local)+len(remote))
	for clientID, order := range local {
		merged[clientID] = order
	}
	for _, raw := range remote {
		orderIndex, _ := int64Value(firstValue(raw, "order_index", "index"))
		clientIndex, _ := int64Value(firstValue(raw, "client_order_index", "client_index"))
		exchangeID := stringValue(firstValue(raw, "order_id", "order_index", "index"))
		clientID := ""
		if clientIndex != 0 {
			for candidate, index := range l.clientIndex {
				if index == clientIndex {
					clientID = candidate
					break
				}
			}
		}
		if clientID == "" {
			clientID = stringValue(raw["client_order_id"])
		}
		if clientID == "" {
			if clientIndex != 0 {
				clientID = strconv.FormatInt(clientIndex, 10)
			} else if orderIndex != 0 {
				// Some account-active-orders responses omit the client index.
				// Keep the exchange order index as a stable local key so the
				// position watchdog can still cancel the remote order.
				clientID = strconv.FormatInt(orderIndex, 10)
			}
		}
		if clientID == "" || clientID == "0" {
			continue
		}
		order, exists := merged[clientID]
		if !exists {
			order = domain.ExchangeOrder{ClientOrderID: clientID, Role: domain.OrderRoleInitial, Level: 0}
		}
		order.ExchangeOrderID = exchangeID
		order.MarketIndex = intOr(firstValue(raw, "market_index", "market_id"), l.cfg.MarketIndex)
		order.Side = domain.OrderSideBuy
		if boolValue(raw["is_ask"]) || strings.EqualFold(stringValue(raw["side"]), "ask") || strings.EqualFold(stringValue(raw["side"]), "sell") {
			order.Side = domain.OrderSideSell
		}
		order.Price = decimalValue(raw["price"])
		order.Quantity = decimalValue(firstValue(raw, "remaining_base_amount", "initial_base_amount"))
		order.ReduceOnly = boolValue(raw["reduce_only"])
		order.OrderType = domain.OrderTypeLimit
		if strings.Contains(strings.ToLower(stringValue(raw["type"])), "market") {
			order.OrderType = domain.OrderTypeMarket
		}
		order.Status = stringOr(raw["status"], "open")
		if orderIndex != 0 {
			l.orderIndex[clientID] = orderIndex
		}
		if clientIndex != 0 {
			l.clientIndex[clientID] = clientIndex
		}
		l.orders[clientID] = order
		l.remaining[clientID] = order.Quantity
		merged[clientID] = order
	}
	return mapOrders(merged)
}

func mapOrders(orders map[string]domain.ExchangeOrder) []domain.ExchangeOrder {
	result := make([]domain.ExchangeOrder, 0, len(orders))
	for _, order := range orders {
		result = append(result, order)
	}
	return result
}

func (l *Lighter) ProcessPrice(context.Context, string) ([]domain.Fill, error) {
	return nil, errors.New("Lighter fills are consumed from the authenticated WebSocket")
}

func (l *Lighter) SubscribeFills(ctx context.Context) (<-chan domain.Fill, <-chan error) {
	fills := make(chan domain.Fill, 64)
	errs := make(chan error, 1)
	wsCtx, cancel := context.WithCancel(ctx)
	l.mu.Lock()
	if l.wsCancel != nil {
		l.wsCancel()
	}
	l.wsCancel = cancel
	l.mu.Unlock()
	go l.runWebSocket(wsCtx, fills, errs)
	return fills, errs
}

func (l *Lighter) runWebSocket(ctx context.Context, fills chan<- domain.Fill, errs chan<- error) {
	defer close(fills)
	defer close(errs)
	dialer := websocket.DefaultDialer
	delay := l.cfg.InitialDelay
	if delay <= 0 {
		delay = time.Second
	}
	attempts := 0
	for {
		if ctx.Err() != nil {
			return
		}
		conn, _, err := dialer.DialContext(ctx, strings.TrimRight(l.cfg.WSURL, "/"), nil)
		if err == nil {
			attempts = 0
			delay = l.cfg.InitialDelay
			l.mu.Lock()
			l.ws = conn
			l.mu.Unlock()
			err = l.consumeWebSocket(ctx, conn, fills)
			_ = conn.Close()
			l.mu.Lock()
			if l.ws == conn {
				l.ws = nil
			}
			l.mu.Unlock()
			if ctx.Err() != nil {
				return
			}
		}
		if err == nil {
			err = errors.New("Lighter WebSocket closed")
		}
		if attempts >= l.cfg.MaxReconnect {
			select {
			case errs <- err:
			default:
			}
			return
		}
		attempts++
		if !waitForReconnect(ctx, delay) {
			return
		}
		delay *= 2
		if delay > l.cfg.MaxDelay {
			delay = l.cfg.MaxDelay
		}
	}
}

func (l *Lighter) consumeWebSocket(ctx context.Context, conn *websocket.Conn, fills chan<- domain.Fill) error {
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	subscribed := false
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}
		var message map[string]any
		if json.Unmarshal(data, &message) != nil {
			continue
		}
		messageType := stringValue(message["type"])
		if messageType == "ping" {
			if err := conn.WriteJSON(map[string]string{"type": "pong"}); err != nil {
				return err
			}
			continue
		}
		if messageType == "connected" && !subscribed {
			if err := conn.WriteJSON(map[string]any{"type": "subscribe", "channel": fmt.Sprintf("account_all/%d", l.cfg.AccountIndex)}); err != nil {
				return err
			}
			subscribed = true
			_ = conn.SetReadDeadline(time.Time{})
			continue
		}
		if !subscribed {
			continue
		}
		for _, fill := range l.fillsFromMessage(message) {
			select {
			case fills <- fill:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func waitForReconnect(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (l *Lighter) fillsFromMessage(message map[string]any) []domain.Fill {
	messageType := stringValue(message["type"])
	if messageType != "update/account_all" && messageType != "subscribed/account_all" {
		return nil
	}
	containers := []map[string]any{message}
	for _, key := range []string{"account", "data"} {
		if nested, ok := message[key].(map[string]any); ok {
			containers = append(containers, nested)
		}
	}
	fills := make([]domain.Fill, 0)
	for _, container := range containers {
		trades := container["trades"]
		if mapped, ok := trades.(map[string]any); ok {
			trades = mapped[strconv.Itoa(l.cfg.MarketIndex)]
			if trades == nil {
				trades = mapped[fmt.Sprint(l.cfg.MarketIndex)]
			}
			if trades == nil && (mapped["trade_id"] != nil || mapped["price"] != nil) {
				trades = mapped
			}
		}
		for _, item := range rawList(trades) {
			if fill, ok := l.fillFromTrade(item); ok {
				fills = append(fills, fill)
			}
		}
	}
	return fills
}

func (l *Lighter) fillFromTrade(trade map[string]any) (domain.Fill, bool) {
	tradeID := stringValue(firstValue(trade, "trade_id", "trade_id_str"))
	if tradeID == "" {
		return domain.Fill{}, false
	}
	account := strconv.FormatInt(l.cfg.AccountIndex, 10)
	side := domain.OrderSideBuy
	orderID := ""
	clientIndexValue := any(nil)
	if stringValue(trade["ask_account_id"]) == account {
		side = domain.OrderSideSell
		orderID = stringValue(firstValue(trade, "ask_id", "ask_id_str"))
		clientIndexValue = firstValue(trade, "ask_client_id", "ask_client_id_str")
	} else if stringValue(trade["bid_account_id"]) == account {
		side = domain.OrderSideBuy
		orderID = stringValue(firstValue(trade, "bid_id", "bid_id_str"))
		clientIndexValue = firstValue(trade, "bid_client_id", "bid_client_id_str")
	} else {
		return domain.Fill{}, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var clientID string
	if index, ok := int64Value(clientIndexValue); ok {
		for candidate, value := range l.clientIndex {
			if value == index {
				clientID = candidate
				break
			}
		}
	}
	if clientID == "" {
		for candidate, order := range l.orders {
			if order.ExchangeOrderID == orderID {
				clientID = candidate
				break
			}
		}
	}
	if clientID == "" {
		for candidate, order := range l.orders {
			if order.MarketIndex == l.cfg.MarketIndex && order.Role == domain.OrderRoleInitial && order.OrderType == domain.OrderTypeMarket && order.Side == side {
				clientID = candidate
				break
			}
		}
	}
	if clientID == "" {
		return domain.Fill{}, false
	}
	order := l.orders[clientID]
	quantity := decimalValue(trade["size"])
	if remain, ok := l.remaining[clientID]; ok && quantity.GreaterThan(remain) {
		quantity = remain
	}
	if !quantity.GreaterThan(decimal.Zero) {
		return domain.Fill{}, false
	}
	if remain, ok := l.remaining[clientID]; ok {
		remain = remain.Sub(quantity)
		if remain.LessThanOrEqual(decimal.Zero) {
			delete(l.orders, clientID)
			delete(l.orderIndex, clientID)
			delete(l.clientIndex, clientID)
			delete(l.remaining, clientID)
		} else {
			l.remaining[clientID] = remain
		}
	}
	return domain.Fill{
		ExchangeOrderID: order.ExchangeOrderID, ClientOrderID: clientID, MarketIndex: order.MarketIndex,
		Side: side, Role: order.Role, Level: order.Level, Price: decimalValue(trade["price"]), Quantity: quantity, TradeID: tradeID,
	}, true
}

func (l *Lighter) reserveNonce(ctx context.Context) (int64, error) {
	l.nonceMu.Lock()
	defer l.nonceMu.Unlock()
	if l.nextNonce == nil {
		api := lighterhttp.NewClient(strings.TrimRight(l.cfg.BaseURL, "/"))
		nonce, err := api.GetNextNonce(l.cfg.AccountIndex, l.cfg.APIKeyIndex)
		if err != nil {
			return 0, err
		}
		l.nextNonce = &nonce
	}
	nonce := *l.nextNonce
	next := nonce + 1
	l.nextNonce = &next
	return nonce, nil
}

func (l *Lighter) ops(nonce int64) *types.TransactOpts {
	account := l.cfg.AccountIndex
	apiKey := l.cfg.APIKeyIndex
	return &types.TransactOpts{FromAccountIndex: &account, ApiKeyIndex: &apiKey, Nonce: &nonce, ExpiredAt: time.Now().Add(10 * time.Minute).UnixMilli()}
}

func (l *Lighter) sendTx(ctx context.Context, txType int, info string) error {
	_, err := l.postForm(ctx, "/api/v1/sendTx", url.Values{"tx_type": {strconv.Itoa(txType)}, "tx_info": {info}})
	return err
}

func (l *Lighter) sendTxBatch(ctx context.Context, txTypes []int, infos []string) error {
	typesJSON, _ := json.Marshal(txTypes)
	infosJSON, _ := json.Marshal(infos)
	_, err := l.postForm(ctx, "/api/v1/sendTxBatch", url.Values{"tx_types": {string(typesJSON)}, "tx_infos": {string(infosJSON)}})
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "invalid nonce") {
		l.nonceMu.Lock()
		l.nextNonce = nil
		l.nonceMu.Unlock()
	}
	return err
}

func (l *Lighter) cancelChunk(ctx context.Context, ids []string) error {
	txTypes := make([]int, 0, len(ids))
	infos := make([]string, 0, len(ids))
	indices := make([]string, 0, len(ids))
	for _, clientID := range ids {
		l.mu.Lock()
		index, ok := l.orderIndex[clientID]
		l.mu.Unlock()
		if !ok {
			continue
		}
		nonce, err := l.reserveNonce(ctx)
		if err != nil {
			return err
		}
		tx, err := l.txClient.GetCancelOrderTransaction(&types.CancelOrderTxReq{MarketIndex: int16(l.cfg.MarketIndex), Index: index}, l.ops(nonce))
		if err != nil {
			return err
		}
		txTypes = append(txTypes, int(tx.GetTxType()))
		infos = append(infos, txInfo(tx))
		indices = append(indices, clientID)
	}
	if len(infos) == 0 {
		return nil
	}
	if err := l.sendTxBatch(ctx, txTypes, infos); err != nil {
		return err
	}
	l.mu.Lock()
	for _, clientID := range indices {
		delete(l.orders, clientID)
		delete(l.orderIndex, clientID)
		delete(l.clientIndex, clientID)
		delete(l.remaining, clientID)
	}
	l.mu.Unlock()
	return nil
}

func (l *Lighter) remember(intent domain.OrderIntent, clientIndex int64) domain.ExchangeOrder {
	order := domain.ExchangeOrder{ExchangeOrderID: strconv.FormatInt(clientIndex, 10), ClientOrderID: intent.ClientOrderID, MarketIndex: intent.MarketIndex, Side: intent.Side, Role: intent.Role, Level: intent.Level, Price: intent.Price, Quantity: intent.Quantity, ReduceOnly: intent.ReduceOnly, PostOnly: intent.PostOnly, OrderType: intent.OrderType, Status: "open"}
	l.mu.Lock()
	l.orders[intent.ClientOrderID] = order
	l.orderIndex[intent.ClientOrderID] = clientIndex
	l.clientIndex[intent.ClientOrderID] = clientIndex
	l.remaining[intent.ClientOrderID] = intent.Quantity
	l.mu.Unlock()
	return order
}

func (l *Lighter) marketPriceLimit(ctx context.Context, intent domain.OrderIntent) (decimal.Decimal, error) {
	book, err := l.OrderBook(ctx, intent.MarketIndex, intent.Symbol)
	if err != nil {
		return decimal.Zero, err
	}
	bookSide := domain.OrderSideSell
	if intent.Side == domain.OrderSideSell {
		bookSide = domain.OrderSideBuy
	}
	best, err := book.BestPrice(bookSide)
	if err != nil {
		return decimal.Zero, fmt.Errorf("Lighter order book has no liquidity: %w", err)
	}
	ratio := intent.MaxSlippageRatio
	if ratio.IsZero() {
		ratio = decimal.NewFromFloat(0.005)
	}
	protected := best.Mul(decimal.NewFromInt(1).Add(ratio))
	if intent.Side == domain.OrderSideSell {
		protected = best.Mul(decimal.NewFromInt(1).Sub(ratio))
	}
	return l.roundToStep(protected, l.market.PriceStep, intent.Side == domain.OrderSideBuy), nil
}

func (l *Lighter) OrderBook(ctx context.Context, marketIndex int, _ string) (domain.OrderBook, error) {
	if marketIndex == 0 {
		marketIndex = l.market.MarketIndex
		if marketIndex == 0 {
			marketIndex = l.cfg.MarketIndex
		}
	}
	result, err := l.get(ctx, "/api/v1/orderBookOrders", url.Values{"market_id": {strconv.Itoa(marketIndex)}, "limit": {"100"}}, nil)
	if err != nil {
		return domain.OrderBook{}, err
	}
	book := domain.OrderBook{Bids: make([]domain.OrderBookLevel, 0), Asks: make([]domain.OrderBookLevel, 0)}
	for _, item := range rawList(result["bids"]) {
		price := decimalValue(item["price"])
		if price.GreaterThan(decimal.Zero) {
			book.Bids = append(book.Bids, domain.OrderBookLevel{Price: price, Quantity: decimalValue(firstValue(item, "remaining_base_amount", "base_amount", "quantity"))})
		}
	}
	for _, item := range rawList(result["asks"]) {
		price := decimalValue(item["price"])
		if price.GreaterThan(decimal.Zero) {
			book.Asks = append(book.Asks, domain.OrderBookLevel{Price: price, Quantity: decimalValue(firstValue(item, "remaining_base_amount", "base_amount", "quantity"))})
		}
	}
	return book, nil
}

func (l *Lighter) toUnits(value, step decimal.Decimal) (int64, error) {
	if !value.GreaterThan(decimal.Zero) || !step.GreaterThan(decimal.Zero) {
		return 0, errors.New("order value and market step must be positive")
	}
	rawUnits := value.Div(step)
	units := rawUnits.Truncate(0)
	if units.LessThanOrEqual(decimal.Zero) || !rawUnits.Equal(units) {
		return 0, errors.New("order value is below market precision")
	}
	parsed, err := strconv.ParseInt(units.String(), 10, 64)
	if err != nil || parsed <= 0 || parsed > txtypes.MaxOrderBaseAmount {
		return 0, errors.New("order value exceeds Lighter integer range")
	}
	return parsed, nil
}

func (l *Lighter) toPriceUnits(value, step decimal.Decimal) (int64, error) {
	units, err := l.toUnits(value, step)
	if err != nil {
		return 0, err
	}
	if units > int64(txtypes.MaxOrderPrice) {
		return 0, errors.New("order price exceeds Lighter integer range")
	}
	return units, nil
}

func (l *Lighter) roundToStep(value, step decimal.Decimal, ceil bool) decimal.Decimal {
	units := value.Div(step)
	if ceil {
		units = units.Ceil()
	} else {
		units = units.Truncate(0)
	}
	return units.Mul(step)
}

func (l *Lighter) get(ctx context.Context, path string, query url.Values, headers http.Header) (map[string]any, error) {
	requestURL := strings.TrimRight(l.cfg.BaseURL, "/") + path
	if query != nil {
		requestURL += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			request.Header.Add(key, value)
		}
	}
	return l.doJSON(request)
}

func (l *Lighter) postForm(ctx context.Context, path string, form url.Values) (map[string]any, error) {
	requestURL := strings.TrimRight(l.cfg.BaseURL, "/") + path
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return l.doJSON(request)
}

func (l *Lighter) doJSON(request *http.Request) (map[string]any, error) {
	response, err := l.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("Lighter HTTP %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}
	if code, ok := intValue(result["code"]); ok && code != 200 {
		return nil, fmt.Errorf("Lighter API rejected request (code=%d): %s", code, stringValue(result["message"]))
	}
	return result, nil
}

func txInfo(info interface{ GetTxInfo() (string, error) }) string {
	value, _ := info.GetTxInfo()
	return value
}

func (l *Lighter) opsWithNonce(nonce int64) *types.TransactOpts { return l.ops(nonce) }

func clientOrderIndex(id string) int64 {
	sum := sha256.Sum256([]byte(id))
	value := int64(binary.BigEndian.Uint64(sum[:8]) & ((1 << 48) - 1))
	if value == 0 {
		return 1
	}
	return value
}

func (l *Lighter) toMarketSpec(market domain.MarketDescriptor) {
	l.market = domain.MarketSpec{MarketIndex: market.MarketIndex, Symbol: market.Symbol, PriceStep: market.PriceStep, SizeStep: market.SizeStep, MinBaseAmount: market.MinBaseAmount, MinQuoteAmount: market.MinQuoteAmount, ContractMult: decimal.NewFromInt(1)}
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != "" {
			if _, ok := seen[value]; !ok {
				seen[value] = struct{}{}
				result = append(result, value)
			}
		}
	}
	return result
}

func rawList(value any) []map[string]any {
	if value == nil {
		return nil
	}
	if one, ok := value.(map[string]any); ok {
		return []map[string]any{one}
	}
	array, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]map[string]any, 0, len(array))
	for _, item := range array {
		if object, ok := item.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

func firstValue(values map[string]any, keys ...string) any {
	for _, key := range keys {
		if value, ok := values[key]; ok && value != nil {
			return value
		}
	}
	return nil
}
func stringOr(value any, fallback string) string {
	if result := stringValue(value); result != "" {
		return result
	}
	return fallback
}

func boolValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(typed)
		return err == nil && parsed
	case float64:
		return typed != 0
	case int:
		return typed != 0
	case int64:
		return typed != 0
	default:
		return false
	}
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprint(value)
	}
}
func intOr(value any, fallback int) int {
	if parsed, ok := intValue(value); ok {
		return parsed
	}
	return fallback
}
func intValue(value any) (int, bool) {
	parsed, ok := int64Value(value)
	if !ok || parsed > int64(^uint(0)>>1) || parsed < -int64(^uint(0)>>1)-1 {
		return 0, false
	}
	return int(parsed), true
}
func int64Value(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		return int64(typed), true
	case json.Number:
		result, err := typed.Int64()
		return result, err == nil
	case string:
		result, err := strconv.ParseInt(typed, 10, 64)
		return result, err == nil
	}
	return 0, false
}
func decimalValue(value any) decimal.Decimal {
	parsed, err := decimal.NewFromString(stringValue(value))
	if err != nil {
		return decimal.Zero
	}
	return parsed
}

var _ Exchange = (*Lighter)(nil)
