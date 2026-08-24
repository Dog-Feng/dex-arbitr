package exchange

import (
	"bytes"
	"context"
	"crypto/ecdsa"
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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/shopspring/decimal"
	sodexclient "github.com/sodex-tech/sodex-go-sdk-public/client"
	"github.com/sodex-tech/sodex-go-sdk-public/common/enums"
	sodexptypes "github.com/sodex-tech/sodex-go-sdk-public/perps/types"
	sodexws "github.com/sodex-tech/sodex-go-sdk-public/ws"
)

// SodexConfig contains the connection and account settings for the SoDEX
// perpetuals engine. AccountAddress is required for read-only account calls;
// it can be derived from PrivateKey when trading is enabled.
type SodexConfig struct {
	BaseURL        string
	AccountAddress string
	AccountID      uint64
	PrivateKey     string
	APIKeyName     string
	ChainID        uint64
	MarketIndex    int
	EnableTrading  bool
	RequestTimeout time.Duration
}

// Sodex implements the project's Exchange interface with the official SoDEX
// Go SDK. SoDEX perps are one-way/netted positions, therefore every order is
// submitted with PositionSide=BOTH and its buy/sell side controls the net.
type Sodex struct {
	cfg             SodexConfig
	client          *sodexclient.Client
	http            *http.Client
	mu              sync.Mutex
	market          domain.MarketSpec
	markets         map[int]domain.MarketDescriptor
	symbols         map[int]sodexclient.Symbol
	symbolIDs       map[string]uint64
	orders          map[string]domain.ExchangeOrder
	orderIDs        map[string]uint64
	byOrderID       map[uint64]string
	remaining       map[string]decimal.Decimal
	orderUpdatedAt  map[string]time.Time
	processedTrades map[int64]struct{}
	account         string
	ws              *sodexws.Client
	wsCancel        context.CancelFunc
}

type sodexCancelTarget struct {
	clientOrderID string
	orderID       uint64
	symbolID      uint64
}

func NewSodex(cfg SodexConfig) *Sodex {
	if cfg.BaseURL == "" {
		cfg.BaseURL = sodexclient.DefaultBaseURL
	}
	if cfg.ChainID == 0 {
		cfg.ChainID = sodexclient.DefaultChainID
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	return &Sodex{
		cfg:             cfg,
		markets:         make(map[int]domain.MarketDescriptor),
		symbols:         make(map[int]sodexclient.Symbol),
		symbolIDs:       make(map[string]uint64),
		orders:          make(map[string]domain.ExchangeOrder),
		orderIDs:        make(map[string]uint64),
		byOrderID:       make(map[uint64]string),
		remaining:       make(map[string]decimal.Decimal),
		orderUpdatedAt:  make(map[string]time.Time),
		processedTrades: make(map[int64]struct{}),
	}
}

func (s *Sodex) Connect(ctx context.Context) error {
	var privateKey *ecdsa.PrivateKey
	if strings.TrimSpace(s.cfg.PrivateKey) != "" {
		keyText := strings.TrimPrefix(strings.TrimSpace(s.cfg.PrivateKey), "0x")
		key, err := crypto.HexToECDSA(keyText)
		if err != nil {
			return fmt.Errorf("parse SODEX_PRIVATE_KEY: %w", err)
		}
		privateKey = key
	}
	if s.cfg.EnableTrading && privateKey == nil {
		return errors.New("SODEX_ENABLE_TRADING is true but SODEX_PRIVATE_KEY is empty")
	}
	if s.cfg.EnableTrading && strings.TrimSpace(s.cfg.APIKeyName) == "" {
		return errors.New("SODEX_ENABLE_TRADING is true but SODEX_API_KEY_NAME is empty")
	}
	httpClient := &http.Client{Timeout: s.cfg.RequestTimeout}
	s.http = httpClient
	s.client = sodexclient.New(sodexclient.Config{
		BaseURL:    strings.TrimRight(s.cfg.BaseURL, "/"),
		ChainID:    s.cfg.ChainID,
		PrivateKey: privateKey,
		APIKeyName: s.cfg.APIKeyName,
		HTTPClient: httpClient,
	})
	s.account = strings.TrimSpace(s.cfg.AccountAddress)
	if s.account == "" && s.client != nil {
		s.account = s.client.Address()
	}
	if s.account == "" {
		return errors.New("SODEX_ACCOUNT_ADDRESS is required for account and position monitoring")
	}
	if s.cfg.EnableTrading && s.cfg.AccountID == 0 {
		if err := s.discoverAccountID(ctx); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *Sodex) discoverAccountID(ctx context.Context) error {
	requestURL := strings.TrimRight(s.cfg.BaseURL, "/") + "/api/v1/perps/accounts/" + url.PathEscape(s.account) + "/state"
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return fmt.Errorf("build SoDEX account state request: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	response, err := s.http.Do(request)
	if err != nil {
		return fmt.Errorf("query SoDEX account state: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("read SoDEX account state: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("SoDEX account state HTTP %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	var envelope struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			AccountID    uint64 `json:"aid"`
			AccountIDAlt uint64 `json:"accountID"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode SoDEX account state: %w", err)
	}
	if envelope.Code != 0 {
		return fmt.Errorf("SoDEX account state rejected (code=%d): %s", envelope.Code, envelope.Message)
	}
	s.cfg.AccountID = envelope.Data.AccountID
	if s.cfg.AccountID == 0 {
		s.cfg.AccountID = envelope.Data.AccountIDAlt
	}
	if s.cfg.AccountID == 0 {
		return errors.New("SoDEX account state did not return aid; set SODEX_ACCOUNT_ID only for a sub-account")
	}
	return nil
}

func (s *Sodex) Close() error {
	s.mu.Lock()
	if s.wsCancel != nil {
		s.wsCancel()
		s.wsCancel = nil
	}
	if s.ws != nil {
		_ = s.ws.Close()
		s.ws = nil
	}
	s.mu.Unlock()
	return nil
}

func (s *Sodex) SetMarket(market domain.MarketSpec) { s.market = market }

// SetLeverage synchronizes the strategy leverage with the SoDEX perps
// account before the first order is submitted. The current application uses
// cross margin; SoDEX's perps engine remains one-way/netted.
func (s *Sodex) SetLeverage(ctx context.Context, marketIndex int, leverage decimal.Decimal) error {
	if !s.cfg.EnableTrading {
		return errors.New("SoDEX trading is disabled")
	}
	if s.client == nil {
		return errors.New("SoDEX is not connected")
	}
	if !leverage.GreaterThan(decimal.Zero) || !leverage.Equal(leverage.Truncate(0)) || leverage.GreaterThan(decimal.NewFromInt(int64(^uint32(0)))) {
		return fmt.Errorf("SoDEX leverage must be a positive integer: %s", leverage)
	}
	symbolID, err := s.symbolIDForIntent(domain.OrderIntent{MarketIndex: marketIndex})
	if err != nil {
		return err
	}
	_, err = s.client.UpdateLeverage(ctx, &sodexptypes.UpdateLeverageRequest{
		AccountID: s.cfg.AccountID, SymbolID: symbolID, Leverage: uint32(leverage.IntPart()), MarginMode: enums.MarginModeCross,
	})
	if err != nil {
		return fmt.Errorf("SoDEX set leverage %s: %w", leverage, err)
	}
	return nil
}

func (s *Sodex) Markets(ctx context.Context) ([]domain.MarketDescriptor, error) {
	if s.client == nil {
		return nil, errors.New("SoDEX is not connected")
	}
	symbols, err := s.client.PerpsSymbols(ctx)
	if err != nil {
		return nil, fmt.Errorf("SoDEX perps symbols: %w", err)
	}
	tickers, err := s.client.PerpsTickers(ctx)
	if err != nil {
		return nil, fmt.Errorf("SoDEX perps tickers: %w", err)
	}
	tickerBySymbol := make(map[string]sodexclient.Ticker, len(tickers))
	for _, ticker := range tickers {
		tickerBySymbol[strings.ToUpper(strings.TrimSpace(ticker.Symbol))] = ticker
	}

	result := make([]domain.MarketDescriptor, 0, len(symbols))
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, symbol := range symbols {
		if symbol.SymbolID > uint64(^uint(0)>>1) || strings.TrimSpace(symbol.Symbol) == "" {
			continue
		}
		marketIndex := int(symbol.SymbolID)
		priceStep := decimalFromString(symbol.TickSize)
		if !priceStep.GreaterThan(decimal.Zero) && symbol.PricePrecision >= 0 {
			priceStep = decimal.New(1, -int32(symbol.PricePrecision))
		}
		sizeStep := decimalFromString(symbol.StepSize)
		if !sizeStep.GreaterThan(decimal.Zero) && symbol.QuantityPrecision >= 0 {
			sizeStep = decimal.New(1, -int32(symbol.QuantityPrecision))
		}
		contract := decimalFromString(pointerString(symbol.ContractSize))
		if !contract.GreaterThan(decimal.Zero) {
			contract = decimal.NewFromInt(1)
		}
		last := decimal.Zero
		if ticker, ok := tickerBySymbol[strings.ToUpper(strings.TrimSpace(symbol.Symbol))]; ok {
			last = decimalFromString(ticker.LastPrice)
		}
		var lastPtr *decimal.Decimal
		if last.GreaterThan(decimal.Zero) {
			copyLast := last
			lastPtr = &copyLast
		}
		base := symbol.BaseAsset
		if base == "" {
			base = symbol.Symbol
		}
		quote := symbol.QuoteAsset
		if quote == "" {
			quote = "USD"
		}
		makerFeeRate := decimalFromString(symbol.MakerFee)
		takerFeeRate := decimalFromString(symbol.TakerFee)
		market := domain.MarketDescriptor{
			DEX: "sodex", MarketIndex: marketIndex, Symbol: symbol.Symbol, ExchangeSymbol: symbol.Symbol,
			BaseAsset: base, QuoteAsset: quote, MarketType: "perp", PriceStep: priceStep, SizeStep: sizeStep,
			MinBaseAmount: decimalFromString(symbol.MinQuantity), MinQuoteAmount: decimalFromString(symbol.MinNotional),
			ContractMult: contract, MakerFeeRate: makerFeeRate, TakerFeeRate: takerFeeRate,
			FeeRateKnown: strings.TrimSpace(symbol.MakerFee) != "", LastPrice: lastPtr,
		}
		s.markets[marketIndex] = market
		s.symbols[marketIndex] = symbol
		s.symbolIDs[strings.ToUpper(strings.TrimSpace(symbol.Symbol))] = symbol.SymbolID
		result = append(result, market)
	}
	if len(result) == 0 {
		return nil, errors.New("SoDEX returned no usable perpetual markets")
	}
	return result, nil
}

func (s *Sodex) AccountSnapshot(ctx context.Context) (domain.AccountSnapshot, error) {
	if s.client == nil {
		return domain.AccountSnapshot{}, errors.New("SoDEX is not connected")
	}
	balances, err := s.client.PerpsBalances(ctx, s.account)
	if err != nil {
		return domain.AccountSnapshot{}, fmt.Errorf("SoDEX balances: %w", err)
	}
	positions, err := s.client.PerpsPositions(ctx, s.account)
	if err != nil {
		return domain.AccountSnapshot{}, fmt.Errorf("SoDEX positions: %w", err)
	}
	tickers, tickerErr := s.client.PerpsTickers(ctx)
	tickerBySymbol := make(map[string]sodexclient.Ticker, len(tickers))
	if tickerErr == nil {
		for _, ticker := range tickers {
			tickerBySymbol[strings.ToUpper(strings.TrimSpace(ticker.Symbol))] = ticker
		}
	}

	assets := make([]domain.AssetSnapshot, 0, len(balances))
	collateral := decimal.Zero
	available := decimal.Zero
	for _, balance := range balances {
		total := decimalFromString(balance.Total)
		locked := decimalFromString(balance.Locked)
		free := total.Sub(locked)
		if free.LessThan(decimal.Zero) {
			free = decimal.Zero
		}
		assets = append(assets, domain.AssetSnapshot{
			Symbol: balance.Coin, AssetID: int(balance.CoinID), Balance: total, LockedBalance: locked,
			MarginBalance: free, MarginMode: "cross",
		})
		if isStableCoin(balance.Coin) {
			collateral = collateral.Add(total)
			available = available.Add(free)
		}
	}
	if collateral.IsZero() {
		for _, balance := range balances {
			collateral = collateral.Add(decimalFromString(balance.Total))
			available = available.Add(decimalFromString(balance.Total).Sub(decimalFromString(balance.Locked)))
		}
	}

	resultPositions := make([]domain.PositionSnapshot, 0, len(positions))
	for _, position := range positions {
		size := decimalFromString(position.Size)
		if size.IsZero() || !position.Active && size.IsZero() {
			continue
		}
		symbol := strings.TrimSpace(position.Symbol)
		marketIndex, market, ok := s.marketForSymbol(symbol)
		if !ok {
			marketIndex = s.cfg.MarketIndex
		}
		entry := decimalFromString(position.AvgEntryPrice)
		mark := decimal.Zero
		if ticker, exists := tickerBySymbol[strings.ToUpper(symbol)]; exists {
			if ticker.MarkPrice != nil {
				mark = decimalFromString(*ticker.MarkPrice)
			}
			if !mark.GreaterThan(decimal.Zero) {
				mark = decimalFromString(ticker.LastPrice)
			}
		}
		if !mark.GreaterThan(decimal.Zero) {
			mark = entry
		}
		if !market.ContractMult.GreaterThan(decimal.Zero) {
			market.ContractMult = decimal.NewFromInt(1)
		}
		side := "long"
		if strings.EqualFold(position.PositionSide, "SHORT") || size.LessThan(decimal.Zero) {
			side = "short"
		}
		size = size.Abs()
		unrealized := mark.Sub(entry).Mul(size).Mul(market.ContractMult)
		if side == "short" {
			unrealized = unrealized.Neg()
		}
		resultPositions = append(resultPositions, domain.PositionSnapshot{
			MarketIndex: marketIndex, Symbol: symbol, Side: side, Quantity: size,
			AverageEntryPrice: entry, PositionValue: mark.Mul(size).Mul(market.ContractMult),
			UnrealizedPnl: unrealized, RealizedPnl: decimalFromString(position.RealizedPnL),
			LiquidationPrice: decimalFromString(position.TakeOverPrice),
			AllocatedMargin:  decimalFromString(position.InitialMargin), MarginMode: marginModeNumber(position.MarginMode),
		})
	}
	return domain.AccountSnapshot{
		AccountIndex: int(s.cfg.AccountID), Collateral: collateral, AvailableBalance: available,
		TotalAssetValue: collateral, CrossAssetValue: collateral, Assets: assets, Positions: resultPositions,
	}, nil
}

func (s *Sodex) PlaceOrder(ctx context.Context, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	orders, err := s.PlaceOrdersBatch(ctx, []domain.OrderIntent{intent})
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	if len(orders) != 1 {
		return domain.ExchangeOrder{}, errors.New("SoDEX returned no placed order")
	}
	return orders[0], nil
}

func (s *Sodex) PlaceOrdersBatch(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	if len(intents) == 0 {
		return []domain.ExchangeOrder{}, nil
	}
	if !s.cfg.EnableTrading {
		return nil, errors.New("SoDEX trading is disabled")
	}
	if s.client == nil {
		return nil, errors.New("SoDEX is not connected")
	}
	if len(intents) > 100 {
		result := make([]domain.ExchangeOrder, 0, len(intents))
		for start := 0; start < len(intents); start += 100 {
			end := start + 100
			if end > len(intents) {
				end = len(intents)
			}
			batch, err := s.PlaceOrdersBatch(ctx, intents[start:end])
			if err != nil {
				return result, err
			}
			result = append(result, batch...)
		}
		return result, nil
	}

	s.mu.Lock()
	for _, intent := range intents {
		if intent.ClientOrderID == "" {
			s.mu.Unlock()
			return nil, errors.New("SoDEX order requires client_order_id")
		}
		// Register metadata before sending the request so an immediate market
		// fill can still be mapped when the websocket event arrives first.
		s.orders[intent.ClientOrderID] = orderFromIntent(intent, "pending")
		s.remaining[intent.ClientOrderID] = intent.Quantity
		s.orderUpdatedAt[intent.ClientOrderID] = time.Now()
	}
	s.mu.Unlock()

	request := &sodexptypes.NewOrderRequest{AccountID: s.cfg.AccountID, Orders: make([]*sodexptypes.RawOrder, 0, len(intents))}
	for _, intent := range intents {
		symbolID, err := s.symbolIDForIntent(intent)
		if err != nil {
			return nil, err
		}
		if request.SymbolID == 0 || request.SymbolID == symbolID {
			request.SymbolID = symbolID
		} else {
			return nil, errors.New("SoDEX only supports one symbol per signed order batch")
		}
		raw, err := s.rawOrder(ctx, intent)
		if err != nil {
			return nil, err
		}
		request.Orders = append(request.Orders, raw)
	}
	results, err := s.client.PlacePerpsOrder(ctx, request)
	if err != nil {
		s.forgetPendingOrders(intents)
		return nil, fmt.Errorf("SoDEX place order: %w", err)
	}
	if len(results) != len(intents) {
		s.forgetPendingOrders(intents)
		return nil, fmt.Errorf("SoDEX returned %d order results for %d requests", len(results), len(intents))
	}
	placed := make([]domain.ExchangeOrder, 0, len(intents))
	for index, result := range results {
		if failedOrderStatus(result.Status) {
			s.forgetPendingOrders(intents[index:])
			return placed, fmt.Errorf("SoDEX order %s rejected: %s %s", result.ClOrdID, result.Status, result.Message)
		}
		intent := intents[index]
		clientID := result.ClOrdID
		if clientID == "" {
			clientID = intent.ClientOrderID
		}
		order := orderFromIntent(intent, "open")
		order.ClientOrderID = clientID
		order.ExchangeOrderID = strconv.FormatUint(result.OrderID, 10)
		s.mu.Lock()
		if clientID != intent.ClientOrderID {
			delete(s.orders, intent.ClientOrderID)
			delete(s.remaining, intent.ClientOrderID)
			delete(s.orderUpdatedAt, intent.ClientOrderID)
		}
		s.orders[clientID] = order
		s.orderIDs[clientID] = result.OrderID
		if result.OrderID != 0 {
			s.byOrderID[result.OrderID] = clientID
		}
		s.remaining[clientID] = intent.Quantity
		s.orderUpdatedAt[clientID] = time.Now()
		s.mu.Unlock()
		placed = append(placed, order)
	}
	return placed, nil
}

func (s *Sodex) forgetPendingOrders(intents []domain.OrderIntent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, intent := range intents {
		if order, ok := s.orders[intent.ClientOrderID]; ok && order.Status == "pending" {
			delete(s.orders, intent.ClientOrderID)
			delete(s.remaining, intent.ClientOrderID)
			delete(s.orderUpdatedAt, intent.ClientOrderID)
		}
	}
}

func (s *Sodex) ModifyOrder(ctx context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	if !s.cfg.EnableTrading {
		return domain.ExchangeOrder{}, errors.New("SoDEX trading is disabled")
	}
	symbolID, err := s.symbolIDForIntent(intent)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	trigger := intent.Price
	if intent.TriggerPrice != nil {
		trigger = *intent.TriggerPrice
	}
	price := intent.Price
	if intent.Role == domain.OrderRoleTakeProfit {
		price, err = takeProfitMakerPrice(trigger, intent.Side, s.market.PriceStep)
		if err != nil {
			return domain.ExchangeOrder{}, err
		}
	}
	request := &sodexptypes.ModifyOrderRequest{AccountID: s.cfg.AccountID, SymbolID: symbolID}
	s.mu.Lock()
	exchangeOrderID := s.orderIDs[clientOrderID]
	s.mu.Unlock()
	if exchangeOrderID != 0 {
		request.OrderID = &exchangeOrderID
	} else {
		request.ClOrdID = &clientOrderID
	}
	if price.GreaterThan(decimal.Zero) {
		request.Price = &price
	}
	if intent.Quantity.GreaterThan(decimal.Zero) {
		quantity := intent.Quantity
		request.Quantity = &quantity
	}
	if intent.Role == domain.OrderRoleTakeProfit && trigger.GreaterThan(decimal.Zero) {
		request.StopPrice = &trigger
	}
	result, err := s.client.ModifyPerpsOrder(ctx, request)
	if err != nil {
		return domain.ExchangeOrder{}, fmt.Errorf("SoDEX modify order: %w", err)
	}
	if result != nil && result.Code != 0 {
		return domain.ExchangeOrder{}, fmt.Errorf("SoDEX modify order rejected: %s", result.Error)
	}
	s.mu.Lock()
	order, ok := s.orders[clientOrderID]
	if !ok {
		order = orderFromIntent(intent, "open")
	}
	order.MarketIndex, order.Side, order.Role, order.Level = intent.MarketIndex, intent.Side, intent.Role, intent.Level
	order.Price, order.Quantity, order.ReduceOnly, order.PostOnly, order.OrderType = price, intent.Quantity, intent.ReduceOnly, intent.PostOnly, intent.OrderType
	s.orders[clientOrderID] = order
	s.remaining[clientOrderID] = intent.Quantity
	s.orderUpdatedAt[clientOrderID] = time.Now()
	s.mu.Unlock()
	return order, nil
}

func (s *Sodex) CancelOrder(ctx context.Context, clientOrderID string) error {
	return s.CancelOrdersBatch(ctx, []string{clientOrderID})
}

func (s *Sodex) CancelOrdersBatch(ctx context.Context, ids []string) error {
	if !s.cfg.EnableTrading {
		return errors.New("SoDEX trading is disabled")
	}
	unique := uniqueStrings(ids)
	for start := 0; start < len(unique); start += 100 {
		end := start + 100
		if end > len(unique) {
			end = len(unique)
		}
		if err := s.cancelChunk(ctx, unique[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sodex) cancelChunk(ctx context.Context, ids []string) error {
	request := &sodexptypes.CancelOrderRequest{AccountID: s.cfg.AccountID}
	targets := make([]sodexCancelTarget, 0, len(ids))
	unknown := make([]string, 0)
	s.mu.Lock()
	for _, id := range ids {
		order, ok := s.orders[id]
		if !ok {
			unknown = append(unknown, id)
			continue
		}
		symbolID := uint64(order.MarketIndex)
		if symbolID == 0 {
			symbolID = uint64(s.cfg.MarketIndex)
		}
		target := sodexCancelTarget{clientOrderID: id, symbolID: symbolID}
		if parsed, err := strconv.ParseUint(order.ExchangeOrderID, 10, 64); err == nil && parsed > 0 {
			target.orderID = parsed
		}
		targets = append(targets, target)
	}
	s.mu.Unlock()
	if len(unknown) > 0 {
		remote, err := s.remoteOpenOrders(ctx)
		if err != nil {
			return fmt.Errorf("resolve SoDEX cancel targets: %w", err)
		}
		for _, id := range unknown {
			for _, item := range remote {
				matchesClientID := item.ClOrdID == id
				matchesOrderID := false
				if parsed, parseErr := strconv.ParseUint(id, 10, 64); parseErr == nil {
					matchesOrderID = item.OrderID == parsed
				}
				if !matchesClientID && !matchesOrderID {
					continue
				}
				targets = append(targets, sodexCancelTarget{clientOrderID: id, orderID: item.OrderID, symbolID: uint64(s.marketIndexForSymbol(item.Symbol))})
				break
			}
		}
	}
	for _, target := range targets {
		cancel := &sodexptypes.CancelOrder{SymbolID: target.symbolID}
		if target.orderID > 0 {
			orderID := target.orderID
			cancel.OrderID = &orderID
		} else {
			clientID := target.clientOrderID
			cancel.ClOrdID = &clientID
		}
		request.Cancels = append(request.Cancels, cancel)
	}
	if len(request.Cancels) == 0 {
		// The remote snapshot already confirmed that every requested ID is gone.
		return nil
	}
	results, err := s.client.CancelPerpsOrders(ctx, request)
	if err != nil {
		return fmt.Errorf("SoDEX cancel orders: %w", err)
	}
	failed := false
	for _, result := range results {
		if failedOrderStatus(result.Status) {
			failed = true
		}
	}
	if err := s.waitForCanceled(ctx, targets); err != nil {
		if failed {
			return fmt.Errorf("SoDEX cancel order rejected: %w", err)
		}
		return err
	}
	s.mu.Lock()
	for _, target := range targets {
		id := target.clientOrderID
		delete(s.orders, id)
		if orderID := s.orderIDs[id]; orderID != 0 {
			delete(s.byOrderID, orderID)
		}
		delete(s.orderIDs, id)
		delete(s.remaining, id)
		delete(s.orderUpdatedAt, id)
	}
	s.mu.Unlock()
	return nil
}

func (s *Sodex) waitForCanceled(ctx context.Context, targets []sodexCancelTarget) error {
	// The SoDEX cancel endpoint can acknowledge before the account order view
	// reflects the cancellation. Verify the remote state before allowing a new
	// replacement order to become the only local truth.
	for attempt := 0; attempt < 8; attempt++ {
		remote, err := s.remoteOpenOrders(ctx)
		if err != nil {
			return fmt.Errorf("verify SoDEX cancellation: %w", err)
		}
		active := false
		for _, target := range targets {
			for _, item := range remote {
				if terminalOrderStatus(item.Status) {
					continue
				}
				if (target.clientOrderID != "" && item.ClOrdID == target.clientOrderID) || (target.orderID != 0 && item.OrderID == target.orderID) {
					active = true
					break
				}
			}
			if active {
				break
			}
		}
		if !active {
			return nil
		}
		if attempt == 7 {
			return errors.New("SoDEX canceled order is still active")
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return nil
}

func (s *Sodex) OpenOrders(ctx context.Context) ([]domain.ExchangeOrder, error) {
	if s.client == nil || s.http == nil {
		return nil, errors.New("SoDEX is not connected")
	}
	remote, err := s.remoteOpenOrders(ctx)
	if err != nil {
		return nil, fmt.Errorf("SoDEX open orders: %w", err)
	}
	s.mu.Lock()
	merged := make(map[string]domain.ExchangeOrder, len(remote)+len(s.orders))
	remoteSeenAt := time.Now()
	for _, item := range remote {
		if terminalOrderStatus(item.Status) {
			continue
		}
		clientID := item.ClOrdID
		if clientID == "" && item.OrderID != 0 {
			if mapped := s.byOrderID[item.OrderID]; mapped != "" {
				clientID = mapped
			}
		}
		if clientID == "" {
			clientID = strconv.FormatUint(item.OrderID, 10)
		}
		order, managed := s.orders[clientID]
		if !managed && item.OrderID != 0 {
			if mapped := s.byOrderID[item.OrderID]; mapped != "" {
				clientID = mapped
				order, managed = s.orders[clientID]
			}
		}
		if !managed {
			order = domain.ExchangeOrder{ClientOrderID: clientID, Role: domain.OrderRoleInitial, Level: 0}
		}
		order.ExchangeOrderID = strconv.FormatUint(item.OrderID, 10)
		order.MarketIndex = s.marketIndexForSymbolLocked(item.Symbol)
		order.Side = sodexSide(item.Side)
		order.Price = decimalFromString(item.Price)
		order.Quantity = decimalFromString(item.OrigQty).Sub(decimalFromString(item.ExecutedQty))
		if order.Quantity.LessThan(decimal.Zero) {
			order.Quantity = decimal.Zero
		}
		order.ReduceOnly = order.ReduceOnly || order.Role == domain.OrderRoleTakeProfit
		order.OrderType = sodexOrderType(item.Type)
		order.Status = item.Status
		merged[clientID] = order
		if managed {
			s.orders[clientID] = order
			s.orderIDs[clientID] = item.OrderID
			if item.OrderID != 0 {
				s.byOrderID[item.OrderID] = clientID
			}
			s.remaining[clientID] = order.Quantity
			s.orderUpdatedAt[clientID] = remoteSeenAt
		}
	}
	// A just-submitted order can take a short time to appear in the remote
	// account snapshot. Keep it visible during that propagation window, while
	// allowing older local entries to disappear from the active-order view.
	localCutoff := remoteSeenAt.Add(-5 * time.Second)
	for id, order := range s.orders {
		if _, exists := merged[id]; exists {
			continue
		}
		if updatedAt, ok := s.orderUpdatedAt[id]; ok && updatedAt.After(localCutoff) && !failedOrderStatus(order.Status) {
			merged[id] = order
		}
	}
	result := make([]domain.ExchangeOrder, 0, len(merged))
	for _, order := range merged {
		if order.Quantity.IsZero() && order.Status == "" {
			continue
		}
		result = append(result, order)
	}
	s.mu.Unlock()
	return result, nil
}

func (s *Sodex) remoteOpenOrders(ctx context.Context) ([]sodexclient.Order, error) {
	query := url.Values{}
	query.Set("accountID", strconv.FormatUint(s.cfg.AccountID, 10))
	var response struct {
		Orders []sodexclient.Order `json:"orders"`
	}
	err := s.getPerpsAccountJSON(ctx, "orders", query, &response)
	if err != nil {
		return nil, err
	}
	return response.Orders, nil
}

// ReconcileFills backfills fills that may have happened before the accountTrade
// WebSocket subscription was ready. SoDEX's trade stream is push-only, so a
// fast market order must also be checked through the REST trade history.
func (s *Sodex) ReconcileFills(ctx context.Context) ([]domain.Fill, error) {
	if s.client == nil || s.http == nil {
		return nil, errors.New("SoDEX is not connected")
	}
	s.mu.Lock()
	symbols := make(map[string]struct{}, len(s.orders))
	for _, order := range s.orders {
		symbol := ""
		if market, ok := s.markets[order.MarketIndex]; ok {
			symbol = market.Symbol
		}
		if symbol == "" {
			symbol = s.market.Symbol
		}
		if symbol = strings.TrimSpace(symbol); symbol != "" {
			symbols[symbol] = struct{}{}
		}
	}
	s.mu.Unlock()
	result := make([]domain.Fill, 0)
	for symbol := range symbols {
		query := url.Values{}
		query.Set("accountID", strconv.FormatUint(s.cfg.AccountID, 10))
		query.Set("symbol", symbol)
		query.Set("limit", "100")
		var trades []sodexclient.UserTrade
		if err := s.getPerpsAccountJSON(ctx, "trades", query, &trades); err != nil {
			return result, fmt.Errorf("SoDEX trade reconciliation: %w", err)
		}
		for _, trade := range trades {
			fill, ok := s.fillFromTrade(sodexws.AccountTrade{
				TradeID: int64(trade.TradeID), Symbol: trade.Symbol, OrderID: int64(trade.OrderID),
				ClOrdID: trade.ClOrdID, Side: trade.Side, Price: trade.Price,
				Quantity: trade.Quantity, Fee: trade.Fee,
			})
			if ok {
				result = append(result, fill)
			}
		}
	}
	return result, nil
}

func (s *Sodex) getPerpsAccountJSON(ctx context.Context, resource string, query url.Values, result any) error {
	requestURL := strings.TrimRight(s.cfg.BaseURL, "/") + "/api/v1/perps/accounts/" + url.PathEscape(s.account) + "/" + resource
	if len(query) > 0 {
		requestURL += "?" + query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := s.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	var envelope struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	if envelope.Code != 0 {
		return fmt.Errorf("API code %d: %s", envelope.Code, envelope.Message)
	}
	if result == nil || len(envelope.Data) == 0 || string(envelope.Data) == "null" {
		return nil
	}
	return json.Unmarshal(envelope.Data, result)
}

func (s *Sodex) ProcessPrice(context.Context, string) ([]domain.Fill, error) {
	return nil, errors.New("SoDEX fills are consumed from accountTrade WebSocket")
}

func (s *Sodex) SubscribeFills(ctx context.Context) (<-chan domain.Fill, <-chan error) {
	fills := make(chan domain.Fill, 64)
	errs := make(chan error, 4)
	wsCtx, cancel := context.WithCancel(ctx)
	wsClient, err := sodexws.NewClient(strings.TrimRight(s.cfg.BaseURL, "/"), "perps")
	if err != nil {
		cancel()
		errs <- err
		close(fills)
		close(errs)
		return fills, errs
	}
	s.mu.Lock()
	if s.wsCancel != nil {
		s.wsCancel()
	}
	if s.ws != nil {
		_ = s.ws.Close()
	}
	s.ws = wsClient
	s.wsCancel = cancel
	s.mu.Unlock()
	wsClient.OnError(func(err error) {
		select {
		case errs <- err:
		default:
		}
	})
	if _, err := wsClient.Subscribe(sodexws.SubscribeParams{Channel: sodexws.ChannelAccountTrade, User: s.account}, func(push sodexws.Push) {
		for _, trade := range decodeAccountTrades(push.Data) {
			fill, ok := s.fillFromTrade(trade)
			if !ok {
				continue
			}
			select {
			case fills <- fill:
			case <-wsCtx.Done():
				return
			}
		}
	}); err != nil {
		errs <- fmt.Errorf("SoDEX accountTrade subscribe: %w", err)
	}
	go func() {
		if err := wsClient.Connect(wsCtx); err != nil && !errors.Is(err, context.Canceled) {
			select {
			case errs <- err:
			default:
			}
		}
	}()
	return fills, errs
}

func (s *Sodex) rawOrder(ctx context.Context, intent domain.OrderIntent) (*sodexptypes.RawOrder, error) {
	if intent.Quantity.LessThanOrEqual(decimal.Zero) {
		return nil, fmt.Errorf("SoDEX order %s quantity must be positive", intent.ClientOrderID)
	}
	if _, err := s.symbolIDForIntent(intent); err != nil {
		return nil, err
	}
	order := &sodexptypes.RawOrder{
		ClOrdID: intent.ClientOrderID, Modifier: enums.OrderModifierNormal,
		Side: sodexOrderSideValue(intent.Side), Type: sodexOrderTypeValue(intent.OrderType),
		TimeInForce: enums.TimeInForceGTC, Quantity: decimalPtr(intent.Quantity),
		PositionSide: enums.PositionSideBoth, ReduceOnly: intent.ReduceOnly,
	}
	if intent.OrderType == domain.OrderTypeMarket {
		order.Type = enums.OrderTypeMarket
		order.TimeInForce = enums.TimeInForceIOC
		protected, err := s.marketPriceLimit(ctx, intent)
		if err != nil {
			return nil, err
		}
		if protected.GreaterThan(decimal.Zero) {
			order.Price = decimalPtr(protected)
		}
	} else {
		if !intent.Price.GreaterThan(decimal.Zero) {
			return nil, fmt.Errorf("SoDEX limit order %s price must be positive", intent.ClientOrderID)
		}
		order.Price = decimalPtr(intent.Price)
		if intent.PostOnly {
			order.TimeInForce = enums.TimeInForceGTX
		}
	}
	if intent.Role == domain.OrderRoleTakeProfit {
		trigger := intent.Price
		if intent.TriggerPrice != nil {
			trigger = *intent.TriggerPrice
		}
		if !trigger.GreaterThan(decimal.Zero) {
			return nil, fmt.Errorf("SoDEX take-profit order %s trigger price must be positive", intent.ClientOrderID)
		}
		price, err := takeProfitMakerPrice(trigger, intent.Side, s.market.PriceStep)
		if err != nil {
			return nil, err
		}
		order.Modifier = enums.OrderModifierStop
		order.Type = enums.OrderTypeLimit
		order.TimeInForce = enums.TimeInForceGTX
		order.Price = decimalPtr(price)
		order.StopPrice = decimalPtr(trigger)
		stopType := enums.StopTypeTakeProfit
		// SoDEX mainnet TP/SL orders are triggered by mark price. The API
		// rejects LAST_PRICE for perps stop orders.
		triggerType := enums.TriggerTypeMarkPrice
		order.StopType = &stopType
		order.TriggerType = &triggerType
		order.ReduceOnly = true
	}
	return order, nil
}

func (s *Sodex) OrderBook(ctx context.Context, _ int, symbol string) (domain.OrderBook, error) {
	if strings.TrimSpace(symbol) == "" {
		symbol = s.market.Symbol
	}
	book, err := s.client.PerpsOrderBook(ctx, symbol, 20)
	if err != nil {
		return domain.OrderBook{}, fmt.Errorf("SoDEX order book: %w", err)
	}
	result := domain.OrderBook{Bids: make([]domain.OrderBookLevel, 0, len(book.Bids)), Asks: make([]domain.OrderBookLevel, 0, len(book.Asks))}
	for _, level := range book.Bids {
		result.Bids = append(result.Bids, domain.OrderBookLevel{Price: decimalFromString(level.Price)})
	}
	for _, level := range book.Asks {
		result.Asks = append(result.Asks, domain.OrderBookLevel{Price: decimalFromString(level.Price)})
	}
	return result, nil
}

func (s *Sodex) marketPriceLimit(ctx context.Context, intent domain.OrderIntent) (decimal.Decimal, error) {
	book, err := s.OrderBook(ctx, intent.MarketIndex, intent.Symbol)
	if err != nil {
		return decimal.Zero, err
	}
	bookSide := domain.OrderSideSell
	if intent.Side == domain.OrderSideSell {
		bookSide = domain.OrderSideBuy
	}
	best, err := book.BestPrice(bookSide)
	if err != nil {
		return decimal.Zero, fmt.Errorf("SoDEX order book has no liquidity: %w", err)
	}
	ratio := intent.MaxSlippageRatio
	if ratio.IsZero() {
		ratio = decimal.RequireFromString("0.005")
	}
	protected := best.Mul(decimal.NewFromInt(1).Add(ratio))
	if intent.Side == domain.OrderSideSell {
		protected = best.Mul(decimal.NewFromInt(1).Sub(ratio))
	}
	return roundToStep(protected, s.market.PriceStep, intent.Side == domain.OrderSideBuy), nil
}

func (s *Sodex) symbolIDForIntent(intent domain.OrderIntent) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if symbol, ok := s.symbols[intent.MarketIndex]; ok {
		return symbol.SymbolID, nil
	}
	if id, ok := s.symbolIDs[strings.ToUpper(strings.TrimSpace(intent.Symbol))]; ok {
		return id, nil
	}
	if intent.MarketIndex >= 0 {
		return uint64(intent.MarketIndex), nil
	}
	return 0, fmt.Errorf("SoDEX market is not configured: index=%d symbol=%s", intent.MarketIndex, intent.Symbol)
}

func (s *Sodex) marketForSymbol(symbol string) (int, domain.MarketDescriptor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	index := s.marketIndexForSymbolLocked(symbol)
	market, ok := s.markets[index]
	return index, market, ok
}

func (s *Sodex) marketIndexForSymbol(symbol string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.marketIndexForSymbolLocked(symbol)
}

func (s *Sodex) marketIndexForSymbolLocked(symbol string) int {
	upper := strings.ToUpper(strings.TrimSpace(symbol))
	for index, market := range s.markets {
		if strings.EqualFold(market.Symbol, symbol) || strings.EqualFold(market.ExchangeSymbol, symbol) {
			return index
		}
	}
	if id, ok := s.symbolIDs[upper]; ok {
		return int(id)
	}
	return s.cfg.MarketIndex
}

func (s *Sodex) fillFromTrade(trade sodexws.AccountTrade) (domain.Fill, bool) {
	if trade.TradeID <= 0 {
		// Without a stable trade ID the websocket and REST history paths cannot
		// be made idempotent. Dropping it is safer than processing it repeatedly.
		return domain.Fill{}, false
	}
	quantity := decimalFromString(trade.Quantity)
	price := decimalFromString(trade.Price)
	if !quantity.GreaterThan(decimal.Zero) || !price.GreaterThan(decimal.Zero) {
		return domain.Fill{}, false
	}
	orderID := uint64(0)
	if trade.OrderID > 0 {
		orderID = uint64(trade.OrderID)
	}
	s.mu.Lock()
	if trade.TradeID > 0 {
		if _, exists := s.processedTrades[trade.TradeID]; exists {
			s.mu.Unlock()
			return domain.Fill{}, false
		}
	}
	clientID := strings.TrimSpace(trade.ClOrdID)
	if orderID != 0 {
		// The exchange order ID is authoritative. Some SoDEX trade payloads
		// omit clOrdID or return the value from the original order after a
		// replace, so a non-empty but stale client ID must not hide a managed
		// fill.
		if mapped := s.byOrderID[orderID]; mapped != "" {
			clientID = mapped
		}
	}
	order, ok := s.orders[clientID]
	if !ok {
		// A trade that is not mapped to an order submitted by this runtime is
		// historical/external state. It must not be injected as a synthetic
		// initial fill, otherwise reconciliation creates duplicate grid orders.
		s.mu.Unlock()
		return domain.Fill{}, false
	}
	if trade.TradeID > 0 {
		s.processedTrades[trade.TradeID] = struct{}{}
	}
	if orderID == 0 {
		orderID = s.orderIDs[clientID]
	}
	if remaining, exists := s.remaining[clientID]; exists && remaining.GreaterThan(decimal.Zero) && quantity.GreaterThan(remaining) {
		quantity = remaining
	}
	if remaining, exists := s.remaining[clientID]; exists {
		remaining = remaining.Sub(quantity)
		if remaining.LessThanOrEqual(decimal.Zero) {
			delete(s.orders, clientID)
			delete(s.remaining, clientID)
			delete(s.orderIDs, clientID)
			delete(s.orderUpdatedAt, clientID)
			if orderID != 0 {
				delete(s.byOrderID, orderID)
			}
		} else {
			s.remaining[clientID] = remaining
		}
	}
	s.mu.Unlock()
	var fee *decimal.Decimal
	if value, err := decimal.NewFromString(strings.TrimSpace(trade.Fee)); err == nil {
		fee = &value
	}
	exchangeOrderID := order.ExchangeOrderID
	if exchangeOrderID == "" && orderID != 0 {
		exchangeOrderID = strconv.FormatUint(orderID, 10)
	}
	return domain.Fill{
		ExchangeOrderID: exchangeOrderID, ClientOrderID: clientID, MarketIndex: order.MarketIndex,
		Side: sodexSide(trade.Side), Role: order.Role, Level: order.Level, Price: price,
		Quantity: quantity, TradeID: strconv.FormatInt(trade.TradeID, 10), Fee: fee,
	}, true
}

func decodeAccountTrades(raw []byte) []sodexws.AccountTrade {
	raw = bytes.TrimSpace(raw)
	var one sodexws.AccountTrade
	if len(raw) > 0 && raw[0] == '[' {
		var many []sodexws.AccountTrade
		if jsonUnmarshal(raw, &many) == nil {
			return many
		}
		return nil
	}
	if jsonUnmarshal(raw, &one) == nil {
		return []sodexws.AccountTrade{one}
	}
	return nil
}

func orderFromIntent(intent domain.OrderIntent, status string) domain.ExchangeOrder {
	return domain.ExchangeOrder{ClientOrderID: intent.ClientOrderID, MarketIndex: intent.MarketIndex, Side: intent.Side,
		Role: intent.Role, Level: intent.Level, Price: intent.Price, Quantity: intent.Quantity,
		ReduceOnly: intent.ReduceOnly, PostOnly: intent.PostOnly, OrderType: intent.OrderType, Status: status}
}

func sodexOrderSide(side string) enums.OrderSide {
	if strings.EqualFold(string(domain.OrderSideSell), side) {
		return enums.OrderSideSell
	}
	return enums.OrderSideBuy
}

func sodexOrderSideValue(side domain.OrderSide) enums.OrderSide {
	if side == domain.OrderSideSell {
		return enums.OrderSideSell
	}
	return enums.OrderSideBuy
}

func sodexOrderType(typeName string) domain.OrderType {
	if strings.Contains(strings.ToUpper(typeName), "MARKET") {
		return domain.OrderTypeMarket
	}
	return domain.OrderTypeLimit
}

func sodexOrderTypeValue(orderType domain.OrderType) enums.OrderType {
	if orderType == domain.OrderTypeMarket {
		return enums.OrderTypeMarket
	}
	return enums.OrderTypeLimit
}

func sodexSide(value string) domain.OrderSide {
	if strings.EqualFold(value, "SELL") {
		return domain.OrderSideSell
	}
	return domain.OrderSideBuy
}

func failedOrderStatus(status string) bool {
	status = strings.ToUpper(strings.TrimSpace(status))
	return status == "REJECTED" || status == "ERROR" || status == "FAILED" || status == "INVALID"
}

func terminalOrderStatus(status string) bool {
	status = strings.ToUpper(strings.TrimSpace(status))
	return status == "FILLED" || status == "CANCELED" || status == "CANCELLED" || status == "REJECTED" || status == "EXPIRED"
}

func decimalPtr(value decimal.Decimal) *decimal.Decimal { return &value }

func decimalFromString(value string) decimal.Decimal {
	parsed, err := decimal.NewFromString(strings.TrimSpace(value))
	if err != nil {
		return decimal.Zero
	}
	return parsed
}

func pointerString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func isStableCoin(symbol string) bool {
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	return symbol == "USD" || symbol == "USDC" || symbol == "USDT" || symbol == "VUSDC" || symbol == "VUSD"
}

func marginModeNumber(mode string) int {
	if strings.EqualFold(mode, "ISOLATED") {
		return 1
	}
	return 0
}

func roundToStep(value, step decimal.Decimal, ceil bool) decimal.Decimal {
	if !step.GreaterThan(decimal.Zero) {
		return value
	}
	units := value.Div(step)
	if ceil {
		return units.Ceil().Mul(step)
	}
	return units.Truncate(0).Mul(step)
}

// jsonUnmarshal is kept as a small indirection to make the websocket decoder
// easy to exercise without coupling the exchange implementation to transport
// details elsewhere in the project.
var jsonUnmarshal = func(data []byte, target any) error {
	return json.Unmarshal(data, target)
}

var _ Exchange = (*Sodex)(nil)
