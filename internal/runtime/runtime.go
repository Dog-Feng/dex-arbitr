package runtime

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"dex-grid-arbitr/internal/config"
	"dex-grid-arbitr/internal/domain"
	"dex-grid-arbitr/internal/exchange"
	"dex-grid-arbitr/internal/persistence"
	"dex-grid-arbitr/internal/strategy"
	"github.com/shopspring/decimal"
)

var ErrExistingPosition = errors.New("market already has an open position")

const (
	// Lighter's current fill stream does not carry the actual fee amount, so
	// the runtime uses this Lighter-only fallback rate when Fee is absent.
	lighterFallbackFeeRate            = "0.00005" // 0.0050%
	positionWatchInterval             = 500 * time.Millisecond
	positionWatchMissingConfirmations = 2
	positionWatchStabilization        = time.Second
	orderWatchMissingConfirmations    = 2
	orderWatchStabilization           = 3 * time.Second
	defaultMakerRequoteSeconds        = 10
	defaultSodexGridBatchSize         = 10
	defaultSodexGridBatchInterval     = time.Second
	maxSodexGridBatchSize             = 10
	normalGridRollbackTimeout         = 15 * time.Second
)

type Runtime struct {
	mu                         sync.Mutex
	orderMu                    sync.Mutex
	settings                   config.Settings
	exchange                   exchange.Exchange
	store                      *persistence.EventStore
	connected                  bool
	backendMode                string
	currentDEX                 string
	market                     domain.MarketSpec
	marketCatalog              map[string]domain.MarketDescriptor
	strategy                   strategy.Strategy
	lastPrice                  *decimal.Decimal
	activeOrders               map[string]domain.ExchangeOrder
	events                     []domain.RuntimeEvent
	cumulativePnl              decimal.Decimal
	completedCycles            int
	unrealizedPnl              decimal.Decimal
	accountAvailableBalance    decimal.Decimal
	accountBalanceKnown        bool
	accountPositionQuantity    decimal.Decimal
	accountPositionAverage     decimal.Decimal
	accountPositionKnown       bool
	tradingFees                decimal.Decimal
	runCapital                 decimal.Decimal
	strategyStartedAt          time.Time
	strategyEndedAt            time.Time
	runtimeCtx                 context.Context
	runtimeCancel              context.CancelFunc
	fillCancel                 context.CancelFunc
	positionWatchArmed         bool
	positionWatchCancelPending bool
	positionWatchPresentCount  int
	positionWatchMissingCount  int
	positionWatchGraceUntil    time.Time
	orderWatchMissingKey       string
	orderWatchMissingCount     int
	orderWatchGraceUntil       time.Time
	entryMode                  domain.EntryMode
	makerRequoteSeconds        int
	entryOrderPlacedAt         time.Time
	entryRequotePending        bool
	entryRequoteCount          int
	processedTradeIDs          map[string]struct{}
	normalGridBatchInterval    time.Duration
}

func New(settings config.Settings, ex exchange.Exchange, store *persistence.EventStore) *Runtime {
	mode := "paper"
	switch settings.Mode {
	case "lighter_testnet", "lighter_mainnet":
		mode = "lighter"
	case "sodex_testnet", "sodex_mainnet":
		mode = "sodex"
	case "multi":
		mode = "multi"
	}
	runtimeCtx, runtimeCancel := context.WithCancel(context.Background())
	runtime := &Runtime{
		settings: settings, exchange: ex, store: store, backendMode: mode,
		currentDEX:              strings.ToLower(strings.TrimSpace(settings.DefaultDEX)),
		marketCatalog:           make(map[string]domain.MarketDescriptor),
		activeOrders:            make(map[string]domain.ExchangeOrder),
		events:                  make([]domain.RuntimeEvent, 0, 200),
		runtimeCtx:              runtimeCtx,
		runtimeCancel:           runtimeCancel,
		processedTradeIDs:       make(map[string]struct{}),
		entryMode:               domain.EntryModeMarket,
		makerRequoteSeconds:     defaultMakerRequoteSeconds,
		normalGridBatchInterval: sodexGridBatchInterval(settings),
	}
	if runtime.currentDEX == "" && mode != "multi" {
		runtime.currentDEX = mode
	}
	go runtime.positionWatchdog()
	return runtime
}

func (r *Runtime) isLiveBackend() bool { return r.backendMode != "paper" }

func (r *Runtime) configuredMarketIndex() int {
	if r.currentDEX == "sodex" {
		if r.settings.SodexSymbolID != 0 {
			return r.settings.SodexSymbolID
		}
		return r.settings.SodexMarketIndex
	}
	return r.settings.LighterMarketIndex
}

func marketCatalogKey(dex string, marketIndex int) string {
	return strings.ToLower(strings.TrimSpace(dex)) + ":" + fmt.Sprint(marketIndex)
}

func (r *Runtime) selectDEXLocked(dex string) error {
	dex = strings.ToLower(strings.TrimSpace(dex))
	if dex == "" {
		dex = r.currentDEX
	}
	if dex == "" && r.backendMode != "multi" {
		dex = r.backendMode
	}
	if dex == "" {
		return errors.New("dex is required when multiple exchanges are enabled")
	}
	if selector, ok := r.exchange.(interface{ SelectDEX(string) error }); ok {
		if err := selector.SelectDEX(dex); err != nil {
			return err
		}
	} else if r.backendMode != "multi" && dex != r.backendMode {
		return fmt.Errorf("DEX %s is not available in this process", dex)
	}
	r.currentDEX = dex
	return nil
}

func (r *Runtime) Connect(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.connected {
		return nil
	}
	if err := r.exchange.Connect(ctx); err != nil {
		return err
	}
	markets, err := r.exchange.Markets(ctx)
	if err != nil {
		return err
	}
	r.catalogMarketsLocked(markets)
	if len(r.marketCatalog) == 0 {
		return errors.New("no market metadata available")
	}
	selected, ok := r.marketCatalog[marketCatalogKey(r.currentDEX, r.configuredMarketIndex())]
	if r.backendMode == "paper" {
		selected, ok = r.marketCatalog[marketCatalogKey("paper", 0)]
	}
	if !ok {
		for _, item := range r.marketCatalog {
			selected = item
			break
		}
	}
	if err := r.selectDEXLocked(selected.DEX); err != nil {
		return err
	}
	r.market = specFromDescriptor(selected)
	r.setExchangeMarket()
	r.connected = true
	return nil
}

func (r *Runtime) Close() error {
	r.mu.Lock()
	if r.fillCancel != nil {
		r.fillCancel()
		r.fillCancel = nil
	}
	if r.runtimeCancel != nil {
		r.runtimeCancel()
		r.runtimeCancel = nil
	}
	r.mu.Unlock()
	if err := r.exchange.Close(); err != nil {
		return err
	}
	if r.store != nil {
		return r.store.Close()
	}
	return nil
}

func isMakerEntryMode(mode domain.EntryMode) bool {
	return mode == domain.EntryModeMaker || mode == domain.EntryModeLimitMaker
}

func (r *Runtime) orderBookLocked(ctx context.Context) (domain.OrderBook, error) {
	provider, ok := r.exchange.(exchange.OrderBookProvider)
	if !ok {
		return domain.OrderBook{}, errors.New("exchange does not provide an order book")
	}
	marketIndex := r.market.MarketIndex
	if r.strategy != nil {
		marketIndex = r.strategy.MarketIndex()
	}
	return provider.OrderBook(ctx, marketIndex, r.market.Symbol)
}

func makerEntryPrice(book domain.OrderBook, direction domain.Direction) (decimal.Decimal, error) {
	if direction == domain.DirectionShort {
		return book.BestAsk()
	}
	return book.BestBid()
}

func validateMakerLimitPrice(book domain.OrderBook, direction domain.Direction, price decimal.Decimal) error {
	if !price.GreaterThan(decimal.Zero) {
		return errors.New("Maker entry price must be positive")
	}
	if direction == domain.DirectionShort {
		bestBid, err := book.BestBid()
		if err != nil {
			return err
		}
		if !price.GreaterThan(bestBid) {
			return fmt.Errorf("short Maker entry price %s would take the bid %s", price, bestBid)
		}
		return nil
	}
	bestAsk, err := book.BestAsk()
	if err != nil {
		return err
	}
	if !price.LessThan(bestAsk) {
		return fmt.Errorf("long Maker entry price %s would take the ask %s", price, bestAsk)
	}
	return nil
}

func (r *Runtime) quantizeEntryPrice(price decimal.Decimal, direction domain.Direction) decimal.Decimal {
	step := r.market.PriceStep
	if !price.GreaterThan(decimal.Zero) || !step.GreaterThan(decimal.Zero) {
		return price
	}
	units := price.Div(step)
	if direction == domain.DirectionShort {
		return units.Ceil().Mul(step)
	}
	return units.Floor().Mul(step)
}

func (r *Runtime) prepareInitialActionLocked(ctx context.Context, action *strategy.Action, reprice bool) error {
	if action == nil || r.strategy == nil {
		return nil
	}
	for index, intent := range action.Place {
		if intent.Role != domain.OrderRoleInitial || !isMakerEntryMode(r.entryMode) {
			continue
		}
		if r.entryMode == domain.EntryModeMaker && reprice {
			book, err := r.orderBookLocked(ctx)
			if err != nil {
				return err
			}
			price, err := makerEntryPrice(book, r.strategyDirection())
			if err != nil {
				return err
			}
			intent.Price = r.quantizeEntryPrice(price, r.strategyDirection())
		}
		intent.OrderType = domain.OrderTypeLimit
		intent.PostOnly = true
		if r.entryMode == domain.EntryModeLimitMaker {
			book, err := r.orderBookLocked(ctx)
			if err != nil {
				return err
			}
			if err := validateMakerLimitPrice(book, r.strategyDirection(), intent.Price); err != nil {
				return err
			}
		}
		action.Place[index] = intent
		r.strategy.UpdatePendingInitialOrder(intent)
	}
	return nil
}

func (r *Runtime) strategyDirection() domain.Direction {
	if r.strategy == nil {
		return domain.DirectionLong
	}
	return r.strategy.Direction()
}

func (r *Runtime) Start(ctx context.Context, cfg domain.StrategyConfig, initialPrice *decimal.Decimal) (domain.StrategyView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.connected {
		if err := r.connectLocked(ctx); err != nil {
			return domain.StrategyView{}, err
		}
	}
	if r.strategy != nil && (r.strategy.StrategyStatus() == domain.StrategyRunning || r.strategy.StrategyStatus() == domain.StrategyStarting) {
		return domain.StrategyView{}, errors.New("strategy is already running")
	}
	if cfg.StrategyType == "" {
		cfg.StrategyType = "martingale"
	}
	cfg.StrategyType = strings.ToLower(strings.TrimSpace(cfg.StrategyType))
	if cfg.StrategyType != "martingale" && cfg.StrategyType != "normal" && cfg.StrategyType != "normal_grid" {
		return domain.StrategyView{}, fmt.Errorf("strategy_type %q is not implemented", cfg.StrategyType)
	}
	if cfg.StrategyType == "normal_grid" {
		cfg.StrategyType = "normal"
	}
	if cfg.InitialPrice == nil {
		cfg.InitialPrice = initialPrice
	}
	cfg.EntryMode = domain.NormalizeEntryMode(string(cfg.EntryMode), cfg.InitialPrice != nil)
	if cfg.EntryMode != domain.EntryModeMarket && cfg.EntryMode != domain.EntryModeMaker && cfg.EntryMode != domain.EntryModeLimitMaker {
		return domain.StrategyView{}, fmt.Errorf("entry_mode %q is not implemented", cfg.EntryMode)
	}
	if cfg.MakerRequoteSeconds <= 0 {
		cfg.MakerRequoteSeconds = defaultMakerRequoteSeconds
	}
	if cfg.EntryMode == domain.EntryModeLimitMaker && cfg.InitialPrice == nil {
		return domain.StrategyView{}, errors.New("limit_maker entry requires an initial price")
	}
	if cfg.DEX == "" {
		cfg.DEX = r.currentDEX
	}
	cfg.DEX = strings.ToLower(strings.TrimSpace(cfg.DEX))
	if err := r.selectDEXLocked(cfg.DEX); err != nil {
		return domain.StrategyView{}, err
	}
	selected, ok := r.marketCatalog[marketCatalogKey(cfg.DEX, cfg.MarketIndex)]
	if !ok {
		return domain.StrategyView{}, fmt.Errorf("market is not available: %s #%d", cfg.DEX, cfg.MarketIndex)
	}
	cfg.DEX = selected.DEX
	r.market = specFromDescriptor(selected)
	r.setExchangeMarket()
	if account, err := r.exchange.AccountSnapshot(ctx); err == nil {
		for _, position := range account.Positions {
			if positionMatchesMarket(position, cfg.MarketIndex, cfg.Symbol) && position.Quantity.GreaterThan(decimal.Zero) {
				return domain.StrategyView{}, fmt.Errorf("%w: %s %s @ %s", ErrExistingPosition, position.Side, position.Quantity, position.AverageEntryPrice)
			}
		}
	}
	if setter, ok := r.exchange.(interface {
		SetLeverage(context.Context, int, decimal.Decimal) error
	}); ok {
		if err := setter.SetLeverage(ctx, cfg.MarketIndex, cfg.Leverage); err != nil {
			return domain.StrategyView{}, err
		}
	}
	price := initialPrice
	if cfg.EntryMode == domain.EntryModeLimitMaker {
		fixedPrice := r.quantizeEntryPrice(*cfg.InitialPrice, cfg.Direction)
		cfg.InitialPrice = &fixedPrice
		price = &fixedPrice
	}
	if cfg.EntryMode == domain.EntryModeMaker {
		book, err := r.orderBookLocked(ctx)
		if err != nil {
			return domain.StrategyView{}, err
		}
		makerPrice, err := makerEntryPrice(book, cfg.Direction)
		if err != nil {
			return domain.StrategyView{}, err
		}
		makerPrice = r.quantizeEntryPrice(makerPrice, cfg.Direction)
		price = &makerPrice
	}
	if price == nil && selected.LastPrice != nil {
		value := *selected.LastPrice
		price = &value
	}
	if price == nil && cfg.StrategyType == "normal" {
		midpoint := cfg.GridLowerPrice.Add(cfg.GridUpperPrice).Div(decimal.NewFromInt(2))
		price = &midpoint
	}
	if price == nil || !price.GreaterThan(decimal.Zero) {
		return domain.StrategyView{}, errors.New("initial price is empty and no market reference price is available")
	}
	// These metrics belong to this strategy run only. They are intentionally
	// kept in memory and reset whenever a new run is started.
	r.cumulativePnl = decimal.Zero
	r.completedCycles = 0
	r.unrealizedPnl = decimal.Zero
	r.accountAvailableBalance = decimal.Zero
	r.accountBalanceKnown = false
	r.accountPositionQuantity = decimal.Zero
	r.accountPositionAverage = decimal.Zero
	r.accountPositionKnown = false
	r.tradingFees = decimal.Zero
	r.runCapital = decimal.Zero
	r.strategyStartedAt = time.Now()
	r.strategyEndedAt = time.Time{}
	r.positionWatchArmed = false
	r.positionWatchPresentCount = 0
	r.positionWatchMissingCount = 0
	r.resetOrderWatchLocked()
	r.orderWatchGraceUntil = time.Time{}
	r.positionWatchGraceUntil = time.Time{}
	r.entryMode = cfg.EntryMode
	r.makerRequoteSeconds = cfg.MakerRequoteSeconds
	r.entryOrderPlacedAt = time.Time{}
	r.entryRequotePending = false
	r.entryRequoteCount = 0
	r.processedTradeIDs = make(map[string]struct{})
	if cfg.StrategyType == "normal" {
		r.strategy = strategy.NewNormalGrid(cfg, r.market)
	} else {
		r.strategy = strategy.NewMartingaleGrid(cfg, r.market)
	}
	marketEntry := cfg.EntryMode == domain.EntryModeMarket && (cfg.StrategyType != "normal" || cfg.Direction != domain.DirectionNeutral)
	action, err := r.strategy.Start(*price, marketEntry)
	if err != nil {
		r.strategy.Stop()
		return domain.StrategyView{}, err
	}
	if cycle := r.strategy.CycleSnapshot(); cycle != nil {
		r.runCapital = cycle.PlannedMargin
	}
	if isMakerEntryMode(cfg.EntryMode) && cfg.EntryMode == domain.EntryModeLimitMaker {
		book, err := r.orderBookLocked(ctx)
		if err != nil {
			r.strategy.Stop()
			return domain.StrategyView{}, err
		}
		if err := validateMakerLimitPrice(book, cfg.Direction, action.Place[0].Price); err != nil {
			r.strategy.Stop()
			return domain.StrategyView{}, err
		}
	}
	if err := r.prepareInitialActionLocked(ctx, &action, false); err != nil {
		r.strategy.Stop()
		return domain.StrategyView{}, err
	}
	// Keep the initial reference price available while the first grid batch is
	// being ordered. This lets the batch planner submit levels closest to the
	// market first instead of walking the grid from its lower bound.
	r.lastPrice = price
	if r.isLiveBackend() {
		if marketEntry || isMakerEntryMode(cfg.EntryMode) {
			r.strategy.SetStatus(domain.StrategyStarting)
		}
		r.startFillConsumerLocked(ctx)
	}
	if err := r.placeActionLocked(ctx, action); err != nil {
		r.strategy.Stop()
		r.strategyEndedAt = time.Now()
		return domain.StrategyView{}, err
	}
	r.lastPrice = price
	pendingEntry := marketEntry || isMakerEntryMode(cfg.EntryMode)
	if pendingEntry {
		// Paper fills are synchronous and the legacy API exposed them as running
		// immediately. Keep that behavior while live Maker/market entries remain
		// in starting state until the initial order fills.
		if r.isLiveBackend() {
			if cycle := r.strategy.CycleSnapshot(); cycle == nil || cycle.PositionQuantity.IsZero() {
				r.strategy.SetStatus(domain.StrategyStarting)
			}
		} else {
			r.strategy.SetStatus(domain.StrategyRunning)
		}
		r.eventLocked(ctx, "strategy_started", "strategy started with pending entry order", map[string]any{"entry_mode": string(cfg.EntryMode), "price": price.String()})
	} else {
		r.strategy.SetStatus(domain.StrategyRunning)
		r.eventLocked(ctx, "strategy_started", "strategy started", map[string]any{"entry_mode": string(cfg.EntryMode), "price": price.String()})
	}
	return r.viewLocked(), nil
}

func (r *Runtime) Tick(ctx context.Context, price decimal.Decimal) (domain.StrategyView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.strategy == nil {
		return domain.StrategyView{}, errors.New("strategy is not started")
	}
	r.lastPrice = &price
	fills, err := r.exchange.ProcessPrice(ctx, price.String())
	if err != nil {
		return domain.StrategyView{}, err
	}
	for _, fill := range fills {
		if err := r.handleFillLocked(ctx, fill); err != nil {
			return domain.StrategyView{}, err
		}
	}
	priceAction, err := r.strategy.OnPrice(price)
	if err != nil {
		return domain.StrategyView{}, err
	}
	if len(priceAction.Place) > 0 || len(priceAction.CancelClientOrderIDs) > 0 || priceAction.Modify != nil {
		if err := r.placeActionLocked(ctx, priceAction); err != nil {
			return domain.StrategyView{}, err
		}
	}
	if err := r.syncOrdersLocked(ctx); err != nil {
		return domain.StrategyView{}, err
	}
	return r.viewLocked(), nil
}

func (r *Runtime) Stop(ctx context.Context, emergency bool, marketIndex *int) (domain.StrategyView, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.strategy == nil {
		return domain.StrategyView{}, errors.New("strategy is not started")
	}
	if marketIndex != nil && *marketIndex != r.strategy.MarketIndex() {
		return domain.StrategyView{}, errors.New("requested market does not match running strategy")
	}
	if r.fillCancel != nil {
		r.fillCancel()
		r.fillCancel = nil
	}
	if emergency {
		r.strategy.EmergencyStop()
	} else {
		r.strategy.Stop()
	}
	r.strategyEndedAt = time.Now()
	r.positionWatchArmed = false
	r.entryOrderPlacedAt = time.Time{}
	r.entryRequotePending = false
	market := r.strategy.MarketIndex()
	stopOrders := make([]domain.ExchangeOrder, 0, len(r.activeOrders))
	if orders, err := r.exchange.OpenOrders(ctx); err == nil {
		r.replaceActiveOrdersLocked(orders)
		stopOrders = orders
	} else {
		// A snapshot failure must not prevent cancelling the orders already
		// known locally. The cancellation loop below will keep verifying and
		// retrying when the strategy uses the staged SoDEX path.
		r.eventLocked(ctx, "strategy_stop_order_sync_failed", "could not refresh active orders before stopping; using local order snapshot", map[string]any{
			"market_index": market, "error": err.Error(),
		})
		for _, order := range r.activeOrders {
			stopOrders = append(stopOrders, order)
		}
	}
	ids := orderIDsForMarket(stopOrders, market)
	if r.shouldBatchNormalGridLifecycle() {
		if err := r.cancelSodexNormalGridBatchesLocked(ctx, stopOrders, market); err != nil {
			return domain.StrategyView{}, err
		}
	} else if err := r.cancelOrdersLocked(ctx, ids); err != nil {
		return domain.StrategyView{}, err
	}
	for _, id := range ids {
		delete(r.activeOrders, id)
	}
	r.syncOrdersBestEffortLocked(ctx)
	r.eventLocked(ctx, "strategy_stopped", "strategy stopped; orders cancelled and positions left untouched", map[string]any{
		"market_index": r.strategy.MarketIndex(), "emergency": emergency,
	})
	return r.viewLocked(), nil
}

func (r *Runtime) View() domain.StrategyView {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.viewLocked()
}

// ViewWithAccount refreshes the in-memory unrealized PnL from a live account
// snapshot before building the strategy view. The snapshot itself is fetched
// by the HTTP layer so the runtime view remains free of network I/O.
func (r *Runtime) ViewWithAccount(account domain.AccountSnapshot) domain.StrategyView {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.accountAvailableBalance = account.AvailableBalance
	if r.accountAvailableBalance.LessThan(decimal.Zero) {
		r.accountAvailableBalance = decimal.Zero
	}
	r.accountBalanceKnown = true
	r.accountPositionQuantity = decimal.Zero
	r.accountPositionAverage = decimal.Zero
	r.accountPositionKnown = false
	r.unrealizedPnl = decimal.Zero
	marketIndex := r.configuredMarketIndex()
	marketSymbol := ""
	if r.strategy != nil {
		marketIndex = r.strategy.MarketIndex()
		marketSymbol = r.strategy.Symbol()
	} else {
		marketSymbol = r.market.Symbol
	}
	for _, position := range account.Positions {
		if positionMatchesMarket(position, marketIndex, marketSymbol) {
			r.unrealizedPnl = r.unrealizedPnl.Add(position.UnrealizedPnl)
			if position.Quantity.GreaterThan(decimal.Zero) && position.AverageEntryPrice.GreaterThan(decimal.Zero) {
				positionQuantity := position.Quantity.Abs()
				if !r.accountPositionKnown {
					r.accountPositionQuantity = positionQuantity
					r.accountPositionAverage = position.AverageEntryPrice
					r.accountPositionKnown = true
				} else {
					totalQuantity := r.accountPositionQuantity.Add(positionQuantity)
					r.accountPositionAverage = r.accountPositionAverage.Mul(r.accountPositionQuantity).Add(position.AverageEntryPrice.Mul(positionQuantity)).Div(totalQuantity)
					r.accountPositionQuantity = totalQuantity
				}
			}
		}
	}
	return r.viewLocked()
}

func (r *Runtime) Markets() []domain.MarketDescriptor {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make([]domain.MarketDescriptor, 0, len(r.marketCatalog))
	for _, market := range r.marketCatalog {
		result = append(result, market)
	}
	return result
}

// RefreshMarkets updates the cached market descriptors, including latest
// tickers, without changing the selected strategy market.
func (r *Runtime) RefreshMarkets(ctx context.Context) error {
	r.mu.Lock()
	if !r.connected {
		if err := r.connectLocked(ctx); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Unlock()

	markets := []domain.MarketDescriptor(nil)
	var err error
	if refresher, ok := r.exchange.(interface {
		RefreshMarkets(context.Context) ([]domain.MarketDescriptor, error)
	}); ok {
		markets, err = refresher.RefreshMarkets(ctx)
	} else {
		markets, err = r.exchange.Markets(ctx)
	}
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	r.catalogMarketsLocked(markets)
	marketIndex := r.configuredMarketIndex()
	if r.strategy != nil {
		marketIndex = r.strategy.MarketIndex()
	}
	if selected, ok := r.marketCatalog[marketCatalogKey(r.currentDEX, marketIndex)]; ok && selected.LastPrice != nil {
		price := *selected.LastPrice
		r.lastPrice = &price
	}
	return nil
}

func (r *Runtime) AddMarket(market domain.MarketDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if market.DEX == "" {
		market.DEX = r.currentDEX
	}
	key := marketCatalogKey(market.DEX, market.MarketIndex)
	if _, exists := r.marketCatalog[key]; exists {
		return fmt.Errorf("market already exists: %s #%d", market.DEX, market.MarketIndex)
	}
	r.marketCatalog[key] = market
	return nil
}

func (r *Runtime) catalogMarketsLocked(markets []domain.MarketDescriptor) {
	for _, market := range markets {
		if r.backendMode != "multi" && r.backendMode != "paper" {
			// A single-exchange runtime owns the adapter even when a test double
			// reports a generic "paper" descriptor.
			market.DEX = r.backendMode
		} else if market.DEX == "" {
			market.DEX = r.currentDEX
		}
		r.marketCatalog[marketCatalogKey(market.DEX, market.MarketIndex)] = market
	}
}

func (r *Runtime) AccountSnapshot(ctx context.Context) (domain.AccountSnapshot, error) {
	return r.exchange.AccountSnapshot(ctx)
}

// ClosePosition submits a reduce-only market order for the current position.
// The caller must stop the strategy first so its grid orders cannot race the
// manual close order.
func (r *Runtime) ClosePosition(ctx context.Context, marketIndex int) (domain.ExchangeOrder, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.strategy != nil && (r.strategy.StrategyStatus() == domain.StrategyRunning || r.strategy.StrategyStatus() == domain.StrategyStarting) {
		return domain.ExchangeOrder{}, errors.New("stop strategy before closing the position")
	}
	account, err := r.exchange.AccountSnapshot(ctx)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	var position *domain.PositionSnapshot
	marketSymbol := ""
	if market, ok := r.marketCatalog[marketCatalogKey(r.currentDEX, marketIndex)]; ok {
		marketSymbol = market.Symbol
	}
	if r.strategy != nil && r.strategy.MarketIndex() == marketIndex {
		marketSymbol = r.strategy.Symbol()
	}
	for index := range account.Positions {
		candidate := &account.Positions[index]
		if positionMatchesMarket(*candidate, marketIndex, marketSymbol) && candidate.Quantity.GreaterThan(decimal.Zero) {
			position = candidate
			break
		}
	}
	if position == nil {
		return domain.ExchangeOrder{}, fmt.Errorf("no open position for market %d", marketIndex)
	}
	if r.isLiveBackend() {
		// Stop cancels the strategy fill consumer. Start a runtime-owned
		// consumer again so the reduce-only close fill clears local order state.
		r.startFillConsumerLocked(ctx)
	}
	side := domain.OrderSideSell
	if position.Side == string(domain.DirectionShort) {
		side = domain.OrderSideBuy
	}
	intent := domain.OrderIntent{
		ClientOrderID:    fmt.Sprintf("mg-close-%d-%d", marketIndex, time.Now().UnixNano()),
		MarketIndex:      marketIndex,
		Symbol:           position.Symbol,
		Side:             side,
		Role:             domain.OrderRoleClose,
		Price:            decimal.Zero,
		Quantity:         position.Quantity,
		ReduceOnly:       true,
		OrderType:        domain.OrderTypeMarket,
		MaxSlippageRatio: decimal.RequireFromString("0.005"),
	}
	order, err := r.placeOrderLocked(ctx, intent)
	if err != nil {
		return domain.ExchangeOrder{}, err
	}
	r.eventLocked(ctx, "position_close_submitted", "reduce-only market close submitted", map[string]any{
		"market_index": marketIndex, "client_order_id": order.ClientOrderID,
		"side": side, "quantity": position.Quantity.String(),
	})
	return order, nil
}

func (r *Runtime) History(ctx context.Context, limit int) ([]map[string]any, error) {
	if r.store == nil {
		return []map[string]any{}, nil
	}
	return r.store.Recent(ctx, limit)
}

func (r *Runtime) connectLocked(ctx context.Context) error {
	if r.connected {
		return nil
	}
	if err := r.exchange.Connect(ctx); err != nil {
		return err
	}
	markets, err := r.exchange.Markets(ctx)
	if err != nil {
		return err
	}
	r.catalogMarketsLocked(markets)
	if len(r.marketCatalog) == 0 {
		return errors.New("no market metadata available")
	}
	selected, ok := r.marketCatalog[marketCatalogKey(r.currentDEX, r.configuredMarketIndex())]
	if r.backendMode == "paper" {
		selected, ok = r.marketCatalog[marketCatalogKey("paper", 0)]
	}
	if !ok {
		for _, item := range r.marketCatalog {
			selected = item
			break
		}
	}
	if err := r.selectDEXLocked(selected.DEX); err != nil {
		return err
	}
	r.market = specFromDescriptor(selected)
	r.setExchangeMarket()
	r.connected = true
	return nil
}

func (r *Runtime) startFillConsumerLocked(parent context.Context) {
	if r.fillCancel != nil {
		return
	}
	// The fill consumer is a runtime-owned background task. The context passed
	// to /api/strategy/start belongs to the HTTP request and is cancelled as
	// soon as that request returns, which would abort follow-up grid orders.
	parent = r.runtimeCtx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	r.fillCancel = cancel
	fills, errorsCh := r.exchange.SubscribeFills(ctx)
	go func() {
		fillReconcileTicker := time.NewTicker(2 * time.Second)
		defer fillReconcileTicker.Stop()
		processFill := func(fill domain.Fill) {
			r.mu.Lock()
			if err := r.handleFillLocked(ctx, fill); err != nil {
				r.eventLocked(ctx, "fill_processing_error", "fill received but strategy processing failed", map[string]any{
					"client_order_id": fill.ClientOrderID, "error": err.Error(),
				})
			}
			r.syncOrdersBestEffortLocked(ctx)
			r.mu.Unlock()
		}
		for {
			select {
			case <-ctx.Done():
				return
			case fill, ok := <-fills:
				if !ok {
					return
				}
				processFill(fill)
				continue
				/*
					r.mu.Lock()
					if err := r.handleFillLocked(ctx, fill); err != nil {
						r.eventLocked(ctx, "fill_processing_error", "成交已收到但后续策略处理失败", map[string]any{
							"client_order_id": fill.ClientOrderID, "error": err.Error(),
						})
					}
					r.syncOrdersBestEffortLocked(ctx)
					r.mu.Unlock()
				*/
			case <-fillReconcileTicker.C:
				reconciler, ok := r.exchange.(interface {
					ReconcileFills(context.Context) ([]domain.Fill, error)
				})
				if !ok {
					continue
				}
				reconcileCtx, reconcileCancel := context.WithTimeout(ctx, 5*time.Second)
				reconciled, err := reconciler.ReconcileFills(reconcileCtx)
				reconcileCancel()
				if err != nil {
					r.mu.Lock()
					r.eventLocked(ctx, "fill_reconcile_error", "periodic fill reconciliation failed", map[string]any{"error": err.Error()})
					r.mu.Unlock()
					continue
				}
				for _, reconciledFill := range reconciled {
					processFill(reconciledFill)
				}
			case err, ok := <-errorsCh:
				if !ok {
					return
				}
				if err != nil {
					r.mu.Lock()
					r.eventLocked(ctx, "exchange_stream_error", "exchange event stream stopped", map[string]any{"error": err.Error()})
					r.mu.Unlock()
				}
				continue
			}
		}
	}()
}

func (r *Runtime) positionWatchdog() {
	ticker := time.NewTicker(positionWatchInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.runtimeCtx.Done():
			return
		case <-ticker.C:
			r.checkPositionWatch()
			r.checkEntryWatch()
			r.checkOrderWatch()
		}
	}
}

func (r *Runtime) checkPositionWatch() {
	if !r.isLiveBackend() {
		return
	}
	r.mu.Lock()
	if r.strategy == nil || r.strategy.StrategyType() != "martingale" || r.strategy.StrategyStatus() != domain.StrategyRunning || r.strategy.CycleSnapshot() == nil || r.strategy.CycleSnapshot().PositionQuantity.IsZero() {
		r.positionWatchArmed = false
		r.positionWatchPresentCount = 0
		r.positionWatchMissingCount = 0
		r.mu.Unlock()
		return
	}
	if time.Now().Before(r.positionWatchGraceUntil) {
		r.positionWatchPresentCount = 0
		r.positionWatchMissingCount = 0
		r.mu.Unlock()
		return
	}
	marketIndex := r.strategy.MarketIndex()
	marketSymbol := r.strategy.Symbol()
	r.mu.Unlock()

	account, err := r.exchange.AccountSnapshot(r.runtimeCtx)
	if err != nil {
		// Paper and test doubles do not expose account snapshots. For a live backend,
		// an unavailable snapshot is not evidence that the position is gone.
		return
	}
	hasPosition := accountHasPosition(account, marketIndex, marketSymbol)

	r.mu.Lock()
	if r.strategy == nil || r.strategy.StrategyType() != "martingale" || r.strategy.StrategyStatus() != domain.StrategyRunning || r.strategy.MarketIndex() != marketIndex {
		r.positionWatchArmed = false
		r.positionWatchPresentCount = 0
		r.positionWatchMissingCount = 0
		r.mu.Unlock()
		return
	}
	if hasPosition {
		r.positionWatchPresentCount++
		if r.positionWatchPresentCount >= positionWatchMissingConfirmations {
			r.positionWatchArmed = true
		}
		r.positionWatchMissingCount = 0
		r.mu.Unlock()
		return
	}
	r.positionWatchPresentCount = 0
	if !r.positionWatchArmed || r.positionWatchCancelPending {
		r.mu.Unlock()
		return
	}
	r.positionWatchMissingCount++
	if r.positionWatchMissingCount < positionWatchMissingConfirmations {
		r.mu.Unlock()
		return
	}
	r.positionWatchArmed = false
	r.positionWatchPresentCount = 0
	r.positionWatchMissingCount = 0
	r.positionWatchCancelPending = true
	r.eventLocked(r.runtimeCtx, "position_watch_triggered", "position disappeared; cancelling all orders for the market", map[string]any{
		"market_index": marketIndex, "interval_ms": int(positionWatchInterval / time.Millisecond),
		"missing_confirmations": positionWatchMissingConfirmations,
	})
	r.mu.Unlock()
	go r.cancelMarketOrdersUntilEmpty(marketIndex)
}

// checkEntryWatch keeps a Maker initial order at the current top of book. It
// deliberately operates only while the initial quantity is still pending;
// once the entry is fully filled, the normal fill path owns TP/grid orders.
func (r *Runtime) checkEntryWatch() {
	if !r.isLiveBackend() {
		return
	}
	r.mu.Lock()
	if r.strategy == nil || r.entryMode != domain.EntryModeMaker || r.entryRequotePending ||
		(r.strategy.StrategyStatus() != domain.StrategyStarting && r.strategy.StrategyStatus() != domain.StrategyRunning) {
		r.mu.Unlock()
		return
	}
	pending, ok := r.strategy.PendingInitialOrder()
	if !ok {
		r.entryOrderPlacedAt = time.Time{}
		r.mu.Unlock()
		return
	}
	interval := time.Duration(r.makerRequoteSeconds) * time.Second
	if r.entryOrderPlacedAt.IsZero() || time.Since(r.entryOrderPlacedAt) < interval {
		r.mu.Unlock()
		return
	}
	r.entryRequotePending = true
	marketIndex := r.strategy.MarketIndex()
	clientOrderID := pending.ClientOrderID
	direction := r.strategy.Direction()
	r.mu.Unlock()

	orders, err := r.exchange.OpenOrders(r.runtimeCtx)
	if err != nil {
		r.finishEntryRequote(false, fmt.Errorf("read open entry orders: %w", err))
		return
	}
	hasCurrentOrder := false
	var currentOrder domain.ExchangeOrder
	for _, order := range orders {
		if order.MarketIndex == marketIndex && order.ClientOrderID == clientOrderID {
			hasCurrentOrder = true
			currentOrder = order
			break
		}
	}
	if !hasCurrentOrder {
		if supported, hadFills, reconcileErr := r.reconcileFillsForOrderWatch(); supported {
			if reconcileErr != nil {
				r.finishEntryRequote(false, reconcileErr)
				return
			}
			if hadFills {
				r.finishEntryRequote(true, nil)
				return
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.strategy == nil || r.entryMode != domain.EntryModeMaker || r.strategy.MarketIndex() != marketIndex {
		r.entryRequotePending = false
		return
	}
	pending, ok = r.strategy.PendingInitialOrder()
	if !ok || pending.ClientOrderID != clientOrderID {
		r.entryRequotePending = false
		return
	}
	book, err := r.orderBookLocked(r.runtimeCtx)
	if err != nil {
		r.entryOrderPlacedAt = time.Now()
		r.entryRequotePending = false
		r.eventLocked(r.runtimeCtx, "initial_order_requote_failed", "order book unavailable; Maker entry will retry", map[string]any{"error": err.Error(), "client_order_id": clientOrderID})
		return
	}
	price, err := makerEntryPrice(book, direction)
	if err != nil {
		r.entryOrderPlacedAt = time.Now()
		r.entryRequotePending = false
		r.eventLocked(r.runtimeCtx, "initial_order_requote_failed", "order book has no entry liquidity; Maker entry will retry", map[string]any{"error": err.Error(), "client_order_id": clientOrderID})
		return
	}
	price = r.quantizeEntryPrice(price, direction)
	if price.Equal(pending.Price) && hasCurrentOrder {
		r.activeOrders[clientOrderID] = currentOrder
		r.entryOrderPlacedAt = time.Now()
		r.entryRequotePending = false
		return
	}
	intent := pending
	intent.Price = price
	intent.OrderType = domain.OrderTypeLimit
	intent.PostOnly = true
	var order domain.ExchangeOrder
	if hasCurrentOrder {
		order, err = r.modifyOrderLocked(r.runtimeCtx, clientOrderID, intent)
	} else {
		order, err = r.placeOrderLocked(r.runtimeCtx, intent)
	}
	if err != nil {
		r.entryOrderPlacedAt = time.Now()
		r.entryRequotePending = false
		r.eventLocked(r.runtimeCtx, "initial_order_requote_failed", "Maker entry order could not be refreshed; retry will continue", map[string]any{"error": err.Error(), "client_order_id": clientOrderID, "price": price.String()})
		return
	}
	if order.ClientOrderID == "" {
		order.ClientOrderID = intent.ClientOrderID
	}
	r.activeOrders[order.ClientOrderID] = order
	r.strategy.UpdatePendingInitialOrder(intent)
	r.bindStrategyOrderIDLocked(intent, order)
	r.entryOrderPlacedAt = time.Now()
	r.entryRequotePending = false
	r.entryRequoteCount++
	r.eventLocked(r.runtimeCtx, "initial_order_requoted", "Maker entry order moved to the latest top of book", map[string]any{
		"client_order_id": clientOrderID, "price": price.String(), "requote_count": r.entryRequoteCount,
	})
}

func (r *Runtime) finishEntryRequote(hadFill bool, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entryRequotePending = false
	if hadFill {
		r.entryOrderPlacedAt = time.Time{}
		return
	}
	r.entryOrderPlacedAt = time.Now()
	if err != nil {
		r.eventLocked(r.runtimeCtx, "initial_order_requote_failed", "Maker entry refresh will retry", map[string]any{"error": err.Error()})
	}
}

// checkOrderWatch verifies that the exchange still has every order the
// running strategy expects after a position-opening fill. The fill stream can
// report a market fill successfully while the subsequent batch of safety and
// take-profit orders is delayed, partially accepted, or lost by a transient
// exchange response. In that case the strategy state still contains the
// authoritative intents and the missing orders can be submitted again.
func (r *Runtime) checkOrderWatch() {
	if !r.isLiveBackend() {
		return
	}
	r.mu.Lock()
	if r.strategy == nil || r.positionWatchCancelPending ||
		(r.strategy.StrategyStatus() != domain.StrategyRunning && r.strategy.StrategyStatus() != domain.StrategyStarting) {
		r.resetOrderWatchLocked()
		r.mu.Unlock()
		return
	}
	if r.entryMode == domain.EntryModeMaker {
		if _, pending := r.strategy.PendingInitialOrder(); pending {
			r.resetOrderWatchLocked()
			r.mu.Unlock()
			return
		}
	}
	if r.strategy.StrategyType() == "martingale" && time.Now().Before(r.orderWatchGraceUntil) {
		r.resetOrderWatchLocked()
		r.mu.Unlock()
		return
	}
	isNormalGrid := r.strategy.StrategyType() == "normal"
	cycle := r.strategy.CycleSnapshot()
	if cycle == nil || (!isNormalGrid && cycle.PositionQuantity.IsZero()) {
		r.resetOrderWatchLocked()
		r.mu.Unlock()
		return
	}
	expected := r.strategy.ExpectedOrderIntents()
	if len(expected) == 0 {
		r.resetOrderWatchLocked()
		r.mu.Unlock()
		return
	}
	marketIndex := r.strategy.MarketIndex()
	knownActiveIDs := make(map[string]struct{})
	for id, order := range r.activeOrders {
		if id != "" && order.MarketIndex == marketIndex {
			knownActiveIDs[id] = struct{}{}
		}
	}
	r.mu.Unlock()

	orders, err := r.exchange.OpenOrders(r.runtimeCtx)
	if err != nil {
		return
	}
	openByClientID := make(map[string]domain.ExchangeOrder, len(orders))
	for _, order := range orders {
		if order.MarketIndex == marketIndex && order.ClientOrderID != "" {
			openByClientID[order.ClientOrderID] = order
		}
	}
	missing := make([]domain.OrderIntent, 0, len(expected))
	mismatched := make([]domain.OrderIntent, 0)
	takeProfitMismatched := make([]domain.OrderIntent, 0, 1)
	for _, intent := range expected {
		if intent.ClientOrderID == "" {
			continue
		}
		order, exists := openByClientID[intent.ClientOrderID]
		if !exists {
			missing = append(missing, intent)
		} else if !r.exchangeOrderMatchesIntent(order, intent) {
			if intent.Role == domain.OrderRoleTakeProfit {
				// A live TP is modified in place below. Never put it in the
				// cancel-and-replace set: the existing order is the position's
				// protection while the account snapshot catches up.
				takeProfitMismatched = append(takeProfitMismatched, intent)
			} else {
				mismatched = append(mismatched, intent)
			}
		}
	}
	unexpected := make([]string, 0)
	expectedIDs := make(map[string]struct{}, len(expected))
	for _, intent := range expected {
		if intent.ClientOrderID != "" {
			expectedIDs[intent.ClientOrderID] = struct{}{}
		}
	}
	if isNormalGrid {
		for id := range knownActiveIDs {
			if _, exists := expectedIDs[id]; !exists {
				if _, stillOpen := openByClientID[id]; stillOpen {
					unexpected = append(unexpected, id)
				}
			}
		}
	}
	sort.Strings(unexpected)

	r.mu.Lock()
	if r.strategy == nil || r.positionWatchCancelPending || r.strategy.MarketIndex() != marketIndex ||
		(r.strategy.StrategyStatus() != domain.StrategyRunning && r.strategy.StrategyStatus() != domain.StrategyStarting) {
		r.resetOrderWatchLocked()
		r.mu.Unlock()
		return
	}
	if len(missing) == 0 && len(mismatched) == 0 && len(takeProfitMismatched) == 0 && len(unexpected) == 0 {
		r.resetOrderWatchLocked()
		r.replaceActiveOrdersLocked(orders)
		r.mu.Unlock()
		return
	}
	missingKeyParts := make([]string, 0, len(missing)+len(mismatched)+len(takeProfitMismatched)+len(unexpected))
	for _, intent := range missing {
		missingKeyParts = append(missingKeyParts, "missing:"+intent.ClientOrderID)
	}
	for _, intent := range mismatched {
		missingKeyParts = append(missingKeyParts, "mismatch:"+intent.ClientOrderID)
	}
	for _, intent := range takeProfitMismatched {
		missingKeyParts = append(missingKeyParts, "take-profit-modify:"+intent.ClientOrderID)
	}
	for _, id := range unexpected {
		missingKeyParts = append(missingKeyParts, "unexpected:"+id)
	}
	missingKey := strings.Join(missingKeyParts, ",")
	if missingKey != r.orderWatchMissingKey {
		r.orderWatchMissingKey = missingKey
		r.orderWatchMissingCount = 1
	} else {
		r.orderWatchMissingCount++
	}
	if r.orderWatchMissingCount < orderWatchMissingConfirmations {
		r.mu.Unlock()
		return
	}
	r.orderWatchMissingCount = 0
	r.replaceActiveOrdersLocked(orders)
	r.mu.Unlock()

	// Give a supported exchange one last chance to deliver the fill that
	// removed an order before we recreate it.
	if supported, hadFills, reconcileErr := r.reconcileFillsForOrderWatch(); supported {
		if reconcileErr != nil {
			r.mu.Lock()
			r.eventLocked(r.runtimeCtx, "order_watch_reconcile_error", "order watchdog fill reconciliation failed", map[string]any{
				"market_index": marketIndex, "error": reconcileErr.Error(),
			})
			r.resetOrderWatchLocked()
			r.mu.Unlock()
			return
		}
		if hadFills {
			r.mu.Lock()
			r.resetOrderWatchLocked()
			r.mu.Unlock()
			return
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.strategy == nil || r.strategy.MarketIndex() != marketIndex ||
		(r.strategy.StrategyStatus() != domain.StrategyRunning && r.strategy.StrategyStatus() != domain.StrategyStarting) {
		r.resetOrderWatchLocked()
		return
	}
	place := append(append([]domain.OrderIntent(nil), missing...), mismatched...)
	var takeProfitModify *strategy.ModifyAction
	if len(takeProfitMismatched) > 0 {
		takeProfitModify = &strategy.ModifyAction{
			ClientOrderID: takeProfitMismatched[0].ClientOrderID,
			Intent:        takeProfitMismatched[0],
		}
	}
	cancelIDs := make([]string, 0, len(mismatched)+len(unexpected))
	for _, intent := range mismatched {
		cancelIDs = append(cancelIDs, intent.ClientOrderID)
	}
	cancelIDs = append(cancelIDs, unexpected...)
	if err := r.placeActionLocked(r.runtimeCtx, strategy.Action{
		Place: place, Modify: takeProfitModify, CancelClientOrderIDs: uniqueOrderIDs(cancelIDs), CancelBeforePlace: len(cancelIDs) > 0,
	}); err != nil {
		r.strategy.SetStatus(domain.StrategyStarting)
		r.eventLocked(r.runtimeCtx, "order_watch_repair_failed", "missing strategy orders could not be recreated; watchdog will retry", map[string]any{
			"market_index": marketIndex, "missing_count": len(missing), "mismatched_count": len(mismatched), "take_profit_modify_count": len(takeProfitMismatched), "unexpected_count": len(unexpected), "error": err.Error(),
		})
		return
	}
	r.eventLocked(r.runtimeCtx, "order_watch_repaired", "strategy orders were reconciled with the exchange", map[string]any{
		"market_index": marketIndex, "recreated_count": len(place), "take_profit_modified_count": len(takeProfitMismatched), "cancelled_count": len(cancelIDs),
	})
	r.resetOrderWatchLocked()
}

func (r *Runtime) reconcileFillsForOrderWatch() (supported, hadFills bool, err error) {
	reconciler, ok := r.exchange.(interface {
		ReconcileFills(context.Context) ([]domain.Fill, error)
	})
	if !ok {
		return false, false, nil
	}
	reconcileCtx, cancel := context.WithTimeout(r.runtimeCtx, 5*time.Second)
	fills, err := reconciler.ReconcileFills(reconcileCtx)
	cancel()
	if err != nil {
		return true, false, err
	}
	for _, fill := range fills {
		r.mu.Lock()
		if r.strategy != nil {
			if processErr := r.handleFillLocked(r.runtimeCtx, fill); processErr != nil {
				r.mu.Unlock()
				return true, true, processErr
			}
			r.syncOrdersBestEffortLocked(r.runtimeCtx)
		}
		r.mu.Unlock()
	}
	return true, len(fills) > 0, nil
}

func (r *Runtime) resetOrderWatchLocked() {
	r.orderWatchMissingKey = ""
	r.orderWatchMissingCount = 0
}

func (r *Runtime) cancelMarketOrdersUntilEmpty(marketIndex int) {
	defer func() {
		r.mu.Lock()
		r.positionWatchCancelPending = false
		r.positionWatchArmed = false
		r.positionWatchPresentCount = 0
		r.positionWatchMissingCount = 0
		r.mu.Unlock()
	}()

	attempt := 0
	for {
		if r.runtimeCtx.Err() != nil {
			return
		}
		orders, err := r.exchange.OpenOrders(r.runtimeCtx)
		if err != nil {
			attempt++
			if attempt == 1 || attempt%10 == 0 {
				r.recordPositionWatchEvent("position_watch_retry", "failed to read active orders; retrying", marketIndex, attempt, err)
			}
			if !waitForPositionWatchRetry(r.runtimeCtx) {
				return
			}
			continue
		}
		r.mu.Lock()
		r.replaceActiveOrdersLocked(orders)
		r.mu.Unlock()
		ids := orderIDsForMarket(orders, marketIndex)
		if len(ids) == 0 {
			r.recordPositionWatchEvent("position_watch_cleared", "all market orders cancelled after position disappeared", marketIndex, attempt, nil)
			return
		}
		attempt++
		if err := r.cancelOrders(r.runtimeCtx, ids); err != nil {
			if attempt == 1 || attempt%10 == 0 {
				r.recordPositionWatchEvent("position_watch_retry", "order cancellation failed; retrying until empty", marketIndex, attempt, err)
			}
		} else if attempt == 1 {
			r.recordPositionWatchEvent("position_watch_cancel_submitted", "market order cancellation submitted; verifying until empty", marketIndex, attempt, nil)
		}
		if !waitForPositionWatchRetry(r.runtimeCtx) {
			return
		}
	}
}

func (r *Runtime) recordPositionWatchEvent(kind, message string, marketIndex, attempt int, err error) {
	r.mu.Lock()
	data := map[string]any{"market_index": marketIndex, "retry_attempt": attempt}
	if err != nil {
		data["error"] = err.Error()
	}
	r.eventLocked(r.runtimeCtx, kind, message, data)
	r.mu.Unlock()
}

func waitForPositionWatchRetry(ctx context.Context) bool {
	timer := time.NewTimer(positionWatchInterval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func accountHasPosition(account domain.AccountSnapshot, marketIndex int, marketSymbol string) bool {
	for _, position := range account.Positions {
		if positionMatchesMarket(position, marketIndex, marketSymbol) && position.Quantity.GreaterThan(decimal.Zero) {
			return true
		}
	}
	return false
}

func positionMatchesMarket(position domain.PositionSnapshot, marketIndex int, marketSymbol string) bool {
	if position.MarketIndex == marketIndex {
		return true
	}
	if position.Symbol == "" || marketSymbol == "" {
		return false
	}
	normalize := func(value string) string {
		value = strings.ToUpper(strings.TrimSpace(value))
		if separator := strings.IndexByte(value, '-'); separator >= 0 {
			value = value[:separator]
		}
		return value
	}
	return normalize(position.Symbol) == normalize(marketSymbol)
}

func orderIDsForMarket(orders []domain.ExchangeOrder, marketIndex int) []string {
	ids := make([]string, 0, len(orders))
	seen := make(map[string]struct{}, len(orders))
	for _, order := range orders {
		if order.MarketIndex != marketIndex || order.ClientOrderID == "" {
			continue
		}
		if _, exists := seen[order.ClientOrderID]; exists {
			continue
		}
		seen[order.ClientOrderID] = struct{}{}
		ids = append(ids, order.ClientOrderID)
	}
	return ids
}

func (r *Runtime) handleFillLocked(ctx context.Context, fill domain.Fill) error {
	if fill.TradeID != "" {
		if _, exists := r.processedTradeIDs[fill.TradeID]; exists {
			return nil
		}
		r.processedTradeIDs[fill.TradeID] = struct{}{}
	}
	if r.strategy == nil {
		return nil
	}
	if fill.ClientOrderID != "" {
		delete(r.activeOrders, fill.ClientOrderID)
	}
	fee := r.fillFee(fill)
	r.tradingFees = r.tradingFees.Add(fee)
	r.eventLocked(ctx, "fill", "order filled", map[string]any{"client_order_id": fill.ClientOrderID, "price": fill.Price.String(), "quantity": fill.Quantity.String(), "trading_fee": fee.String()})
	if fill.Role == domain.OrderRoleClose {
		r.eventLocked(ctx, "position_close_fill", "reduce-only market close filled", map[string]any{
			"client_order_id": fill.ClientOrderID, "price": fill.Price.String(), "quantity": fill.Quantity.String(), "trading_fee": fee.String(),
		})
		return nil
	}
	action, err := r.strategy.OnFill(fill)
	if err != nil {
		return err
	}
	if err := r.prepareInitialActionLocked(ctx, &action, true); err != nil {
		return err
	}
	if err := r.placeActionLocked(ctx, action); err != nil {
		cycle := r.strategy.CycleSnapshot()
		if cycle != nil && !cycle.PositionQuantity.IsZero() && len(r.strategy.ExpectedOrderIntents()) > 0 {
			// Keep the strategy recoverable. The order watchdog will compare
			// the expected intents with remote open orders and retry them.
			r.strategy.SetStatus(domain.StrategyStarting)
			r.eventLocked(ctx, "grid_order_failed", "order placement failed; strategy is waiting for watchdog compensation", map[string]any{"error": err.Error()})
		} else {
			r.strategy.Stop()
			r.strategyEndedAt = time.Now()
			r.eventLocked(ctx, "grid_order_failed", "order placement failed; strategy stopped without closing the position", map[string]any{"error": err.Error()})
		}
		return err
	}
	if action.CycleCompleted {
		r.cumulativePnl = r.cumulativePnl.Add(action.CompletedCycleRealizedPnl)
		r.completedCycles++
		if r.strategy.Continuous() && (r.strategy.CycleSnapshot() == nil || r.strategy.CycleSnapshot().PositionQuantity.IsZero()) {
			r.strategy.SetStatus(domain.StrategyStarting)
		}
		r.eventLocked(ctx, "cycle_completed", "cycle completed", map[string]any{
			"market_index": r.strategy.MarketIndex(),
			"realized_pnl": action.CompletedCycleRealizedPnl.String(),
		})
		if r.strategy.StrategyStatus() == domain.StrategyStopped || r.strategy.StrategyStatus() == domain.StrategyEmergencyStop {
			r.strategyEndedAt = time.Now()
		}
	}
	if cycle := r.strategy.CycleSnapshot(); r.strategy.StrategyStatus() == domain.StrategyStarting && cycle != nil && !cycle.PositionQuantity.IsZero() {
		r.strategy.SetStatus(domain.StrategyRunning)
	}
	return nil
}

func (r *Runtime) placeActionLocked(ctx context.Context, action strategy.Action) error {
	needsFillReconciliation := false
	if r.shouldPrioritizeNormalGridOrders() && len(action.Place) > 1 {
		action.Place = r.orderIntentsNearPriceLocked(action.Place)
	}
	if action.CancelBeforePlace && len(action.CancelClientOrderIDs) > 0 {
		if err := r.cancelOrdersLocked(ctx, action.CancelClientOrderIDs); err != nil {
			return err
		}
		for _, id := range action.CancelClientOrderIDs {
			delete(r.activeOrders, id)
		}
	}
	if len(action.CancelClientOrderIDs) > 0 && action.CycleCompleted {
		if err := r.cancelCycleOrdersLocked(ctx, r.strategy.MarketIndex(), action.CancelClientOrderIDs); err != nil {
			return err
		}
		r.eventLocked(ctx, "cycle_orders_cancelled", "previous cycle orders cancelled before rollover", map[string]any{
			"market_index": r.strategy.MarketIndex(), "requested_count": len(action.CancelClientOrderIDs),
		})
	}
	if action.Modify != nil {
		order, err := r.modifyOrderLocked(ctx, action.Modify.ClientOrderID, action.Modify.Intent)
		if err != nil {
			return err
		}
		if order.ClientOrderID == "" {
			order.ClientOrderID = action.Modify.ClientOrderID
		}
		r.activeOrders[order.ClientOrderID] = order
		r.bindStrategyOrderIDLocked(action.Modify.Intent, order)
	}
	if len(action.Place) > 0 {
		orders, err := r.placeIntentsLocked(ctx, action.Place)
		if err != nil {
			return err
		}
		for index, order := range orders {
			if order.ClientOrderID == "" && index < len(action.Place) {
				order.ClientOrderID = action.Place[index].ClientOrderID
			}
			r.activeOrders[order.ClientOrderID] = order
			if index < len(action.Place) {
				r.bindStrategyOrderIDLocked(action.Place[index], order)
				if action.Place[index].OrderType == domain.OrderTypeMarket {
					needsFillReconciliation = true
				}
			}
		}
	}
	if len(action.CancelClientOrderIDs) > 0 && !action.CycleCompleted && !action.CancelBeforePlace {
		if err := r.cancelOrdersLocked(ctx, action.CancelClientOrderIDs); err != nil {
			return err
		}
		for _, id := range action.CancelClientOrderIDs {
			delete(r.activeOrders, id)
		}
	}
	if needsFillReconciliation {
		if reconciler, ok := r.exchange.(interface {
			ReconcileFills(context.Context) ([]domain.Fill, error)
		}); ok {
			reconcileCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			fills, err := reconciler.ReconcileFills(reconcileCtx)
			cancel()
			if err != nil {
				r.eventLocked(ctx, "fill_reconcile_error", "failed to reconcile recent fills", map[string]any{"error": err.Error()})
			} else {
				for _, fill := range fills {
					if err := r.handleFillLocked(ctx, fill); err != nil {
						return err
					}
				}
			}
		}
	}
	if len(action.Place) > 0 {
		roles := make([]string, 0, len(action.Place))
		for _, intent := range action.Place {
			roles = append(roles, string(intent.Role))
		}
		r.eventLocked(ctx, "orders_submitted", "strategy orders submitted", map[string]any{
			"count": len(action.Place), "roles": roles,
		})
	}
	if action.Modify != nil {
		r.eventLocked(ctx, "order_modified", "strategy order modified", map[string]any{
			"client_order_id": action.Modify.ClientOrderID, "role": action.Modify.Intent.Role,
			"price": action.Modify.Intent.Price.String(), "quantity": action.Modify.Intent.Quantity.String(),
		})
	}
	if r.strategy != nil && isMakerEntryMode(r.entryMode) {
		if _, pending := r.strategy.PendingInitialOrder(); pending {
			r.entryOrderPlacedAt = time.Now()
		} else {
			r.entryOrderPlacedAt = time.Time{}
			r.entryRequotePending = false
		}
	}
	if r.strategy != nil && r.strategy.StrategyType() == "martingale" && (action.Modify != nil || len(action.Place) > 0) {
		// The exchange order view can lag a successful place/modify request. Do
		// not interpret that propagation window as a missing or mismatched TP.
		now := time.Now()
		r.orderWatchGraceUntil = now.Add(orderWatchStabilization)
		r.positionWatchGraceUntil = now.Add(positionWatchStabilization)
	}
	return nil
}

func (r *Runtime) placeIntentsLocked(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	if r.shouldThrottleNormalGrid(intents) {
		return r.placeSodexNormalGridBatchesLocked(ctx, intents)
	}
	if len(intents) > 1 {
		return r.placeOrdersBatchLocked(ctx, intents)
	}
	order, err := r.placeOrderLocked(ctx, intents[0])
	if err != nil {
		return nil, err
	}
	return []domain.ExchangeOrder{order}, nil
}

func (r *Runtime) shouldThrottleNormalGrid(intents []domain.OrderIntent) bool {
	return r.strategy != nil && r.strategy.StrategyType() == "normal" &&
		strings.EqualFold(r.currentDEX, "sodex") && len(intents) > r.sodexGridBatchSize()
}

func (r *Runtime) shouldPrioritizeNormalGridOrders() bool {
	return r.strategy != nil && r.strategy.StrategyType() == "normal" && strings.EqualFold(r.currentDEX, "sodex")
}

func (r *Runtime) shouldBatchNormalGridLifecycle() bool {
	return r.shouldPrioritizeNormalGridOrders()
}

func (r *Runtime) orderReferencePriceLocked() decimal.Decimal {
	if r.lastPrice != nil && r.lastPrice.GreaterThan(decimal.Zero) {
		return *r.lastPrice
	}
	return decimal.Zero
}

func (r *Runtime) orderIntentsNearPriceLocked(intents []domain.OrderIntent) []domain.OrderIntent {
	ordered := append([]domain.OrderIntent(nil), intents...)
	reference := r.orderReferencePriceLocked()
	sort.SliceStable(ordered, func(i, j int) bool {
		if reference.GreaterThan(decimal.Zero) {
			iDistance := ordered[i].Price.Sub(reference).Abs()
			jDistance := ordered[j].Price.Sub(reference).Abs()
			if !iDistance.Equal(jDistance) {
				return iDistance.LessThan(jDistance)
			}
		}
		if !ordered[i].Price.Equal(ordered[j].Price) {
			return ordered[i].Price.LessThan(ordered[j].Price)
		}
		if ordered[i].Level != ordered[j].Level {
			return ordered[i].Level < ordered[j].Level
		}
		return ordered[i].ClientOrderID < ordered[j].ClientOrderID
	})
	return ordered
}

func (r *Runtime) sodexGridBatchSize() int {
	size := r.settings.SodexGridBatchSize
	if size <= 0 {
		size = defaultSodexGridBatchSize
	}
	if size > maxSodexGridBatchSize {
		size = maxSodexGridBatchSize
	}
	return size
}

func sodexGridBatchInterval(settings config.Settings) time.Duration {
	if settings.SodexGridBatchIntervalMS <= 0 {
		return defaultSodexGridBatchInterval
	}
	interval := time.Duration(settings.SodexGridBatchIntervalMS) * time.Millisecond
	if interval < defaultSodexGridBatchInterval {
		return defaultSodexGridBatchInterval
	}
	return interval
}

func (r *Runtime) placeSodexNormalGridBatchesLocked(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	batchSize := r.sodexGridBatchSize()
	totalBatches := (len(intents) + batchSize - 1) / batchSize
	submitted := make([]domain.ExchangeOrder, 0, len(intents))
	for start := 0; start < len(intents); start += batchSize {
		if start > 0 {
			if !waitForGridBatchInterval(ctx, r.normalGridBatchInterval) {
				if err := r.rollbackNormalGridOrdersLocked(ctx, submitted, nil); err != nil {
					return nil, err
				}
				return nil, ctx.Err()
			}
		}
		end := start + batchSize
		if end > len(intents) {
			end = len(intents)
		}
		batch := intents[start:end]
		batchNumber := start/batchSize + 1
		orders, err := r.placeOrdersBatchLocked(ctx, batch)
		for index := range orders {
			if orders[index].ClientOrderID == "" && index < len(batch) {
				orders[index].ClientOrderID = batch[index].ClientOrderID
			}
		}
		submitted = append(submitted, orders...)
		if err != nil {
			return nil, r.rollbackNormalGridOrdersLocked(ctx, submitted, batch, fmt.Errorf("SoDEX normal-grid batch %d/%d failed: %w", batchNumber, totalBatches, err))
		}
		if len(orders) != len(batch) {
			return nil, r.rollbackNormalGridOrdersLocked(ctx, submitted, batch, fmt.Errorf("SoDEX normal-grid batch %d/%d returned %d orders for %d requests", batchNumber, totalBatches, len(orders), len(batch)))
		}
		r.eventLocked(ctx, "normal_grid_batch_submitted", "normal-grid order batch submitted", map[string]any{
			"batch": batchNumber, "batches": totalBatches, "count": len(orders), "submitted": len(submitted), "total": len(intents),
		})
	}
	return submitted, nil
}

func (r *Runtime) rollbackNormalGridOrdersLocked(ctx context.Context, submitted []domain.ExchangeOrder, intents []domain.OrderIntent, cause ...error) error {
	ids := make([]string, 0, len(submitted))
	seen := make(map[string]struct{}, len(submitted))
	for _, order := range submitted {
		if order.ClientOrderID == "" {
			continue
		}
		if _, exists := seen[order.ClientOrderID]; exists {
			continue
		}
		seen[order.ClientOrderID] = struct{}{}
		ids = append(ids, order.ClientOrderID)
	}
	for _, intent := range intents {
		if intent.ClientOrderID == "" {
			continue
		}
		if _, exists := seen[intent.ClientOrderID]; exists {
			continue
		}
		seen[intent.ClientOrderID] = struct{}{}
		ids = append(ids, intent.ClientOrderID)
	}
	if len(ids) == 0 {
		if len(cause) > 0 {
			return cause[0]
		}
		return ctx.Err()
	}
	cleanupCtx, cancel := context.WithTimeout(r.runtimeCtx, normalGridRollbackTimeout)
	cleanupErr := r.cancelOrders(cleanupCtx, ids)
	cancel()
	if cleanupErr != nil {
		r.syncOrdersBestEffortLocked(r.runtimeCtx)
		r.eventLocked(ctx, "normal_grid_rollback_failed", "normal-grid partial orders could not be fully cancelled", map[string]any{
			"count": len(ids), "error": cleanupErr.Error(),
		})
		if len(cause) > 0 {
			return fmt.Errorf("%w; normal-grid rollback failed: %v", cause[0], cleanupErr)
		}
		return fmt.Errorf("normal-grid rollback failed: %w", cleanupErr)
	}
	r.eventLocked(ctx, "normal_grid_rollback", "cancelled partial normal-grid orders after batch failure", map[string]any{"count": len(ids)})
	if len(cause) > 0 {
		return cause[0]
	}
	return ctx.Err()
}

func uniqueOrderIDs(ids []string) []string {
	result := make([]string, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result
}

func (r *Runtime) exchangeOrderMatchesIntent(order domain.ExchangeOrder, intent domain.OrderIntent) bool {
	if intent.Role == domain.OrderRoleTakeProfit {
		// Stop-limit TP metadata is not exposed consistently by the live
		// adapters: price may be the trigger or the maker limit, while
		// PostOnly/ReduceOnly/order type may be normalized by the venue. The
		// client order ID, market, side, role, level, and remaining quantity are
		// the stable fields available for reconciliation.
		return order.ClientOrderID == intent.ClientOrderID &&
			order.MarketIndex == intent.MarketIndex &&
			order.Side == intent.Side &&
			order.Role == intent.Role &&
			order.Level == intent.Level &&
			order.Quantity.Equal(intent.Quantity)
	}
	expectedPrice := intent.Price
	return order.ClientOrderID == intent.ClientOrderID &&
		order.MarketIndex == intent.MarketIndex &&
		order.Side == intent.Side &&
		order.Role == intent.Role &&
		order.Level == intent.Level &&
		order.Price.Equal(expectedPrice) &&
		order.Quantity.Equal(intent.Quantity) &&
		order.ReduceOnly == intent.ReduceOnly &&
		order.PostOnly == intent.PostOnly &&
		order.OrderType == intent.OrderType
}

// cancelSodexNormalGridBatchesLocked cancels the stop-time order set in
// price-distance order. It keeps checking the exchange after each batch so a
// cancel acknowledgement is never treated as proof that the remote order is
// already gone. If the read or cancel fails, it retries until the caller's
// context is cancelled.
func (r *Runtime) cancelSodexNormalGridBatchesLocked(ctx context.Context, initial []domain.ExchangeOrder, marketIndex int) error {
	target := make(map[string]domain.ExchangeOrder, len(initial))
	for _, order := range initial {
		if order.MarketIndex == marketIndex && order.ClientOrderID != "" {
			target[order.ClientOrderID] = order
		}
	}
	if len(target) == 0 {
		return nil
	}
	cancelledWithoutVerification := make(map[string]struct{}, len(target))
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		remote, queryErr := r.exchange.OpenOrders(ctx)
		if queryErr == nil {
			active := make(map[string]domain.ExchangeOrder, len(target))
			for _, order := range remote {
				if order.MarketIndex != marketIndex || order.ClientOrderID == "" {
					continue
				}
				if _, wanted := target[order.ClientOrderID]; wanted {
					active[order.ClientOrderID] = order
				}
			}
			target = active
			cancelledWithoutVerification = make(map[string]struct{}, len(target))
			r.replaceActiveOrdersLocked(remote)
			if len(target) == 0 {
				return nil
			}
		} else if attempt == 0 || attempt%10 == 0 {
			r.eventLocked(ctx, "normal_grid_cancel_retry", "failed to verify stopped normal-grid orders; retrying", map[string]any{
				"market_index": marketIndex, "attempt": attempt + 1, "error": queryErr.Error(),
			})
		}

		candidates := make([]domain.ExchangeOrder, 0, len(target))
		for id, order := range target {
			if _, alreadySent := cancelledWithoutVerification[id]; !alreadySent {
				candidates = append(candidates, order)
			}
		}
		if len(candidates) == 0 {
			if !waitForGridBatchInterval(ctx, r.normalGridBatchInterval) {
				return ctx.Err()
			}
			continue
		}
		sort.SliceStable(candidates, func(i, j int) bool {
			reference := r.orderReferencePriceLocked()
			if reference.GreaterThan(decimal.Zero) {
				iDistance := candidates[i].Price.Sub(reference).Abs()
				jDistance := candidates[j].Price.Sub(reference).Abs()
				if !iDistance.Equal(jDistance) {
					return iDistance.LessThan(jDistance)
				}
			}
			if !candidates[i].Price.Equal(candidates[j].Price) {
				return candidates[i].Price.LessThan(candidates[j].Price)
			}
			return candidates[i].ClientOrderID < candidates[j].ClientOrderID
		})
		if len(candidates) > r.sodexGridBatchSize() {
			candidates = candidates[:r.sodexGridBatchSize()]
		}
		ids := make([]string, 0, len(candidates))
		for _, order := range candidates {
			ids = append(ids, order.ClientOrderID)
		}
		attempt++
		if err := r.cancelOrders(ctx, ids); err != nil {
			r.eventLocked(ctx, "normal_grid_cancel_retry", "normal-grid stop batch cancellation failed; retrying", map[string]any{
				"market_index": marketIndex, "attempt": attempt, "count": len(ids), "error": err.Error(),
			})
			if !waitForGridBatchInterval(ctx, r.normalGridBatchInterval) {
				return ctx.Err()
			}
			continue
		}
		for _, id := range ids {
			cancelledWithoutVerification[id] = struct{}{}
		}
		r.eventLocked(ctx, "normal_grid_cancel_batch_submitted", "normal-grid stop cancellation batch submitted from nearest to farthest", map[string]any{
			"market_index": marketIndex, "count": len(ids), "remaining": len(target) - len(cancelledWithoutVerification),
		})
		if !waitForGridBatchInterval(ctx, r.normalGridBatchInterval) {
			return ctx.Err()
		}
	}
}

func waitForGridBatchInterval(ctx context.Context, interval time.Duration) bool {
	if interval <= 0 {
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Runtime) bindStrategyOrderIDLocked(intent domain.OrderIntent, order domain.ExchangeOrder) {
	if r.strategy == nil {
		return
	}
	r.strategy.BindOrderID(intent, order)
}

// cancelCycleOrdersLocked removes every active order for the completed
// cycle's market before the next cycle is allowed to open. Explicit IDs come
// from the strategy; the live query catches orders missed by the stream.
// Cancellation is retried until the exchange reports the market empty.
func (r *Runtime) cancelCycleOrdersLocked(ctx context.Context, marketIndex int, expected []string) error {
	known := make(map[string]struct{}, len(expected))
	for _, id := range expected {
		if id != "" {
			known[id] = struct{}{}
		}
	}
	attempt := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		attempt++
		ids := make([]string, 0, len(known))
		for id := range known {
			ids = append(ids, id)
		}
		orders, queryErr := r.exchange.OpenOrders(ctx)
		if queryErr == nil {
			for _, order := range orders {
				if order.MarketIndex == marketIndex && order.ClientOrderID != "" {
					known[order.ClientOrderID] = struct{}{}
				}
			}
			ids = ids[:0]
			for id := range known {
				ids = append(ids, id)
			}
		} else if attempt == 1 || attempt%10 == 0 {
			r.eventLocked(ctx, "cycle_order_cancel_retry", "读取周期挂单失败，继续重试撤单", map[string]any{"market_index": marketIndex, "attempt": attempt, "error": queryErr.Error()})
		}
		if len(ids) == 0 && queryErr == nil {
			return nil
		}
		if len(ids) > 0 {
			if cancelErr := r.cancelOrdersLocked(ctx, ids); cancelErr != nil {
				if attempt == 1 || attempt%10 == 0 {
					r.eventLocked(ctx, "cycle_order_cancel_retry", "周期挂单撤销失败，继续重试直到全部撤完", map[string]any{"market_index": marketIndex, "attempt": attempt, "error": cancelErr.Error()})
				}
			} else {
				for _, id := range ids {
					delete(known, id)
					delete(r.activeOrders, id)
				}
			}
		}
		if !waitForPositionWatchRetry(ctx) {
			return ctx.Err()
		}
	}
}

// syncOrdersBestEffortLocked refreshes the UI's active-order view without
// turning a successfully submitted exchange order into a failed strategy.
// Account endpoints can be temporarily slow; they must not hold the runtime
// lock for the full HTTP request timeout.
func (r *Runtime) syncOrdersBestEffortLocked(ctx context.Context) {
	syncCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err := r.syncOrdersLocked(syncCtx)
	cancel()
	if err != nil {
		r.eventLocked(ctx, "order_sync_error", "active-order synchronization failed after order submission", map[string]any{"error": err.Error()})
	}
}

func (r *Runtime) placeOrderLocked(ctx context.Context, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	r.orderMu.Lock()
	defer r.orderMu.Unlock()
	return r.exchange.PlaceOrder(ctx, intent)
}

func (r *Runtime) placeOrdersBatchLocked(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	r.orderMu.Lock()
	defer r.orderMu.Unlock()
	return r.exchange.PlaceOrdersBatch(ctx, intents)
}

func (r *Runtime) modifyOrderLocked(ctx context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	r.orderMu.Lock()
	defer r.orderMu.Unlock()
	return r.exchange.ModifyOrder(ctx, clientOrderID, intent)
}

func (r *Runtime) cancelOrdersLocked(ctx context.Context, ids []string) error {
	return r.cancelOrders(ctx, ids)
}

func (r *Runtime) cancelOrders(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	r.orderMu.Lock()
	defer r.orderMu.Unlock()
	return r.exchange.CancelOrdersBatch(ctx, ids)
}

func (r *Runtime) syncOrdersLocked(ctx context.Context) error {
	orders, err := r.exchange.OpenOrders(ctx)
	if err != nil {
		return err
	}
	r.replaceActiveOrdersLocked(orders)
	return nil
}

func (r *Runtime) replaceActiveOrdersLocked(orders []domain.ExchangeOrder) {
	r.activeOrders = make(map[string]domain.ExchangeOrder, len(orders))
	for _, order := range orders {
		r.activeOrders[order.ClientOrderID] = order
	}
}

func (r *Runtime) eventLocked(ctx context.Context, kind, message string, data map[string]any) {
	event := domain.RuntimeEvent{Kind: kind, Message: message, Data: data}
	if len(r.events) >= 200 {
		r.events = r.events[1:]
	}
	r.events = append(r.events, event)
	if r.store != nil {
		_ = r.store.Append(ctx, event)
	}
}

func (r *Runtime) viewLocked() domain.StrategyView {
	status := domain.StrategyIdle
	strategyType := ""
	marketIndex := 0
	symbol := ""
	var startedAt *time.Time
	durationSeconds := int64(0)
	var cycle *domain.CycleSnapshot
	var grid *domain.GridSnapshot
	projectedAverageEntryPrice := decimal.Zero
	projectedLiquidationPrice := decimal.Zero
	if !r.strategyStartedAt.IsZero() {
		started := r.strategyStartedAt
		startedAt = &started
		end := time.Now()
		if !r.strategyEndedAt.IsZero() {
			end = r.strategyEndedAt
		}
		if end.After(r.strategyStartedAt) {
			durationSeconds = int64(end.Sub(r.strategyStartedAt) / time.Second)
		}
	}
	if r.strategy != nil {
		status = r.strategy.StrategyStatus()
		strategyType = r.strategy.StrategyType()
		marketIndex = r.strategy.MarketIndex()
		symbol = r.strategy.Symbol()
		cycle = r.strategy.CycleSnapshot()
		grid = r.strategy.GridSnapshot()
		projectedAverageEntryPrice, projectedLiquidationPrice = r.strategy.ProjectedFullGrid()
		if martingale, ok := r.strategy.(*strategy.MartingaleGrid); ok && r.accountBalanceKnown {
			if r.accountPositionKnown {
				projectedAverageEntryPrice, projectedLiquidationPrice = martingale.ProjectedFullGridWithAccountState(r.accountAvailableBalance, r.accountPositionQuantity, r.accountPositionAverage)
			} else {
				projectedAverageEntryPrice, projectedLiquidationPrice = martingale.ProjectedFullGridWithAvailableBalance(r.accountAvailableBalance)
			}
		}
	}
	orders := make([]domain.ExchangeOrder, 0, len(r.activeOrders))
	for _, order := range r.activeOrders {
		if r.strategy != nil && r.strategy.StrategyType() == "martingale" && order.MarketIndex != r.strategy.MarketIndex() {
			continue
		}
		if r.strategy != nil && r.strategy.StrategyType() == "martingale" {
			if martingale, ok := r.strategy.(*strategy.MartingaleGrid); ok {
				metrics := martingale.ProjectedFillMetrics(order)
				if r.accountBalanceKnown {
					if r.accountPositionKnown {
						metrics = martingale.ProjectedFillMetricsWithAccountState(order, r.accountAvailableBalance, r.accountPositionQuantity, r.accountPositionAverage)
					} else {
						metrics = martingale.ProjectedFillMetricsWithAvailableBalance(order, r.accountAvailableBalance)
					}
				}
				if metrics.HasPosition {
					averageEntry := metrics.AverageEntryPrice
					takeProfit := metrics.TakeProfitPrice
					liquidation := metrics.LiquidationPrice
					order.ProjectedAverageEntryPrice = &averageEntry
					order.ProjectedTakeProfitPrice = &takeProfit
					order.ProjectedLiquidationPrice = &liquidation
				}
			}
		}
		orders = append(orders, order)
	}
	return domain.StrategyView{
		StrategyType: strategyType, Status: status, DEX: r.currentDEX, MarketIndex: marketIndex, Symbol: symbol,
		StartedAt: startedAt, DurationSeconds: durationSeconds, Cycle: cycle, Grid: grid,
		CumulativeRealizedPnl:      r.netPnlLocked(),
		CumulativeCyclePnl:         r.cumulativeCyclePnlLocked(),
		TradingFees:                r.tradingFees,
		AnnualizedReturn:           r.annualizedReturnLocked(time.Now()),
		CompletedCycleCount:        r.completedCycles,
		ActiveOrders:               orders,
		LastPrice:                  r.lastPrice,
		ProjectedAverageEntryPrice: projectedAverageEntryPrice,
		ProjectedLiquidationPrice:  projectedLiquidationPrice,
		Events:                     append([]domain.RuntimeEvent(nil), r.events...),
	}
}

func (r *Runtime) annualizedReturnLocked(now time.Time) decimal.Decimal {
	if !r.runCapital.GreaterThan(decimal.Zero) || r.strategyStartedAt.IsZero() {
		return decimal.Zero
	}
	end := now
	if !r.strategyEndedAt.IsZero() {
		end = r.strategyEndedAt
	}
	elapsed := end.Sub(r.strategyStartedAt)
	if elapsed <= 0 {
		return decimal.Zero
	}
	elapsedSeconds := decimal.NewFromInt(int64(elapsed)).Div(decimal.NewFromInt(int64(time.Second)))
	if !elapsedSeconds.GreaterThan(decimal.Zero) {
		return decimal.Zero
	}
	yearSeconds := decimal.NewFromInt(int64((365 * 24 * time.Hour) / time.Second))
	return r.netPnlLocked().Div(r.runCapital).Mul(yearSeconds).Div(elapsedSeconds).Mul(decimal.NewFromInt(100))
}

func (r *Runtime) netPnlLocked() decimal.Decimal {
	return r.cumulativeCyclePnlLocked().Sub(r.tradingFees)
}

func (r *Runtime) cumulativeCyclePnlLocked() decimal.Decimal {
	currentPnl := decimal.Zero
	if cycle := r.strategyCycleSnapshot(); cycle != nil && cycle.Status != "completed" {
		currentPnl = cycle.RealizedPnl
	}
	return r.cumulativePnl.Add(currentPnl).Add(r.unrealizedPnl)
}

func (r *Runtime) strategyCycleSnapshot() *domain.CycleSnapshot {
	if r.strategy == nil {
		return nil
	}
	return r.strategy.CycleSnapshot()
}

func (r *Runtime) fillFee(fill domain.Fill) decimal.Decimal {
	if fill.Fee != nil {
		return *fill.Fee
	}
	backendMode := r.currentDEX
	if backendMode == "" {
		backendMode = r.backendMode
	}
	switch backendMode {
	case "lighter":
		if !fill.Price.GreaterThan(decimal.Zero) || !fill.Quantity.GreaterThan(decimal.Zero) {
			return decimal.Zero
		}
		contractMultiplier := r.market.ContractMult
		if !contractMultiplier.GreaterThan(decimal.Zero) {
			contractMultiplier = decimal.NewFromInt(1)
		}
		return fill.Price.Mul(fill.Quantity).Mul(contractMultiplier).Mul(decimal.RequireFromString(lighterFallbackFeeRate))
	case "sodex":
		// SoDEX reports the absolute fee on each accountTrade. Do not estimate
		// it from a global rate: a limit fill may be maker or taker, and the
		// SoDEX maker/taker rates differ from Lighter's fallback rate.
		return decimal.Zero
	default:
		return decimal.Zero
	}
}

func specFromDescriptor(market domain.MarketDescriptor) domain.MarketSpec {
	contract := decimal.NewFromInt(1)
	if market.ContractMult.GreaterThan(decimal.Zero) {
		contract = market.ContractMult
	}
	feeRateKnown := market.FeeRateKnown || strings.EqualFold(market.DEX, "paper")
	return domain.MarketSpec{
		MarketIndex: market.MarketIndex, Symbol: market.Symbol,
		PriceStep: market.PriceStep, SizeStep: market.SizeStep,
		MinBaseAmount: market.MinBaseAmount, MinQuoteAmount: market.MinQuoteAmount,
		ContractMult: contract, MakerFeeRate: market.MakerFeeRate, TakerFeeRate: market.TakerFeeRate, FeeRateKnown: feeRateKnown,
	}
}

func (r *Runtime) setExchangeMarket() {
	if setter, ok := r.exchange.(interface{ SetMarket(domain.MarketSpec) }); ok {
		setter.SetMarket(r.market)
	}
}

func (r *Runtime) BackendMode() string { return r.backendMode }
func (r *Runtime) ActiveDEX() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentDEX
}

func (r *Runtime) AvailableDEXes() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[string]struct{})
	for _, market := range r.marketCatalog {
		if market.DEX != "" {
			seen[market.DEX] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for dex := range seen {
		result = append(result, dex)
	}
	sort.Strings(result)
	return result
}
func (r *Runtime) Connected() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connected
}
func (r *Runtime) TradingEnabled() bool {
	backendMode := r.currentDEX
	if backendMode == "" {
		backendMode = r.backendMode
	}
	switch backendMode {
	case "lighter":
		return r.settings.LighterEnableTrading
	case "sodex":
		return r.settings.SodexEnableTrading
	default:
		return false
	}
}
func (r *Runtime) ConfiguredSymbols() []string { return []string{"BTC", "ETH", "SOL"} }
