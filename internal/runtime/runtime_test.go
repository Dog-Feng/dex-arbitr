package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dex-grid-arbitr/internal/config"
	"dex-grid-arbitr/internal/domain"
	"dex-grid-arbitr/internal/exchange"
	"dex-grid-arbitr/internal/persistence"
	"github.com/shopspring/decimal"
)

type runtimeFillExchange struct {
	*exchange.Paper
	fills        chan domain.Fill
	streamErrors chan error
}

type runtimeImmediateFillExchange struct {
	*runtimeFillExchange
}

type runtimePositionExchange struct {
	*exchange.Paper
	fills          chan domain.Fill
	streamErrors   chan error
	accountMu      sync.Mutex
	account        domain.AccountSnapshot
	cancelFailures int
	cancelAttempts int
}

type retryConnectExchange struct {
	*exchange.Paper
	attempts int
}

type runtimeMakerExchange struct {
	*exchange.Paper
}

type runtimeTakeProfitWatchExchange struct {
	*runtimeFillExchange
	mu          sync.Mutex
	cancelCalls int
	modifyCalls int
}

type runtimeSodexGridExchange struct {
	*exchange.Paper
	mu          sync.Mutex
	batchCalls  int
	batchSizes  []int
	batchPrices [][]decimal.Decimal
	failBatch   int
	cancelCalls int
	cancelIDs   [][]string
}

func (e *runtimeSodexGridExchange) Markets(context.Context) ([]domain.MarketDescriptor, error) {
	return []domain.MarketDescriptor{{
		DEX: "sodex", MarketIndex: 0, Symbol: "SOL-USD", ExchangeSymbol: "SOL-USD",
		BaseAsset: "SOL", QuoteAsset: "USD", MarketType: "perp",
		PriceStep: decimal.RequireFromString("0.001"), SizeStep: decimal.RequireFromString("0.001"),
	}}, nil
}

func (e *runtimeSodexGridExchange) PlaceOrdersBatch(ctx context.Context, intents []domain.OrderIntent) ([]domain.ExchangeOrder, error) {
	e.mu.Lock()
	e.batchCalls++
	call := e.batchCalls
	e.batchSizes = append(e.batchSizes, len(intents))
	prices := make([]decimal.Decimal, 0, len(intents))
	for _, intent := range intents {
		prices = append(prices, intent.Price)
	}
	e.batchPrices = append(e.batchPrices, prices)
	fail := e.failBatch == call
	e.mu.Unlock()
	if fail {
		return nil, errors.New("simulated normal-grid batch failure")
	}
	return e.Paper.PlaceOrdersBatch(ctx, intents)
}

func (e *runtimeSodexGridExchange) CancelOrdersBatch(ctx context.Context, ids []string) error {
	e.mu.Lock()
	e.cancelCalls++
	e.cancelIDs = append(e.cancelIDs, append([]string(nil), ids...))
	e.mu.Unlock()
	return e.Paper.CancelOrdersBatch(ctx, ids)
}

func (e *runtimeSodexGridExchange) BatchStats() (int, []int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.batchCalls, append([]int(nil), e.batchSizes...), e.cancelCalls
}

func (e *runtimeSodexGridExchange) BatchDetails() ([][]decimal.Decimal, [][]string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	prices := make([][]decimal.Decimal, len(e.batchPrices))
	for index, batch := range e.batchPrices {
		prices[index] = append([]decimal.Decimal(nil), batch...)
	}
	ids := make([][]string, len(e.cancelIDs))
	for index, batch := range e.cancelIDs {
		ids[index] = append([]string(nil), batch...)
	}
	return prices, ids
}

func (e *runtimeMakerExchange) Markets(context.Context) ([]domain.MarketDescriptor, error) {
	return []domain.MarketDescriptor{{
		DEX: "lighter", MarketIndex: 0, Symbol: "BTC-USD", ExchangeSymbol: "BTC-USD",
		BaseAsset: "BTC", QuoteAsset: "USD", MarketType: "perp",
		PriceStep: decimal.RequireFromString("0.01"), SizeStep: decimal.RequireFromString("0.01"),
	}}, nil
}

func (e *retryConnectExchange) Connect(context.Context) error {
	e.attempts++
	if e.attempts == 1 {
		return errors.New("simulated exchange outage")
	}
	return nil
}

func (e *runtimePositionExchange) SubscribeFills(context.Context) (<-chan domain.Fill, <-chan error) {
	return e.fills, e.streamErrors
}

func (e *runtimePositionExchange) AccountSnapshot(context.Context) (domain.AccountSnapshot, error) {
	e.accountMu.Lock()
	defer e.accountMu.Unlock()
	if e.account.Positions == nil {
		e.account.Positions = []domain.PositionSnapshot{}
	}
	return e.account, nil
}

func (e *runtimePositionExchange) SetPositions(positions []domain.PositionSnapshot) {
	e.accountMu.Lock()
	defer e.accountMu.Unlock()
	e.account.Positions = positions
}

func (e *runtimePositionExchange) CancelOrdersBatch(ctx context.Context, ids []string) error {
	e.accountMu.Lock()
	e.cancelAttempts++
	if e.cancelFailures > 0 {
		e.cancelFailures--
		e.accountMu.Unlock()
		return errors.New("simulated cancellation failure")
	}
	e.accountMu.Unlock()
	return e.Paper.CancelOrdersBatch(ctx, ids)
}

func (e *runtimePositionExchange) CancelAttempts() int {
	e.accountMu.Lock()
	defer e.accountMu.Unlock()
	return e.cancelAttempts
}

func (e *runtimeFillExchange) SubscribeFills(context.Context) (<-chan domain.Fill, <-chan error) {
	return e.fills, e.streamErrors
}

func (e *runtimeTakeProfitWatchExchange) CancelOrdersBatch(ctx context.Context, ids []string) error {
	e.mu.Lock()
	e.cancelCalls++
	e.mu.Unlock()
	return e.Paper.CancelOrdersBatch(ctx, ids)
}

func (e *runtimeTakeProfitWatchExchange) ModifyOrder(ctx context.Context, clientOrderID string, intent domain.OrderIntent) (domain.ExchangeOrder, error) {
	e.mu.Lock()
	e.modifyCalls++
	e.mu.Unlock()
	return e.Paper.ModifyOrder(ctx, clientOrderID, intent)
}

func (e *runtimeTakeProfitWatchExchange) repairStats() (int, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.cancelCalls, e.modifyCalls
}

func (e *runtimeImmediateFillExchange) ReconcileFills(ctx context.Context) ([]domain.Fill, error) {
	orders, err := e.Paper.OpenOrders(ctx)
	if err != nil {
		return nil, err
	}
	for _, order := range orders {
		if order.OrderType != domain.OrderTypeMarket {
			continue
		}
		if err := e.Paper.CancelOrder(ctx, order.ClientOrderID); err != nil {
			return nil, err
		}
		return []domain.Fill{{
			ExchangeOrderID: order.ExchangeOrderID, ClientOrderID: order.ClientOrderID,
			MarketIndex: order.MarketIndex, Side: order.Side, Role: order.Role,
			Level: order.Level, Price: order.Price, Quantity: order.Quantity,
			TradeID: "immediate-" + order.ClientOrderID,
		}}, nil
	}
	return nil, nil
}

func runtimeTestConfig() domain.MartingaleConfig {
	return domain.MartingaleConfig{
		MarketIndex: 0, Symbol: "BTC-USD", Direction: domain.DirectionLong,
		Leverage: decimal.NewFromInt(5), InitialMargin: decimal.NewFromInt(1), SafetyMargin: decimal.NewFromInt(1),
		PriceStepPct: decimal.RequireFromString("0.01"), PriceStepMultiplier: decimal.NewFromInt(1), AmountMultiplier: decimal.NewFromInt(1),
		MaxSafetyOrders: 2, TakeProfitPct: decimal.RequireFromString("0.01"), MarketMaxSlippage: decimal.RequireFromString("0.005"),
		MarketMaxRetries: 2, Continuous: true,
	}
}

func runtimeNormalGridSodexConfig() domain.NormalGridConfig {
	return domain.NormalGridConfig{
		StrategyType: "normal", DEX: "sodex", MarketIndex: 0, Symbol: "SOL-USD", Direction: domain.DirectionNeutral,
		Leverage: decimal.NewFromInt(5), GridLowerPrice: decimal.NewFromInt(90),
		GridUpperPrice: decimal.NewFromInt(110), GridCount: 20, GridMargin: decimal.NewFromInt(1000),
		GridMode: "arithmetic",
	}
}

func TestPaperRuntimeLifecycle(t *testing.T) {
	ctx := context.Background()
	store, err := persistence.OpenEventStore(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), store)
	t.Cleanup(func() { _ = rt.Close() })

	if err := rt.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(ctx, runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.StrategyRunning || len(view.ActiveOrders) != 1 {
		t.Fatalf("unexpected start view: %+v", view)
	}

	view, err = rt.Tick(ctx, decimal.NewFromInt(100))
	if err != nil {
		t.Fatal(err)
	}
	if view.Cycle == nil || view.Cycle.PositionQuantity.IsZero() || len(view.ActiveOrders) != 3 {
		t.Fatalf("initial fill did not create grid: %+v", view)
	}

	view, err = rt.Tick(ctx, decimal.NewFromInt(101))
	if err != nil {
		t.Fatal(err)
	}
	if view.CompletedCycleCount != 1 || view.Status != domain.StrategyStarting || len(view.ActiveOrders) != 1 {
		t.Fatalf("take-profit rollover failed: %+v", view)
	}
	if !view.CumulativeCyclePnl.GreaterThan(decimal.Zero) {
		t.Fatalf("completed cycle pnl was not accumulated: %+v", view)
	}

	view, err = rt.Tick(ctx, decimal.NewFromInt(101))
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.StrategyRunning || len(view.ActiveOrders) != 3 || !view.CumulativeRealizedPnl.GreaterThan(decimal.Zero) {
		t.Fatalf("continuous cycle did not restart: %+v", view)
	}

	view, err = rt.Stop(ctx, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.StrategyStopped || len(view.ActiveOrders) != 0 {
		t.Fatalf("stop did not cancel orders: %+v", view)
	}
	history, err := rt.History(ctx, 100)
	if err != nil || len(history) < 4 {
		t.Fatalf("event history unavailable: len=%d err=%v", len(history), err)
	}
}

func TestMakerEntryRequotesAtLatestTopOfBook(t *testing.T) {
	ctx := context.Background()
	ex := &runtimeMakerExchange{Paper: exchange.NewPaper()}
	ex.SetOrderBook(0, domain.OrderBook{
		Bids: []domain.OrderBookLevel{{Price: decimal.NewFromInt(100)}},
		Asks: []domain.OrderBookLevel{{Price: decimal.NewFromInt(101)}},
	})
	rt := New(config.Settings{Mode: "lighter_testnet", DefaultDEX: "lighter", LighterMarketIndex: 0}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })

	cfg := runtimeTestConfig()
	cfg.DEX = "lighter"
	cfg.EntryMode = domain.EntryModeMaker
	cfg.MakerRequoteSeconds = 10
	view, err := rt.Start(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != domain.StrategyStarting || len(view.ActiveOrders) != 1 {
		t.Fatalf("Maker entry did not start with one pending order: %+v", view)
	}
	if !view.ActiveOrders[0].Price.Equal(decimal.NewFromInt(100)) || !view.ActiveOrders[0].PostOnly || view.ActiveOrders[0].OrderType != domain.OrderTypeLimit {
		t.Fatalf("unexpected initial Maker order: %+v", view.ActiveOrders[0])
	}

	ex.SetOrderBook(0, domain.OrderBook{
		Bids: []domain.OrderBookLevel{{Price: decimal.NewFromInt(99)}},
		Asks: []domain.OrderBookLevel{{Price: decimal.NewFromInt(100)}},
	})
	rt.mu.Lock()
	rt.entryOrderPlacedAt = time.Now().Add(-11 * time.Second)
	rt.mu.Unlock()
	rt.checkEntryWatch()

	orders, err := ex.OpenOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 1 || !orders[0].Price.Equal(decimal.NewFromInt(99)) || !orders[0].PostOnly || orders[0].OrderType != domain.OrderTypeLimit {
		t.Fatalf("Maker entry was not re-quoted to the best bid: %+v", orders)
	}
}

func TestRefreshMarketsRetriesAnInitialConnectionFailure(t *testing.T) {
	ex := &retryConnectExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "paper"}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })

	if err := rt.Connect(context.Background()); err == nil {
		t.Fatal("expected the first connection attempt to fail")
	}
	if rt.Connected() {
		t.Fatal("runtime was marked connected after a failed attempt")
	}
	if err := rt.RefreshMarkets(context.Background()); err != nil {
		t.Fatalf("refresh did not retry the connection: %v", err)
	}
	if !rt.Connected() || ex.attempts != 2 {
		t.Fatalf("unexpected retry state: connected=%t attempts=%d", rt.Connected(), ex.attempts)
	}
}

func TestPaperRuntimeNormalGridLifecycle(t *testing.T) {
	ctx := context.Background()
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	cfg := domain.NormalGridConfig{
		StrategyType: "normal", MarketIndex: 0, Symbol: "BTC-USD", Direction: domain.DirectionNeutral,
		Leverage: decimal.NewFromInt(5), GridLowerPrice: decimal.NewFromInt(90),
		GridUpperPrice: decimal.NewFromInt(110), GridCount: 4, GridMargin: decimal.NewFromInt(100),
		GridMode: "arithmetic",
	}
	view, err := rt.Start(ctx, cfg, &price)
	if err != nil {
		t.Fatal(err)
	}
	if view.StrategyType != "normal" || view.Grid == nil || len(view.ActiveOrders) != 4 {
		t.Fatalf("unexpected normal grid start view: %+v", view)
	}

	view, err = rt.Tick(ctx, decimal.NewFromInt(95))
	if err != nil {
		t.Fatal(err)
	}
	if view.Cycle == nil || view.Cycle.PositionQuantity.IsZero() || len(view.ActiveOrders) != 4 {
		t.Fatalf("normal grid buy fill did not maintain four active levels: %+v", view)
	}

	view, err = rt.Tick(ctx, decimal.NewFromInt(100))
	if err != nil {
		t.Fatal(err)
	}
	if view.CompletedCycleCount != 0 || view.Cycle.PositionQuantity.GreaterThan(decimal.Zero) || len(view.ActiveOrders) != 4 {
		t.Fatalf("normal grid did not close and replace the adjacent level: %+v", view)
	}
}

func TestSodexNormalGridSubmitsLargeInitialGridInRateLimitedBatches(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	view, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	calls, sizes, cancels := ex.BatchStats()
	if calls != 3 || len(sizes) != 3 || sizes[0] != 10 || sizes[1] != 10 || sizes[2] != 1 || cancels != 0 {
		t.Fatalf("unexpected SoDEX normal-grid batching: calls=%d sizes=%v cancels=%d", calls, sizes, cancels)
	}
	if len(view.ActiveOrders) != 21 {
		t.Fatalf("expected all 21 grid levels to be active after staged submission, got %d", len(view.ActiveOrders))
	}
}

func TestSodexNormalGridRollsBackSubmittedBatchesOnFailure(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper(), failBatch: 2}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	if _, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price); err == nil {
		t.Fatal("expected normal-grid batch failure")
	}
	calls, sizes, cancels := ex.BatchStats()
	if calls != 2 || len(sizes) != 2 || sizes[0] != 10 || sizes[1] != 10 || cancels != 1 {
		t.Fatalf("unexpected normal-grid rollback activity: calls=%d sizes=%v cancels=%d", calls, sizes, cancels)
	}
	orders, err := ex.OpenOrders(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(orders) != 0 {
		t.Fatalf("partial normal-grid orders were left on the exchange: %+v", orders)
	}
	if view := rt.View(); view.Status != domain.StrategyStopped || len(view.ActiveOrders) != 0 {
		t.Fatalf("runtime retained failed normal-grid state: %+v", view)
	}
}

func TestSodexNormalGridBatchesNearestLevelsFirst(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	if _, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price); err != nil {
		t.Fatal(err)
	}
	batches, _ := ex.BatchDetails()
	flattened := make([]decimal.Decimal, 0)
	for _, batch := range batches {
		flattened = append(flattened, batch...)
	}
	want := []decimal.Decimal{
		decimal.NewFromInt(100), decimal.NewFromInt(101), decimal.NewFromInt(99),
		decimal.NewFromInt(102), decimal.NewFromInt(98),
	}
	if len(flattened) < len(want) {
		t.Fatalf("not enough placed prices: %v", flattened)
	}
	for index, expected := range want {
		if !flattened[index].Equal(expected) {
			t.Fatalf("normal-grid placement was not nearest-first at index %d: got=%s want=%s all=%v", index, flattened[index], expected, flattened)
		}
	}
}

func TestSodexNormalGridWatchdogRepairsMissingOrderBeforeAnyFill(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	view, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	if view.Cycle == nil || !view.Cycle.PositionQuantity.IsZero() {
		t.Fatalf("test must start with no position: %+v", view.Cycle)
	}
	victim := view.ActiveOrders[0]
	if err := ex.Paper.CancelOrder(context.Background(), victim.ClientOrderID); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for {
		current := rt.View()
		repaired := false
		for _, event := range current.Events {
			if event.Kind == "order_watch_repaired" {
				repaired = true
				break
			}
		}
		if repaired && len(current.ActiveOrders) == len(view.ActiveOrders) {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("normal-grid watchdog did not repair startup missing order: %+v", current)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSodexNormalGridWatchdogRepairsMismatchedOrder(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	view, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	victim := view.ActiveOrders[0]
	wrongIntent := domain.OrderIntent{
		ClientOrderID: victim.ClientOrderID, MarketIndex: victim.MarketIndex, Symbol: victim.ClientOrderID,
		Side: victim.Side, Role: victim.Role, Level: victim.Level, Price: victim.Price.Add(decimal.NewFromInt(1)),
		Quantity: victim.Quantity, ReduceOnly: victim.ReduceOnly, PostOnly: victim.PostOnly, OrderType: victim.OrderType,
	}
	if _, err := ex.Paper.ModifyOrder(context.Background(), victim.ClientOrderID, wrongIntent); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for {
		current := rt.View()
		for _, event := range current.Events {
			if event.Kind != "order_watch_repaired" {
				continue
			}
			orders, openErr := ex.OpenOrders(context.Background())
			if openErr != nil {
				t.Fatal(openErr)
			}
			for _, order := range orders {
				if order.ClientOrderID == victim.ClientOrderID && order.Price.Equal(victim.Price) {
					return
				}
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("normal-grid watchdog did not repair mismatched order: %+v", current)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestOrderWatchdogMatchesMakerTakeProfitPrice(t *testing.T) {
	step := decimal.RequireFromString("0.1")
	rt := &Runtime{market: domain.MarketSpec{PriceStep: step}}
	cases := []struct {
		name       string
		side       domain.OrderSide
		trigger    string
		makerPrice string
	}{
		{name: "long sell", side: domain.OrderSideSell, trigger: "100", makerPrice: "100.1"},
		{name: "short buy", side: domain.OrderSideBuy, trigger: "100", makerPrice: "99.9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trigger := decimal.RequireFromString(tc.trigger)
			intent := domain.OrderIntent{
				ClientOrderID: "take-profit-1", MarketIndex: 0, Side: tc.side,
				Role: domain.OrderRoleTakeProfit, Level: -1, Price: trigger,
				Quantity: decimal.NewFromInt(1), ReduceOnly: true,
				TriggerPrice: &trigger, OrderType: domain.OrderTypeLimit,
			}
			order := domain.ExchangeOrder{
				ClientOrderID: intent.ClientOrderID, MarketIndex: intent.MarketIndex,
				Side: intent.Side, Role: intent.Role, Level: intent.Level,
				Price: decimal.RequireFromString(tc.makerPrice), Quantity: intent.Quantity,
				ReduceOnly: intent.ReduceOnly, OrderType: intent.OrderType,
			}
			if !rt.exchangeOrderMatchesIntent(order, intent) {
				t.Fatalf("maker-priced take-profit was treated as mismatched: order=%+v intent=%+v", order, intent)
			}
		})
	}
}

func TestOrderWatchdogModifiesMismatchedTakeProfitWithoutCancel(t *testing.T) {
	ex := &runtimeTakeProfitWatchExchange{runtimeFillExchange: &runtimeFillExchange{
		Paper: exchange.NewPaper(), fills: make(chan domain.Fill, 4), streamErrors: make(chan error),
	}}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	initial := view.ActiveOrders[0]
	if err := ex.Paper.CancelOrder(context.Background(), initial.ClientOrderID); err != nil {
		t.Fatal(err)
	}
	ex.fills <- domain.Fill{
		ExchangeOrderID: initial.ExchangeOrderID, ClientOrderID: initial.ClientOrderID,
		MarketIndex: initial.MarketIndex, Side: initial.Side, Role: initial.Role,
		Price: initial.Price, Quantity: initial.Quantity,
	}

	var takeProfit domain.ExchangeOrder
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	for takeProfit.ClientOrderID == "" {
		current := rt.View()
		for _, order := range current.ActiveOrders {
			if order.Role == domain.OrderRoleTakeProfit {
				takeProfit = order
				break
			}
		}
		if takeProfit.ClientOrderID != "" {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("take-profit order was not placed: %+v", current)
		case <-time.After(10 * time.Millisecond):
		}
	}

	wrongQuantity := takeProfit.Quantity.Add(decimal.NewFromInt(1))
	wrongIntent := domain.OrderIntent{
		ClientOrderID: takeProfit.ClientOrderID, MarketIndex: takeProfit.MarketIndex,
		Side: takeProfit.Side, Role: takeProfit.Role, Level: takeProfit.Level,
		Price: takeProfit.Price, Quantity: wrongQuantity, ReduceOnly: takeProfit.ReduceOnly,
		PostOnly: takeProfit.PostOnly, OrderType: takeProfit.OrderType,
	}
	if _, err := ex.Paper.ModifyOrder(context.Background(), takeProfit.ClientOrderID, wrongIntent); err != nil {
		t.Fatal(err)
	}

	for {
		cancelCalls, modifyCalls := ex.repairStats()
		orders, openErr := ex.OpenOrders(context.Background())
		if openErr != nil {
			t.Fatal(openErr)
		}
		for _, order := range orders {
			if order.ClientOrderID == takeProfit.ClientOrderID && order.Quantity.Equal(takeProfit.Quantity) {
				if cancelCalls != 0 {
					t.Fatalf("take-profit mismatch triggered cancellation: calls=%d modifies=%d", cancelCalls, modifyCalls)
				}
				if modifyCalls == 0 {
					t.Fatalf("take-profit mismatch was not repaired in place")
				}
				return
			}
		}
		select {
		case <-deadline.C:
			cancelCalls, modifyCalls := ex.repairStats()
			t.Fatalf("take-profit was not modified in place: cancels=%d modifies=%d", cancelCalls, modifyCalls)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestSodexNormalGridStopCancelsNearestLevelsInBatches(t *testing.T) {
	ex := &runtimeSodexGridExchange{Paper: exchange.NewPaper()}
	rt := New(config.Settings{Mode: "sodex_testnet", DefaultDEX: "sodex", SodexGridBatchSize: 10}, ex, nil)
	rt.normalGridBatchInterval = 0
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.RequireFromString("100.5")
	view, err := rt.Start(context.Background(), runtimeNormalGridSodexConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	priceByID := make(map[string]decimal.Decimal, len(view.ActiveOrders))
	for _, order := range view.ActiveOrders {
		priceByID[order.ClientOrderID] = order.Price
	}
	stopped, err := rt.Stop(context.Background(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if stopped.Status != domain.StrategyStopped || len(stopped.ActiveOrders) != 0 {
		t.Fatalf("normal-grid stop left active orders: %+v", stopped)
	}
	_, _, cancelCalls := ex.BatchStats()
	if cancelCalls != 3 {
		t.Fatalf("expected three stop cancellation batches, got %d", cancelCalls)
	}
	_, cancelBatches := ex.BatchDetails()
	flattened := make([]decimal.Decimal, 0)
	for _, batch := range cancelBatches {
		for _, id := range batch {
			flattened = append(flattened, priceByID[id])
		}
	}
	want := []decimal.Decimal{
		decimal.NewFromInt(100), decimal.NewFromInt(101), decimal.NewFromInt(99),
		decimal.NewFromInt(102), decimal.NewFromInt(98),
	}
	for index, expected := range want {
		if !flattened[index].Equal(expected) {
			t.Fatalf("normal-grid stop was not nearest-first at index %d: got=%s want=%s all=%v", index, flattened[index], expected, flattened)
		}
	}
}

func TestContinuousRolloverHandlesImmediateMarketFill(t *testing.T) {
	ex := &runtimeImmediateFillExchange{runtimeFillExchange: &runtimeFillExchange{
		Paper: exchange.NewPaper(), fills: make(chan domain.Fill, 1), streamErrors: make(chan error),
	}}
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	if _, err := rt.Start(context.Background(), runtimeTestConfig(), &price); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Tick(context.Background(), price); err != nil {
		t.Fatal(err)
	}
	view, err := rt.Tick(context.Background(), decimal.NewFromInt(101))
	if err != nil {
		t.Fatal(err)
	}
	if view.CompletedCycleCount != 1 || view.Status != domain.StrategyRunning || len(view.ActiveOrders) != 3 {
		t.Fatalf("immediate rollover did not create the next grid: %+v", view)
	}
}

func TestPaperRuntimeKeepsOneTakeProfitAfterSafetyFill(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	if _, err := rt.Start(context.Background(), runtimeTestConfig(), &price); err != nil {
		t.Fatal(err)
	}
	view, err := rt.Tick(context.Background(), price)
	if err != nil {
		t.Fatal(err)
	}
	var tpID string
	var initialTPPrice decimal.Decimal
	for _, order := range view.ActiveOrders {
		if order.Role == domain.OrderRoleTakeProfit {
			tpID = order.ClientOrderID
			initialTPPrice = order.Price
		}
	}
	if tpID == "" {
		t.Fatalf("initial take-profit order missing: %+v", view.ActiveOrders)
	}
	view, err = rt.Tick(context.Background(), decimal.NewFromInt(99))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, order := range view.ActiveOrders {
		if order.Role == domain.OrderRoleTakeProfit {
			count++
			if order.ClientOrderID != tpID {
				t.Fatalf("take-profit replacement created a second client order: %+v", view.ActiveOrders)
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one active take-profit, got %d: %+v", count, view.ActiveOrders)
	}
	for _, order := range view.ActiveOrders {
		if order.Role == domain.OrderRoleTakeProfit && !order.Price.LessThan(initialTPPrice) {
			t.Fatalf("take-profit price was not updated after safety fill: initial=%s current=%s", initialTPPrice, order.Price)
		}
	}
}

func TestMartingaleViewFiltersOrdersAndAddsProjectedMetrics(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	if _, err := rt.Start(context.Background(), runtimeTestConfig(), &price); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Tick(context.Background(), price); err != nil {
		t.Fatal(err)
	}

	rt.mu.Lock()
	rt.activeOrders["other-market-order"] = domain.ExchangeOrder{
		ClientOrderID: "other-market-order", MarketIndex: 99, Role: domain.OrderRoleSafety,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(90), Quantity: decimal.NewFromInt(1), Status: "open",
	}
	rt.mu.Unlock()

	view := rt.View()
	if len(view.ActiveOrders) != 3 {
		t.Fatalf("martingale view exposed orders from another market: %+v", view.ActiveOrders)
	}
	for _, order := range view.ActiveOrders {
		if order.MarketIndex != runtimeTestConfig().MarketIndex {
			t.Fatalf("unexpected market in martingale view: %+v", order)
		}
		if order.Role == domain.OrderRoleSafety && (order.ProjectedAverageEntryPrice == nil || order.ProjectedTakeProfitPrice == nil || order.ProjectedLiquidationPrice == nil) {
			t.Fatalf("safety order is missing projected metrics: %+v", order)
		}
	}
}

func TestNormalGridViewRemainsUnfilteredAndWithoutProjectedMetrics(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	cfg := domain.NormalGridConfig{
		StrategyType: "normal", MarketIndex: 0, Symbol: "BTC-USD", Direction: domain.DirectionNeutral,
		Leverage: decimal.NewFromInt(5), GridLowerPrice: decimal.NewFromInt(90),
		GridUpperPrice: decimal.NewFromInt(110), GridCount: 4, GridMargin: decimal.NewFromInt(100),
		GridMode: "arithmetic",
	}
	if _, err := rt.Start(context.Background(), cfg, &price); err != nil {
		t.Fatal(err)
	}
	rt.mu.Lock()
	rt.activeOrders["other-market-order"] = domain.ExchangeOrder{
		ClientOrderID: "other-market-order", MarketIndex: 99, Role: domain.OrderRoleGrid,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(90), Quantity: decimal.NewFromInt(1), Status: "open",
	}
	rt.mu.Unlock()

	view := rt.View()
	if view.StrategyType != "normal" || len(view.ActiveOrders) != 5 {
		t.Fatalf("normal grid view was changed by martingale-only behavior: %+v", view)
	}
	for _, order := range view.ActiveOrders {
		if order.ProjectedAverageEntryPrice != nil || order.ProjectedTakeProfitPrice != nil || order.ProjectedLiquidationPrice != nil {
			t.Fatalf("normal grid order unexpectedly has martingale metrics: %+v", order)
		}
	}
}

func TestRuntimeMetricsArePerStrategyRunAndIncludeUnrealizedPnl(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	if !view.CumulativeRealizedPnl.IsZero() || view.CompletedCycleCount != 0 {
		t.Fatalf("new strategy run did not start metrics at zero: %+v", view)
	}

	view = rt.ViewWithAccount(domain.AccountSnapshot{Positions: []domain.PositionSnapshot{{
		MarketIndex: 0, Quantity: decimal.NewFromInt(1), UnrealizedPnl: decimal.RequireFromString("0.25"),
	}}})
	if !view.CumulativeRealizedPnl.Equal(decimal.RequireFromString("0.25")) {
		t.Fatalf("unrealized pnl was not included: %+v", view)
	}
	if _, err := rt.Stop(context.Background(), false, nil); err != nil {
		t.Fatal(err)
	}

	view, err = rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	if !view.CumulativeRealizedPnl.IsZero() || view.CompletedCycleCount != 0 {
		t.Fatalf("metrics were not reset for the next strategy run: %+v", view)
	}
}

func TestLighterTradingFeeIsDeductedFromRuntimePnl(t *testing.T) {
	ex := &runtimeFillExchange{
		Paper:        exchange.NewPaper(),
		fills:        make(chan domain.Fill, 1),
		streamErrors: make(chan error),
	}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	initial := view.ActiveOrders[0]
	ex.fills <- domain.Fill{
		ExchangeOrderID: initial.ExchangeOrderID, ClientOrderID: initial.ClientOrderID,
		MarketIndex: initial.MarketIndex, Side: initial.Side, Role: initial.Role,
		Level: initial.Level, Price: initial.Price, Quantity: initial.Quantity,
	}

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		current := rt.View()
		if current.Cycle != nil && current.Cycle.PositionQuantity.GreaterThan(decimal.Zero) {
			expectedFee := initial.Price.Mul(initial.Quantity).Mul(decimal.RequireFromString(lighterFallbackFeeRate))
			if !current.TradingFees.Equal(expectedFee) {
				t.Fatalf("unexpected trading fee: got=%s want=%s", current.TradingFees, expectedFee)
			}
			if !current.CumulativeRealizedPnl.Equal(expectedFee.Neg()) {
				t.Fatalf("fee was not deducted from pnl: got=%s want=%s", current.CumulativeRealizedPnl, expectedFee.Neg())
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("initial fill was not processed: %+v", current)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestTakeProfitFeeIsIncludedAfterCycleCompletion(t *testing.T) {
	ex := &runtimeFillExchange{
		Paper:        exchange.NewPaper(),
		fills:        make(chan domain.Fill, 4),
		streamErrors: make(chan error),
	}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	initial := view.ActiveOrders[0]
	ex.fills <- domain.Fill{
		ExchangeOrderID: initial.ExchangeOrderID, ClientOrderID: initial.ClientOrderID,
		MarketIndex: initial.MarketIndex, Side: initial.Side, Role: initial.Role,
		Price: price, Quantity: initial.Quantity,
	}

	var takeProfit domain.ExchangeOrder
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for takeProfit.ClientOrderID == "" {
		current := rt.View()
		for _, order := range current.ActiveOrders {
			if order.Role == domain.OrderRoleTakeProfit {
				takeProfit = order
				break
			}
		}
		if takeProfit.ClientOrderID != "" {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("take-profit order was not placed: %+v", current)
		case <-time.After(5 * time.Millisecond):
		}
	}

	closePrice := decimal.NewFromInt(101)
	ex.fills <- domain.Fill{
		ExchangeOrderID: takeProfit.ExchangeOrderID, ClientOrderID: takeProfit.ClientOrderID,
		MarketIndex: takeProfit.MarketIndex, Side: takeProfit.Side, Role: takeProfit.Role,
		Price: closePrice, Quantity: takeProfit.Quantity,
	}
	deadline.Reset(2 * time.Second)
	for {
		current := rt.View()
		if current.CompletedCycleCount == 1 {
			entryFee := price.Mul(initial.Quantity).Mul(decimal.RequireFromString(lighterFallbackFeeRate))
			closeFee := closePrice.Mul(takeProfit.Quantity).Mul(decimal.RequireFromString(lighterFallbackFeeRate))
			grossPnl := closePrice.Sub(price).Mul(initial.Quantity)
			if !current.TradingFees.Equal(entryFee.Add(closeFee)) {
				t.Fatalf("take-profit fee was not included: got=%s want=%s", current.TradingFees, entryFee.Add(closeFee))
			}
			if !current.CumulativeRealizedPnl.Equal(grossPnl.Sub(entryFee).Sub(closeFee)) {
				t.Fatalf("take-profit net pnl was not fee-adjusted: got=%s want=%s", current.CumulativeRealizedPnl, grossPnl.Sub(entryFee).Sub(closeFee))
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("take-profit fill was not processed: %+v", current)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestSoDexFeePolicyUsesReportedFeeWithoutLighterFallback(t *testing.T) {
	rt := New(config.Settings{Mode: "sodex_mainnet"}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	reported := decimal.RequireFromString("0.123456")
	if got := rt.fillFee(domain.Fill{Fee: &reported}); !got.Equal(reported) {
		t.Fatalf("SoDEX reported fee was not used: got=%s want=%s", got, reported)
	}
	if got := rt.fillFee(domain.Fill{Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1)}); !got.IsZero() {
		t.Fatalf("SoDEX missing fee incorrectly used a fallback rate: got=%s", got)
	}

	lighter := New(config.Settings{Mode: "lighter_mainnet"}, exchange.NewPaper(), nil)
	if got := lighter.fillFee(domain.Fill{Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1)}); !got.Equal(decimal.RequireFromString("0.005")) {
		t.Fatalf("Lighter fallback fee changed unexpectedly: got=%s want=0.005", got)
	}
}

func TestViewWithAccountMatchesPositionBySymbolFallback(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	if _, err := rt.Start(context.Background(), runtimeTestConfig(), &price); err != nil {
		t.Fatal(err)
	}
	withoutAvailableBalance := rt.View()
	view := rt.ViewWithAccount(domain.AccountSnapshot{AvailableBalance: decimal.NewFromInt(1), Positions: []domain.PositionSnapshot{{
		MarketIndex: 99, Symbol: "BTC", Quantity: decimal.NewFromInt(1), UnrealizedPnl: decimal.RequireFromString("0.25"),
	}}})
	if !view.CumulativeRealizedPnl.Equal(decimal.RequireFromString("0.25")) {
		t.Fatalf("position symbol fallback was not used: %+v", view)
	}
	if !view.ProjectedLiquidationPrice.GreaterThan(withoutAvailableBalance.ProjectedLiquidationPrice) {
		t.Fatalf("account available balance was not applied to the strategy projection: without=%s with=%s", withoutAvailableBalance.ProjectedLiquidationPrice, view.ProjectedLiquidationPrice)
	}
}

func TestViewWithAccountUsesLivePositionForProjectedOrderMetrics(t *testing.T) {
	rt := New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	if _, err := rt.Start(context.Background(), runtimeTestConfig(), &price); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Tick(context.Background(), price); err != nil {
		t.Fatal(err)
	}

	view := rt.ViewWithAccount(domain.AccountSnapshot{
		AvailableBalance: decimal.NewFromInt(1),
		Positions: []domain.PositionSnapshot{{
			MarketIndex: 0, Symbol: "BTC-USD", Quantity: decimal.RequireFromString("0.10"),
			AverageEntryPrice: decimal.RequireFromString("99.50"),
		}},
	})
	if !view.ProjectedAverageEntryPrice.Equal(decimal.RequireFromString("98.99")) || !view.ProjectedLiquidationPrice.Equal(decimal.RequireFromString("94.03")) {
		t.Fatalf("full-grid projection did not use live position plus pending safety orders: average=%s liquidation=%s", view.ProjectedAverageEntryPrice, view.ProjectedLiquidationPrice)
	}
	for _, order := range view.ActiveOrders {
		if order.Role != domain.OrderRoleSafety || order.Level != 1 {
			continue
		}
		if order.ProjectedAverageEntryPrice == nil || order.ProjectedLiquidationPrice == nil {
			t.Fatalf("safety order is missing account-backed projection: %+v", order)
		}
		if !order.ProjectedAverageEntryPrice.Equal(decimal.RequireFromString("99.33")) || !order.ProjectedLiquidationPrice.Equal(decimal.RequireFromString("92.68")) {
			t.Fatalf("projected metrics did not use live position as the base: %+v", order)
		}
		return
	}
	t.Fatal("level 1 safety order was not found")
}

func TestAnnualizedReturnUsesNetPnlAndPlannedCapital(t *testing.T) {
	rt := New(config.Settings{Mode: "lighter_mainnet"}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	rt.runCapital = decimal.NewFromInt(100)
	rt.cumulativePnl = decimal.NewFromInt(10)
	rt.unrealizedPnl = decimal.NewFromInt(1)
	rt.tradingFees = decimal.NewFromInt(1)
	rt.strategyStartedAt = time.Unix(0, 0)
	rt.strategyEndedAt = time.Unix(24*60*60, 0)

	rt.mu.Lock()
	cumulativeCyclePnl := rt.cumulativeCyclePnlLocked()
	annualized := rt.annualizedReturnLocked(time.Unix(2*24*60*60, 0))
	rt.mu.Unlock()
	if !cumulativeCyclePnl.Equal(decimal.NewFromInt(11)) {
		t.Fatalf("current unrealized pnl was not included in cumulative cycle pnl: got=%s want=11", cumulativeCyclePnl)
	}
	if !annualized.Equal(decimal.NewFromInt(3650)) {
		t.Fatalf("unexpected annualized return: got=%s want=3650", annualized)
	}
}

func TestViewExposesStrategyStartTimeAndStableDuration(t *testing.T) {
	rt := New(config.Settings{Mode: "paper"}, exchange.NewPaper(), nil)
	t.Cleanup(func() { _ = rt.Close() })
	rt.strategyStartedAt = time.Unix(100, 0)
	rt.strategyEndedAt = time.Unix(100+3661, 0)

	view := rt.View()
	if view.StartedAt == nil || !view.StartedAt.Equal(rt.strategyStartedAt) {
		t.Fatalf("strategy start time was not exposed: %+v", view.StartedAt)
	}
	if view.DurationSeconds != 3661 {
		t.Fatalf("unexpected strategy duration: got=%d want=3661", view.DurationSeconds)
	}
}

func TestPositionWatchdogRetriesCancellationUntilMarketOrdersAreGone(t *testing.T) {
	ex := &runtimePositionExchange{
		Paper:          exchange.NewPaper(),
		fills:          make(chan domain.Fill, 1),
		streamErrors:   make(chan error),
		cancelFailures: 2,
	}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	initial := view.ActiveOrders[0]
	ex.fills <- domain.Fill{
		ExchangeOrderID: initial.ExchangeOrderID, ClientOrderID: initial.ClientOrderID,
		MarketIndex: initial.MarketIndex, Side: initial.Side, Role: initial.Role,
		Level: initial.Level, Price: initial.Price, Quantity: initial.Quantity,
	}
	ex.SetPositions([]domain.PositionSnapshot{{MarketIndex: 0, Quantity: initial.Quantity}})

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		current := rt.View()
		rt.mu.Lock()
		armed := rt.positionWatchArmed
		rt.mu.Unlock()
		if current.Cycle != nil && current.Cycle.PositionQuantity.GreaterThan(decimal.Zero) && armed {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("position watchdog did not arm: %+v", current)
		case <-time.After(5 * time.Millisecond):
		}
	}

	ex.SetPositions(nil)
	for {
		current := rt.View()
		if ex.CancelAttempts() >= 3 && len(current.ActiveOrders) == 0 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("watchdog did not retry until all orders were cancelled: attempts=%d view=%+v", ex.CancelAttempts(), current)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestLighterFillConsumerSurvivesStartRequestCancellation(t *testing.T) {
	ex := &runtimeFillExchange{
		Paper:        exchange.NewPaper(),
		fills:        make(chan domain.Fill, 1),
		streamErrors: make(chan error),
	}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	price := decimal.NewFromInt(100)
	view, err := rt.Start(requestCtx, runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	if len(view.ActiveOrders) != 1 {
		t.Fatalf("unexpected start view: %+v", view)
	}

	initial := view.ActiveOrders[0]
	ex.fills <- domain.Fill{
		ExchangeOrderID: initial.ExchangeOrderID,
		ClientOrderID:   initial.ClientOrderID,
		MarketIndex:     initial.MarketIndex,
		Side:            initial.Side,
		Role:            initial.Role,
		Level:           initial.Level,
		Price:           initial.Price,
		Quantity:        initial.Quantity,
	}

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		current := rt.View()
		for _, order := range current.ActiveOrders {
			if order.Role == domain.OrderRoleSafety || order.Role == domain.OrderRoleTakeProfit {
				return
			}
		}
		select {
		case <-deadline.C:
			t.Fatalf("fill was not processed after request context cancellation: %+v", current)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestOrderWatchdogRepairsMissingOrdersAfterContinuousRollover(t *testing.T) {
	ex := &runtimeFillExchange{
		Paper:        exchange.NewPaper(),
		fills:        make(chan domain.Fill, 16),
		streamErrors: make(chan error),
	}
	rt := New(config.Settings{Mode: "lighter_mainnet", LighterMarketIndex: 0, LighterEnableTrading: true}, ex, nil)
	t.Cleanup(func() { _ = rt.Close() })
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	fillOrder := func(order domain.ExchangeOrder) {
		// The Paper adapter does not receive live fill callbacks itself. Remove
		// the remote open order here to model the exchange-side fill first.
		_ = ex.Paper.CancelOrder(context.Background(), order.ClientOrderID)
		ex.fills <- domain.Fill{
			ExchangeOrderID: order.ExchangeOrderID, ClientOrderID: order.ClientOrderID,
			MarketIndex: order.MarketIndex, Side: order.Side, Role: order.Role,
			Level: order.Level, Price: order.Price, Quantity: order.Quantity,
		}
	}
	findOrder := func(view domain.StrategyView, role domain.OrderRole) (domain.ExchangeOrder, bool) {
		for _, order := range view.ActiveOrders {
			if order.Role == role {
				return order, true
			}
		}
		return domain.ExchangeOrder{}, false
	}
	waitFor := func(description string, predicate func(domain.StrategyView) bool) domain.StrategyView {
		deadline := time.NewTimer(5 * time.Second)
		defer deadline.Stop()
		for {
			view := rt.View()
			if predicate(view) {
				return view
			}
			select {
			case <-deadline.C:
				t.Fatalf("timeout waiting for %s: %+v", description, view)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}

	price := decimal.NewFromInt(100)
	view, err := rt.Start(context.Background(), runtimeTestConfig(), &price)
	if err != nil {
		t.Fatal(err)
	}
	initial, ok := findOrder(view, domain.OrderRoleInitial)
	if !ok {
		t.Fatalf("initial order missing: %+v", view)
	}
	fillOrder(initial)
	view = waitFor("first cycle orders", func(view domain.StrategyView) bool {
		return view.Cycle != nil && !view.Cycle.PositionQuantity.IsZero() && len(view.ActiveOrders) == 3
	})

	for cycle := 0; cycle < 2; cycle++ {
		takeProfit, ok := findOrder(view, domain.OrderRoleTakeProfit)
		if !ok {
			t.Fatalf("take-profit missing before cycle rollover %d: %+v", cycle+1, view)
		}
		fillOrder(takeProfit)
		view = waitFor("next market entry", func(view domain.StrategyView) bool {
			_, exists := findOrder(view, domain.OrderRoleInitial)
			return view.CompletedCycleCount == cycle+1 && exists
		})
		nextInitial, _ := findOrder(view, domain.OrderRoleInitial)
		fillOrder(nextInitial)
		view = waitFor("cycle grid orders after market fill", func(view domain.StrategyView) bool {
			return view.Cycle != nil && !view.Cycle.PositionQuantity.IsZero() && len(view.ActiveOrders) == 3
		})
	}
	if view.CompletedCycleCount != 2 {
		t.Fatalf("test did not reach the third cycle: %+v", view)
	}

	ids := make([]string, 0, len(view.ActiveOrders))
	for _, order := range view.ActiveOrders {
		ids = append(ids, order.ClientOrderID)
	}
	if err := ex.Paper.CancelOrdersBatch(context.Background(), ids); err != nil {
		t.Fatal(err)
	}
	repaired := waitFor("watchdog order repair", func(view domain.StrategyView) bool {
		for _, event := range view.Events {
			if event.Kind == "order_watch_repaired" {
				return len(view.ActiveOrders) == 3
			}
		}
		return false
	})
	if repaired.CompletedCycleCount != 2 {
		t.Fatalf("watchdog repair changed cycle count unexpectedly: %+v", repaired)
	}
}
