package strategy

import (
	"testing"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

func testConfig() domain.MartingaleConfig {
	return domain.MartingaleConfig{
		MarketIndex: 0, Symbol: "BTC-USD", Direction: domain.DirectionLong,
		Leverage: decimal.NewFromInt(5), InitialMargin: decimal.NewFromInt(1), SafetyMargin: decimal.NewFromInt(1),
		PriceStepPct: decimal.RequireFromString("0.01"), PriceStepMultiplier: decimal.NewFromInt(1), AmountMultiplier: decimal.NewFromInt(1),
		MaxSafetyOrders: 2, TakeProfitPct: decimal.RequireFromString("0.01"), MarketMaxSlippage: decimal.RequireFromString("0.005"),
		MarketMaxRetries: 2, Continuous: true,
	}
}

func testMarket() domain.MarketSpec {
	return domain.MarketSpec{MarketIndex: 0, Symbol: "BTC-USD", PriceStep: decimal.NewFromInt(1), SizeStep: decimal.RequireFromString("0.01"), ContractMult: decimal.NewFromInt(1)}
}

func TestSafetyLevelsAndAverageCost(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	levels := grid.SafetyLevels(decimal.NewFromInt(100))
	if len(levels) != 2 || !levels[0].Price.Equal(decimal.NewFromInt(99)) || !levels[1].Price.Equal(decimal.NewFromInt(98)) {
		t.Fatalf("unexpected safety levels: %+v", levels)
	}
	action, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil || len(action.Place) != 1 {
		t.Fatalf("start failed: action=%+v err=%v", action, err)
	}
	_, err = grid.OnFill(domain.Fill{ClientOrderID: action.Place[0].ClientOrderID, Role: domain.OrderRoleInitial, Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	_, err = grid.OnFill(domain.Fill{ClientOrderID: "safety-1", Role: domain.OrderRoleSafety, Side: domain.OrderSideBuy, Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	if !grid.Cycle.AverageEntryPrice.Equal(decimal.RequireFromString("99.5")) || !grid.Cycle.PositionQuantity.Equal(decimal.NewFromInt(2)) {
		t.Fatalf("unexpected average/position: %+v", grid.Cycle)
	}
}

func TestProjectedFullGridCostAndLiquidation(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	if _, err := grid.Start(decimal.NewFromInt(100), false); err != nil {
		t.Fatal(err)
	}
	average, liquidation := grid.ProjectedFullGrid()
	if !average.Equal(decimal.NewFromInt(99)) || !liquidation.Equal(decimal.NewFromInt(79)) {
		t.Fatalf("unexpected projected full-grid metrics: average=%s liquidation=%s", average, liquidation)
	}
	average, liquidation = grid.ProjectedFullGridWithAvailableBalance(decimal.NewFromInt(1))
	if !average.Equal(decimal.NewFromInt(99)) || !liquidation.Equal(decimal.NewFromInt(92)) {
		t.Fatalf("unexpected available-balance full-grid metrics: average=%s liquidation=%s", average, liquidation)
	}
}

func TestProjectedLiquidationUsesAvailableBalance(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	if _, err := grid.Start(decimal.NewFromInt(100), false); err != nil {
		t.Fatal(err)
	}
	withoutAvailable, withoutAvailableLiquidation := grid.ProjectedFullGrid()
	withAvailable, withAvailableLiquidation := grid.ProjectedFullGridWithAvailableBalance(decimal.NewFromInt(1))
	if !withAvailable.Equal(withoutAvailable) || !withAvailableLiquidation.GreaterThan(withoutAvailableLiquidation) {
		t.Fatalf("available balance did not replace configured margin in long liquidation estimate: without=(%s,%s) with=(%s,%s)", withoutAvailable, withoutAvailableLiquidation, withAvailable, withAvailableLiquidation)
	}
}

func TestProjectedFillMetricsForSafetyOrder(t *testing.T) {
	market := testMarket()
	market.PriceStep = decimal.RequireFromString("0.01")
	grid := NewMartingaleGrid(testConfig(), market)
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: start.Place[0].Quantity,
	}); err != nil {
		t.Fatal(err)
	}

	metrics := grid.ProjectedFillMetrics(domain.ExchangeOrder{
		MarketIndex: 0, Role: domain.OrderRoleSafety, Level: 1,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(99), Quantity: decimal.RequireFromString("0.05"),
	})
	if !metrics.HasPosition || !metrics.AverageEntryPrice.Equal(decimal.RequireFromString("99.5")) ||
		!metrics.TakeProfitPrice.Equal(decimal.RequireFromString("100.49")) ||
		!metrics.LiquidationPrice.Equal(decimal.RequireFromString("79.5")) {
		t.Fatalf("unexpected projected safety-fill metrics: %+v", metrics)
	}
}

func TestProjectedFillMetricsUsesAvailableBalance(t *testing.T) {
	market := testMarket()
	market.PriceStep = decimal.RequireFromString("0.01")
	grid := NewMartingaleGrid(testConfig(), market)
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: start.Place[0].Quantity,
	}); err != nil {
		t.Fatal(err)
	}
	safety := domain.ExchangeOrder{
		MarketIndex: 0, Role: domain.OrderRoleSafety, Level: 1,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(99), Quantity: decimal.RequireFromString("0.05"),
	}
	withoutAvailable := grid.ProjectedFillMetrics(safety)
	withAvailable := grid.ProjectedFillMetricsWithAvailableBalance(safety, decimal.NewFromInt(1))
	if !withAvailable.HasPosition || !withAvailable.AverageEntryPrice.Equal(decimal.RequireFromString("99.5")) ||
		!withAvailable.TakeProfitPrice.Equal(decimal.RequireFromString("100.49")) ||
		!withAvailable.LiquidationPrice.Equal(decimal.RequireFromString("89.5")) ||
		!withAvailable.LiquidationPrice.GreaterThan(withoutAvailable.LiquidationPrice) {
		t.Fatalf("available balance did not replace configured margin in projected safety liquidation estimate: without=%+v with=%+v", withoutAvailable, withAvailable)
	}
}

func TestProjectedFillMetricsUsesAccountPositionAsBase(t *testing.T) {
	market := testMarket()
	market.PriceStep = decimal.RequireFromString("0.01")
	grid := NewMartingaleGrid(testConfig(), market)
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: start.Place[0].Quantity,
	}); err != nil {
		t.Fatal(err)
	}
	safety := domain.ExchangeOrder{
		MarketIndex: 0, Role: domain.OrderRoleSafety, Level: 1,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(99), Quantity: decimal.RequireFromString("0.05"),
	}
	metrics := grid.ProjectedFillMetricsWithAccountState(safety, decimal.NewFromInt(1), decimal.RequireFromString("0.10"), decimal.RequireFromString("99.50"))
	if !metrics.HasPosition || !metrics.AverageEntryPrice.Equal(decimal.RequireFromString("99.33")) ||
		!metrics.TakeProfitPrice.Equal(decimal.RequireFromString("100.32")) ||
		!metrics.LiquidationPrice.Equal(decimal.RequireFromString("92.66")) {
		t.Fatalf("account position was not used for projected safety-fill metrics: %+v", metrics)
	}
}

func TestProjectedFillMetricsForFullTakeProfitHasNoPosition(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: start.Place[0].Quantity,
	}); err != nil {
		t.Fatal(err)
	}
	tp := grid.TakeProfitIntent()
	if tp == nil {
		t.Fatal("take-profit intent was not created")
	}
	metrics := grid.ProjectedFillMetrics(domain.ExchangeOrder{
		MarketIndex: 0, Role: domain.OrderRoleTakeProfit, Level: -1,
		Side: domain.OrderSideSell, Price: tp.Price, Quantity: tp.Quantity, ReduceOnly: true,
	})
	if metrics.HasPosition || !metrics.AverageEntryPrice.IsZero() || !metrics.TakeProfitPrice.IsZero() || !metrics.LiquidationPrice.IsZero() {
		t.Fatalf("full take-profit should project no position: %+v", metrics)
	}
}

func TestContinuousTakeProfitStartsMarketCycle(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	first, err := grid.OnFill(domain.Fill{ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial, Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1)})
	if err != nil {
		t.Fatal(err)
	}
	tp := first.Place[0]
	action, err := grid.OnFill(domain.Fill{ClientOrderID: tp.ClientOrderID, Role: domain.OrderRoleTakeProfit, Side: domain.OrderSideSell, Price: tp.Price, Quantity: tp.Quantity})
	if err != nil {
		t.Fatal(err)
	}
	if !action.CycleCompleted || len(action.Place) != 1 || action.Place[0].OrderType != domain.OrderTypeMarket {
		t.Fatalf("unexpected cycle rollover: %+v", action)
	}
}

func TestMakerEntryUsesPostOnlyLimitOrder(t *testing.T) {
	config := testConfig()
	config.EntryMode = domain.EntryModeMaker
	grid := NewMartingaleGrid(config, testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(start.Place) != 1 || start.Place[0].OrderType != domain.OrderTypeLimit || !start.Place[0].PostOnly {
		t.Fatalf("maker entry was not a post-only limit order: %+v", start)
	}
}

func TestFixedMakerEntryUsesConfiguredPrice(t *testing.T) {
	config := testConfig()
	config.EntryMode = domain.EntryModeLimitMaker
	price := decimal.NewFromInt(100)
	config.InitialPrice = &price
	grid := NewMartingaleGrid(config, testMarket())
	start, err := grid.Start(decimal.NewFromInt(105), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(start.Place) != 1 || !start.Place[0].Price.Equal(price) || start.Place[0].OrderType != domain.OrderTypeLimit || !start.Place[0].PostOnly {
		t.Fatalf("fixed Maker entry did not use the configured post-only price: %+v", start)
	}
}

func TestPartialMakerEntryWaitsBeforeCreatingGridOrders(t *testing.T) {
	config := testConfig()
	config.EntryMode = domain.EntryModeMaker
	grid := NewMartingaleGrid(config, testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: decimal.RequireFromString("0.02"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(partial.Place) != 0 || len(grid.ExpectedOrderIntents()) != 1 {
		t.Fatalf("partial Maker fill created follow-up orders too early: action=%+v expected=%+v", partial, grid.ExpectedOrderIntents())
	}
	remaining := grid.ExpectedOrderIntents()[0]
	if !remaining.Quantity.Equal(start.Place[0].Quantity.Sub(decimal.RequireFromString("0.02"))) {
		t.Fatalf("initial remaining quantity was not reduced: %+v", remaining)
	}
	completed, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: remaining.Quantity,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(completed.Place) != 3 {
		t.Fatalf("completed Maker entry did not create TP and safety orders: %+v", completed)
	}
}

func TestTakeProfitReplacementModifiesExistingOrder(t *testing.T) {
	grid := NewMartingaleGrid(testConfig(), testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	entry, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	oldTP := entry.Place[0]
	action, err := grid.OnFill(domain.Fill{
		ClientOrderID: "safety-1", Role: domain.OrderRoleSafety,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(99), Quantity: decimal.NewFromInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if action.Modify == nil || action.Modify.ClientOrderID != oldTP.ClientOrderID {
		t.Fatalf("expected in-place TP modification: %+v", action)
	}
	if len(action.Place) != 0 || len(action.CancelClientOrderIDs) != 0 {
		t.Fatalf("TP replacement must not place/cancel a second order: %+v", action)
	}
	if action.Modify.Intent.ClientOrderID != oldTP.ClientOrderID {
		t.Fatalf("modified TP changed client order ID: %+v", action.Modify.Intent)
	}
}
