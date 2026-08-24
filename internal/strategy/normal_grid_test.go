package strategy

import (
	"testing"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

func normalGridConfig(direction domain.Direction) domain.NormalGridConfig {
	return domain.NormalGridConfig{
		MarketIndex: 0, Symbol: "BTC-USD", Direction: direction,
		Leverage: decimal.NewFromInt(5), GridLowerPrice: decimal.NewFromInt(90),
		GridUpperPrice: decimal.NewFromInt(110), GridCount: 4, GridMargin: decimal.NewFromInt(100),
		GridMode: "arithmetic", MarketMaxSlippage: decimal.RequireFromString("0.005"),
	}
}

func TestNormalGridArithmeticNeutralStartAndReplacement(t *testing.T) {
	grid := NewNormalGrid(normalGridConfig(domain.DirectionNeutral), testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(start.Place) != 4 {
		t.Fatalf("expected four orders around the anchor, got %d", len(start.Place))
	}
	for _, order := range start.Place {
		if order.Role != domain.OrderRoleGrid || !order.PostOnly || order.Price.Equal(decimal.NewFromInt(100)) {
			t.Fatalf("unexpected neutral grid order: %+v", order)
		}
	}
	if !start.Place[0].Price.Equal(decimal.NewFromInt(90)) || !start.Place[1].Price.Equal(decimal.NewFromInt(95)) ||
		!start.Place[2].Price.Equal(decimal.NewFromInt(105)) || !start.Place[3].Price.Equal(decimal.NewFromInt(110)) {
		t.Fatalf("grid is not arithmetic: %+v", start.Place)
	}

	replacement, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[1].ClientOrderID, Role: domain.OrderRoleGrid,
		Side: domain.OrderSideBuy, Level: 1, Price: decimal.NewFromInt(95), Quantity: decimal.NewFromInt(1),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(replacement.Place) != 1 || replacement.Place[0].Level != 2 || replacement.Place[0].Side != domain.OrderSideSell ||
		!replacement.Place[0].Price.Equal(decimal.NewFromInt(100)) {
		t.Fatalf("filled grid level was not replaced by the adjacent sell: %+v", replacement)
	}
}

func TestNormalGridDirectionalSeedAndFollow(t *testing.T) {
	config := normalGridConfig(domain.DirectionLong)
	config.GridFollowUp = true
	grid := NewNormalGrid(config, testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil || len(start.Place) != 1 || start.Place[0].Role != domain.OrderRoleInitial ||
		start.Place[0].Side != domain.OrderSideBuy || start.Place[0].OrderType != domain.OrderTypeMarket {
		t.Fatalf("unexpected long grid seed: action=%+v err=%v", start, err)
	}
	afterSeed, err := grid.OnFill(domain.Fill{
		ClientOrderID: start.Place[0].ClientOrderID, Role: domain.OrderRoleInitial,
		Side: domain.OrderSideBuy, Price: decimal.NewFromInt(100), Quantity: decimal.NewFromInt(1),
	})
	if err != nil || len(afterSeed.Place) != 4 {
		t.Fatalf("long grid did not place its range orders: action=%+v err=%v", afterSeed, err)
	}
	for _, order := range afterSeed.Place {
		if order.Side == domain.OrderSideSell && !order.ReduceOnly {
			t.Fatalf("long grid sell order must be reduce-only: %+v", order)
		}
	}

	shift, err := grid.OnPrice(decimal.NewFromInt(112))
	if err != nil {
		t.Fatal(err)
	}
	if !shift.CancelBeforePlace || len(shift.CancelClientOrderIDs) != 4 || len(shift.Place) == 0 {
		t.Fatalf("grid did not rebuild after following price: %+v", shift)
	}
	snapshot := grid.GridSnapshot()
	if snapshot == nil || !snapshot.LowerPrice.Equal(decimal.NewFromInt(95)) || !snapshot.UpperPrice.Equal(decimal.NewFromInt(115)) {
		t.Fatalf("unexpected shifted range: %+v", snapshot)
	}
}

func TestNormalGridMakerEntryUsesPostOnlyLimitOrder(t *testing.T) {
	config := normalGridConfig(domain.DirectionLong)
	config.EntryMode = domain.EntryModeMaker
	grid := NewNormalGrid(config, testMarket())
	start, err := grid.Start(decimal.NewFromInt(100), false)
	if err != nil {
		t.Fatal(err)
	}
	if len(start.Place) != 1 || start.Place[0].OrderType != domain.OrderTypeLimit || !start.Place[0].PostOnly {
		t.Fatalf("maker entry was not a post-only limit order: %+v", start)
	}
}

func TestNormalGridSnapshotIncludesNetProfitPerGrid(t *testing.T) {
	market := testMarket()
	market.ContractMult = decimal.NewFromInt(1)
	market.MakerFeeRate = decimal.RequireFromString("0.001")
	market.FeeRateKnown = true
	grid := NewNormalGrid(normalGridConfig(domain.DirectionNeutral), market)
	if _, err := grid.Start(decimal.NewFromInt(100), false); err != nil {
		t.Fatal(err)
	}
	snapshot := grid.GridSnapshot()
	if snapshot == nil || snapshot.ExpectedProfitPerGrid == nil {
		t.Fatalf("normal-grid snapshot did not include expected profit: %+v", snapshot)
	}
	// Grid step=5, quantity=1, gross profit=5. Two fees on the midpoint
	// notional (100 * 1 * 0.1%) reduce it to 4.8.
	if !snapshot.ExpectedProfitPerGrid.Equal(decimal.RequireFromString("4.8")) {
		t.Fatalf("unexpected net profit per grid: got=%s want=4.8", snapshot.ExpectedProfitPerGrid)
	}
}

func TestNormalGridRejectsNeutralFollowAndNonArithmeticMode(t *testing.T) {
	config := normalGridConfig(domain.DirectionNeutral)
	config.GridFollowUp = true
	if _, err := NewNormalGrid(config, testMarket()).Start(decimal.NewFromInt(100), false); err == nil {
		t.Fatal("neutral grid accepted follow-price configuration")
	}
	config = normalGridConfig(domain.DirectionLong)
	config.GridMode = "geometric"
	if _, err := NewNormalGrid(config, testMarket()).Start(decimal.NewFromInt(100), false); err == nil {
		t.Fatal("normal grid accepted unsupported mode")
	}
}
