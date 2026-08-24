package martingale

import (
	"errors"
	"testing"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testMarket() market.Market {
	return market.Market{
		Symbol: "SOL", TickSize: d("0.01"), LotSize: d("0.001"),
		MinQty: d("0.001"), MinNotional: d("1"), MaxLeverage: 50,
		MakerFeeRate: d("0.0002"), TakerFeeRate: d("0.0005"),
		MaintMarginRate: d("0.025"),
	}
}

func testParams() Params {
	p := DefaultParams()
	p.Symbol = "SOL"
	p.Leverage = 10
	p.Martingale.AddDropPct = d("2")
	p.Martingale.TakeProfitPct = d("1.5")
	p.Martingale.InitialMargin = d("50")
	p.Martingale.AddMargin = d("50")
	p.Martingale.MaxAddTimes = 3
	p.Martingale.AddMultiplier = d("1")
	return p
}

func TestBuildPlanLongFromLast(t *testing.T) {
	plan, err := BuildPlan(testParams(), testMarket(), d("100"))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Levels) != 4 {
		t.Fatalf("levels = %d, want 4", len(plan.Levels))
	}
	if !plan.Levels[0].TriggerPrice.Equal(d("100")) {
		t.Fatalf("p0 = %s", plan.Levels[0].TriggerPrice)
	}
	if !plan.Levels[1].TriggerPrice.Equal(d("98")) {
		t.Fatalf("p1 = %s, want 98", plan.Levels[1].TriggerPrice)
	}
	if !plan.Levels[2].TriggerPrice.Equal(d("96.04")) {
		t.Fatalf("p2 = %s, want 96.04", plan.Levels[2].TriggerPrice)
	}
	if !plan.TotalMargin.Equal(d("200")) {
		t.Fatalf("margin = %s, want 200", plan.TotalMargin)
	}
	if !plan.Levels[0].TakeProfit.Equal(d("101.5")) {
		t.Fatalf("tp0 = %s, want 101.5", plan.Levels[0].TakeProfit)
	}
}

func TestPreviewRejectsNeutralFeeAndBalance(t *testing.T) {
	p := testParams()
	p.Martingale.TakeProfitPct = d("0.01")
	_, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("100")})
	var issue *strategy.Issue
	if !errors.As(err, &issue) || issue.Code != CodeInvalidTPSL {
		t.Fatalf("want INVALID_TP_SL, got %v", err)
	}

	p = testParams()
	_, err = Preview(PreviewInput{
		Params: p, Market: testMarket(), Mark: d("100"), Available: d("10"),
	})
	if !errors.As(err, &issue) || issue.Code != CodeInsufficientBalance {
		t.Fatalf("want INSUFFICIENT_BALANCE, got %v", err)
	}
}

func TestPreviewWarnsMissingStopLoss(t *testing.T) {
	derv, err := Preview(PreviewInput{Params: testParams(), Market: testMarket(), Mark: d("100")})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range derv.Warnings {
		if w.Code == WarnNoStopLoss {
			found = true
		}
	}
	if !found {
		t.Fatal("expected NO_STOP_LOSS warning")
	}
}
