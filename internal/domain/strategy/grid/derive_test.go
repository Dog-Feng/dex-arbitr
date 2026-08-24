package grid

import (
	"testing"

	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// prototypeParams 复现控制台原型里的那组配置：
// BTC 中性网格，60000-66000，80 格，每格 0.002 币，30 倍杠杆。
func prototypeParams() Params {
	p := DefaultParams()
	p.Symbol = "BTC"
	p.Direction = Neutral
	p.Leverage = 30
	p.Grid.LowerPrice = d("60000")
	p.Grid.UpperPrice = d("66000")
	p.Grid.GridCount = 80
	p.Grid.SizingMode = PerGridQty
	p.Grid.PerGridQty = d("0.002")
	p.ApplyDefaults()
	return p
}

// 派生量必须与原型图上展示的数字一致：
// 单格间距 75.00 (0.12%) · 每格毛利 0.150 · 名义敞口 10080 · 约需保证金 336 USDC (30x)
func TestPreviewMatchesPrototypeNumbers(t *testing.T) {
	got, err := Preview(PreviewInput{
		Params: prototypeParams(),
		Market: testMarket(),
		Mark:   d("63000"),
	})
	if err != nil {
		t.Fatalf("preview failed: %v", err)
	}

	checks := []struct {
		name string
		got  decimal.Decimal
		want string
	}{
		{"step", got.Step, "75"},
		{"total qty", got.TotalQty, "0.16"},
		{"notional", got.Notional, "10080"},
		{"margin required", got.MarginRequired, "336"},
		{"grid profit", got.GridProfit, "0.15"},
	}
	for _, c := range checks {
		if !c.got.Equal(d(c.want)) {
			t.Errorf("%s = %s, want %s", c.name, c.got, c.want)
		}
	}
	if got.StepPct.StringFixed(2) != "0.12" {
		t.Errorf("step pct = %s, want 0.12", got.StepPct.StringFixed(2))
	}
	if got.GridCount != 80 {
		t.Errorf("grid count = %d, want 80", got.GridCount)
	}
	// 80 个格子对应 80 笔挂单，现价所在的那条价格线上没有单。
	if got.OrderCount != 80 {
		t.Errorf("order count = %d, want 80", got.OrderCount)
	}
	if !got.InitialPosition.IsZero() {
		t.Errorf("neutral grid initial position = %s, want 0", got.InitialPosition)
	}
}

// 30 倍杠杆下强平价落在网格区间内，这是致命配置，必须给出警告。
func TestPreviewWarnsWhenLiquidationInsideRange(t *testing.T) {
	got, err := Preview(PreviewInput{
		Params: prototypeParams(),
		Market: testMarket(),
		Mark:   d("63000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasWarning(got, WarnLiqInsideRange) {
		t.Fatalf("expected %s warning, got %+v", WarnLiqInsideRange, got.Warnings)
	}
	if !hasWarning(got, WarnHighLeverage) {
		t.Errorf("expected %s warning", WarnHighLeverage)
	}
	if !hasWarning(got, WarnNoStopLoss) {
		t.Errorf("expected %s warning", WarnNoStopLoss)
	}
}

// 网格太密时单格毛利覆盖不了双边手续费，必须硬拦截并给出可行的格数。
func TestPreviewRejectsGridTooDense(t *testing.T) {
	p := prototypeParams()
	p.Grid.GridCount = 200 // 步长 30，最小毛利率 0.045% < 双边费率 0.04% 的 2 倍

	_, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("63000")})
	issue := requireIssue(t, err, CodeGridTooDense)
	if issue.Field != "grid.grid_count" {
		t.Errorf("field = %s, want grid.grid_count", issue.Field)
	}
}

func TestPreviewRejectsInsufficientBalance(t *testing.T) {
	_, err := Preview(PreviewInput{
		Params:    prototypeParams(),
		Market:    testMarket(),
		Mark:      d("63000"),
		Available: d("100"), // 需要 336
	})
	requireIssue(t, err, CodeInsufficientBalance)
}

func TestPreviewRejectsTooManyOrders(t *testing.T) {
	_, err := Preview(PreviewInput{
		Params:        prototypeParams(),
		Market:        testMarket(),
		Mark:          d("63000"),
		MaxOpenOrders: 50,
	})
	requireIssue(t, err, CodeTooManyOrders)
}

func TestPreviewRejectsInvalidRange(t *testing.T) {
	p := prototypeParams()
	p.Grid.UpperPrice = d("60000")
	_, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("60000")})
	requireIssue(t, err, CodeInvalidRange)
}

func TestPreviewRejectsNonPostOnly(t *testing.T) {
	p := prototypeParams()
	p.Order.MakerTIF = 0 // GTC
	_, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("63000")})
	requireIssue(t, err, CodeInvalidMakerTIF)
}

func TestValidateTPSLByDirection(t *testing.T) {
	cases := []struct {
		name    string
		dir     Direction
		tp, sl  string
		wantErr bool
	}{
		{"long ok", Long, "68000", "58000", false},
		{"long tp inside range", Long, "63000", "58000", true},
		{"long sl above lower", Long, "68000", "61000", true},
		{"short ok", Short, "58000", "68000", false},
		{"short tp above lower", Short, "61000", "68000", true},
		{"short sl below upper", Short, "58000", "65000", true},
		{"neutral ok", Neutral, "68000", "58000", false},
		{"neutral tp inside", Neutral, "63000", "58000", true},
		{"neutral same side", Neutral, "68000", "69000", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := prototypeParams()
			p.Direction = c.dir
			p.Risk.TakeProfitPrice = d(c.tp)
			p.Risk.StopLossPrice = d(c.sl)
			err := validateTPSL(p)
			if c.wantErr && err == nil {
				t.Fatal("expected a validation error")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// 指定价格建仓时，挂单价必须在正确的一侧，否则 post-only 会被立即拒绝。
func TestPreviewRejectsCrossingEntryPrice(t *testing.T) {
	p := prototypeParams()
	p.Direction = Long
	p.Risk.TakeProfitPrice = d("68000")
	p.Risk.StopLossPrice = d("58000")
	p.Entry.Mode = strategy.EntryLimitPrice
	p.Entry.Price = d("64000") // 建多仓却挂在现价之上

	_, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("63000")})
	requireIssue(t, err, CodeInvalidEntryPrice)
}

func TestPreviewWarnsWhenMarkOutsideRange(t *testing.T) {
	got, err := Preview(PreviewInput{
		Params: prototypeParams(),
		Market: testMarket(),
		Mark:   d("59000"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !hasWarning(got, WarnMarkOutOfRange) {
		t.Fatalf("expected %s warning, got %+v", WarnMarkOutOfRange, got.Warnings)
	}
}

func TestPreviewLongInitialPosition(t *testing.T) {
	p := prototypeParams()
	p.Direction = Long
	p.Risk.TakeProfitPrice = d("68000")
	p.Risk.StopLossPrice = d("58000")

	got, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("63000")})
	if err != nil {
		t.Fatal(err)
	}
	// 现价 63000 正好是第 40 条价格线，上方 40 个格子 × 0.002 = 0.08
	if !got.InitialPosition.Equal(d("0.08")) {
		t.Fatalf("initial position = %s, want 0.08", got.InitialPosition)
	}
}

func TestPreviewShortInitialPositionIsNegative(t *testing.T) {
	p := prototypeParams()
	p.Direction = Short
	p.Risk.TakeProfitPrice = d("58000")
	p.Risk.StopLossPrice = d("68000")

	got, err := Preview(PreviewInput{Params: p, Market: testMarket(), Mark: d("63000")})
	if err != nil {
		t.Fatal(err)
	}
	if !got.InitialPosition.Equal(d("-0.08")) {
		t.Fatalf("initial position = %s, want -0.08", got.InitialPosition)
	}
}
