package lighter

import (
	"testing"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestStateFromStatus(t *testing.T) {
	full := dec("1")
	cases := []struct {
		status     string
		filled     string
		wantState  order.State
		wantReason bool
	}{
		{"open", "0", order.StateOpen, false},
		{"pending", "0", order.StateOpen, false},
		{"in-progress", "0", order.StateOpen, false},
		{"open", "0.4", order.StatePartiallyFilled, false},
		{"filled", "1", order.StateFilled, false},
		{"canceled", "0", order.StateCanceled, false},
		{"cancelled", "0", order.StateCanceled, false},
		{"expired", "0", order.StateExpired, true},
		{"canceled-expired", "0", order.StateExpired, true},

		// 主网实测：会立即成交的 post-only 单以这个状态回来。
		{"canceled-post-only", "0", order.StateRejected, true},
		{"canceled-self-trade", "0", order.StateRejected, true},
		{"canceled-reduce-only", "0", order.StateRejected, true},
	}
	for _, c := range cases {
		t.Run(c.status, func(t *testing.T) {
			state, reason := stateFromStatus(c.status, dec(c.filled), full)
			if state != c.wantState {
				t.Errorf("状态 = %s，期望 %s", state, c.wantState)
			}
			if c.wantReason && reason == "" {
				t.Error("应当保留原始状态文本")
			}
		})
	}
}

// 未知状态必须按「订单已不在场上」处理。
//
// 反过来把未知状态当成 open 会留下幽灵挂单：格子永远停在 Resting、
// 永远不补单，而且不报错。这种静默的网格空洞比多下一笔单危险得多。
func TestUnknownStatusIsNotTreatedAsOpen(t *testing.T) {
	state, reason := stateFromStatus("some-future-status", decimal.Zero, dec("1"))
	if state == order.StateOpen || state == order.StatePartiallyFilled {
		t.Fatalf("未知状态被当成了存活订单（%s），会造成幽灵挂单", state)
	}
	if !state.IsTerminal() {
		t.Fatalf("未知状态应当是终态，实际 %s", state)
	}
	if reason == "" {
		t.Error("未知状态必须把原文带出来，否则排查时无从下手")
	}
}

// 完整解析一遍主网抓到的 canceled-post-only 报文。
func TestToOrderPostOnlyRejection(t *testing.T) {
	raw := apiOrder{
		OrderIndex:          1125898835206873,
		ClientOrderIndex:    439412092944,
		MarketIndex:         2,
		InitialBaseAmount:   num{dec("0.150")},
		RemainingBaseAmount: num{decimal.Zero},
		FilledBaseAmount:    num{decimal.Zero},
		Price:               num{dec("75.565")},
		IsAsk:               false,
		Type:                "limit",
		TimeInForce:         "post-only",
		ReduceOnly:          false,
		Status:              "canceled-post-only",
	}

	o := toOrder(raw, "SOL")
	if o.State != order.StateRejected {
		t.Fatalf("状态 = %s，期望 rejected（策略据此递增重挂计数）", o.State)
	}
	if o.Side != order.Buy {
		t.Errorf("方向 = %s，期望 buy", o.Side)
	}
	if o.TIF != order.PostOnly {
		t.Errorf("有效期 = %s，期望 post_only（交易所写的是 \"post-only\"）", o.TIF)
	}
	if o.ClientOrderID != order.ClientOrderID(439412092944) {
		t.Errorf("客户端订单号 = %d", o.ClientOrderID)
	}
	if o.RejectReason != "canceled-post-only" {
		t.Errorf("拒绝原因 = %q，期望保留交易所原文", o.RejectReason)
	}
}

func TestToOrderComputesAverageFillPrice(t *testing.T) {
	raw := apiOrder{
		ClientOrderIndex:  1,
		InitialBaseAmount: num{dec("0.14")},
		FilledBaseAmount:  num{dec("0.14")},
		FilledQuoteAmount: num{dec("10.57084")},
		Price:             num{dec("75.884")}, // 市价单里这是滑点上限，不是成交价
		IsAsk:             false,
		Status:            "filled",
	}
	o := toOrder(raw, "SOL")
	if !o.AvgFillPrice.Equal(dec("75.506")) {
		t.Fatalf("成交均价 = %s，期望 75.506", o.AvgFillPrice)
	}
	if !o.Price.Equal(dec("75.884")) {
		t.Fatalf("挂单价字段应当原样保留滑点上限，实际 %s", o.Price)
	}
}

func TestToMarketConvertsFeeAndMarginUnits(t *testing.T) {
	d := orderBookDetail{
		orderBook: orderBook{
			Symbol:         "SOL",
			MarketID:       2,
			MarketType:     perpMarketType,
			Status:         activeStatus,
			MakerFee:       num{dec("0.0200")}, // 交易所以百分数返回
			TakerFee:       num{dec("0.0500")},
			MinBaseAmount:  num{dec("0.100")},
			MinQuoteAmount: num{dec("10")},
		},
		SizeDecimals:              3,
		PriceDecimals:             3,
		MinInitialMarginFraction:  400, // 万分之一 → 4% → 25 倍
		MaintenanceMarginFraction: 240, // → 2.4%
	}
	m := toMarket(d)

	if !m.TickSize.Equal(dec("0.001")) || !m.LotSize.Equal(dec("0.001")) {
		t.Errorf("精度换算错误: tick=%s lot=%s", m.TickSize, m.LotSize)
	}
	if m.MaxLeverage != 25 {
		t.Errorf("最大杠杆 = %d，期望 25", m.MaxLeverage)
	}
	if !m.MakerFeeRate.Equal(dec("0.0002")) {
		t.Errorf("maker 费率 = %s，期望 0.0002（0.02%% 转成小数）", m.MakerFeeRate)
	}
	if !m.MaintMarginRate.Equal(dec("0.024")) {
		t.Errorf("维持保证金率 = %s，期望 0.024", m.MaintMarginRate)
	}
}

func TestPriceAndSizeConversion(t *testing.T) {
	p, err := priceToInt(dec("75.565"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if p != 75565 {
		t.Errorf("价格换算 = %d，期望 75565", p)
	}
	s, err := sizeToInt(dec("0.150"), 3)
	if err != nil {
		t.Fatal(err)
	}
	if s != 150 {
		t.Errorf("数量换算 = %d，期望 150", s)
	}

	// 不是最小变动单位整数倍的价格必须被拒绝，不能悄悄截断。
	if _, err := priceToInt(dec("75.5651"), 3); err == nil {
		t.Error("超出报价精度的价格应当报错")
	}
}
