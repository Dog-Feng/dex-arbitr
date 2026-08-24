package martingale

import (
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

const (
	CodeInvalidRange        = "INVALID_RANGE"
	CodeInvalidLeverage     = "INVALID_LEVERAGE"
	CodeInvalidSizing       = "INVALID_SIZING"
	CodeNotionalTooSmall    = "NOTIONAL_TOO_SMALL"
	CodeInsufficientBalance = "INSUFFICIENT_BALANCE"
	CodeInvalidTPSL         = "INVALID_TP_SL"
	CodeTooManyOrders       = "TOO_MANY_ORDERS"
	CodeInvalidMarket       = "INVALID_MARKET"
	CodeNoMarkPrice         = "NO_MARK_PRICE"
	CodeInvalidDirection    = "INVALID_DIRECTION"

	WarnNoStopLoss      = "NO_STOP_LOSS"
	WarnHighLeverage    = "HIGH_LEVERAGE"
	WarnHighMultiplier  = "HIGH_MULTIPLIER"
	WarnLiqTooClose     = "LIQUIDATION_TOO_CLOSE"
	WarnMarginShortfall = "MARGIN_SHORTFALL"
)

func strategyErr(code, field, format string, args ...any) *strategy.Issue {
	return strategy.Errf(code, field, format, args...)
}

// PreviewInput 是马丁派生量计算的输入。
type PreviewInput struct {
	Params        Params
	Market        market.Market
	Mark          decimal.Decimal
	Available     decimal.Decimal
	Position      decimal.Decimal
	MaxOpenOrders int
}

// Derived 是马丁配置的派生量。
type Derived struct {
	Levels           []Level          `json:"levels"`
	TotalMargin      decimal.Decimal  `json:"total_margin"`
	TotalNotional    decimal.Decimal  `json:"total_notional"`
	MarginRequired   decimal.Decimal  `json:"margin_required"`
	LiquidationPrice decimal.Decimal  `json:"liquidation_price"`
	MaxDrawdownPct   decimal.Decimal  `json:"max_drawdown_pct"`
	OrderCount       int              `json:"order_count"`
	TakeProfit       decimal.Decimal  `json:"take_profit"`
	Warnings         []strategy.Issue `json:"warnings"`
}

// Preview 校验参数并计算加仓计划。
func Preview(in PreviewInput) (Derived, error) {
	var d Derived
	p := in.Params
	m := in.Market
	if err := m.Validate(); err != nil {
		return d, strategyErr(CodeInvalidMarket, "", "市场元数据无效：%v", err)
	}
	if p.Symbol != "" && p.Symbol != m.Symbol {
		return d, strategyErr(CodeInvalidMarket, "symbol", "交易对 %s 与市场 %s 不一致", p.Symbol, m.Symbol)
	}
	if !in.Mark.IsPositive() {
		return d, strategyErr(CodeNoMarkPrice, "", "没有有效的标记价")
	}
	if err := validateStatic(p); err != nil {
		return d, err
	}
	if p.Leverage > m.MaxLeverage && m.MaxLeverage > 0 {
		return d, strategyErr(CodeInvalidLeverage, "leverage", "杠杆 %d 超过市场上限 %d", p.Leverage, m.MaxLeverage)
	}

	p0 := m.RoundPrice(in.Mark, market.RoundNearest)
	plan, err := BuildPlan(p, m, p0)
	if err != nil {
		return d, err
	}
	d.Levels = plan.Levels
	d.TotalMargin = plan.TotalMargin
	d.TotalNotional = plan.TotalNotional
	d.MarginRequired = plan.TotalMargin
	d.LiquidationPrice = plan.LiquidationPrice
	d.MaxDrawdownPct = plan.MaxDrawdownPct
	if len(plan.Levels) > 0 {
		d.TakeProfit = plan.Levels[0].TakeProfit
	}
	d.OrderCount = p.Martingale.MaxAddTimes
	if p.Martingale.ShouldPreplace() {
		d.OrderCount = p.Martingale.MaxAddTimes + 1 // 加仓单 + 止盈
	} else {
		d.OrderCount = 2 // 下一笔加仓 + 止盈
	}

	fee := m.RoundTripFeeRate().Mul(hundred)
	if p.Martingale.TakeProfitPct.LessThanOrEqual(fee) {
		return d, strategyErr(CodeInvalidTPSL, "martingale.take_profit_pct",
			"止盈 %.4f%% 无法覆盖双边手续费 %.4f%%", p.Martingale.TakeProfitPct.InexactFloat64(), fee.InexactFloat64())
	}
	firstMargin := p.Martingale.InitialMargin
	covered := false
	if len(plan.Levels) > 0 {
		lot := plan.Levels[0].Qty
		if p.Direction == Long && in.Position.GreaterThanOrEqual(lot) {
			covered = true
		}
		if p.Direction == Short && in.Position.LessThanOrEqual(lot.Neg()) {
			covered = true
		}
	}
	if !covered && in.Available.IsPositive() && firstMargin.GreaterThan(in.Available) {
		return d, strategyErr(CodeInsufficientBalance, "martingale.initial_margin",
			"首单需要保证金 %s，可用 %s", firstMargin.StringFixed(2), in.Available.StringFixed(2))
	}
	if in.MaxOpenOrders > 0 && d.OrderCount > in.MaxOpenOrders {
		return d, strategyErr(CodeTooManyOrders, "martingale.max_add_times",
			"预挂 %d 笔超过交易所上限 %d", d.OrderCount, in.MaxOpenOrders)
	}
	d.Warnings = collectWarnings(p, plan, in.Mark)
	if in.Available.IsPositive() && plan.TotalMargin.GreaterThan(in.Available) {
		d.Warnings = append(d.Warnings, strategy.Warnf(WarnMarginShortfall, "martingale.add_margin",
			"加满仓需保证金 %s，当前可用 %s，加仓过程中可能资金不足",
			plan.TotalMargin.StringFixed(2), in.Available.StringFixed(2)))
	}
	return d, nil
}

func validateStatic(p Params) error {
	if p.Leverage < 1 {
		return strategyErr(CodeInvalidLeverage, "leverage", "杠杆必须 ≥ 1")
	}
	m := p.Martingale
	if m.AddDropPct.LessThanOrEqual(decimal.Zero) || m.AddDropPct.GreaterThanOrEqual(decimal.RequireFromString("50")) {
		return strategyErr(CodeInvalidRange, "martingale.add_drop_pct", "加仓间距必须在 (0, 50) 之间")
	}
	if !m.TakeProfitPct.IsPositive() {
		return strategyErr(CodeInvalidTPSL, "martingale.take_profit_pct", "止盈目标必须大于 0")
	}
	if !m.InitialMargin.IsPositive() || !m.AddMargin.IsPositive() {
		return strategyErr(CodeInvalidSizing, "martingale.initial_margin", "初次与加仓保证金必须大于 0")
	}
	if m.MaxAddTimes < 1 || m.MaxAddTimes > 20 {
		return strategyErr(CodeInvalidSizing, "martingale.max_add_times", "最大加仓次数必须在 1–20")
	}
	if m.AddMultiplier.LessThan(decimal.RequireFromString("0.1")) || m.AddMultiplier.GreaterThan(decimal.RequireFromString("3")) {
		return strategyErr(CodeInvalidSizing, "martingale.add_multiplier", "加仓倍数必须在 0.1–3.0")
	}
	return nil
}

func collectWarnings(p Params, plan Plan, mark decimal.Decimal) []strategy.Issue {
	var ws []strategy.Issue
	if !p.Risk.HasStopLoss() {
		ws = append(ws, strategy.Warnf(WarnNoStopLoss, "risk.stop_loss_price", "马丁没有天然止损，强烈建议配置止损价"))
	}
	if p.Leverage > 20 {
		ws = append(ws, strategy.Warnf(WarnHighLeverage, "leverage", "%d 倍杠杆下加满仓后强平风险很高", p.Leverage))
	}
	if p.Martingale.AddMultiplier.GreaterThan(decimal.RequireFromString("2")) {
		ws = append(ws, strategy.Warnf(WarnHighMultiplier, "martingale.add_multiplier", "加仓倍数大于 2，资金需求指数增长"))
	}
	if plan.LiquidationPrice.IsPositive() && mark.IsPositive() {
		dist := mark.Sub(plan.LiquidationPrice).Abs().Div(mark).Mul(hundred)
		if dist.LessThan(decimal.NewFromInt(8)) {
			ws = append(ws, strategy.Warnf(WarnLiqTooClose, "",
				"加满仓强平价 %s，距现价仅 %s%%", plan.LiquidationPrice.StringFixed(2), dist.StringFixed(1)))
		}
	}
	return ws
}
