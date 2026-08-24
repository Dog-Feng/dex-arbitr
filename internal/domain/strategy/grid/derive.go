package grid

import (
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// feeSafetyMultiple 是单格毛利率相对双边手续费的最低倍数。
//
// 网格最常见的亏损原因就是格子太密，每完成一格赚的价差还不够付两笔手续费。
// 这里用 2 倍作为硬门槛：低于它直接拒绝保存，而不是让用户自己发现。
var feeSafetyMultiple = decimal.NewFromInt(2)

// highLeverageThreshold 超过它就在页面上提示高杠杆风险。
const highLeverageThreshold = 20

// PreviewInput 是派生量计算的输入。
//
// 这是一个纯函数的入参：市场元数据与行情作为参数传入而不是内部去查，
// 这样 API 的 /preview 端点可以做到毫秒级响应，支持表单实时预览。
type PreviewInput struct {
	Params Params
	Market market.Market
	// Mark 是当前标记价，用于确定初始仓位与敞口。
	Mark decimal.Decimal
	// Available 是账户可用保证金。为零时跳过余额校验。
	Available decimal.Decimal
	// MaxOpenOrders 是交易所单市场挂单上限。为零时跳过挂单数校验。
	MaxOpenOrders int
}

// Derived 是配置的派生量，对应页面表单下方的实时预览。
type Derived struct {
	Lower     decimal.Decimal `json:"lower"`
	Upper     decimal.Decimal `json:"upper"`
	GridCount int             `json:"grid_count"`

	Step    decimal.Decimal `json:"step"`
	StepPct decimal.Decimal `json:"step_pct"` // 相对现价的百分比

	TotalQty       decimal.Decimal `json:"total_qty"`
	Notional       decimal.Decimal `json:"notional"`
	MarginRequired decimal.Decimal `json:"margin_required"`

	// GridProfit 是现价所在格完成一个循环的毛利。
	GridProfit       decimal.Decimal `json:"grid_profit"`
	GridProfitRate   decimal.Decimal `json:"grid_profit_rate"`    // 百分比
	MinProfitRate    decimal.Decimal `json:"min_profit_rate"`     // 百分比
	MaxProfitRate    decimal.Decimal `json:"max_profit_rate"`     // 百分比
	RoundTripFeeRate decimal.Decimal `json:"round_trip_fee_rate"` // 百分比
	NetGridProfit    decimal.Decimal `json:"net_grid_profit"`

	InitialPosition  decimal.Decimal `json:"initial_position"`
	OrderCount       int             `json:"order_count"`
	LiquidationPrice decimal.Decimal `json:"liquidation_price"`

	Cells    []Cell  `json:"cells"`
	Warnings []Issue `json:"warnings"`
}

var (
	hundred = decimal.NewFromInt(100)
	one     = decimal.NewFromInt(1)
)

// Preview 校验参数并计算派生量。
//
// 返回的 error 一定是 *Issue，调用方可以用 errors.As 取出 Code 与 Field
// 直接回给前端做字段级高亮。
func Preview(in PreviewInput) (Derived, error) {
	var d Derived
	p := in.Params
	m := in.Market

	if err := m.Validate(); err != nil {
		return d, errf(CodeInvalidMarket, "", "市场元数据无效：%v", err)
	}
	if !in.Mark.IsPositive() {
		return d, errf(CodeNoMarkPrice, "", "缺少有效的标记价，无法计算派生量")
	}
	if err := validateStatic(p, m); err != nil {
		return d, err
	}

	g, err := Build(p, m)
	if err != nil {
		return d, err
	}
	g.Arm(p.Direction, in.Mark)

	if err := validateCells(g, m); err != nil {
		return d, err
	}

	d.Lower, d.Upper = g.Lower(), g.Upper()
	d.GridCount = g.Count()
	d.Cells = g.Cells

	d.Step = g.Cells[0].Spread()
	d.StepPct = d.Step.Div(in.Mark).Mul(hundred)

	d.TotalQty = g.TotalQty()
	d.Notional = d.TotalQty.Mul(in.Mark)
	d.MarginRequired = d.Notional.Div(decimal.NewFromInt(int64(p.Leverage)))

	d.RoundTripFeeRate = m.RoundTripFeeRate().Mul(hundred)
	d.MinProfitRate, d.MaxProfitRate = profitRateRange(g)

	pivot := pivotCell(g, in.Mark)
	d.GridProfit = g.Cells[pivot].GrossProfit()
	d.GridProfitRate = g.Cells[pivot].Spread().Div(g.Cells[pivot].Low).Mul(hundred)
	fee := m.FeeFor(g.Cells[pivot].Low, g.Cells[pivot].Qty, true).
		Add(m.FeeFor(g.Cells[pivot].High, g.Cells[pivot].Qty, true))
	d.NetGridProfit = d.GridProfit.Sub(fee)

	d.InitialPosition = g.TargetPosition(p.Direction, p.Grid.NeutralBaseRatio)
	d.OrderCount = len(g.ActiveWindow(in.Mark, p.Grid.MaxActiveOrders))

	if err := validateEconomics(p, m, g, d, in); err != nil {
		return d, err
	}

	d.LiquidationPrice = estimateLiquidation(p, m, in.Mark)
	d.Warnings = collectWarnings(p, m, g, d, in)
	return d, nil
}

// validateStatic 做不需要构造网格就能完成的检查。
func validateStatic(p Params, m market.Market) error {
	if p.Grid.LowerPrice.LessThanOrEqual(decimal.Zero) {
		return errf(CodeInvalidRange, "grid.lower_price", "下边界必须大于 0")
	}
	if p.Grid.UpperPrice.LessThanOrEqual(p.Grid.LowerPrice) {
		return errf(CodeInvalidRange, "grid.upper_price", "上边界 %s 必须大于下边界 %s",
			p.Grid.UpperPrice, p.Grid.LowerPrice)
	}
	if p.Grid.GridCount < 2 || p.Grid.GridCount > order.MaxCell+1 {
		return errf(CodeInvalidGridCount, "grid.grid_count", "网格数量必须在 2 - %d 之间", order.MaxCell+1)
	}
	if p.Leverage < 1 || p.Leverage > m.MaxLeverage {
		return errf(CodeInvalidLeverage, "leverage", "杠杆必须在 1 - %d 之间", m.MaxLeverage)
	}
	if p.Order.MakerTIF != order.PostOnly {
		return errf(CodeInvalidMakerTIF, "order.maker_tif",
			"网格挂单只允许 post_only，当前为 %s", p.Order.MakerTIF)
	}
	if r := p.Grid.NeutralBaseRatio; r.IsNegative() || r.GreaterThan(one) {
		return errf(CodeInvalidNeutralRatio, "grid.neutral_base_ratio", "中性底仓比例必须在 0 - 1 之间")
	}

	switch p.Grid.SizingMode {
	case PerGridQty:
		if !p.Grid.PerGridQty.IsPositive() {
			return errf(CodeInvalidSizing, "grid.per_grid_qty", "每格数量必须大于 0")
		}
	case MarginBased:
		if !p.Grid.Margin.IsPositive() {
			return errf(CodeInvalidSizing, "grid.margin", "保证金必须大于 0")
		}
	}
	return validateTPSL(p)
}

// validateTPSL 检查止盈止损与网格方向的关系。
func validateTPSL(p Params) error {
	lower, upper := p.Grid.LowerPrice, p.Grid.UpperPrice
	tp, sl := p.Risk.TakeProfitPrice, p.Risk.StopLossPrice

	switch p.Direction {
	case Long:
		if p.Risk.HasStopLoss() && sl.GreaterThanOrEqual(lower) {
			return errf(CodeInvalidTPSL, "risk.stop_loss_price",
				"做多网格的止损价 %s 必须低于区间下边界 %s", sl, lower)
		}
		if p.Risk.HasTakeProfit() && tp.LessThanOrEqual(upper) {
			return errf(CodeInvalidTPSL, "risk.take_profit_price",
				"做多网格的止盈价 %s 必须高于区间上边界 %s", tp, upper)
		}
	case Short:
		if p.Risk.HasStopLoss() && sl.LessThanOrEqual(upper) {
			return errf(CodeInvalidTPSL, "risk.stop_loss_price",
				"做空网格的止损价 %s 必须高于区间上边界 %s", sl, upper)
		}
		if p.Risk.HasTakeProfit() && tp.GreaterThanOrEqual(lower) {
			return errf(CodeInvalidTPSL, "risk.take_profit_price",
				"做空网格的止盈价 %s 必须低于区间下边界 %s", tp, lower)
		}
	default:
		// 中性网格没有天然方向，只要求触发价落在区间之外且分处两侧。
		if p.Risk.HasTakeProfit() && tp.GreaterThanOrEqual(lower) && tp.LessThanOrEqual(upper) {
			return errf(CodeInvalidTPSL, "risk.take_profit_price",
				"中性网格的止盈价 %s 不能落在区间 [%s, %s] 之内", tp, lower, upper)
		}
		if p.Risk.HasStopLoss() && sl.GreaterThanOrEqual(lower) && sl.LessThanOrEqual(upper) {
			return errf(CodeInvalidTPSL, "risk.stop_loss_price",
				"中性网格的止损价 %s 不能落在区间 [%s, %s] 之内", sl, lower, upper)
		}
		if p.Risk.HasTakeProfit() && p.Risk.HasStopLoss() {
			bothAbove := tp.GreaterThan(upper) && sl.GreaterThan(upper)
			bothBelow := tp.LessThan(lower) && sl.LessThan(lower)
			if bothAbove || bothBelow {
				return errf(CodeInvalidTPSL, "risk.stop_loss_price",
					"中性网格的止盈价与止损价必须分处区间两侧")
			}
		}
	}
	return nil
}

// validateCells 检查每个格子的数量是否满足交易所的最小限制。
func validateCells(g *Grid, m market.Market) error {
	for i := range g.Cells {
		c := &g.Cells[i]
		if !c.Qty.IsPositive() {
			return errf(CodeQtyTooSmall, "grid.per_grid_qty",
				"第 %d 格数量按最小变动单位 %s 规整后为 0，请提高每格数量或保证金", i, m.LotSize)
		}
		if err := m.CheckOrder(c.Low, c.Qty); err != nil {
			minQty := m.MinNotional.Div(c.Low)
			return errf(CodeNotionalTooSmall, "grid.per_grid_qty",
				"第 %d 格（价格 %s，数量 %s）不满足交易所最小下单要求：%v；该价位至少需要 %s",
				i, c.Low, c.Qty, err, m.RoundQty(minQty.Add(m.LotSize)))
		}
	}
	return nil
}

// validateEconomics 检查手续费覆盖、挂单数与余额。
func validateEconomics(p Params, m market.Market, g *Grid, d Derived, in PreviewInput) error {
	// 单格毛利率必须显著高于双边手续费，否则每完成一格都在亏钱。
	threshold := m.RoundTripFeeRate().Mul(feeSafetyMultiple).Mul(hundred)
	if m.MakerFeeRate.IsPositive() && d.MinProfitRate.LessThanOrEqual(threshold) {
		maxCount := maxGridCountForFee(p.Grid, m)
		return errf(CodeGridTooDense, "grid.grid_count",
			"最小单格毛利率 %s%% 低于双边手续费 %s%% 的 %s 倍，网格数建议不超过 %d",
			d.MinProfitRate.StringFixed(4), d.RoundTripFeeRate.StringFixed(4),
			feeSafetyMultiple, maxCount)
	}

	if in.MaxOpenOrders > 0 && d.OrderCount > in.MaxOpenOrders {
		return errf(CodeTooManyOrders, "grid.grid_count",
			"需要挂 %d 单，超过交易所单市场上限 %d，请减少网格数或设置最大同时挂单数",
			d.OrderCount, in.MaxOpenOrders)
	}

	if in.Available.IsPositive() && d.MarginRequired.GreaterThan(in.Available) {
		return errf(CodeInsufficientBalance, "grid.margin",
			"需要保证金 %s，账户可用 %s，还差 %s",
			d.MarginRequired.StringFixed(2), in.Available.StringFixed(2),
			d.MarginRequired.Sub(in.Available).StringFixed(2))
	}

	// 指定价格建仓时，挂单价必须在正确的一侧，否则 post-only 会被立即拒绝。
	if p.Entry.Mode == strategy.EntryLimitPrice && !d.InitialPosition.IsZero() {
		if !p.Entry.Price.IsPositive() {
			return errf(CodeInvalidEntryPrice, "entry.price", "指定价格建仓必须填写建仓价")
		}
		if d.InitialPosition.IsPositive() && p.Entry.Price.GreaterThan(in.Mark) {
			return errf(CodeInvalidEntryPrice, "entry.price",
				"建多仓的指定价 %s 高于现价 %s，post-only 单会被立即拒绝", p.Entry.Price, in.Mark)
		}
		if d.InitialPosition.IsNegative() && p.Entry.Price.LessThan(in.Mark) {
			return errf(CodeInvalidEntryPrice, "entry.price",
				"建空仓的指定价 %s 低于现价 %s，post-only 单会被立即拒绝", p.Entry.Price, in.Mark)
		}
	}
	return nil
}

func collectWarnings(p Params, m market.Market, g *Grid, d Derived, in PreviewInput) []Issue {
	var ws []Issue

	if !p.Risk.HasStopLoss() {
		if p.Grid.TrailingUp || p.Grid.TrailingDown {
			ws = append(ws, warnf(WarnTrailingNoStop, "risk.stop_loss_price",
				"开启了网格随价格移动但未设置止损价，追价过程中亏损可能持续放大"))
		} else {
			ws = append(ws, warnf(WarnNoStopLoss, "risk.stop_loss_price",
				"未设置止损价。区间外策略默认只是挂起等待回归，本身不构成保护"))
		}
	}
	if p.Leverage > highLeverageThreshold {
		ws = append(ws, warnf(WarnHighLeverage, "leverage",
			"%d 倍杠杆属于高杠杆，价格小幅逆向波动即可能强平", p.Leverage))
	}
	if liq := d.LiquidationPrice; liq.IsPositive() && g.Contains(liq) {
		ws = append(ws, warnf(WarnLiqInsideRange, "leverage",
			"估算强平价 %s 落在网格区间 [%s, %s] 之内，价格还没走到边界就会先爆仓",
			liq.StringFixed(2), d.Lower, d.Upper))
	}
	if !g.Contains(in.Mark) {
		ws = append(ws, warnf(WarnMarkOutOfRange, "grid.lower_price",
			"当前价 %s 不在区间 [%s, %s] 内，启动后会立即触发区间外策略",
			in.Mark, d.Lower, d.Upper))
	}
	if p.Direction == Neutral && !p.Grid.NeutralBaseRatio.IsPositive() &&
		p.Entry.Mode != strategy.EntryMakerFollow {
		ws = append(ws, warnf(WarnNeutralNoEntry, "entry.mode",
			"中性网格初始目标仓位为 0，不需要建仓，建仓方式配置不会生效"))
	}
	if d.MinProfitRate.LessThan(d.RoundTripFeeRate.Mul(decimal.NewFromInt(4))) {
		ws = append(ws, warnf(WarnThinProfit, "grid.grid_count",
			"最小单格毛利率 %s%% 仅为双边手续费的 %s 倍，滑点或费率变化会明显侵蚀收益",
			d.MinProfitRate.StringFixed(4),
			d.MinProfitRate.Div(d.RoundTripFeeRate).StringFixed(1)))
	}
	if d.OrderCount > 200 {
		ws = append(ws, warnf(WarnOrderCountLarge, "grid.grid_count",
			"需要挂 %d 单，铺满耗时较长且占用较多请求配额，可考虑设置最大同时挂单数", d.OrderCount))
	}
	return ws
}

// profitRateRange 返回所有格子毛利率的最小值与最大值（百分比）。
func profitRateRange(g *Grid) (minRate, maxRate decimal.Decimal) {
	for i := range g.Cells {
		c := &g.Cells[i]
		rate := c.Spread().Div(c.Low).Mul(hundred)
		if i == 0 || rate.LessThan(minRate) {
			minRate = rate
		}
		if i == 0 || rate.GreaterThan(maxRate) {
			maxRate = rate
		}
	}
	return minRate, maxRate
}

// pivotCell 返回现价所在的格子索引。
func pivotCell(g *Grid, mark decimal.Decimal) int {
	for i := range g.Cells {
		if mark.GreaterThanOrEqual(g.Cells[i].Low) && mark.LessThan(g.Cells[i].High) {
			return i
		}
	}
	if mark.LessThan(g.Lower()) {
		return 0
	}
	return len(g.Cells) - 1
}

// maxGridCountForFee 反推在手续费约束下最多能开多少格。
func maxGridCountForFee(p GridParams, m market.Market) int {
	threshold := m.RoundTripFeeRate().Mul(feeSafetyMultiple)
	if !threshold.IsPositive() {
		return p.GridCount
	}
	// 最小毛利率出现在最高价的格子，用上边界做保守估计：
	//   step / upper > threshold  =>  n < (upper - lower) / (upper * threshold)
	n := p.UpperPrice.Sub(p.LowerPrice).Div(p.UpperPrice.Mul(threshold)).IntPart()
	if n < 2 {
		return 2
	}
	return int(n)
}

// estimateLiquidation 粗略估算强平价，只用于风险提示。
//
// 假设整个网格的仓位都在现价一次性建立，这比实际情况保守（真实网格是
// 分批建仓，均价更有利），因此不会给出过于乐观的结论。
func estimateLiquidation(p Params, m market.Market, mark decimal.Decimal) decimal.Decimal {
	dir := position.Long
	switch p.Direction {
	case Short:
		dir = position.Short
	case Neutral:
		// 中性网格两侧都有风险，取更接近区间的一侧作为提示。
		liqLong := position.EstimateLiquidationPrice(mark, p.Leverage, m.MaintMarginRate, position.Long)
		liqShort := position.EstimateLiquidationPrice(mark, p.Leverage, m.MaintMarginRate, position.Short)
		if p.Grid.UpperPrice.Sub(mark).LessThan(mark.Sub(p.Grid.LowerPrice)) {
			return liqShort
		}
		return liqLong
	}
	return position.EstimateLiquidationPrice(mark, p.Leverage, m.MaintMarginRate, dir)
}
