package lighter

import (
	"errors"
	"fmt"
	"strings"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"

	"github.com/elliottech/lighter-go/types/txtypes"

	"github.com/shopspring/decimal"
)

// errorsAs 是 errors.As 的薄封装，避免每个文件都 import errors。
func errorsAs(err error, target any) bool { return errors.As(err, target) }

// marginFractionTick 是 Lighter 保证金率的单位：万分之一。
// 例如 default_initial_margin_fraction = 666 表示 6.66%。
const marginFractionTick = 10000

// toMarket 把交易所的市场详情映射成领域模型。
func toMarket(d orderBookDetail) market.Market {
	priceDecimals := d.PriceDecimals
	if priceDecimals == 0 {
		priceDecimals = d.SupportedPriceDecimals
	}
	sizeDecimals := d.SizeDecimals
	if sizeDecimals == 0 {
		sizeDecimals = d.SupportedSizeDecimals
	}

	maxLeverage := 1
	if d.MinInitialMarginFraction > 0 {
		maxLeverage = marginFractionTick / d.MinInitialMarginFraction
	}

	m := market.Market{
		Symbol:        d.Symbol,
		TickSize:      tickFor(priceDecimals),
		LotSize:       tickFor(sizeDecimals),
		MinQty:        d.MinBaseAmount.Decimal,
		MinNotional:   d.MinQuoteAmount.Decimal,
		MaxLeverage:   maxLeverage,
		PriceDecimals: priceDecimals,
		SizeDecimals:  sizeDecimals,
		// 交易所把费率以百分数返回（"0.0200" 表示 0.02%），这里转成小数比率。
		MakerFeeRate:    d.MakerFee.Decimal.Div(decimal.NewFromInt(100)),
		TakerFeeRate:    d.TakerFee.Decimal.Div(decimal.NewFromInt(100)),
		MaintMarginRate: decimal.NewFromInt(int64(d.MaintenanceMarginFraction)).Div(decimal.NewFromInt(marginFractionTick)),
	}
	return m
}

// tickFor 把小数位数换算成最小变动单位：3 → 0.001。
func tickFor(decimals int32) decimal.Decimal {
	return decimal.New(1, -decimals)
}

// priceToInt 把价格换算成交易所的定点整数表示。
func priceToInt(p decimal.Decimal, decimals int32) (uint32, error) {
	scaled := p.Shift(decimals)
	if !scaled.Equal(scaled.Truncate(0)) {
		return 0, fmt.Errorf("价格 %s 不是最小报价单位的整数倍（%d 位小数）", p, decimals)
	}
	v := scaled.IntPart()
	if v < int64(txtypes.MinOrderPrice) {
		return 0, fmt.Errorf("价格 %s 换算后为 %d，低于交易所下限 %d", p, v, txtypes.MinOrderPrice)
	}
	if v > int64(txtypes.MaxOrderPrice) {
		return 0, fmt.Errorf("价格 %s 换算后为 %d，超过交易所上限 %d", p, v, txtypes.MaxOrderPrice)
	}
	return uint32(v), nil
}

// sizeToInt 把数量换算成交易所的定点整数表示。
func sizeToInt(q decimal.Decimal, decimals int32) (int64, error) {
	scaled := q.Shift(decimals)
	if !scaled.Equal(scaled.Truncate(0)) {
		return 0, fmt.Errorf("数量 %s 不是最小变动单位的整数倍（%d 位小数）", q, decimals)
	}
	v := scaled.IntPart()
	if v < txtypes.MinOrderBaseAmount {
		return 0, fmt.Errorf("数量 %s 换算后为 %d，低于交易所下限 %d", q, v, txtypes.MinOrderBaseAmount)
	}
	if v > txtypes.MaxOrderBaseAmount {
		return 0, fmt.Errorf("数量 %s 换算后为 %d，超过交易所上限 %d", q, v, txtypes.MaxOrderBaseAmount)
	}
	return v, nil
}

// isAsk 把领域方向翻译成 Lighter 的 is_ask 标记。
func isAsk(s order.Side) uint8 {
	if s == order.Sell {
		return 1
	}
	return 0
}

func sideFromAsk(ask bool) order.Side {
	if ask {
		return order.Sell
	}
	return order.Buy
}

func boolToUint8(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

// toPosition 把交易所仓位映射成领域模型。Sign 为 -1 时是空头。
func toPosition(p accountPosition, mark decimal.Decimal) position.Position {
	size := p.Position.Decimal
	if p.Sign < 0 {
		size = size.Neg()
	}
	leverage := 0
	if imf := p.InitialMarginFraction.Decimal; imf.IsPositive() {
		leverage = int(decimal.NewFromInt(100).Div(imf).IntPart())
	}
	mode := market.MarginCross
	if p.MarginMode == 1 {
		mode = market.MarginIsolated
	}
	return position.Position{
		Symbol:           p.Symbol,
		Size:             size,
		EntryPrice:       p.AvgEntryPrice.Decimal,
		MarkPrice:        mark,
		UnrealizedPnL:    p.UnrealizedPnL.Decimal,
		LiquidationPrice: p.LiquidationPrice.Decimal,
		Leverage:         leverage,
		MarginMode:       mode,
	}
}

// stateFromStatus 把 Lighter 的订单状态映射成领域状态，并在需要时保留原始状态文本。
//
// Lighter 用 "canceled-<原因>" 表达各种被动取消，最常见的是 post-only 单
// 会立即成交时的 "canceled-post-only"。这类状态必须映射成 Rejected 而不是
// Canceled：策略据此递增重挂计数，等价格移开再试。
//
// 未知状态一律按「订单已不在场上」处理。这个兜底方向是刻意选的：
// 反过来把未知状态当成 open 会留下一笔幽灵挂单，格子永远停在 Resting、
// 永远不补单，而且不会报错——这种静默的网格空洞比多下一笔单危险得多。
// 多下的单会被交易所以「重复客户端订单号」拒绝，对账也会兜住。
func stateFromStatus(status string, filled, total decimal.Decimal) (order.State, string) {
	switch status {
	case "open", "pending", "in-progress":
		if filled.IsPositive() && filled.LessThan(total) {
			return order.StatePartiallyFilled, ""
		}
		return order.StateOpen, ""
	case "filled":
		return order.StateFilled, ""
	case "expired", "canceled-expired":
		return order.StateExpired, status
	case "canceled", "cancelled":
		// 不带原因的取消是我们自己发起的。
		return order.StateCanceled, ""
	}
	if strings.HasPrefix(status, "canceled-") || strings.HasPrefix(status, "cancelled-") {
		// 带原因的取消是交易所主动拒绝，例如 canceled-post-only。
		return order.StateRejected, status
	}
	return order.StateCanceled, "未知订单状态: " + status
}

// toOrder 把交易所挂单映射成领域模型。
func toOrder(o apiOrder, symbol string) order.Order {
	filled := o.FilledBaseAmount.Decimal
	total := o.InitialBaseAmount.Decimal
	state, reason := stateFromStatus(o.Status, filled, total)

	avg := decimal.Zero
	if filled.IsPositive() && o.FilledQuoteAmount.Decimal.IsPositive() {
		avg = o.FilledQuoteAmount.Decimal.Div(filled)
	}

	return order.Order{
		ClientOrderID: order.ClientOrderID(o.ClientOrderIndex),
		ExchangeID:    itoa(o.OrderIndex),
		Symbol:        symbol,
		Side:          sideFromAsk(o.IsAsk.Bool()),
		Type:          order.Limit,
		TIF:           tifFromString(o.TimeInForce),
		Price:         o.Price.Decimal,
		Quantity:      total,
		FilledQty:     filled,
		AvgFillPrice:  avg,
		ReduceOnly:    o.ReduceOnly.Bool(),
		State:         state,
		RejectReason:  reason,
	}
}

func tifFromString(s string) order.TIF {
	switch s {
	case "post-only", "post_only":
		return order.PostOnly
	case "immediate-or-cancel", "ioc":
		return order.IOC
	default:
		return order.GTC
	}
}

// toTIF 把领域的有效期策略翻译成 Lighter 常量。
func toTIF(t order.TIF) (uint8, error) {
	switch t {
	case order.PostOnly:
		return txtypes.PostOnly, nil
	case order.IOC:
		return txtypes.ImmediateOrCancel, nil
	case order.GTC:
		return txtypes.GoodTillTime, nil
	default:
		return 0, fmt.Errorf("不支持的有效期策略 %s", t)
	}
}

// marginModeToUint8 把保证金模式翻译成 Lighter 常量。
func marginModeToUint8(m market.MarginMode) uint8 {
	if m == market.MarginIsolated {
		return txtypes.IsolatedMargin
	}
	return txtypes.CrossMargin
}
