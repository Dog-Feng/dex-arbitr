// Package market 定义市场元数据与价格/数量的精度规整。
//
// 本包是纯领域代码：不做任何 IO，不依赖任何交易所 SDK。
package market

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// MarginMode 保证金模式。
type MarginMode uint8

const (
	MarginCross MarginMode = iota
	MarginIsolated
)

func (m MarginMode) String() string {
	switch m {
	case MarginCross:
		return "cross"
	case MarginIsolated:
		return "isolated"
	default:
		return "unknown"
	}
}

func ParseMarginMode(s string) (MarginMode, error) {
	switch s {
	case "cross":
		return MarginCross, nil
	case "isolated", "":
		return MarginIsolated, nil
	default:
		return 0, fmt.Errorf("unknown margin mode %q", s)
	}
}

// Rounding 规整方向。
type Rounding uint8

const (
	RoundNearest Rounding = iota
	RoundDown
	RoundUp
)

// Market 是交易对的元数据快照，由交易所适配器在启动时拉取后填充。
type Market struct {
	Symbol string

	// TickSize 最小价格变动，LotSize 最小数量变动。
	TickSize decimal.Decimal
	LotSize  decimal.Decimal

	MinQty      decimal.Decimal // 单笔最小数量
	MinNotional decimal.Decimal // 单笔最小名义价值（计价币）
	MaxLeverage int

	// PriceDecimals/SizeDecimals 是交易所定点整数表示所用的小数位，
	// 领域层不使用，由适配器在最后一步换算时读取。
	PriceDecimals int32
	SizeDecimals  int32

	MakerFeeRate decimal.Decimal // 例如 0.0002 表示 0.02%
	TakerFeeRate decimal.Decimal

	// MaintMarginRate 维持保证金率，用于估算强平价。
	MaintMarginRate decimal.Decimal
}

// Validate 检查元数据自身是否自洽。适配器填充完应当调用一次。
func (m Market) Validate() error {
	switch {
	case m.Symbol == "":
		return fmt.Errorf("market: empty symbol")
	case m.TickSize.LessThanOrEqual(decimal.Zero):
		return fmt.Errorf("market %s: tick size must be positive", m.Symbol)
	case m.LotSize.LessThanOrEqual(decimal.Zero):
		return fmt.Errorf("market %s: lot size must be positive", m.Symbol)
	case m.MaxLeverage < 1:
		return fmt.Errorf("market %s: max leverage must be >= 1", m.Symbol)
	case m.MinQty.IsNegative() || m.MinNotional.IsNegative():
		return fmt.Errorf("market %s: negative minimums", m.Symbol)
	}
	return nil
}

// RoundPrice 把价格规整到 TickSize 的整数倍。
func (m Market) RoundPrice(p decimal.Decimal, mode Rounding) decimal.Decimal {
	return roundTo(p, m.TickSize, mode)
}

// RoundQty 把数量向下规整到 LotSize 的整数倍。
//
// 数量一律向下取整：宁可少下一点，也不要因为向上取整导致保证金不足。
func (m Market) RoundQty(q decimal.Decimal) decimal.Decimal {
	return roundTo(q, m.LotSize, RoundDown)
}

// CheckOrder 校验规整后的价格与数量是否满足交易所的最小限制。
func (m Market) CheckOrder(price, qty decimal.Decimal) error {
	if qty.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("quantity %s must be positive", qty)
	}
	if price.LessThanOrEqual(decimal.Zero) {
		return fmt.Errorf("price %s must be positive", price)
	}
	if m.MinQty.IsPositive() && qty.LessThan(m.MinQty) {
		return fmt.Errorf("quantity %s below market minimum %s", qty, m.MinQty)
	}
	if notional := price.Mul(qty); m.MinNotional.IsPositive() && notional.LessThan(m.MinNotional) {
		return fmt.Errorf("notional %s below market minimum %s", notional, m.MinNotional)
	}
	return nil
}

// FeeFor 返回一笔成交的手续费（正数）。
func (m Market) FeeFor(price, qty decimal.Decimal, maker bool) decimal.Decimal {
	rate := m.TakerFeeRate
	if maker {
		rate = m.MakerFeeRate
	}
	return price.Mul(qty).Mul(rate).Abs()
}

// RoundTripFeeRate 是一买一卖两笔 maker 成交的合计费率，
// 网格单格毛利率必须显著高于它，否则每完成一格都在亏钱。
func (m Market) RoundTripFeeRate() decimal.Decimal {
	return m.MakerFeeRate.Mul(decimal.NewFromInt(2))
}

func roundTo(v, step decimal.Decimal, mode Rounding) decimal.Decimal {
	if step.LessThanOrEqual(decimal.Zero) {
		return v
	}
	q := v.Div(step)
	switch mode {
	case RoundDown:
		q = q.Floor()
	case RoundUp:
		q = q.Ceil()
	default:
		q = q.Round(0)
	}
	return q.Mul(step)
}
