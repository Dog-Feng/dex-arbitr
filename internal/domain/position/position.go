// Package position 定义仓位模型与派生计算。
package position

import (
	"time"

	"dex-grid/internal/domain/market"

	"github.com/shopspring/decimal"
)

// Direction 仓位方向。
type Direction uint8

const (
	Flat Direction = iota
	Long
	Short
)

func (d Direction) String() string {
	switch d {
	case Long:
		return "long"
	case Short:
		return "short"
	default:
		return "flat"
	}
}

// Position 是某个交易对的仓位快照。
//
// Size 带符号：正数为多头，负数为空头，零为空仓。
type Position struct {
	Symbol           string            `json:"symbol"`
	Size             decimal.Decimal   `json:"size"`
	EntryPrice       decimal.Decimal   `json:"entry_price"`
	MarkPrice        decimal.Decimal   `json:"mark_price"`
	UnrealizedPnL    decimal.Decimal   `json:"unrealized_pnl"`
	LiquidationPrice decimal.Decimal   `json:"liquidation_price"`
	Leverage         int               `json:"leverage"`
	MarginMode       market.MarginMode `json:"margin_mode"`
	UpdatedAt        time.Time         `json:"updated_at"`
}

func (p Position) IsFlat() bool { return p.Size.IsZero() }

func (p Position) Direction() Direction {
	switch {
	case p.Size.IsPositive():
		return Long
	case p.Size.IsNegative():
		return Short
	default:
		return Flat
	}
}

// AbsSize 返回仓位绝对值。
func (p Position) AbsSize() decimal.Decimal { return p.Size.Abs() }

// Notional 返回以标记价计算的名义价值（正数）。
func (p Position) Notional() decimal.Decimal {
	price := p.MarkPrice
	if price.IsZero() {
		price = p.EntryPrice
	}
	return p.Size.Abs().Mul(price)
}

// EstimateLiquidationPrice 在交易所未提供强平价时做一个粗略估算。
//
// 用的是最简化的逐仓模型：忽略资金费、手续费与保证金余额中的未实现盈亏，
// 只考虑「价格变动吃掉初始保证金减去维持保证金」的那一刻。
// 结果只用于配置阶段的风险提示，不能用于实际风控决策。
func EstimateLiquidationPrice(entry decimal.Decimal, leverage int, maintMarginRate decimal.Decimal, dir Direction) decimal.Decimal {
	if entry.LessThanOrEqual(decimal.Zero) || leverage < 1 || dir == Flat {
		return decimal.Zero
	}
	imr := decimal.NewFromInt(1).Div(decimal.NewFromInt(int64(leverage)))
	drop := imr.Sub(maintMarginRate)
	if drop.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	if dir == Long {
		return entry.Mul(decimal.NewFromInt(1).Sub(drop))
	}
	return entry.Mul(decimal.NewFromInt(1).Add(drop))
}
