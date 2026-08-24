// Package account 定义账户资金快照。
package account

import (
	"time"

	"github.com/shopspring/decimal"
)

// Snapshot 是账户资金状态，对应控制台「账户状态」卡片中的资金部分。
type Snapshot struct {
	// Balance 账户余额（不含未实现盈亏）。
	Balance decimal.Decimal `json:"balance"`
	// Equity 权益 = Balance + UnrealizedPnL。
	Equity decimal.Decimal `json:"equity"`
	// Available 可用保证金，能用于开新仓的部分。
	Available decimal.Decimal `json:"available"`
	// MarginUsed 已占用保证金。
	MarginUsed decimal.Decimal `json:"margin_used"`

	UnrealizedPnL decimal.Decimal `json:"unrealized_pnl"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// MarginRatio 返回保证金率 = 权益 / 已占用保证金。
// 未占用保证金时返回零值，调用方需自行区分「无仓位」与「保证金率为 0」。
func (s Snapshot) MarginRatio() decimal.Decimal {
	if s.MarginUsed.LessThanOrEqual(decimal.Zero) {
		return decimal.Zero
	}
	return s.Equity.Div(s.MarginUsed)
}

// HasMargin 判断可用保证金是否足以覆盖 required。
func (s Snapshot) HasMargin(required decimal.Decimal) bool {
	return s.Available.GreaterThanOrEqual(required)
}
