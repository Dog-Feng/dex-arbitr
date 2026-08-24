// Package order 定义订单模型、状态机与客户端订单号编解码。
package order

import (
	"time"

	"github.com/shopspring/decimal"
)

// Side 买卖方向。
type Side uint8

const (
	Buy Side = iota
	Sell
)

func (s Side) String() string {
	if s == Buy {
		return "buy"
	}
	return "sell"
}

// Opposite 返回相反方向。
func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

// Sign 返回该方向对仓位的影响符号：买 +1，卖 -1。
func (s Side) Sign() decimal.Decimal {
	if s == Buy {
		return decimal.NewFromInt(1)
	}
	return decimal.NewFromInt(-1)
}

// Type 订单类型。
type Type uint8

const (
	Limit Type = iota
	Market
)

func (t Type) String() string {
	if t == Limit {
		return "limit"
	}
	return "market"
}

// TIF 订单有效期策略。
type TIF uint8

const (
	// GTC 挂单直到成交或过期。部分交易所（如 Lighter）没有永久单，
	// 适配器会用一个足够长的过期时间来实现。
	GTC TIF = iota
	// IOC 立即成交否则取消，用于市价单。
	IOC
	// PostOnly 只做 maker，会立即成交则被拒绝。网格挂单一律用这个。
	PostOnly
)

func (t TIF) String() string {
	switch t {
	case GTC:
		return "gtc"
	case IOC:
		return "ioc"
	case PostOnly:
		return "post_only"
	default:
		return "unknown"
	}
}

// State 订单状态。
type State uint8

const (
	StateNew             State = iota // 本地创建，尚未发送
	StatePending                      // 已发送，等待交易所确认
	StateOpen                         // 交易所已确认挂单
	StatePartiallyFilled              // 部分成交
	StateFilled                       // 完全成交
	StateCanceled                     // 已撤销
	StateRejected                     // 被拒绝
	StateExpired                      // 已过期
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StatePending:
		return "pending"
	case StateOpen:
		return "open"
	case StatePartiallyFilled:
		return "partially_filled"
	case StateFilled:
		return "filled"
	case StateCanceled:
		return "canceled"
	case StateRejected:
		return "rejected"
	case StateExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// IsTerminal 表示订单已经结束，不会再有后续更新。
func (s State) IsTerminal() bool {
	switch s {
	case StateFilled, StateCanceled, StateRejected, StateExpired:
		return true
	default:
		return false
	}
}

// IsActive 表示订单还挂在交易所上，占用挂单额度。
func (s State) IsActive() bool {
	switch s {
	case StatePending, StateOpen, StatePartiallyFilled:
		return true
	default:
		return false
	}
}

// Order 是一笔订单的完整状态。
type Order struct {
	ClientOrderID ClientOrderID
	ExchangeID    string
	Symbol        string

	Side  Side
	Type  Type
	TIF   TIF
	Price decimal.Decimal

	Quantity     decimal.Decimal
	FilledQty    decimal.Decimal
	AvgFillPrice decimal.Decimal
	Fee          decimal.Decimal
	IsMaker      bool

	ReduceOnly   bool
	State        State
	RejectReason string
	UpdatedAt    time.Time
}

// Remaining 返回未成交数量。
func (o Order) Remaining() decimal.Decimal {
	r := o.Quantity.Sub(o.FilledQty)
	if r.IsNegative() {
		return decimal.Zero
	}
	return r
}

// FilledNotional 返回已成交的名义价值。
func (o Order) FilledNotional() decimal.Decimal {
	return o.FillPrice().Mul(o.FilledQty)
}

// FillPrice 是这张单的成交均价。没有均价时退回挂单价。
func (o Order) FillPrice() decimal.Decimal {
	if o.AvgFillPrice.IsPositive() {
		return o.AvgFillPrice
	}
	return o.Price
}

// Trade 是交易所推送的一笔逐笔成交。
//
// RealizedPnL 用交易历史里的 ask/bid_account_pnl，已经扣除手续费。
// Fee 只作展示，不要再从 RealizedPnL 里减一次。
type Trade struct {
	ID            int64
	ClientOrderID ClientOrderID
	Symbol        string
	Side          Side
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	Fee           decimal.Decimal
	RealizedPnL   decimal.Decimal
	IsMaker       bool
	Time          time.Time
}
