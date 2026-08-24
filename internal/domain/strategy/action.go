package strategy

import (
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

// Action 是策略的输出：一个待执行的意图。
//
// 策略只描述「想做什么」，由应用层的 Executor 翻译成交易所调用，
// 并负责批量合并、限流与重试。
type Action interface{ actionMarker() }

// PlaceOrder 下单。
type PlaceOrder struct {
	ClientOrderID order.ClientOrderID
	Side          order.Side
	Type          order.Type
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	TIF           order.TIF
	ReduceOnly    bool
}

// ModifyOrder 改挂单价和数量，不撤单重挂。ClientOrderID 必须是已在交易所存活的订单。
type ModifyOrder struct {
	ClientOrderID order.ClientOrderID
	Side          order.Side
	Type          order.Type
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	TIF           order.TIF
	ReduceOnly    bool
}

// CancelOrder 撤销指定订单。
type CancelOrder struct {
	ClientOrderID order.ClientOrderID
}

// CancelAll 撤销本实例的全部挂单。
type CancelAll struct{}

// SetLeverage 设置杠杆与保证金模式。
type SetLeverage struct {
	Leverage int
	Mode     market.MarginMode
}

// EnsurePosition 要求把仓位调整到 Target（带符号的绝对目标，不是增量）。
//
// 具体怎么调整由应用层的建仓触发器决定（maker 跟价 / 指定价格；旧 market 配置同样走 maker），
// 完成后回发 EntryDoneEvent。策略不关心过程。
type EnsurePosition struct {
	Target decimal.Decimal
}

// Urgency 决定平仓的紧迫程度。
type Urgency uint8

const (
	// UrgencyMarket 市价 IOC 吃单平仓，用于止损。
	UrgencyMarket Urgency = iota
	// UrgencyMaker 挂买一/卖一 post-only 平仓。
	UrgencyMaker
)

// ClosePosition 平掉全部仓位。
type ClosePosition struct {
	Urgency Urgency
}

// Stop 请求停止实例。
type Stop struct {
	Reason StopReason
}

func (PlaceOrder) actionMarker()     {}
func (ModifyOrder) actionMarker()    {}
func (CancelOrder) actionMarker()    {}
func (CancelAll) actionMarker()      {}
func (SetLeverage) actionMarker()    {}
func (EnsurePosition) actionMarker() {}
func (ClosePosition) actionMarker()  {}
func (Stop) actionMarker()           {}

// StopReason 说明实例为什么停止。它决定了是否已平仓，页面必须区分展示。
type StopReason uint8

const (
	StopManual      StopReason = iota // 用户手动停止，撤单保留仓位
	StopTakeProfit                    // 止盈触发，已平仓
	StopStopLoss                      // 止损触发，已平仓
	StopOutOfRange                    // 区间外策略触发，撤单但保留仓位
	StopCircuit                       // 连续失败熔断，撤单但保留仓位
	StopEntryFailed                   // 建仓失败，撤单保留仓位
	StopShutdown                      // 进程退出，撤单保留仓位
	StopError                         // 内部错误，撤单保留仓位
)

func (r StopReason) String() string {
	switch r {
	case StopManual:
		return "manual"
	case StopTakeProfit:
		return "take_profit"
	case StopStopLoss:
		return "stop_loss"
	case StopOutOfRange:
		return "out_of_range"
	case StopCircuit:
		return "circuit"
	case StopEntryFailed:
		return "entry_failed"
	case StopShutdown:
		return "shutdown"
	default:
		return "error"
	}
}

// ClosesPosition 表示该停止原因下会主动平仓。
// 止损市价吃单；止盈 maker 跟价。手动停止、关进程、熔断、错误一律只撤单留仓。
func (r StopReason) ClosesPosition() bool {
	switch r {
	case StopTakeProfit, StopStopLoss:
		return true
	default:
		return false
	}
}
