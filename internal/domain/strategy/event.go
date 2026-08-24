// Package strategy 定义策略端口：事件、意图与策略接口。
//
// 策略是纯逻辑：接收事件，返回意图（Action），不持有交易所客户端、不做 IO、
// 不调用 time.Now()。时间一律由事件携带，这样测试可以完全复现。
package strategy

import (
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"

	"github.com/shopspring/decimal"
)

// Event 是策略的输入。所有事件都携带发生时间。
type Event interface {
	eventMarker()
	At() time.Time
}

// BookEvent 盘口最优买卖价变化。
type BookEvent struct {
	Book market.BookTicker
	// Mark 是标记价。交易所不提供时由适配器填成中间价。
	Mark decimal.Decimal
	Now  time.Time
}

// OrderEvent 订单状态变化：确认、部分成交、完全成交、撤销、拒绝。
type OrderEvent struct {
	Order order.Order
	Now   time.Time
}

// TradeEvent 是交易所逐笔成交。已实现盈亏已扣手续费，本事件只记账、不驱动挂单。
type TradeEvent struct {
	Trade order.Trade
	Now   time.Time
}

// PositionEvent 仓位与账户资金变化。
type PositionEvent struct {
	Position position.Position
	Account  account.Snapshot
	Now      time.Time
}

// TickEvent 秒级定时器，驱动跟价、超时判定与区间检查。
type TickEvent struct {
	Now time.Time
}

// EntryDoneEvent 由建仓触发器在建仓完成后发出。
type EntryDoneEvent struct {
	// Filled 是实际建立的仓位增量（带符号）。
	Filled decimal.Decimal
	Now    time.Time
}

// EntryFailedEvent 由建仓触发器在放弃建仓时发出。
type EntryFailedEvent struct {
	Filled decimal.Decimal
	Reason string
	Now    time.Time
}

// ResyncEvent 在事件流重连并完成对账后发出，携带对账结果。
type ResyncEvent struct {
	Position position.Position
	Account  account.Snapshot
	Orders   []order.Order
	Now      time.Time
}

func (BookEvent) eventMarker()        {}
func (OrderEvent) eventMarker()       {}
func (TradeEvent) eventMarker()       {}
func (PositionEvent) eventMarker()    {}
func (TickEvent) eventMarker()        {}
func (EntryDoneEvent) eventMarker()   {}
func (EntryFailedEvent) eventMarker() {}
func (ResyncEvent) eventMarker()      {}

func (e BookEvent) At() time.Time        { return e.Now }
func (e OrderEvent) At() time.Time       { return e.Now }
func (e TradeEvent) At() time.Time       { return e.Now }
func (e PositionEvent) At() time.Time    { return e.Now }
func (e TickEvent) At() time.Time        { return e.Now }
func (e EntryDoneEvent) At() time.Time   { return e.Now }
func (e EntryFailedEvent) At() time.Time { return e.Now }
func (e ResyncEvent) At() time.Time      { return e.Now }
