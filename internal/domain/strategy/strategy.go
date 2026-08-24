package strategy

import (
	"errors"
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"

	"github.com/shopspring/decimal"
)

// ErrUnsupportedCommand 表示策略不处理该命令，由 Runner 自行处理。
var ErrUnsupportedCommand = errors.New("strategy: unsupported command")

// State 是策略启动时拿到的只读上下文。
type State struct {
	Market   market.Market
	Position position.Position
	Account  account.Snapshot
	Book     market.BookTicker
	Mark     decimal.Decimal
	// Orders 是对账后确认属于本实例当前轮次的存活订单。
	Orders []order.Order
	// Epoch 是恢复运行时的轮次；全新启动传 0，策略会自增到 1。
	Epoch uint16
	// Slot 是交易所在注册表中的槽位，用于编码 ClientOrderID。
	Slot uint8
	Now  time.Time
}

// Strategy 是策略端口。所有实现必须是纯逻辑。
type Strategy interface {
	// Init 在启动时调用一次，返回设杠杆、建仓、铺网格等初始意图。
	Init(st State) ([]Action, error)

	// OnEvent 是运行时事件的唯一入口。新增事件类型不会破坏已有实现。
	OnEvent(ev Event) ([]Action, error)

	// OnCommand 处理页面下发的命令。不支持的命令返回 ErrUnsupportedCommand。
	OnCommand(cmd Command) ([]Action, error)

	// OnStop 返回收尾意图。
	OnStop(reason StopReason) ([]Action, error)

	// Snapshot/Restore 用于持久化，崩溃重启后恢复内部状态。
	Snapshot() ([]byte, error)
	Restore(data []byte) error

	// View 返回供页面展示的只读视图。
	View() View
}

// CommandKind 是页面命令类型。
type CommandKind uint8

const (
	CmdAdjustRange  CommandKind = iota + 1 // 调整区间（不停止网格）
	CmdCancelOrders                        // 撤销所有挂单（保留持仓）
	CmdRefill                              // 补齐网格挂单
	CmdResetStats                          // 重置统计
)

func (k CommandKind) String() string {
	switch k {
	case CmdAdjustRange:
		return "adjust_range"
	case CmdCancelOrders:
		return "cancel_orders"
	case CmdRefill:
		return "refill"
	case CmdResetStats:
		return "reset_stats"
	default:
		return "unknown"
	}
}

// Command 是一条页面命令。Payload 的具体类型由各策略定义。
type Command struct {
	Kind    CommandKind
	Payload any
	Mark    decimal.Decimal
	Now     time.Time
}

// Phase 是策略内部阶段，映射到页面上的运行状态。
type Phase uint8

const (
	PhaseIdle       Phase = iota // 未启动
	PhaseEntering                // 建仓中
	PhaseRunning                 // 网格运行中
	PhaseOutOfRange              // 价格超出区间，已挂起
	PhasePaused                  // 已撤单，等待补格
	PhaseStopped                 // 已停止
)

func (p Phase) String() string {
	switch p {
	case PhaseIdle:
		return "idle"
	case PhaseEntering:
		return "entering"
	case PhaseRunning:
		return "running"
	case PhaseOutOfRange:
		return "out_of_range"
	case PhasePaused:
		return "paused"
	default:
		return "stopped"
	}
}

// CellView 是单个网格格子的展示数据。
type CellView struct {
	Index int             `json:"index"`
	Low   decimal.Decimal `json:"low"`
	High  decimal.Decimal `json:"high"`
	Qty   decimal.Decimal `json:"qty"`
	// Side 是该格当前挂单的方向。
	Side string `json:"side"`
	// Price 是该格当前挂单的价格（Buy 挂 Low，Sell 挂 High）。
	Price decimal.Decimal `json:"price"`
	State string          `json:"state"`
}

// Stats 是策略的运行统计，对应页面「账户状态」中的盈亏与完成格数。
type Stats struct {
	Fills     int `json:"fills"`
	BuyFills  int `json:"buy_fills"`
	SellFills int `json:"sell_fills"`
	// PartialFills 是被撤销/过期时只成交了一部分的笔数。
	// 这类成交会造成仓位漂移，由对账兜底，数值持续增长说明需要关注。
	PartialFills int `json:"partial_fills"`
	// PendingTimeouts 是下单后迟迟收不到回报、被超时回收的笔数。
	// 偶发是正常的，持续增长说明回报链路有问题。
	PendingTimeouts int             `json:"pending_timeouts"`
	CompletedGrids  int             `json:"completed_grids"`
	GridProfit      decimal.Decimal `json:"grid_profit"` // 本轮已闭合循环的成交价差毛利
	FeePaid         decimal.Decimal `json:"fee_paid"`    // 本轮所有成交的手续费（含未闭合开腿，仅作统计）
	// CycleFee 是已闭合循环两腿的估算手续费，只作核对，不从页面已实现里再扣。
	CycleFee decimal.Decimal `json:"cycle_fee"`
	// RealizedPnL 是页面「已实现」：交易所成交给出非 0 净盈亏时用该值（DEX 已扣费）；
	// 否则用 GridProfit。RH 的 ask/bid_account_pnl 常为 0，不能再用估算费率二次扣减。
	RealizedPnL decimal.Decimal `json:"realized_pnl"`
	// VenueRealized 表示本轮已收到带非 0 已实现的成交，RealizedPnL 按交易历史累加。
	VenueRealized bool `json:"venue_realized,omitempty"`
	// ResetAt 是统计的起算时间，「重置统计」会更新它。
	ResetAt time.Time `json:"reset_at"`
}

// ForView 填好 realized_pnl。交易所给出非 0 净盈亏则直接用；否则用闭合循环价差，不再扣估算手续费。
func (s Stats) ForView() Stats {
	if s.VenueRealized {
		return s
	}
	s.RealizedPnL = s.GridProfit
	return s
}

// NoteVenueTrade 把一笔交易所成交的已实现（已扣费）计入本轮。重复 trade_id 或外人格子单忽略。
// 盈亏字段为 0 视为交易所未提供（RH 常见），页面改用闭合循环价差，不再用估算费率二次扣减。
func NoteVenueTrade(stats *Stats, seen map[int64]struct{}, slot uint8, epoch uint16, t order.Trade) {
	if stats == nil {
		return
	}
	if t.ID != 0 && seen != nil {
		if _, ok := seen[t.ID]; ok {
			return
		}
	}
	if !t.ClientOrderID.Valid() {
		return
	}
	ref := t.ClientOrderID.Decode()
	if ref.Slot != slot || (epoch > 0 && ref.Epoch != epoch) {
		return
	}
	if seen != nil && t.ID != 0 {
		seen[t.ID] = struct{}{}
	}
	if t.RealizedPnL.IsZero() {
		return
	}
	stats.VenueRealized = true
	stats.RealizedPnL = stats.RealizedPnL.Add(t.RealizedPnL)
}

// View 是策略暴露给页面的只读视图。
type View struct {
	Phase          Phase           `json:"phase"`
	Epoch          uint16          `json:"epoch"`
	Direction      string          `json:"direction"`
	LowerPrice     decimal.Decimal `json:"lower_price"`
	UpperPrice     decimal.Decimal `json:"upper_price"`
	GridCount      int             `json:"grid_count"`
	TargetPosition decimal.Decimal `json:"target_position"`
	// OrderTarget/OrderResting/OrderRetrying 对应页面的「挂单目标 / 已确认 / 待重试」。
	OrderTarget   int        `json:"order_target"`
	OrderResting  int        `json:"order_resting"`
	OrderRetrying int        `json:"order_retrying"`
	Cells         []CellView `json:"cells"`
	Stats         Stats      `json:"stats"`
}
