// Package grid 实现普通合约网格策略（做多 / 做空 / 中性）。
//
// 本包是纯逻辑：不做 IO、不依赖交易所 SDK、不调用 time.Now()。
package grid

import (
	"fmt"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// Direction 网格方向。
type Direction uint8

const (
	Neutral Direction = iota // 中性：上方挂卖开空、下方挂买开多，净仓位围绕 0 波动
	Long                     // 做多：低买高卖，需要初始多头底仓
	Short                    // 做空：高卖低买，需要初始空头底仓
)

func (d Direction) String() string {
	switch d {
	case Long:
		return "long"
	case Short:
		return "short"
	default:
		return "neutral"
	}
}

func ParseDirection(s string) (Direction, error) {
	switch s {
	case "neutral", "":
		return Neutral, nil
	case "long":
		return Long, nil
	case "short":
		return Short, nil
	default:
		return 0, fmt.Errorf("unknown grid direction %q", s)
	}
}

func (d Direction) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Direction) UnmarshalText(b []byte) error {
	v, err := ParseDirection(string(b))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

// SpacingMode 网格间距模式。
type SpacingMode uint8

const (
	Arithmetic SpacingMode = iota // 等差
	Geometric                     // 等比（第二阶段）
)

func (s SpacingMode) String() string {
	if s == Geometric {
		return "geometric"
	}
	return "arithmetic"
}

func ParseSpacingMode(s string) (SpacingMode, error) {
	switch s {
	case "arithmetic", "":
		return Arithmetic, nil
	case "geometric":
		return Geometric, nil
	default:
		return 0, fmt.Errorf("unknown spacing mode %q", s)
	}
}

func (s SpacingMode) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

func (s *SpacingMode) UnmarshalText(b []byte) error {
	v, err := ParseSpacingMode(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// SizingMode 决定用户输入的是每格数量还是总保证金。
type SizingMode uint8

const (
	// PerGridQty 用户填每格数量（币），保证金是派生量。页面默认。
	PerGridQty SizingMode = iota
	// MarginBased 用户填总保证金，每格数量是派生量。
	MarginBased
)

func (s SizingMode) String() string {
	if s == MarginBased {
		return "margin"
	}
	return "per_grid_qty"
}

func ParseSizingMode(s string) (SizingMode, error) {
	switch s {
	case "per_grid_qty", "":
		return PerGridQty, nil
	case "margin":
		return MarginBased, nil
	default:
		return 0, fmt.Errorf("unknown sizing mode %q", s)
	}
}

func (s SizingMode) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

func (s *SizingMode) UnmarshalText(b []byte) error {
	v, err := ParseSizingMode(string(b))
	if err != nil {
		return err
	}
	*s = v
	return nil
}

// QtyMode 在 MarginBased 下决定保证金如何分配到每格。
type QtyMode uint8

const (
	EqualNotional QtyMode = iota // 每格投入金额相同
	EqualQty                     // 每格数量相同
)

func (q QtyMode) String() string {
	if q == EqualQty {
		return "equal_qty"
	}
	return "equal_notional"
}

func ParseQtyMode(s string) (QtyMode, error) {
	switch s {
	case "equal_notional", "":
		return EqualNotional, nil
	case "equal_qty":
		return EqualQty, nil
	default:
		return 0, fmt.Errorf("unknown qty mode %q", s)
	}
}

func (q QtyMode) MarshalText() ([]byte, error) { return []byte(q.String()), nil }

func (q *QtyMode) UnmarshalText(b []byte) error {
	v, err := ParseQtyMode(string(b))
	if err != nil {
		return err
	}
	*q = v
	return nil
}

// GridParams 是网格本身的参数，对应控制台「策略配置」表单。
type GridParams struct {
	LowerPrice  decimal.Decimal `json:"lower_price"`
	UpperPrice  decimal.Decimal `json:"upper_price"`
	GridCount   int             `json:"grid_count"`
	SpacingMode SpacingMode     `json:"spacing_mode"`

	SizingMode SizingMode      `json:"sizing_mode"`
	PerGridQty decimal.Decimal `json:"per_grid_qty,omitempty"`
	Margin     decimal.Decimal `json:"margin,omitempty"`
	QtyMode    QtyMode         `json:"qty_mode"`

	// NeutralBaseRatio 仅中性网格生效：初始底仓占上方总量的比例。
	// 为 0（默认）时中性网格不需要建仓。
	NeutralBaseRatio decimal.Decimal `json:"neutral_base_ratio"`

	// MaxActiveOrders 限制同时挂出的订单数，只挂距现价最近的若干格。0 = 全挂。
	MaxActiveOrders int `json:"max_active_orders"`

	TrailingUp           bool              `json:"trailing_up"`
	TrailingDown         bool              `json:"trailing_down"`
	TrailingStepGrids    int               `json:"trailing_step_grids"`
	TrailingTriggerTicks int               `json:"trailing_trigger_ticks"`
	TrailingMaxShifts    int               `json:"trailing_max_shifts"`
	TrailingCooldown     strategy.Duration `json:"trailing_cooldown"`
}

// Params 是网格策略的完整参数，直接对应 API 的请求体与数据库中的 params 字段。
type Params struct {
	Symbol     string            `json:"symbol"`
	Direction  Direction         `json:"direction"`
	Leverage   int               `json:"leverage"`
	MarginMode market.MarginMode `json:"margin_mode"`
	// Preset 只影响「智能填充」的推荐值，不影响运行逻辑。
	Preset string `json:"preset,omitempty"`

	Grid  GridParams           `json:"grid"`
	Entry strategy.EntryParams `json:"entry"`
	Risk  strategy.RiskParams  `json:"risk"`
	Order strategy.OrderParams `json:"order"`
}

// DefaultParams 返回带默认值的参数，用于填充用户未提供的字段。
func DefaultParams() Params {
	return Params{
		Direction:  Neutral,
		MarginMode: market.MarginCross,
		Grid: GridParams{
			SpacingMode:       Arithmetic,
			SizingMode:        PerGridQty,
			QtyMode:           EqualNotional,
			TrailingStepGrids: 1,
			TrailingCooldown:  strategy.MustParseDuration("30s"),
		},
		Entry: strategy.DefaultEntryParams(),
		Risk:  strategy.DefaultRiskParams(),
		Order: strategy.DefaultOrderParams(),
	}
}

// ApplyDefaults 把零值字段填成默认值。反序列化后应当调用一次。
func (p *Params) ApplyDefaults() {
	d := DefaultParams()
	if p.MarginMode == 0 && p.Leverage == 0 {
		// 全新参数，整体套用默认
		p.MarginMode = d.MarginMode
	}
	if p.Grid.TrailingStepGrids <= 0 {
		p.Grid.TrailingStepGrids = d.Grid.TrailingStepGrids
	}
	if p.Grid.TrailingCooldown == 0 {
		p.Grid.TrailingCooldown = d.Grid.TrailingCooldown
	}
	if p.Entry.Timeout == 0 {
		p.Entry.Timeout = d.Entry.Timeout
	}
	if p.Entry.FillTolerance.IsZero() {
		p.Entry.FillTolerance = d.Entry.FillTolerance
	}
	if p.Entry.SliceCount <= 0 {
		p.Entry.SliceCount = d.Entry.SliceCount
	}
	if p.Entry.SliceInterval == 0 {
		p.Entry.SliceInterval = d.Entry.SliceInterval
	}
	if p.Entry.MaxSlippage.IsZero() {
		p.Entry.MaxSlippage = d.Entry.MaxSlippage
	}
	if p.Entry.DepthLevel <= 0 {
		p.Entry.DepthLevel = d.Entry.DepthLevel
	}
	if p.Entry.RepriceTicks <= 0 {
		p.Entry.RepriceTicks = d.Entry.RepriceTicks
	}
	if p.Entry.RepriceInterval == 0 {
		p.Entry.RepriceInterval = d.Entry.RepriceInterval
	}
	if p.Risk.ResumeConfirm == 0 {
		p.Risk.ResumeConfirm = d.Risk.ResumeConfirm
	}
	if p.Risk.MaxConsecutiveErrors <= 0 {
		p.Risk.MaxConsecutiveErrors = d.Risk.MaxConsecutiveErrors
	}
	if p.Risk.StaleTimeout == 0 {
		p.Risk.StaleTimeout = d.Risk.StaleTimeout
	}
	if p.Risk.CloseOnStop == nil {
		p.Risk.CloseOnStop = d.Risk.CloseOnStop
	}
	if p.Order.PostOnlyRetry <= 0 {
		p.Order.PostOnlyRetry = d.Order.PostOnlyRetry
	}
	if p.Order.ReduceOnlyClose == nil {
		p.Order.ReduceOnlyClose = d.Order.ReduceOnlyClose
	}
}

// isCloseLeg 判断某个方向的成交是不是平仓腿。
//
// 中性网格两侧都可能开仓，因此永远返回 false —— 给中性网格的挂单加
// reduce-only 会导致开仓单被直接拒绝。
func (p Params) isCloseLeg(side order.Side) bool {
	switch p.Direction {
	case Long:
		return side == order.Sell
	case Short:
		return side == order.Buy
	default:
		return false
	}
}

// reduceOnlyFor 返回某笔挂单是否应带 reduce-only 标记。
func (p Params) reduceOnlyFor(side order.Side) bool {
	return p.isCloseLeg(side) && p.Order.ShouldReduceOnlyClose()
}
