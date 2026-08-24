// Package martingale 实现合约马丁网格：首单建仓、按跌幅加仓、均价止盈。
//
// 本包是纯逻辑：不做 IO、不依赖交易所 SDK、不调用 time.Now()。
package martingale

import (
	"fmt"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// Name 是策略在注册表中的名称。
const Name = "martingale"

// Direction 只支持做多 / 做空，没有中性。
type Direction uint8

const (
	Long Direction = iota
	Short
)

func (d Direction) String() string {
	if d == Short {
		return "short"
	}
	return "long"
}

func ParseDirection(s string) (Direction, error) {
	switch s {
	case "long", "":
		return Long, nil
	case "short":
		return Short, nil
	default:
		return 0, fmt.Errorf("martingale 不支持方向 %q", s)
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

// AddDropMode 加仓间距基准。
type AddDropMode uint8

const (
	FromLast AddDropMode = iota
	FromAvg
)

func (m AddDropMode) String() string {
	if m == FromAvg {
		return "from_avg"
	}
	return "from_last"
}

func ParseAddDropMode(s string) (AddDropMode, error) {
	switch s {
	case "from_last", "":
		return FromLast, nil
	case "from_avg":
		return FromAvg, nil
	default:
		return 0, fmt.Errorf("unknown add_drop_mode %q", s)
	}
}

func (m AddDropMode) MarshalText() ([]byte, error) { return []byte(m.String()), nil }

func (m *AddDropMode) UnmarshalText(b []byte) error {
	v, err := ParseAddDropMode(string(b))
	if err != nil {
		return err
	}
	*m = v
	return nil
}

// LadderParams 是马丁专有参数。
type LadderParams struct {
	AddDropPct    decimal.Decimal `json:"add_drop_pct"`
	TakeProfitPct decimal.Decimal `json:"take_profit_pct"`
	InitialMargin decimal.Decimal `json:"initial_margin"`
	AddMargin     decimal.Decimal `json:"add_margin"`
	MaxAddTimes   int             `json:"max_add_times"`
	AddMultiplier decimal.Decimal `json:"add_multiplier"`
	AddDropMode   AddDropMode     `json:"add_drop_mode"`
	CycleRestart  *bool           `json:"cycle_restart,omitempty"`
	MaxCycles     int             `json:"max_cycles"`
	PreplaceAdds  *bool           `json:"preplace_adds,omitempty"`
}

func (p LadderParams) ShouldRestart() bool {
	return p.CycleRestart == nil || *p.CycleRestart
}

func (p LadderParams) ShouldPreplace() bool {
	return p.PreplaceAdds == nil || *p.PreplaceAdds
}

// Params 是马丁策略的完整参数。
type Params struct {
	Strategy   string            `json:"strategy"`
	Symbol     string            `json:"symbol"`
	Direction  Direction         `json:"direction"`
	Leverage   int               `json:"leverage"`
	MarginMode market.MarginMode `json:"margin_mode"`

	Martingale LadderParams         `json:"martingale"`
	Entry      strategy.EntryParams `json:"entry"`
	Risk       strategy.RiskParams  `json:"risk"`
	Order      strategy.OrderParams `json:"order"`
}

// DefaultParams 返回带默认值的参数。
func DefaultParams() Params {
	restart := true
	preplace := true
	return Params{
		Strategy:   Name,
		Direction:  Long,
		Leverage:   10,
		MarginMode: market.MarginCross,
		Martingale: LadderParams{
			AddDropPct:    decimal.RequireFromString("2"),
			TakeProfitPct: decimal.RequireFromString("1.5"),
			InitialMargin: decimal.RequireFromString("50"),
			AddMargin:     decimal.RequireFromString("50"),
			MaxAddTimes:   5,
			AddMultiplier: decimal.RequireFromString("1"),
			AddDropMode:   FromLast,
			CycleRestart:  &restart,
			PreplaceAdds:  &preplace,
		},
		Entry: strategy.DefaultEntryParams(),
		Risk:  strategy.DefaultRiskParams(),
		Order: strategy.DefaultOrderParams(),
	}
}

// ApplyDefaults 把零值字段填成默认值。
func (p *Params) ApplyDefaults() {
	d := DefaultParams()
	if p.Strategy == "" {
		p.Strategy = Name
	}
	if p.Leverage <= 0 {
		p.Leverage = d.Leverage
	}
	if p.MarginMode == 0 && p.Leverage == d.Leverage {
		p.MarginMode = d.MarginMode
	}
	m := &p.Martingale
	if m.AddDropPct.IsZero() {
		m.AddDropPct = d.Martingale.AddDropPct
	}
	if m.TakeProfitPct.IsZero() {
		m.TakeProfitPct = d.Martingale.TakeProfitPct
	}
	if m.InitialMargin.IsZero() {
		m.InitialMargin = d.Martingale.InitialMargin
	}
	if m.AddMargin.IsZero() {
		m.AddMargin = d.Martingale.AddMargin
	}
	if m.MaxAddTimes <= 0 {
		m.MaxAddTimes = d.Martingale.MaxAddTimes
	}
	if m.AddMultiplier.IsZero() {
		m.AddMultiplier = d.Martingale.AddMultiplier
	}
	if m.CycleRestart == nil {
		m.CycleRestart = d.Martingale.CycleRestart
	}
	if m.PreplaceAdds == nil {
		m.PreplaceAdds = d.Martingale.PreplaceAdds
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
