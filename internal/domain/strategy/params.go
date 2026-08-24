package strategy

import (
	"fmt"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

// 本文件是各策略共用的参数结构：建仓、风控、挂单行为。
// 网格与马丁的差异只在各自的专有参数里，这三段是一样的。

// EntryMode 建仓触发模式。
type EntryMode uint8

const (
	// EntryMakerFollow 挂在盘口最优价并跟随，默认。
	EntryMakerFollow EntryMode = iota
	// EntryMarket 已不再吃单：仍接受旧配置名，实际按 maker 跟价限价挂。
	EntryMarket
	// EntryLimitPrice 在指定价挂 post-only 单等待。
	EntryLimitPrice
)

func (m EntryMode) String() string {
	switch m {
	case EntryMarket:
		return "market"
	case EntryLimitPrice:
		return "limit_price"
	default:
		return "maker_follow"
	}
}

func ParseEntryMode(s string) (EntryMode, error) {
	switch s {
	case "maker_follow", "":
		return EntryMakerFollow, nil
	case "market":
		return EntryMarket, nil
	case "limit_price":
		return EntryLimitPrice, nil
	default:
		return 0, fmt.Errorf("unknown entry mode %q", s)
	}
}

func (m EntryMode) MarshalText() ([]byte, error) { return []byte(m.String()), nil }

func (m *EntryMode) UnmarshalText(b []byte) error {
	v, err := ParseEntryMode(string(b))
	if err != nil {
		return err
	}
	*m = v
	return nil
}

// TimeoutPolicy 建仓超时后的处理方式。
type TimeoutPolicy uint8

const (
	TimeoutMarket TimeoutPolicy = iota // 超时后市价 IOC 吃掉剩余量
	TimeoutKeep                        // 继续等待
	TimeoutAbort                       // 放弃建仓并停止实例
)

func (p TimeoutPolicy) String() string {
	switch p {
	case TimeoutKeep:
		return "keep"
	case TimeoutAbort:
		return "abort"
	default:
		return "market"
	}
}

func ParseTimeoutPolicy(s string) (TimeoutPolicy, error) {
	switch s {
	case "market", "":
		return TimeoutMarket, nil
	case "keep":
		return TimeoutKeep, nil
	case "abort":
		return TimeoutAbort, nil
	default:
		return 0, fmt.Errorf("unknown timeout policy %q", s)
	}
}

func (p TimeoutPolicy) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *TimeoutPolicy) UnmarshalText(b []byte) error {
	v, err := ParseTimeoutPolicy(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}

// OutOfRangePolicy 价格运行到区间之外后的行为。
type OutOfRangePolicy uint8

const (
	// OutOfRangePause 不撤单、不平仓、不下新单，挂起等待价格回归。
	OutOfRangePause OutOfRangePolicy = iota
	// OutOfRangeStopAndCancel 撤销全部挂单并停止，仓位保留交给用户处理。
	OutOfRangeStopAndCancel
)

func (p OutOfRangePolicy) String() string {
	if p == OutOfRangeStopAndCancel {
		return "stop_and_cancel"
	}
	return "pause"
}

func ParseOutOfRangePolicy(s string) (OutOfRangePolicy, error) {
	switch s {
	case "pause", "":
		return OutOfRangePause, nil
	case "stop_and_cancel":
		return OutOfRangeStopAndCancel, nil
	default:
		return 0, fmt.Errorf("unknown out-of-range policy %q", s)
	}
}

func (p OutOfRangePolicy) MarshalText() ([]byte, error) { return []byte(p.String()), nil }

func (p *OutOfRangePolicy) UnmarshalText(b []byte) error {
	v, err := ParseOutOfRangePolicy(string(b))
	if err != nil {
		return err
	}
	*p = v
	return nil
}

// EntryParams 建仓触发配置。
type EntryParams struct {
	Mode          EntryMode       `json:"mode"`
	Price         decimal.Decimal `json:"price,omitempty"` // 仅 limit_price 模式
	Timeout       Duration        `json:"timeout"`
	OnTimeout     TimeoutPolicy   `json:"on_timeout"`
	FillTolerance decimal.Decimal `json:"fill_tolerance"`

	// market 模式
	SliceCount    int             `json:"slice_count"`
	SliceInterval Duration        `json:"slice_interval"`
	MaxSlippage   decimal.Decimal `json:"max_slippage"`

	// maker_follow 模式
	DepthLevel      int      `json:"depth_level"`
	RepriceTicks    int      `json:"reprice_ticks"`
	RepriceInterval Duration `json:"reprice_interval"`
	MaxReprice      int      `json:"max_reprice"`
}

func DefaultEntryParams() EntryParams {
	return EntryParams{
		Mode:            EntryMakerFollow,
		Timeout:         MustParseDuration("5m"),
		OnTimeout:       TimeoutMarket,
		FillTolerance:   decimal.RequireFromString("0.01"),
		SliceCount:      1,
		SliceInterval:   MustParseDuration("1s"),
		MaxSlippage:     decimal.RequireFromString("0.005"),
		DepthLevel:      1,
		RepriceTicks:    1,
		RepriceInterval: MustParseDuration("500ms"),
		MaxReprice:      100,
	}
}

// RiskParams 风控配置。
type RiskParams struct {
	TakeProfitPrice decimal.Decimal `json:"take_profit_price,omitempty"`
	StopLossPrice   decimal.Decimal `json:"stop_loss_price,omitempty"`

	OutOfRange      OutOfRangePolicy `json:"out_of_range"`
	ExitBufferTicks int              `json:"exit_buffer_ticks"`
	ResumeConfirm   Duration         `json:"resume_confirm"`

	PriceSource market.PriceSource `json:"price_source"`
	CloseOnStop *bool              `json:"close_on_stop,omitempty"`

	MaxPositionNotional  decimal.Decimal `json:"max_position_notional"`
	MinMarginRatio       decimal.Decimal `json:"min_margin_ratio"`
	MaxConsecutiveErrors int             `json:"max_consecutive_errors"`
	StaleTimeout         Duration        `json:"stale_timeout"`
}

func DefaultRiskParams() RiskParams {
	closeOnStop := false
	return RiskParams{
		OutOfRange:           OutOfRangePause,
		ResumeConfirm:        MustParseDuration("5s"),
		PriceSource:          market.PriceMark,
		CloseOnStop:          &closeOnStop,
		MaxConsecutiveErrors: 10,
		StaleTimeout:         MustParseDuration("60s"),
	}
}

// ShouldCloseOnStop 已废弃：停止策略/关进程一律保留仓位。
// 仅止损由风控 Guard 市价吃单平仓；风控止盈仍 maker 跟价收到 0。
func (r RiskParams) ShouldCloseOnStop() bool {
	return r.CloseOnStop != nil && *r.CloseOnStop
}

// HasTakeProfit / HasStopLoss 判断是否配置了对应的触发价。
func (r RiskParams) HasTakeProfit() bool { return r.TakeProfitPrice.IsPositive() }
func (r RiskParams) HasStopLoss() bool   { return r.StopLossPrice.IsPositive() }

// OrderParams 挂单行为配置。
type OrderParams struct {
	// MakerTIF 只允许 post_only。保留为可配置项是为了让错误配置能被明确拒绝，
	// 而不是被静默接受成 taker 单。
	MakerTIF        order.TIF `json:"maker_tif"`
	PostOnlyRetry   int       `json:"post_only_retry"`
	ReduceOnlyClose *bool     `json:"reduce_only_close,omitempty"`
}

func DefaultOrderParams() OrderParams {
	reduceOnly := true
	return OrderParams{
		MakerTIF:        order.PostOnly,
		PostOnlyRetry:   3,
		ReduceOnlyClose: &reduceOnly,
	}
}

// ShouldReduceOnlyClose 返回平仓腿是否带 reduce-only 标记，默认为 true。
func (o OrderParams) ShouldReduceOnlyClose() bool {
	return o.ReduceOnlyClose == nil || *o.ReduceOnlyClose
}
