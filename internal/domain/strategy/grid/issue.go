package grid

import "fmt"

// 校验错误码。前端按 Code 做本地化与字段高亮，Message 是给人看的中文说明。
const (
	CodeInvalidRange        = "INVALID_RANGE"
	CodeInvalidGridCount    = "INVALID_GRID_COUNT"
	CodeRangeTooNarrow      = "RANGE_TOO_NARROW"
	CodeUnsupportedSpacing  = "UNSUPPORTED_SPACING"
	CodeInvalidLeverage     = "INVALID_LEVERAGE"
	CodeInvalidSizing       = "INVALID_SIZING"
	CodeQtyTooSmall         = "QTY_TOO_SMALL"
	CodeNotionalTooSmall    = "NOTIONAL_TOO_SMALL"
	CodeGridTooDense        = "GRID_TOO_DENSE"
	CodeInsufficientBalance = "INSUFFICIENT_BALANCE"
	CodeInvalidTPSL         = "INVALID_TP_SL"
	CodeTooManyOrders       = "TOO_MANY_ORDERS"
	CodeInvalidNeutralRatio = "INVALID_NEUTRAL_RATIO"
	CodeInvalidMakerTIF     = "INVALID_MAKER_TIF"
	CodeInvalidEntryPrice   = "INVALID_ENTRY_PRICE"
	CodeInvalidMarket       = "INVALID_MARKET"
	CodeNoMarkPrice         = "NO_MARK_PRICE"
)

// 警告码。不阻断保存，页面黄色高亮。
const (
	WarnNoStopLoss      = "NO_STOP_LOSS"
	WarnTrailingNoStop  = "TRAILING_WITHOUT_STOP_LOSS"
	WarnHighLeverage    = "HIGH_LEVERAGE"
	WarnLiqInsideRange  = "LIQUIDATION_INSIDE_RANGE"
	WarnThinProfit      = "THIN_PROFIT_MARGIN"
	WarnMarkOutOfRange  = "MARK_OUT_OF_RANGE"
	WarnNeutralNoEntry  = "NEUTRAL_ENTRY_IGNORED"
	WarnOrderCountLarge = "ORDER_COUNT_LARGE"
)

// Issue 是一条校验结果，既用作错误也用作警告。
type Issue struct {
	Code    string `json:"code"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

func (i *Issue) Error() string { return i.Message }

func errf(code, field, format string, args ...any) *Issue {
	return &Issue{Code: code, Field: field, Message: fmt.Sprintf(format, args...)}
}

func warnf(code, field, format string, args ...any) Issue {
	return Issue{Code: code, Field: field, Message: fmt.Sprintf(format, args...)}
}
