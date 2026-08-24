package domain

import (
	"errors"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

type Direction string

const (
	DirectionLong    Direction = "long"
	DirectionShort   Direction = "short"
	DirectionNeutral Direction = "neutral"
)

type OrderSide string

const (
	OrderSideBuy  OrderSide = "buy"
	OrderSideSell OrderSide = "sell"
)

type OrderRole string

const (
	OrderRoleInitial    OrderRole = "initial"
	OrderRoleSafety     OrderRole = "safety"
	OrderRoleTakeProfit OrderRole = "take_profit"
	OrderRoleGrid       OrderRole = "grid"
	OrderRoleClose      OrderRole = "close"
)

type OrderType string

const (
	OrderTypeLimit  OrderType = "limit"
	OrderTypeMarket OrderType = "market"
)

type EntryMode string

const (
	EntryModeMarket     EntryMode = "market"
	EntryModeMaker      EntryMode = "maker"
	EntryModeLimitMaker EntryMode = "limit_maker"
)

func NormalizeEntryMode(value string, _ bool) EntryMode {
	mode := EntryMode(strings.ToLower(strings.TrimSpace(value)))
	if mode == "" {
		return EntryModeMarket
	}
	if mode == "limit" {
		// Treat the old generic limit spelling as the explicitly-priced Maker
		// mode so a specified entry price can never silently become a taker.
		return EntryModeLimitMaker
	}
	return mode
}

type OrderBookLevel struct {
	Price    decimal.Decimal `json:"price"`
	Quantity decimal.Decimal `json:"quantity"`
}

type OrderBook struct {
	Bids []OrderBookLevel `json:"bids"`
	Asks []OrderBookLevel `json:"asks"`
}

func (book OrderBook) BestPrice(side OrderSide) (decimal.Decimal, error) {
	levels := book.Asks
	best := decimal.Zero
	if side == OrderSideBuy {
		levels = book.Bids
	}
	for _, level := range levels {
		if !level.Price.GreaterThan(decimal.Zero) {
			continue
		}
		if best.IsZero() || (side == OrderSideBuy && level.Price.GreaterThan(best)) || (side == OrderSideSell && level.Price.LessThan(best)) {
			best = level.Price
		}
	}
	if !best.GreaterThan(decimal.Zero) {
		return decimal.Zero, errors.New("order book has no price for requested side")
	}
	return best, nil
}

func (book OrderBook) BestBid() (decimal.Decimal, error) { return book.BestPrice(OrderSideBuy) }
func (book OrderBook) BestAsk() (decimal.Decimal, error) { return book.BestPrice(OrderSideSell) }

type StrategyStatus string

const (
	StrategyIdle          StrategyStatus = "idle"
	StrategyStarting      StrategyStatus = "starting"
	StrategyRunning       StrategyStatus = "running"
	StrategyStopped       StrategyStatus = "stopped"
	StrategyEmergencyStop StrategyStatus = "emergency_stop"
)

type MarketSpec struct {
	MarketIndex    int             `json:"market_index"`
	Symbol         string          `json:"symbol"`
	PriceStep      decimal.Decimal `json:"price_step"`
	SizeStep       decimal.Decimal `json:"size_step"`
	MinBaseAmount  decimal.Decimal `json:"min_base_amount"`
	MinQuoteAmount decimal.Decimal `json:"min_quote_amount"`
	ContractMult   decimal.Decimal `json:"contract_multiplier"`
	MakerFeeRate   decimal.Decimal `json:"maker_fee_rate"`
	TakerFeeRate   decimal.Decimal `json:"taker_fee_rate"`
	FeeRateKnown   bool            `json:"fee_rate_known"`
}

type MarketDescriptor struct {
	DEX            string           `json:"dex"`
	MarketIndex    int              `json:"market_index"`
	Symbol         string           `json:"symbol"`
	ExchangeSymbol string           `json:"exchange_symbol"`
	BaseAsset      string           `json:"base_asset"`
	QuoteAsset     string           `json:"quote_asset"`
	MarketType     string           `json:"market_type"`
	PriceStep      decimal.Decimal  `json:"price_step"`
	SizeStep       decimal.Decimal  `json:"size_step"`
	MinBaseAmount  decimal.Decimal  `json:"min_base_amount"`
	MinQuoteAmount decimal.Decimal  `json:"min_quote_amount"`
	ContractMult   decimal.Decimal  `json:"contract_multiplier"`
	MakerFeeRate   decimal.Decimal  `json:"maker_fee_rate,omitempty"`
	TakerFeeRate   decimal.Decimal  `json:"taker_fee_rate,omitempty"`
	FeeRateKnown   bool             `json:"fee_rate_known,omitempty"`
	LastPrice      *decimal.Decimal `json:"last_price,omitempty"`
}

type PositionSnapshot struct {
	MarketIndex       int             `json:"market_index"`
	Symbol            string          `json:"symbol"`
	Side              string          `json:"side"`
	Quantity          decimal.Decimal `json:"quantity"`
	AverageEntryPrice decimal.Decimal `json:"average_entry_price"`
	PositionValue     decimal.Decimal `json:"position_value"`
	UnrealizedPnl     decimal.Decimal `json:"unrealized_pnl"`
	RealizedPnl       decimal.Decimal `json:"realized_pnl"`
	LiquidationPrice  decimal.Decimal `json:"liquidation_price"`
	AllocatedMargin   decimal.Decimal `json:"allocated_margin"`
	MarginMode        int             `json:"margin_mode"`
}

type AssetSnapshot struct {
	Symbol        string          `json:"symbol"`
	AssetID       int             `json:"asset_id"`
	Balance       decimal.Decimal `json:"balance"`
	LockedBalance decimal.Decimal `json:"locked_balance"`
	MarginBalance decimal.Decimal `json:"margin_balance"`
	MarginMode    string          `json:"margin_mode"`
}

type AccountSnapshot struct {
	AccountIndex                      int                `json:"account_index"`
	Status                            int                `json:"status"`
	Collateral                        decimal.Decimal    `json:"collateral"`
	AvailableBalance                  decimal.Decimal    `json:"available_balance"`
	TotalAssetValue                   decimal.Decimal    `json:"total_asset_value"`
	CrossAssetValue                   decimal.Decimal    `json:"cross_asset_value"`
	CrossInitialMarginRequirement     decimal.Decimal    `json:"cross_initial_margin_requirement"`
	CrossMaintenanceMarginRequirement decimal.Decimal    `json:"cross_maintenance_margin_requirement"`
	Positions                         []PositionSnapshot `json:"positions"`
	Assets                            []AssetSnapshot    `json:"assets"`
}

// StrategyConfig contains the shared strategy settings. The MartingaleConfig
// and NormalGridConfig aliases preserve the existing internal API while both
// strategy types can be selected through one Runtime entry point.
type StrategyConfig struct {
	DEX                 string           `json:"dex"`
	StrategyType        string           `json:"strategy_type"`
	EntryMode           EntryMode        `json:"entry_mode"`
	InitialPrice        *decimal.Decimal `json:"initial_price,omitempty"`
	MakerRequoteSeconds int              `json:"maker_requote_seconds"`
	MarketIndex         int              `json:"market_index"`
	Symbol              string           `json:"symbol"`
	Direction           Direction        `json:"direction"`
	Leverage            decimal.Decimal  `json:"leverage"`
	InitialMargin       decimal.Decimal  `json:"initial_margin"`
	SafetyMargin        decimal.Decimal  `json:"safety_margin"`
	PriceStepPct        decimal.Decimal  `json:"price_step_pct"`
	PriceStepMultiplier decimal.Decimal  `json:"price_step_multiplier"`
	AmountMultiplier    decimal.Decimal  `json:"amount_multiplier"`
	MaxSafetyOrders     int              `json:"max_safety_orders"`
	TakeProfitPct       decimal.Decimal  `json:"take_profit_pct"`
	MarketMaxSlippage   decimal.Decimal  `json:"market_max_slippage"`
	MarketMaxRetries    int              `json:"market_max_retries"`
	TakeProfitReference string           `json:"take_profit_reference"`
	ReinvestProfit      bool             `json:"reinvest_profit"`
	ReinvestRatio       decimal.Decimal  `json:"reinvest_ratio"`
	MaxCycleMargin      decimal.Decimal  `json:"max_cycle_margin"`
	MaxPositionQuantity decimal.Decimal  `json:"max_position_quantity"`
	MaxLossPct          decimal.Decimal  `json:"max_loss_pct"`
	StopLossPrice       *decimal.Decimal `json:"stop_loss_price,omitempty"`
	Continuous          bool             `json:"continuous"`
	GridLowerPrice      decimal.Decimal  `json:"grid_lower_price"`
	GridUpperPrice      decimal.Decimal  `json:"grid_upper_price"`
	GridCount           int              `json:"grid_count"`
	GridMargin          decimal.Decimal  `json:"grid_margin"`
	GridFollowUp        bool             `json:"grid_follow_up"`
	GridFollowDown      bool             `json:"grid_follow_down"`
	GridMode            string           `json:"grid_mode"`
}

type MartingaleConfig = StrategyConfig
type NormalGridConfig = StrategyConfig

type OrderIntent struct {
	ClientOrderID    string           `json:"client_order_id"`
	MarketIndex      int              `json:"market_index"`
	Symbol           string           `json:"symbol"`
	Side             OrderSide        `json:"side"`
	Role             OrderRole        `json:"role"`
	Level            int              `json:"level"`
	Price            decimal.Decimal  `json:"price"`
	Quantity         decimal.Decimal  `json:"quantity"`
	ReduceOnly       bool             `json:"reduce_only"`
	PostOnly         bool             `json:"post_only"`
	TriggerPrice     *decimal.Decimal `json:"trigger_price,omitempty"`
	OrderType        OrderType        `json:"order_type"`
	MaxSlippageRatio decimal.Decimal  `json:"max_slippage_ratio"`
}

type ExchangeOrder struct {
	ExchangeOrderID string          `json:"exchange_order_id"`
	ClientOrderID   string          `json:"client_order_id"`
	MarketIndex     int             `json:"market_index"`
	Side            OrderSide       `json:"side"`
	Role            OrderRole       `json:"role"`
	Level           int             `json:"level"`
	Price           decimal.Decimal `json:"price"`
	Quantity        decimal.Decimal `json:"quantity"`
	ReduceOnly      bool            `json:"reduce_only"`
	PostOnly        bool            `json:"post_only"`
	OrderType       OrderType       `json:"order_type"`
	Status          string          `json:"status"`
	// These fields are populated only in the strategy view for martingale
	// orders. They describe the projected position if this order fills in full.
	ProjectedAverageEntryPrice *decimal.Decimal `json:"projected_average_entry_price,omitempty"`
	ProjectedTakeProfitPrice   *decimal.Decimal `json:"projected_take_profit_price,omitempty"`
	ProjectedLiquidationPrice  *decimal.Decimal `json:"projected_liquidation_price,omitempty"`
}

type Fill struct {
	ExchangeOrderID string           `json:"exchange_order_id"`
	ClientOrderID   string           `json:"client_order_id"`
	MarketIndex     int              `json:"market_index"`
	Side            OrderSide        `json:"side"`
	Role            OrderRole        `json:"role"`
	Level           int              `json:"level"`
	Price           decimal.Decimal  `json:"price"`
	Quantity        decimal.Decimal  `json:"quantity"`
	TradeID         string           `json:"trade_id,omitempty"`
	Fee             *decimal.Decimal `json:"fee,omitempty"`
}

type CycleSnapshot struct {
	CycleID            int             `json:"cycle_id"`
	Status             string          `json:"status"`
	InitialPrice       decimal.Decimal `json:"initial_price"`
	TakeProfitPrice    decimal.Decimal `json:"take_profit_price"`
	PositionQuantity   decimal.Decimal `json:"position_quantity"`
	AverageEntryPrice  decimal.Decimal `json:"average_entry_price"`
	FilledSafetyOrders int             `json:"filled_safety_orders"`
	RealizedPnl        decimal.Decimal `json:"realized_pnl"`
	PlannedMargin      decimal.Decimal `json:"planned_margin"`
}

type GridSnapshot struct {
	Direction             Direction        `json:"direction"`
	LowerPrice            decimal.Decimal  `json:"lower_price"`
	UpperPrice            decimal.Decimal  `json:"upper_price"`
	GridCount             int              `json:"grid_count"`
	GridStep              decimal.Decimal  `json:"grid_step"`
	Margin                decimal.Decimal  `json:"margin"`
	FollowPriceUp         bool             `json:"follow_price_up"`
	FollowPriceDown       bool             `json:"follow_price_down"`
	Mode                  string           `json:"mode"`
	ExpectedProfitPerGrid *decimal.Decimal `json:"expected_profit_per_grid,omitempty"`
}

type StrategyView struct {
	StrategyType               string           `json:"strategy_type,omitempty"`
	Status                     StrategyStatus   `json:"status"`
	DEX                        string           `json:"dex,omitempty"`
	MarketIndex                int              `json:"market_index"`
	Symbol                     string           `json:"symbol,omitempty"`
	StartedAt                  *time.Time       `json:"started_at,omitempty"`
	DurationSeconds            int64            `json:"duration_seconds"`
	Cycle                      *CycleSnapshot   `json:"cycle,omitempty"`
	CumulativeRealizedPnl      decimal.Decimal  `json:"cumulative_realized_pnl"`
	CumulativeCyclePnl         decimal.Decimal  `json:"cumulative_cycle_pnl"`
	TradingFees                decimal.Decimal  `json:"trading_fees"`
	AnnualizedReturn           decimal.Decimal  `json:"annualized_return"`
	CompletedCycleCount        int              `json:"completed_cycle_count"`
	ActiveOrders               []ExchangeOrder  `json:"active_orders"`
	LastPrice                  *decimal.Decimal `json:"last_price,omitempty"`
	ProjectedAverageEntryPrice decimal.Decimal  `json:"projected_average_entry_price"`
	ProjectedLiquidationPrice  decimal.Decimal  `json:"projected_liquidation_price"`
	Grid                       *GridSnapshot    `json:"grid,omitempty"`
	Events                     []RuntimeEvent   `json:"events"`
}

type RuntimeEvent struct {
	Kind    string         `json:"kind"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data"`
}
