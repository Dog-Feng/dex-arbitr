package lighter

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const userAgent = "dex-grid/0.1"

// 端点地址。
const (
	mainnetREST = "https://mainnet.zklighter.elliot.ai"
	testnetREST = "https://testnet.zklighter.elliot.ai"
	mainnetWS   = "wss://mainnet.zklighter.elliot.ai/stream"
	testnetWS   = "wss://testnet.zklighter.elliot.ai/stream"

	mainnetChainID = 304
	testnetChainID = 300
)

// --- 响应结构 ---

// orderBook 是 /orderBooks 返回的市场基础信息。
type orderBook struct {
	Symbol                 string `json:"symbol"`
	MarketID               int    `json:"market_id"`
	MarketType             string `json:"market_type"`
	Status                 string `json:"status"`
	TakerFee               num    `json:"taker_fee"`
	MakerFee               num    `json:"maker_fee"`
	MinBaseAmount          num    `json:"min_base_amount"`
	MinQuoteAmount         num    `json:"min_quote_amount"`
	SupportedSizeDecimals  int32  `json:"supported_size_decimals"`
	SupportedPriceDecimals int32  `json:"supported_price_decimals"`
}

type orderBooksResp struct {
	resultCode
	OrderBooks []orderBook `json:"order_books"`
}

// orderBookDetail 在基础信息之上补充了保证金率与行情。
type orderBookDetail struct {
	orderBook
	SizeDecimals  int32 `json:"size_decimals"`
	PriceDecimals int32 `json:"price_decimals"`
	// 以下三个保证金率的单位是万分之一：666 表示 6.66%。
	DefaultInitialMarginFraction int `json:"default_initial_margin_fraction"`
	MinInitialMarginFraction     int `json:"min_initial_margin_fraction"`
	MaintenanceMarginFraction    int `json:"maintenance_margin_fraction"`

	MarkPrice             num `json:"mark_price"`
	IndexPrice            num `json:"index_price"`
	LastTradePrice        num `json:"last_trade_price"`
	DailyQuoteTokenVolume num `json:"daily_quote_token_volume"`
	OpenInterest          num `json:"open_interest"`
}

type orderBookDetailsResp struct {
	resultCode
	OrderBookDetails []orderBookDetail `json:"order_book_details"`
}

// accountPosition 是账户下某个市场的仓位。
type accountPosition struct {
	MarketID int    `json:"market_id"`
	Symbol   string `json:"symbol"`
	// Sign 为 1 表示多头，-1 表示空头。Position 本身是绝对值。
	Sign             int `json:"sign"`
	Position         num `json:"position"`
	AvgEntryPrice    num `json:"avg_entry_price"`
	PositionValue    num `json:"position_value"`
	UnrealizedPnL    num `json:"unrealized_pnl"`
	RealizedPnL      num `json:"realized_pnl"`
	LiquidationPrice num `json:"liquidation_price"`
	MarginMode       int `json:"margin_mode"`
	AllocatedMargin  num `json:"allocated_margin"`
	OpenOrderCount   int `json:"open_order_count"`
	// InitialMarginFraction 这里是百分比字符串："5.00" 表示 5%，对应 20 倍杠杆。
	InitialMarginFraction num `json:"initial_margin_fraction"`
}

type accountInfo struct {
	AccountIndex     int64             `json:"account_index"`
	L1Address        string            `json:"l1_address"`
	Status           int               `json:"status"`
	Collateral       num               `json:"collateral"`
	AvailableBalance num               `json:"available_balance"`
	TotalAssetValue  num               `json:"total_asset_value"`
	CrossAssetValue  num               `json:"cross_asset_value"`
	TotalOrderCount  int               `json:"total_order_count"`
	PendingOrders    int               `json:"pending_order_count"`
	Positions        []accountPosition `json:"positions"`

	CrossInitialMarginRequirement     num `json:"cross_initial_margin_requirement"`
	CrossMaintenanceMarginRequirement num `json:"cross_maintenance_margin_requirement"`
}

type accountResp struct {
	resultCode
	Accounts []accountInfo `json:"accounts"`
}

// apiOrder 是 /accountActiveOrders 返回的挂单。
type apiOrder struct {
	OrderIndex          int64    `json:"order_index"`
	ClientOrderIndex    int64    `json:"client_order_index"`
	MarketIndex         int      `json:"market_index"`
	OwnerAccountIndex   int64    `json:"owner_account_index"`
	InitialBaseAmount   num      `json:"initial_base_amount"`
	RemainingBaseAmount num      `json:"remaining_base_amount"`
	FilledBaseAmount    num      `json:"filled_base_amount"`
	FilledQuoteAmount   num      `json:"filled_quote_amount"`
	Price               num      `json:"price"`
	IsAsk               flexBool `json:"is_ask"`
	Type                string   `json:"type"`
	TimeInForce         string   `json:"time_in_force"`
	ReduceOnly          flexBool `json:"reduce_only"`
	Status              string   `json:"status"`
	OrderExpiry         int64    `json:"order_expiry"`
	Timestamp           int64    `json:"timestamp"`
	UpdatedAt           int64    `json:"updated_at"`
}

type activeOrdersResp struct {
	resultCode
	Orders []apiOrder `json:"orders"`
}

// bookOrder 是订单簿上的一笔挂单。
//
// 这个端点返回的是逐笔挂单而不是按价格聚合的档位，所以第一条就是最优价，
// 但它的数量只是那一笔的量，不是该价位上的总量。
type bookOrder struct {
	OrderIndex          int64 `json:"order_index"`
	Price               num   `json:"price"`
	InitialBaseAmount   num   `json:"initial_base_amount"`
	RemainingBaseAmount num   `json:"remaining_base_amount"`
}

type orderBookOrdersResp struct {
	resultCode
	TotalAsks int         `json:"total_asks"`
	TotalBids int         `json:"total_bids"`
	Bids      []bookOrder `json:"bids"`
	Asks      []bookOrder `json:"asks"`
}

type sendTxResp struct {
	resultCode
	TxHash string `json:"tx_hash"`
	// PredictedExecutionTimeMs 是预计撮合时间，可用来判断拥堵。
	PredictedExecutionTimeMs int64 `json:"predicted_execution_time_ms"`
}

type nextNonceResp struct {
	resultCode
	Nonce int64 `json:"nonce"`
}

// --- 端点封装 ---

func (c *restClient) orderBooks(ctx context.Context) ([]orderBook, error) {
	var out orderBooksResp
	if err := c.get(ctx, "/api/v1/orderBooks", nil, &out); err != nil {
		return nil, err
	}
	return out.OrderBooks, nil
}

func (c *restClient) orderBookDetails(ctx context.Context, marketID int) ([]orderBookDetail, error) {
	params := url.Values{}
	if marketID >= 0 {
		params.Set("market_id", itoa(int64(marketID)))
	}
	var out orderBookDetailsResp
	if err := c.get(ctx, "/api/v1/orderBookDetails", params, &out); err != nil {
		return nil, err
	}
	return out.OrderBookDetails, nil
}

func (c *restClient) account(ctx context.Context, accountIndex int64) (*accountInfo, error) {
	params := url.Values{"by": {"index"}, "value": {itoa(accountIndex)}}
	var out accountResp
	if err := c.get(ctx, "/api/v1/account", params, &out); err != nil {
		return nil, err
	}
	if len(out.Accounts) == 0 {
		return nil, fmt.Errorf("lighter: 账户 %d 不存在", accountIndex)
	}
	return &out.Accounts[0], nil
}

func (c *restClient) activeOrders(ctx context.Context, accountIndex int64, marketID int, auth string) ([]apiOrder, error) {
	params := url.Values{
		"account_index": {itoa(accountIndex)},
		"market_id":     {itoa(int64(marketID))},
		"auth":          {auth},
	}
	var out activeOrdersResp
	if err := c.get(ctx, "/api/v1/accountActiveOrders", params, &out); err != nil {
		return nil, err
	}
	return out.Orders, nil
}

func (c *restClient) bookTop(ctx context.Context, marketID, depth int) (*orderBookOrdersResp, error) {
	params := url.Values{
		"market_id": {itoa(int64(marketID))},
		"limit":     {itoa(int64(depth))},
	}
	var out orderBookOrdersResp
	if err := c.get(ctx, "/api/v1/orderBookOrders", params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *restClient) nextNonce(ctx context.Context, accountIndex int64, apiKeyIndex uint8) (int64, error) {
	params := url.Values{
		"account_index": {itoa(accountIndex)},
		"api_key_index": {itoa(int64(apiKeyIndex))},
	}
	var out nextNonceResp
	if err := c.get(ctx, "/api/v1/nextNonce", params, &out); err != nil {
		return 0, err
	}
	return out.Nonce, nil
}

func (c *restClient) sendTx(ctx context.Context, txType uint8, txInfo string, priceProtection bool) (*sendTxResp, error) {
	form := url.Values{
		"tx_type":          {itoa(int64(txType))},
		"tx_info":          {txInfo},
		"price_protection": {boolStr(priceProtection)},
	}
	var out sendTxResp
	if err := c.postForm(ctx, "/api/v1/sendTx", form, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

type apiCandle struct {
	T int64 `json:"t"`
	O num   `json:"o"`
	H num   `json:"h"`
	L num   `json:"l"`
	C num   `json:"c"`
	V num   `json:"v"`
}

type candlesResp struct {
	resultCode
	Resolution string      `json:"r"`
	Candles    []apiCandle `json:"c"`
}

func (c *restClient) candles(ctx context.Context, marketID int, resolution string, start, end time.Time, countBack int) ([]apiCandle, error) {
	params := url.Values{
		"market_id":        {itoa(int64(marketID))},
		"resolution":       {resolution},
		"start_timestamp":  {itoa(start.UnixMilli())},
		"end_timestamp":    {itoa(end.UnixMilli())},
		"count_back":       {itoa(int64(countBack))},
	}
	var out candlesResp
	if err := c.get(ctx, "/api/v1/candles", params, &out); err != nil {
		return nil, err
	}
	return out.Candles, nil
}

func candleTime(t int64) time.Time {
	if t >= 1_000_000_000_000 {
		return time.UnixMilli(t).UTC()
	}
	return time.Unix(t, 0).UTC()
}

func normalizeResolution(interval string) (string, time.Duration, bool) {
	switch strings.ToLower(strings.TrimSpace(interval)) {
	case "", "1h", "60m", "60min":
		return "1h", time.Hour, true
	case "1m":
		return "1m", time.Minute, true
	case "5m":
		return "5m", 5 * time.Minute, true
	case "15m":
		return "15m", 15 * time.Minute, true
	case "30m":
		return "30m", 30 * time.Minute, true
	case "4h":
		return "4h", 4 * time.Hour, true
	case "12h":
		return "12h", 12 * time.Hour, true
	case "1d", "1D":
		return "1d", 24 * time.Hour, true
	default:
		return "", 0, false
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
