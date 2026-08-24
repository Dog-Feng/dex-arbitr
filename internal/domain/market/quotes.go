package market

import (
	"time"

	"github.com/shopspring/decimal"
)

// BookTicker 是订单簿最优买卖价快照。
type BookTicker struct {
	Bid     decimal.Decimal
	Ask     decimal.Decimal
	BidSize decimal.Decimal
	AskSize decimal.Decimal
	Time    time.Time
}

func (b BookTicker) Valid() bool {
	return b.Bid.IsPositive() && b.Ask.IsPositive() && b.Ask.GreaterThanOrEqual(b.Bid)
}

// Mid 返回中间价。盘口无效时返回零值，调用方需先 Valid()。
func (b BookTicker) Mid() decimal.Decimal {
	if !b.Valid() {
		return decimal.Zero
	}
	return b.Bid.Add(b.Ask).Div(decimal.NewFromInt(2))
}

// Spread 返回买卖价差。
func (b BookTicker) Spread() decimal.Decimal {
	if !b.Valid() {
		return decimal.Zero
	}
	return b.Ask.Sub(b.Bid)
}

// PriceSource 决定风控与网格判定使用哪个价格。
type PriceSource uint8

const (
	PriceMark PriceSource = iota // 标记价，抗插针，默认
	PriceMid                     // 盘口中间价
	PriceLast                    // 最新成交价
)

func (p PriceSource) String() string {
	switch p {
	case PriceMark:
		return "mark"
	case PriceMid:
		return "mid"
	case PriceLast:
		return "last"
	default:
		return "unknown"
	}
}

func ParsePriceSource(s string) (PriceSource, error) {
	switch s {
	case "mark", "":
		return PriceMark, nil
	case "mid":
		return PriceMid, nil
	case "last":
		return PriceLast, nil
	default:
		return 0, errUnknownPriceSource(s)
	}
}

type errUnknownPriceSource string

func (e errUnknownPriceSource) Error() string {
	return "unknown price source " + string(e)
}

// Kline 是一根 K 线，供行情分析与价格曲线使用。
type Kline struct {
	OpenTime time.Time       `json:"open_time"`
	Open     decimal.Decimal `json:"open"`
	High     decimal.Decimal `json:"high"`
	Low      decimal.Decimal `json:"low"`
	Close    decimal.Decimal `json:"close"`
	Volume   decimal.Decimal `json:"volume"`
}
