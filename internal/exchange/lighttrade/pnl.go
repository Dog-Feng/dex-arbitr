// Package lighttrade 解析 Lighter 协议（Core 与 RH 共用）成交里的本账户一侧。
//
// 已实现盈亏用交易所给出的 ask_account_pnl / bid_account_pnl，该值已经扣除手续费。
// 字段非 0 时策略层直接累加、不再按费率二次扣减；为 0 时视为未提供（RH 常见），
// 页面改用闭合循环价差，不再用估算费率二次扣减。
package lighttrade

import (
	"encoding/json"
	"strings"
	"time"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

// APITrade 是 account_market.trades 与 REST /trades 的成交结构（两家 DEX 字段相同）。
type APITrade struct {
	TradeID       int64 `json:"trade_id"`
	MarketID      int   `json:"market_id"`
	Size          num   `json:"size"`
	Price         num   `json:"price"`
	AskAccountID  int64 `json:"ask_account_id"`
	BidAccountID  int64 `json:"bid_account_id"`
	AskClientID   int64 `json:"ask_client_id"`
	BidClientID   int64 `json:"bid_client_id"`
	AskAccountPnL num   `json:"ask_account_pnl"`
	BidAccountPnL num   `json:"bid_account_pnl"`
	IsMakerAsk    bool  `json:"is_maker_ask"`
	MakerFee      num   `json:"maker_fee"`
	TakerFee      num   `json:"taker_fee"`
	Timestamp     int64 `json:"timestamp"`
}

// FillForAccount 取出本账户在这笔成交中的一侧。不是本账户的成交返回 ok=false。
func FillForAccount(t APITrade, accountIndex int64) (order.Trade, bool) {
	var (
		side  order.Side
		coid  int64
		pnl   decimal.Decimal
		maker bool
		fee   decimal.Decimal
	)
	switch {
	case t.AskAccountID == accountIndex:
		side = order.Sell
		coid = t.AskClientID
		pnl = t.AskAccountPnL.Decimal
		maker = t.IsMakerAsk
		if maker {
			fee = t.MakerFee.Decimal
		} else {
			fee = t.TakerFee.Decimal
		}
	case t.BidAccountID == accountIndex:
		side = order.Buy
		coid = t.BidClientID
		pnl = t.BidAccountPnL.Decimal
		maker = !t.IsMakerAsk
		if maker {
			fee = t.MakerFee.Decimal
		} else {
			fee = t.TakerFee.Decimal
		}
	default:
		return order.Trade{}, false
	}
	if fee.IsNegative() {
		fee = fee.Abs()
	}
	ts := tradeTime(t.Timestamp)
	return order.Trade{
		ID:            t.TradeID,
		ClientOrderID: order.ClientOrderID(coid),
		Side:          side,
		Price:         t.Price.Decimal,
		Quantity:      t.Size.Decimal,
		Fee:           fee,
		RealizedPnL:   pnl,
		IsMaker:       maker,
		Time:          ts,
	}, true
}

func tradeTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	if ts >= 1_000_000_000_000 {
		return time.UnixMilli(ts).UTC()
	}
	return time.Unix(ts, 0).UTC()
}

// num 同时接受 JSON 字符串和数字。
type num struct{ decimal.Decimal }

func (n *num) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		n.Decimal = decimal.Zero
		return nil
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return err
	}
	n.Decimal = v
	return nil
}

func (n num) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.Decimal.String())
}
