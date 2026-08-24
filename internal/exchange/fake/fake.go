// Package fake 是内存撮合器，供 app 层单测与端到端测试使用。
//
// 它实现 exchange.StreamingExchange：限价挂单、价格驱动撮合、post-only
// 穿价拒绝、市价立即成交、错误注入。不连网，时间由调用方推进。
package fake

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/exchange"

	"github.com/shopspring/decimal"
)

// Exchange 是一个可注入故障的内存交易所。
type Exchange struct {
	name string
	mkt  market.Market
	caps exchange.Capabilities

	mu     sync.Mutex
	book   market.BookTicker
	mark   decimal.Decimal
	last   decimal.Decimal
	index  decimal.Decimal
	pos    position.Position
	acct   account.Snapshot
	lev    int
	mode   market.MarginMode
	orders map[order.ClientOrderID]*order.Order
	nextID int64

	subs []chan exchange.StreamEvent

	// 错误注入：下一次 PlaceOrders 整批失败，用完即清。
	nextPlaceErr error
	// failNext 让接下来 N 次 PlaceOrders 返回可重试错误。
	failNext int
	// placeErrs 按客户端订单号注入单笔错误。
	placeErrs map[order.ClientOrderID]error

	closed atomic.Bool
}

var _ exchange.StreamingExchange = (*Exchange)(nil)

// New 构造一个只包含单个永续市场的内存交易所。
func New(mkt market.Market) *Exchange {
	if mkt.Symbol == "" {
		mkt.Symbol = "BTC"
	}
	ex := &Exchange{
		name:      "fake",
		mkt:       mkt,
		caps:      exchange.Capabilities{PostOnly: true, ReduceOnly: true, ModifyOrder: true},
		orders:    map[order.ClientOrderID]*order.Order{},
		placeErrs: map[order.ClientOrderID]error{},
		lev:       1,
		acct: account.Snapshot{
			Balance:   decimal.RequireFromString("100000"),
			Equity:    decimal.RequireFromString("100000"),
			Available: decimal.RequireFromString("100000"),
		},
		pos: position.Position{Symbol: mkt.Symbol},
	}
	return ex
}

func (e *Exchange) Name() string                        { return e.name }
func (e *Exchange) Capabilities() exchange.Capabilities { return e.caps }

// SetCapabilities 覆盖能力声明，用于测试批量拆分等路径。
func (e *Exchange) SetCapabilities(c exchange.Capabilities) { e.caps = c }

func (e *Exchange) Markets(context.Context) ([]exchange.MarketInfo, error) {
	return []exchange.MarketInfo{{
		Symbol:      e.mkt.Symbol,
		Type:        "perp",
		Status:      "active",
		MarkPrice:   e.snapshotMark(),
		MaxLeverage: e.mkt.MaxLeverage,
	}}, nil
}

func (e *Exchange) Market(_ context.Context, symbol string) (market.Market, error) {
	if symbol != e.mkt.Symbol {
		return market.Market{}, fmt.Errorf("fake: unknown perpetual %s", symbol)
	}
	return e.mkt, nil
}

func (e *Exchange) Ticker(context.Context, string) (exchange.Ticker, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.tickerLocked(), nil
}

func (e *Exchange) Klines(_ context.Context, _ string, _ string, limit int) ([]market.Kline, error) {
	if limit <= 0 {
		limit = 24
	}
	e.mu.Lock()
	mark := e.markOrMidLocked()
	e.mu.Unlock()
	if !mark.IsPositive() {
		mark = decimal.RequireFromString("100")
	}
	now := time.Now().UTC().Truncate(time.Hour)
	step := mark.Mul(decimal.RequireFromString("0.002"))
	out := make([]market.Kline, limit)
	for i := 0; i < limit; i++ {
		t := now.Add(-time.Duration(limit-1-i) * time.Hour)
		off := decimal.NewFromInt(int64(i%7 - 3)).Mul(step)
		px := mark.Add(off)
		out[i] = market.Kline{
			OpenTime: t,
			Open:     px,
			High:     px.Add(step),
			Low:      px.Sub(step),
			Close:    px,
			Volume:   decimal.NewFromInt(1),
		}
	}
	return out, nil
}

func (e *Exchange) SetLeverage(_ context.Context, _ string, leverage int, mode market.MarginMode) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lev = leverage
	e.mode = mode
	e.pos.Leverage = leverage
	e.pos.MarginMode = mode
	return nil
}

func (e *Exchange) Leverage() (int, market.MarginMode) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lev, e.mode
}

func (e *Exchange) PlaceOrders(_ context.Context, reqs []exchange.PlaceRequest) ([]exchange.PlaceResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.nextPlaceErr; err != nil {
		e.nextPlaceErr = nil
		return nil, err
	}
	if e.failNext > 0 {
		e.failNext--
		return nil, exchange.Classify(exchange.ClassRetryable, "place_order", fmt.Errorf("fake: injected retryable"))
	}

	out := make([]exchange.PlaceResult, len(reqs))
	now := time.Now().UTC()
	for i, req := range reqs {
		out[i] = exchange.PlaceResult{ClientOrderID: req.ClientOrderID}
		if err, ok := e.placeErrs[req.ClientOrderID]; ok {
			delete(e.placeErrs, req.ClientOrderID)
			out[i].Err = err
			continue
		}
		o, evs, err := e.acceptLocked(req, now)
		if err != nil {
			out[i].Err = err
			continue
		}
		out[i].ExchangeID = o.ExchangeID
		out[i].TxHash = "fake-" + o.ExchangeID
		e.emitLocked(evs...)
	}
	return out, nil
}

func (e *Exchange) ModifyOrders(_ context.Context, reqs []exchange.ModifyRequest) ([]exchange.ModifyResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	out := make([]exchange.ModifyResult, len(reqs))
	for i, req := range reqs {
		out[i] = exchange.ModifyResult{ClientOrderID: req.ClientOrderID}
		o, ok := e.orders[req.ClientOrderID]
		if !ok {
			out[i].Err = exchange.Classify(exchange.ClassInvalidParam, "modify_order",
				fmt.Errorf("fake: order %d not found", req.ClientOrderID))
			continue
		}
		price := e.mkt.RoundPrice(req.Price, market.RoundNearest)
		qty := e.mkt.RoundQty(req.Quantity)
		if err := e.mkt.CheckOrder(price, qty); err != nil {
			out[i].Err = exchange.Classify(exchange.ClassInvalidParam, "modify_order", err)
			continue
		}
		if o.FilledQty.IsPositive() && qty.LessThan(o.FilledQty) {
			out[i].Err = exchange.Classify(exchange.ClassInvalidParam, "modify_order",
				fmt.Errorf("fake: new qty %s below filled %s", qty, o.FilledQty))
			continue
		}
		if o.TIF == order.PostOnly && e.wouldCrossLocked(o.Side, price) {
			out[i].Err = exchange.Classify(exchange.ClassInvalidParam, "modify_order",
				fmt.Errorf("fake: post-only modify would cross"))
			continue
		}
		o.Price = price
		o.Quantity = qty
		o.UpdatedAt = now
		cp := *o
		e.emitLocked(exchange.StreamEvent{Order: &cp, Time: now})
		out[i].TxHash = "fake-mod-" + o.ExchangeID
	}
	return out, nil
}

func (e *Exchange) CancelOrders(_ context.Context, reqs []exchange.CancelRequest) ([]exchange.CancelResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	out := make([]exchange.CancelResult, len(reqs))
	for i, req := range reqs {
		out[i] = exchange.CancelResult{ClientOrderID: req.ClientOrderID}
		o, ok := e.orders[req.ClientOrderID]
		if !ok {
			out[i].Err = exchange.Classify(exchange.ClassInvalidParam, "cancel_order",
				fmt.Errorf("fake: order %d not found", req.ClientOrderID))
			continue
		}
		o.State = order.StateCanceled
		o.UpdatedAt = now
		cp := *o
		delete(e.orders, req.ClientOrderID)
		e.emitLocked(exchange.StreamEvent{Order: &cp, Time: now})
	}
	return out, nil
}

func (e *Exchange) CancelAll(_ context.Context, symbol string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	for id, o := range e.orders {
		if symbol != "" && o.Symbol != symbol {
			continue
		}
		o.State = order.StateCanceled
		o.UpdatedAt = now
		cp := *o
		e.emitLocked(exchange.StreamEvent{Order: &cp, Time: now})
		delete(e.orders, id)
	}
	return nil
}

func (e *Exchange) OpenOrders(context.Context, string) ([]order.Order, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]order.Order, 0, len(e.orders))
	for _, o := range e.orders {
		if o.State.IsActive() {
			out = append(out, *o)
		}
	}
	return out, nil
}

func (e *Exchange) Position(context.Context, string) (position.Position, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p := e.pos
	p.MarkPrice = e.markOrMidLocked()
	return p, nil
}

func (e *Exchange) Account(context.Context) (account.Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.acct, nil
}

func (e *Exchange) Subscribe(ctx context.Context, _ string) (<-chan exchange.StreamEvent, error) {
	ch := make(chan exchange.StreamEvent, 64)
	e.mu.Lock()
	e.subs = append(e.subs, ch)
	tick := e.tickerLocked()
	e.mu.Unlock()

	ch <- exchange.StreamEvent{Ticker: &tick, Time: tick.Time}

	go func() {
		<-ctx.Done()
		e.mu.Lock()
		defer e.mu.Unlock()
		for i, s := range e.subs {
			if s == ch {
				e.subs = append(e.subs[:i], e.subs[i+1:]...)
				close(ch)
				return
			}
		}
	}()
	return ch, nil
}

func (e *Exchange) Close() error {
	if e.closed.Swap(true) {
		return nil
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ch := range e.subs {
		close(ch)
	}
	e.subs = nil
	return nil
}

// --- 测试控制面 ---

// SetBook 更新买一卖一，并推送 ticker。不自动撮合。
func (e *Exchange) SetBook(bid, ask decimal.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	e.book = market.BookTicker{Bid: bid, Ask: ask, BidSize: decimal.NewFromInt(100), AskSize: decimal.NewFromInt(100), Time: now}
	if e.mark.IsZero() {
		e.mark = e.book.Mid()
	}
	tick := e.tickerLocked()
	e.emitLocked(exchange.StreamEvent{Ticker: &tick, Time: now})
}

// SetMark 更新标记价并推送 ticker。
func (e *Exchange) SetMark(mark decimal.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.mark = mark
	e.pos.MarkPrice = mark
	tick := e.tickerLocked()
	e.emitLocked(exchange.StreamEvent{Ticker: &tick, Time: tick.Time})
}

// SetAccount 覆盖账户快照。
func (e *Exchange) SetAccount(a account.Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.acct = a
}

// SetPosition 覆盖仓位（测试里用来预设底仓）。
func (e *Exchange) SetPosition(p position.Position) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if p.Symbol == "" {
		p.Symbol = e.mkt.Symbol
	}
	e.pos = p
}

// Trade 以给定价格撮合所有会被击中的挂单，并更新最新价。
//
// 买单在 price <= 挂单价 时成交，卖单在 price >= 挂单价 时成交。
func (e *Exchange) Trade(price decimal.Decimal) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tradeLocked(price, time.Now().UTC())
}

// FailNextPlaces 让接下来 n 次 PlaceOrders 整批返回可重试错误。
func (e *Exchange) FailNextPlaces(n int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.failNext = n
}

// FailPlace 让指定客户端订单号的下一次下单失败。
func (e *Exchange) FailPlace(id order.ClientOrderID, err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.placeErrs[id] = err
}

// InjectPlaceError 让下一次 PlaceOrders 整批失败。
func (e *Exchange) InjectPlaceError(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.nextPlaceErr = err
}

// InjectOpenOrder 插入一笔已存活挂单，供看门狗「多挂」测试。
func (e *Exchange) InjectOpenOrder(o order.Order) {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := o
	if cp.State == 0 {
		cp.State = order.StateOpen
	}
	if cp.Symbol == "" {
		cp.Symbol = e.mkt.Symbol
	}
	e.orders[o.ClientOrderID] = &cp
}

// DropOrder 静默删除一笔挂单，不发事件，模拟交易所丢单。
func (e *Exchange) DropOrder(id order.ClientOrderID) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.orders, id)
}

// Resting 返回当前存活挂单（测试断言用）。
func (e *Exchange) Resting() []order.Order {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]order.Order, 0, len(e.orders))
	for _, o := range e.orders {
		out = append(out, *o)
	}
	return out
}

// EmitResync 向所有订阅者推一条重连对账信号。
func (e *Exchange) EmitResync() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.emitLocked(exchange.StreamEvent{Resync: true, Time: time.Now().UTC()})
}

func (e *Exchange) acceptLocked(req exchange.PlaceRequest, now time.Time) (*order.Order, []exchange.StreamEvent, error) {
	price := e.mkt.RoundPrice(req.Price, market.RoundNearest)
	qty := e.mkt.RoundQty(req.Quantity)
	if err := e.mkt.CheckOrder(price, qty); err != nil {
		return nil, nil, exchange.Classify(exchange.ClassInvalidParam, "place_order", err)
	}

	e.nextID++
	o := &order.Order{
		ClientOrderID: req.ClientOrderID,
		ExchangeID:    fmt.Sprintf("%d", e.nextID),
		Symbol:        e.mkt.Symbol,
		Side:          req.Side,
		Type:          req.Type,
		TIF:           req.TIF,
		Price:         price,
		Quantity:      qty,
		ReduceOnly:    req.ReduceOnly,
		State:         order.StateOpen,
		UpdatedAt:     now,
	}

	if req.Type == order.Market {
		fillPrice := e.marketPriceLocked(req.Side, price)
		e.fillLocked(o, qty, fillPrice, now)
		cp := *o
		pos := e.pos
		return o, []exchange.StreamEvent{
			{Order: &cp, Time: now},
			{Position: &pos, Time: now},
		}, nil
	}

	if req.TIF == order.PostOnly && e.wouldCrossLocked(req.Side, price) {
		// 对齐 Lighter：提交成功，随后以 rejected 回报。
		o.State = order.StateRejected
		o.RejectReason = "canceled-post-only"
		cp := *o
		return o, []exchange.StreamEvent{{Order: &cp, Time: now}}, nil
	}

	if e.wouldCrossLocked(req.Side, price) {
		fillPrice := e.marketPriceLocked(req.Side, price)
		e.fillLocked(o, qty, fillPrice, now)
		cp := *o
		pos := e.pos
		return o, []exchange.StreamEvent{
			{Order: &cp, Time: now},
			{Position: &pos, Time: now},
		}, nil
	}

	e.orders[req.ClientOrderID] = o
	cp := *o
	return o, []exchange.StreamEvent{{Order: &cp, Time: now}}, nil
}

func (e *Exchange) wouldCrossLocked(side order.Side, price decimal.Decimal) bool {
	if !e.book.Valid() {
		return false
	}
	if side == order.Buy {
		return price.GreaterThanOrEqual(e.book.Ask)
	}
	return price.LessThanOrEqual(e.book.Bid)
}

func (e *Exchange) marketPriceLocked(side order.Side, protection decimal.Decimal) decimal.Decimal {
	if e.book.Valid() {
		if side == order.Buy {
			return e.book.Ask
		}
		return e.book.Bid
	}
	if protection.IsPositive() {
		return protection
	}
	return e.markOrMidLocked()
}

func (e *Exchange) fillLocked(o *order.Order, qty, price decimal.Decimal, now time.Time) {
	o.FilledQty = o.FilledQty.Add(qty)
	if o.FilledQty.GreaterThan(o.Quantity) {
		o.FilledQty = o.Quantity
	}
	o.AvgFillPrice = price
	o.IsMaker = o.Type == order.Limit && o.TIF == order.PostOnly
	o.Fee = e.mkt.FeeFor(price, qty, o.IsMaker)
	if o.FilledQty.Equal(o.Quantity) {
		o.State = order.StateFilled
	} else {
		o.State = order.StatePartiallyFilled
	}
	o.UpdatedAt = now
	e.applyFillLocked(o.Side, qty, price)
	e.last = price
	if o.State.IsTerminal() {
		delete(e.orders, o.ClientOrderID)
	}
}

func (e *Exchange) applyFillLocked(side order.Side, qty, price decimal.Decimal) {
	signed := qty
	if side == order.Sell {
		signed = qty.Neg()
	}
	e.pos.Size = e.pos.Size.Add(signed)
	if e.pos.Size.IsZero() {
		e.pos.EntryPrice = decimal.Zero
	} else if e.pos.EntryPrice.IsZero() {
		e.pos.EntryPrice = price
	}
	e.pos.MarkPrice = e.markOrMidLocked()
	e.pos.Symbol = e.mkt.Symbol
	e.pos.UpdatedAt = time.Now().UTC()
}

func (e *Exchange) tradeLocked(price decimal.Decimal, now time.Time) {
	e.last = price
	if e.mark.IsZero() {
		e.mark = price
	}
	var filled []order.Order
	for id, o := range e.orders {
		hit := (o.Side == order.Buy && price.LessThanOrEqual(o.Price)) ||
			(o.Side == order.Sell && price.GreaterThanOrEqual(o.Price))
		if !hit {
			continue
		}
		e.fillLocked(o, o.Remaining(), price, now)
		filled = append(filled, *o)
		if o.State.IsTerminal() {
			delete(e.orders, id)
		}
	}
	pos := e.pos
	for i := range filled {
		cp := filled[i]
		e.emitLocked(exchange.StreamEvent{Order: &cp, Time: now})
	}
	if len(filled) > 0 {
		e.emitLocked(exchange.StreamEvent{Position: &pos, Time: now})
	}
	tick := e.tickerLocked()
	e.emitLocked(exchange.StreamEvent{Ticker: &tick, Time: now})
}

func (e *Exchange) tickerLocked() exchange.Ticker {
	now := time.Now().UTC()
	e.book.Time = now
	return exchange.Ticker{
		Symbol: e.mkt.Symbol,
		Book:   e.book,
		Mark:   e.markOrMidLocked(),
		Index:  e.index,
		Last:   e.last,
		Time:   now,
	}
}

func (e *Exchange) markOrMidLocked() decimal.Decimal {
	if e.mark.IsPositive() {
		return e.mark
	}
	return e.book.Mid()
}

func (e *Exchange) snapshotMark() decimal.Decimal {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.markOrMidLocked()
}

func (e *Exchange) emitLocked(evs ...exchange.StreamEvent) {
	for _, ev := range evs {
		for _, ch := range e.subs {
			select {
			case ch <- ev:
			default:
				// 订阅方消费不过来时丢最旧语义：这里直接丢这条，避免测试卡死。
			}
		}
	}
}
