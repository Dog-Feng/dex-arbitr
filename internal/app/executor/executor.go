// Package executor 把策略意图翻译成交易所调用。
//
// 默认强制 Limit + PostOnly。仅止损平仓与建仓超时允许市价 IOC 吃单。
// 其余负责归类合并、先撤后挂、批量拆分、可重试错误的退避，
// 以及把结果转成 OrderEvent 回灌给策略。
package executor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"
	"dex-grid/internal/exchange"

	"github.com/shopspring/decimal"
	"golang.org/x/time/rate"
)

// Options 是执行器的运行参数。
type Options struct {
	Symbol     string
	Slot       uint8
	Epoch      uint16
	MaxRetries int
	RetryWait  time.Duration
	RPS        int
	Burst      int
	Log        *slog.Logger
	// Sleep 可替换，测试里用来跳过真实等待。
	Sleep func(ctx context.Context, d time.Duration) error
}

// Progress 对应页面上的「挂单目标 / 已确认 / 待重试」。
type Progress struct {
	Target    int
	Confirmed int
	Retrying  int
}

// Results 是一次 Apply 的汇总。
type Results struct {
	Events   []strategy.Event
	Failures int
	Fatal    error
	Stop     *strategy.Stop
	Ensure   *strategy.EnsurePosition
}

// Executor 把 Action 列表同步执行完再返回。
type Executor struct {
	ex   exchange.Exchange
	opts Options
	lim  *rate.Limiter

	progress Progress
	fails    int
	exitSeq  uint8
}

// New 构造执行器。ex 不能为 nil。
func New(ex exchange.Exchange, opts Options) *Executor {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	if opts.RetryWait <= 0 {
		opts.RetryWait = 200 * time.Millisecond
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.Sleep == nil {
		opts.Sleep = defaultSleep
	}
	var lim *rate.Limiter
	if opts.RPS > 0 {
		burst := opts.Burst
		if burst <= 0 {
			burst = opts.RPS
		}
		lim = rate.NewLimiter(rate.Limit(opts.RPS), burst)
	}
	return &Executor{ex: ex, opts: opts, lim: lim}
}

// SetEpoch 在启动或调整区间后更新轮次，供平仓单编码 ClientOrderID。
func (e *Executor) SetEpoch(epoch uint16) { e.opts.Epoch = epoch }

// Progress 返回最近一次铺单的进度快照。
func (e *Executor) Progress() Progress { return e.progress }

// ConsecutiveFails 返回连续失败次数（post-only 被拒不计入）。
func (e *Executor) ConsecutiveFails() int { return e.fails }

// ResetFails 在一次成功的交易操作后清零连续失败。
func (e *Executor) ResetFails() { e.fails = 0 }

// Apply 同步执行一组意图。顺序：设杠杆 → 撤单 → 改单 → 平仓 → 下单。
//
// EnsurePosition 与 Stop 不在这里执行，原样带回给 Runner。
func (e *Executor) Apply(ctx context.Context, acts []strategy.Action) Results {
	var res Results
	if len(acts) == 0 {
		return res
	}

	var (
		leverage  *strategy.SetLeverage
		cancelAll bool
		cancels   []strategy.CancelOrder
		modifies  []strategy.ModifyOrder
		places    []strategy.PlaceOrder
		closePos  *strategy.ClosePosition
	)
	for _, a := range acts {
		switch v := a.(type) {
		case strategy.SetLeverage:
			cp := v
			leverage = &cp
		case strategy.CancelAll:
			cancelAll = true
		case strategy.CancelOrder:
			cancels = append(cancels, v)
		case strategy.ModifyOrder:
			modifies = append(modifies, v)
		case strategy.PlaceOrder:
			places = append(places, v)
		case strategy.ClosePosition:
			cp := v
			closePos = &cp
		case strategy.Stop:
			cp := v
			res.Stop = &cp
		case strategy.EnsurePosition:
			cp := v
			res.Ensure = &cp
		}
	}

	now := time.Now().UTC()

	if leverage != nil {
		if err := e.withRetry(ctx, "set_leverage", func() error {
			return e.ex.SetLeverage(ctx, e.opts.Symbol, leverage.Leverage, leverage.Mode)
		}); err != nil {
			e.opts.Log.Warn("set_leverage failed, continue", "err", err)
			e.recordClass(exchange.ClassOf(err), &res)
		}
	}

	if cancelAll {
		if err := e.withRetry(ctx, "cancel_all", func() error {
			return e.ex.CancelAll(ctx, e.opts.Symbol)
		}); err != nil {
			e.noteError(err, &res)
			if res.Fatal != nil {
				return res
			}
		}
	} else if len(cancels) > 0 {
		e.cancel(ctx, cancels, now, &res)
		if res.Fatal != nil {
			return res
		}
	}

	if len(modifies) > 0 {
		e.modify(ctx, modifies, now, &res)
		if res.Fatal != nil {
			return res
		}
	}

	if closePos != nil {
		e.closePosition(ctx, *closePos, now, &res)
		if res.Fatal != nil {
			return res
		}
	}

	if len(places) > 0 {
		e.place(ctx, places, now, &res)
	}
	return res
}

func (e *Executor) place(ctx context.Context, places []strategy.PlaceOrder, now time.Time, res *Results) {
	e.progress = Progress{Target: len(places)}
	reqs := make([]exchange.PlaceRequest, len(places))
	for i, p := range places {
		typ, tif := order.Limit, order.PostOnly
		if p.Type == order.Market || p.TIF == order.IOC {
			typ, tif = order.Market, order.IOC
		}
		reqs[i] = exchange.PlaceRequest{
			Symbol:        e.opts.Symbol,
			ClientOrderID: p.ClientOrderID,
			Side:          p.Side,
			Type:          typ,
			Price:         p.Price,
			Quantity:      p.Quantity,
			TIF:           tif,
			ReduceOnly:    p.ReduceOnly,
		}
	}

	batch := e.ex.Capabilities().BatchPlace
	if batch <= 0 {
		batch = 1
	}

	pending := reqs
	for attempt := 0; attempt <= e.opts.MaxRetries && len(pending) > 0; attempt++ {
		if attempt > 0 {
			if err := e.opts.Sleep(ctx, backoff(e.opts.RetryWait, attempt-1)); err != nil {
				res.Fatal = err
				return
			}
		}
		var retry []exchange.PlaceRequest
		for _, chunk := range chunks(pending, batch) {
			if err := e.waitLimit(ctx); err != nil {
				res.Fatal = err
				return
			}
			results, err := e.ex.PlaceOrders(ctx, chunk)
			if err != nil {
				class := exchange.ClassOf(err)
				if class == exchange.ClassFatal {
					res.Fatal = err
					e.fails++
					return
				}
				if class.Retryable() && attempt < e.opts.MaxRetries {
					retry = append(retry, chunk...)
					continue
				}
				e.recordClass(class, res)
				for _, req := range chunk {
					res.Events = append(res.Events, rejectedEvent(req, err, now))
				}
				continue
			}
			for i, r := range results {
				req := chunk[i]
				if r.Err == nil {
					e.progress.Confirmed++
					res.Events = append(res.Events, strategy.OrderEvent{
						Order: order.Order{
							ClientOrderID: req.ClientOrderID,
							ExchangeID:    r.ExchangeID,
							Symbol:        e.opts.Symbol,
							Side:          req.Side,
							Type:          req.Type,
							TIF:           req.TIF,
							Price:         req.Price,
							Quantity:      req.Quantity,
							ReduceOnly:    req.ReduceOnly,
							State:         order.StatePending,
							UpdatedAt:     now,
						},
						Now: now,
					})
					continue
				}
				class := exchange.ClassOf(r.Err)
				if class.Retryable() && attempt < e.opts.MaxRetries {
					retry = append(retry, req)
					continue
				}
				e.recordClass(class, res)
				res.Events = append(res.Events, rejectedEvent(req, r.Err, now))
			}
		}
		pending = retry
		e.progress.Retrying = len(pending)
	}

	if e.progress.Confirmed > 0 && res.Failures == 0 {
		e.fails = 0
	}
	if e.progress.Confirmed > 0 {
		msg := fmt.Sprintf("挂了 %d 笔", e.progress.Confirmed)
		if e.progress.Target > e.progress.Confirmed {
			msg = fmt.Sprintf("挂了 %d 笔（目标 %d）", e.progress.Confirmed, e.progress.Target)
		}
		e.opts.Log.Info(msg, "confirmed", e.progress.Confirmed, "target", e.progress.Target)
	}
}

func (e *Executor) modify(ctx context.Context, mods []strategy.ModifyOrder, now time.Time, res *Results) {
	if !e.ex.Capabilities().ModifyOrder {
		e.opts.Log.Warn("exchange has no modify, falling back to cancel+place")
		cancels := make([]strategy.CancelOrder, len(mods))
		places := make([]strategy.PlaceOrder, len(mods))
		for i, m := range mods {
			cancels[i] = strategy.CancelOrder{ClientOrderID: m.ClientOrderID}
			places[i] = strategy.PlaceOrder{
				ClientOrderID: m.ClientOrderID,
				Side:          m.Side,
				Type:          m.Type,
				Price:         m.Price,
				Quantity:      m.Quantity,
				TIF:           m.TIF,
				ReduceOnly:    m.ReduceOnly,
			}
		}
		e.cancel(ctx, cancels, now, res)
		if res.Fatal != nil {
			return
		}
		e.place(ctx, places, now, res)
		return
	}

	reqs := make([]exchange.ModifyRequest, len(mods))
	for i, m := range mods {
		reqs[i] = exchange.ModifyRequest{
			Symbol:        e.opts.Symbol,
			ClientOrderID: m.ClientOrderID,
			Price:         m.Price,
			Quantity:      m.Quantity,
		}
	}

	pending := reqs
	byID := make(map[order.ClientOrderID]strategy.ModifyOrder, len(mods))
	for _, m := range mods {
		byID[m.ClientOrderID] = m
	}
	for attempt := 0; attempt <= e.opts.MaxRetries && len(pending) > 0; attempt++ {
		if attempt > 0 {
			if err := e.opts.Sleep(ctx, backoff(e.opts.RetryWait, attempt-1)); err != nil {
				res.Fatal = err
				return
			}
		}
		var retry []exchange.ModifyRequest
		for _, chunk := range chunks(pending, 1) {
			if err := e.waitLimit(ctx); err != nil {
				res.Fatal = err
				return
			}
			results, err := e.ex.ModifyOrders(ctx, chunk)
			if err != nil {
				class := exchange.ClassOf(err)
				if class == exchange.ClassFatal {
					res.Fatal = err
					e.fails++
					return
				}
				if class.Retryable() && attempt < e.opts.MaxRetries {
					retry = append(retry, chunk...)
					continue
				}
				e.recordClass(class, res)
				e.opts.Log.Warn("modify_order failed, keep resting order", "err", err, "coid", chunk[0].ClientOrderID)
				continue
			}
			for i, r := range results {
				req := chunk[i]
				src := byID[req.ClientOrderID]
				if r.Err == nil {
					res.Events = append(res.Events, strategy.OrderEvent{
						Order: order.Order{
							ClientOrderID: req.ClientOrderID,
							Symbol:        e.opts.Symbol,
							Side:          src.Side,
							Type:          src.Type,
							TIF:           src.TIF,
							Price:         req.Price,
							Quantity:      req.Quantity,
							ReduceOnly:    src.ReduceOnly,
							State:         order.StateOpen,
							UpdatedAt:     now,
						},
						Now: now,
					})
					continue
				}
				class := exchange.ClassOf(r.Err)
				if class.Retryable() && attempt < e.opts.MaxRetries {
					retry = append(retry, req)
					continue
				}
				e.recordClass(class, res)
				e.opts.Log.Warn("modify_order rejected, keep resting order",
					"err", r.Err, "coid", req.ClientOrderID)
			}
		}
		pending = retry
	}
}

func (e *Executor) cancel(ctx context.Context, cancels []strategy.CancelOrder, now time.Time, res *Results) {
	reqs := make([]exchange.CancelRequest, len(cancels))
	for i, c := range cancels {
		reqs[i] = exchange.CancelRequest{Symbol: e.opts.Symbol, ClientOrderID: c.ClientOrderID}
	}
	batch := e.ex.Capabilities().BatchCancel
	if batch <= 0 {
		batch = 1
	}
	for _, chunk := range chunks(reqs, batch) {
		if err := e.waitLimit(ctx); err != nil {
			res.Fatal = err
			return
		}
		results, err := e.ex.CancelOrders(ctx, chunk)
		if err != nil {
			if exchange.ClassOf(err) == exchange.ClassFatal {
				res.Fatal = err
				e.fails++
				return
			}
			if exchange.ClassOf(err).Retryable() {
				// 整批再试一次。
				if sleepErr := e.opts.Sleep(ctx, e.opts.RetryWait); sleepErr != nil {
					res.Fatal = sleepErr
					return
				}
				results, err = e.ex.CancelOrders(ctx, chunk)
			}
			if err != nil {
				e.recordClass(exchange.ClassOf(err), res)
				continue
			}
		}
		for i, r := range results {
			if r.Err == nil {
				continue
			}
			e.recordClass(exchange.ClassOf(r.Err), res)
			_ = now
			_ = chunk[i]
		}
	}
}

func (e *Executor) closePosition(ctx context.Context, act strategy.ClosePosition, now time.Time, res *Results) {
	pos, err := e.ex.Position(ctx, e.opts.Symbol)
	if err != nil {
		e.noteError(err, res)
		return
	}
	if pos.IsFlat() {
		return
	}
	side := order.Sell
	if pos.Size.IsNegative() {
		side = order.Buy
	}
	price := pos.MarkPrice
	tif := order.PostOnly
	typ := order.Limit
	if tick, terr := e.ex.Ticker(ctx, e.opts.Symbol); terr == nil {
		if act.Urgency == strategy.UrgencyMarket {
			// 止损吃单：Price 是可接受的最差价。
			if side == order.Buy && tick.Book.Ask.IsPositive() {
				price = tick.Book.Ask.Mul(decimal.RequireFromString("1.005"))
			} else if side == order.Sell && tick.Book.Bid.IsPositive() {
				price = tick.Book.Bid.Mul(decimal.RequireFromString("0.995"))
			} else if tick.Mark.IsPositive() {
				price = tick.Mark
			}
			tif = order.IOC
			typ = order.Market
		} else if side == order.Sell && tick.Book.Ask.IsPositive() {
			price = tick.Book.Ask
		} else if side == order.Buy && tick.Book.Bid.IsPositive() {
			price = tick.Book.Bid
		} else if tick.Mark.IsPositive() {
			price = tick.Mark
		}
	} else if act.Urgency == strategy.UrgencyMarket {
		tif = order.IOC
		typ = order.Market
	}
	e.exitSeq = order.NextSeq(e.exitSeq)
	coid, err := order.Encode(order.Ref{
		Slot:    e.opts.Slot,
		Epoch:   e.opts.Epoch,
		Purpose: order.PurposeExit,
		Seq:     e.exitSeq,
	})
	if err != nil {
		res.Fatal = err
		return
	}
	e.place(ctx, []strategy.PlaceOrder{{
		ClientOrderID: coid,
		Side:          side,
		Type:          typ,
		Price:         price,
		Quantity:      pos.AbsSize(),
		TIF:           tif,
		ReduceOnly:    true,
	}}, now, res)
}

func (e *Executor) withRetry(ctx context.Context, op string, fn func() error) error {
	var last error
	for i := 0; i <= e.opts.MaxRetries; i++ {
		if err := e.waitLimit(ctx); err != nil {
			return err
		}
		last = fn()
		if last == nil {
			e.fails = 0
			return nil
		}
		class := exchange.ClassOf(last)
		if class == exchange.ClassFatal {
			return last
		}
		if !class.Retryable() || i == e.opts.MaxRetries {
			return last
		}
		e.opts.Log.Warn("retrying exchange call", "op", op, "attempt", i+1, "err", last)
		if err := e.opts.Sleep(ctx, backoff(e.opts.RetryWait, i)); err != nil {
			return err
		}
	}
	return last
}

func (e *Executor) waitLimit(ctx context.Context) error {
	if e.lim == nil {
		return nil
	}
	return e.lim.Wait(ctx)
}

func (e *Executor) noteError(err error, res *Results) {
	class := exchange.ClassOf(err)
	if class == exchange.ClassFatal {
		res.Fatal = err
		e.fails++
		return
	}
	e.recordClass(class, res)
}

func (e *Executor) recordClass(class exchange.ErrorClass, res *Results) {
	if class.CountsAsFailure() {
		res.Failures++
		e.fails++
	}
}

func rejectedEvent(req exchange.PlaceRequest, err error, now time.Time) strategy.OrderEvent {
	reason := ""
	if err != nil {
		reason = err.Error()
	}
	return strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: req.ClientOrderID,
			Symbol:        req.Symbol,
			Side:          req.Side,
			Type:          req.Type,
			TIF:           req.TIF,
			Price:         req.Price,
			Quantity:      req.Quantity,
			ReduceOnly:    req.ReduceOnly,
			State:         order.StateRejected,
			RejectReason:  reason,
			UpdatedAt:     now,
		},
		Now: now,
	}
}

func chunks[T any](in []T, n int) [][]T {
	if n <= 0 {
		n = 1
	}
	var out [][]T
	for len(in) > 0 {
		k := n
		if k > len(in) {
			k = len(in)
		}
		out = append(out, in[:k])
		in = in[k:]
	}
	return out
}

func backoff(base time.Duration, attempt int) time.Duration {
	d := base
	for i := 0; i < attempt; i++ {
		d *= 2
		if d > 5*time.Second {
			return 5 * time.Second
		}
	}
	return d
}

func defaultSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
