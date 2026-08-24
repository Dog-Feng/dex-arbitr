// Package risk 实现实例级风控：止盈止损、最大持仓、保证金率、连续失败熔断、行情静默。
//
// 区间外策略由网格策略自己处理（它要维护格子与回归确认），Guard 不重复做。
// 止损永远先于策略被 Runner 调用，保证不会被铺单逻辑挡住。
package risk

import (
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// Verdict 是一次检查的结论。
type Verdict struct {
	Actions    []strategy.Action
	Stop       bool
	Reason     strategy.StopReason
	BlockOpens bool
	Reconnect  bool
}

// Guard 持有风控所需的最新快照，所有方法由 Runner 单 goroutine 调用。
type Guard struct {
	params strategy.RiskParams

	mark decimal.Decimal
	last decimal.Decimal
	book market.BookTicker
	pos  position.Position
	acct account.Snapshot

	lastMarket time.Time
	fails      int
	blockOpens bool
	reason     strategy.StopReason
}

// New 构造 Guard。params 应已 ApplyDefaults。
func New(params strategy.RiskParams) *Guard {
	return &Guard{params: params}
}

// Observe 从事件更新快照，不产生动作。
func (g *Guard) Observe(ev strategy.Event) {
	switch e := ev.(type) {
	case strategy.BookEvent:
		g.book = e.Book
		if e.Mark.IsPositive() {
			g.mark = e.Mark
		}
		g.lastMarket = e.Now
	case strategy.PositionEvent:
		g.pos = e.Position
		g.acct = e.Account
		if e.Position.MarkPrice.IsPositive() {
			g.mark = e.Position.MarkPrice
		}
	case strategy.TickEvent:
		// 时间推进，快照不变。
	}
}

// SetPosition 在启动对账后写入初始仓位。
func (g *Guard) SetPosition(p position.Position) { g.pos = p }

// SetAccount 写入账户快照。
func (g *Guard) SetAccount(a account.Snapshot) { g.acct = a }

// SetMark 写入标记价。
func (g *Guard) SetMark(mark decimal.Decimal, book market.BookTicker, now time.Time) {
	g.mark = mark
	g.book = book
	g.lastMarket = now
}

// RecordFailures 计入一次 Apply 的失败数。返回是否应立即熔断。
func (g *Guard) RecordFailures(n int, fatal error) Verdict {
	if fatal != nil {
		g.reason = strategy.StopError
		return Verdict{
			Stop:    true,
			Reason:  strategy.StopError,
			Actions: circuitActions(),
		}
	}
	if n <= 0 {
		g.fails = 0
		return Verdict{}
	}
	g.fails += n
	max := g.params.MaxConsecutiveErrors
	if max <= 0 {
		max = 10
	}
	if g.fails >= max {
		g.reason = strategy.StopCircuit
		return Verdict{
			Stop:    true,
			Reason:  strategy.StopCircuit,
			Actions: circuitActions(),
		}
	}
	return Verdict{}
}

// BlockOpens 表示应暂停开仓腿（最大持仓或保证金率触发）。
func (g *Guard) BlockOpens() bool { return g.blockOpens }

// Reason 返回最近一次停止原因。
func (g *Guard) Reason() strategy.StopReason { return g.reason }

// Check 按优先级检查。ev 用于取时间；价格与仓位用已观察的快照。
func (g *Guard) Check(ev strategy.Event) Verdict {
	g.Observe(ev)
	now := ev.At()
	price := g.price()

	if !g.pos.IsFlat() && price.IsPositive() {
		if v := g.checkTPSL(price); v.Stop {
			return v
		}
	}

	g.blockOpens = false
	if g.params.MaxPositionNotional.IsPositive() && g.pos.Notional().GreaterThan(g.params.MaxPositionNotional) {
		g.blockOpens = true
	}
	if g.params.MinMarginRatio.IsPositive() && g.acct.MarginUsed.IsPositive() {
		if g.acct.MarginRatio().LessThan(g.params.MinMarginRatio) {
			g.blockOpens = true
		}
	}

	if g.params.StaleTimeout > 0 && !g.lastMarket.IsZero() &&
		now.Sub(g.lastMarket) >= g.params.StaleTimeout.Std() {
		return Verdict{Reconnect: true}
	}
	return Verdict{BlockOpens: g.blockOpens}
}

func (g *Guard) checkTPSL(price decimal.Decimal) Verdict {
	long := g.pos.Size.IsPositive()
	short := g.pos.Size.IsNegative()

	if g.params.HasStopLoss() {
		sl := g.params.StopLossPrice
		if (long && price.LessThanOrEqual(sl)) || (short && price.GreaterThanOrEqual(sl)) {
			g.reason = strategy.StopStopLoss
			return Verdict{
				Stop:    true,
				Reason:  strategy.StopStopLoss,
				Actions: closeActions(strategy.StopStopLoss),
			}
		}
	}
	if g.params.HasTakeProfit() {
		tp := g.params.TakeProfitPrice
		if (long && price.GreaterThanOrEqual(tp)) || (short && price.LessThanOrEqual(tp)) {
			g.reason = strategy.StopTakeProfit
			return Verdict{
				Stop:    true,
				Reason:  strategy.StopTakeProfit,
				Actions: closeActions(strategy.StopTakeProfit),
			}
		}
	}
	return Verdict{}
}

func (g *Guard) price() decimal.Decimal {
	switch g.params.PriceSource {
	case market.PriceMid:
		if g.book.Valid() {
			return g.book.Mid()
		}
	case market.PriceLast:
		if g.last.IsPositive() {
			return g.last
		}
	}
	if g.mark.IsPositive() {
		return g.mark
	}
	return g.book.Mid()
}

func closeActions(reason strategy.StopReason) []strategy.Action {
	if reason == strategy.StopStopLoss {
		return []strategy.Action{
			strategy.CancelAll{},
			strategy.ClosePosition{Urgency: strategy.UrgencyMarket},
			strategy.Stop{Reason: reason},
		}
	}
	return []strategy.Action{
		strategy.CancelAll{},
		strategy.EnsurePosition{Target: decimal.Zero},
	}
}

func circuitActions() []strategy.Action {
	return []strategy.Action{
		strategy.CancelAll{},
		strategy.Stop{Reason: strategy.StopCircuit},
	}
}

// FilterOpens 在 BlockOpens 时去掉非 reduce-only 的下单意图。
func FilterOpens(acts []strategy.Action) []strategy.Action {
	var out []strategy.Action
	for _, a := range acts {
		if p, ok := a.(strategy.PlaceOrder); ok && !p.ReduceOnly {
			continue
		}
		out = append(out, a)
	}
	return out
}
