// Package engine 是单个交易所实例的运行时：事件循环、命令通道、建仓与风控编排。
//
// 一个 Runner 对应一个 DEX。行情、回报、定时器、页面命令都进同一个
// goroutine，领域状态无锁。策略只返回意图，真正的 IO 由 Executor 完成。
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"dex-grid/internal/app/entry"
	"dex-grid/internal/app/executor"
	"dex-grid/internal/app/risk"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"
	"dex-grid/internal/exchange"

	"github.com/shopspring/decimal"
)

// Config 是 Runner 的构造参数。
type Config struct {
	Name             string
	Slot             uint8
	TickInterval     time.Duration
	WatchdogInterval time.Duration // 运行中对照交易所挂单：缺补、多撤。0 = 15s
	MaxRetries       int
	Log              *slog.Logger
	Persist          Persist
}

// Runner 驱动一个交易所上的一个网格实例。
type Runner struct {
	log   *slog.Logger
	ex    exchange.StreamingExchange
	strat strategy.Strategy
	exec  *executor.Executor
	trig  *entry.Trigger
	guard *risk.Guard
	cfg   Config

	commands chan Command

	status      Status
	stopReason  strategy.StopReason
	residual    bool
	symbol      string
	epoch       uint16
	lastFillQty map[order.ClientOrderID]decimal.Decimal

	// 热快照，供 View 与建仓使用。
	state strategy.State

	stream       <-chan exchange.StreamEvent
	streamCancel context.CancelFunc

	entering     bool
	entryP       strategy.EntryParams
	riskP        strategy.RiskParams
	stopping     bool
	pendingStop  strategy.StopReason // 风控止盈：先 maker 跟价平到 0，成交后再 finishStop
	lastWatchdog time.Time
}

// New 构造一个处于 Stopped 的 Runner。调用 Run 或 Do 之后才开始工作。
func New(ex exchange.StreamingExchange, strat strategy.Strategy, cfg Config) *Runner {
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = time.Second
	}
	if cfg.WatchdogInterval <= 0 {
		cfg.WatchdogInterval = 15 * time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Runner{
		log:      cfg.Log.With("exchange", cfg.Name),
		ex:       ex,
		strat:    strat,
		cfg:      cfg,
		commands: make(chan Command, 8),
		status:   StatusStopped,
	}
}

// Run 阻塞运行事件循环，直到 ctx 取消。
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.TickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			if r.status != StatusStopped && r.status != StatusError {
				r.finishStop(context.WithoutCancel(ctx), strategy.StopShutdown)
			}
			return ctx.Err()

		case cmd := <-r.commands:
			res := r.handleCommand(ctx, cmd)
			if cmd.Reply != nil {
				cmd.Reply <- res
			}

		case se, ok := <-r.stream:
			if r.stream == nil {
				continue
			}
			if !ok {
				r.status = StatusReconnecting
				_ = r.resubscribe(ctx)
				continue
			}
			r.onStream(ctx, se)

		case t := <-ticker.C:
			if r.status == StatusRunning || r.status == StatusStarting || r.status == StatusPaused {
				r.dispatch(ctx, strategy.TickEvent{Now: t})
			}
			if r.status == StatusRunning && !r.entering {
				r.maybeWatchdog(ctx, t)
			}
		}
	}
}

// Call 投递命令并等待回执。供 API 层使用，必须与 Run 同时运行。
func (r *Runner) Call(ctx context.Context, kind CommandKind, payload any) CommandResult {
	reply := make(chan CommandResult, 1)
	cmd := Command{Kind: kind, Payload: payload, Reply: reply}
	select {
	case r.commands <- cmd:
	case <-ctx.Done():
		return CommandResult{OK: false, Message: ctx.Err().Error()}
	}
	select {
	case res := <-reply:
		return res
	case <-ctx.Done():
		return CommandResult{OK: false, Message: "timeout"}
	}
}

// Do 同步执行命令，不经过 channel。测试或不跑 Run 循环时用。
func (r *Runner) Do(ctx context.Context, kind CommandKind, payload any) CommandResult {
	return r.handleCommand(ctx, Command{Kind: kind, Payload: payload})
}

// Drain 消费当前事件流里已经到达的消息。测试里在推进 fake 交易所后调用。
func (r *Runner) Drain(ctx context.Context) {
	if r.stream == nil {
		return
	}
	for {
		select {
		case se, ok := <-r.stream:
			if !ok {
				r.stream = nil
				return
			}
			r.onStream(ctx, se)
		default:
			return
		}
	}
}

// Status 返回当前对外状态。
func (r *Runner) Status() Status { return r.status }

// View 返回控制台聚合视图。
func (r *Runner) View() InstanceView {
	v := InstanceView{
		Exchange: r.cfg.Name,
		Status:   r.status.String(),
		Residual: r.residual,
		Entering: r.entering,
		Symbol:   r.symbol,
		Mark:     r.state.Mark,
		Position: r.state.Position,
		Account:  r.state.Account,
	}
	if r.stopReason != 0 || r.status == StatusStopped {
		v.StopReason = r.stopReason.String()
	}
	if r.strat != nil {
		v.Strategy = r.strat.View()
	}
	return v
}

func (r *Runner) handleCommand(ctx context.Context, cmd Command) CommandResult {
	switch cmd.Kind {
	case CmdStart:
		p, _ := cmd.Payload.(StartPayload)
		return r.handleStart(ctx, p)
	case CmdStop:
		return r.handleStop(ctx)
	case CmdReconnect:
		if err := r.resubscribe(ctx); err != nil {
			return r.failResult(err.Error())
		}
		return CommandResult{OK: true, View: r.View()}
	case CmdSaveConfig:
		return r.failResult("配置持久化尚未接入")
	case CmdAdjustRange, CmdCancelOrders, CmdRefill, CmdResetStats:
		return r.handleStrategyCommand(ctx, cmd)
	default:
		return r.failResult("未知命令")
	}
}

func (r *Runner) handleStart(ctx context.Context, p StartPayload) CommandResult {
	if r.status == StatusRunning || r.status == StatusStarting {
		return r.failResult("实例已在运行")
	}
	if p.Symbol == "" {
		return r.failResult("必须指定交易对")
	}
	if !r.ex.Capabilities().PostOnly {
		return r.failResult("交易所不支持 post-only，无法运行网格")
	}

	r.symbol = p.Symbol
	r.entryP = p.Entry
	r.riskP = p.Risk
	if r.riskP.MaxConsecutiveErrors == 0 {
		r.riskP = strategy.DefaultRiskParams()
		r.riskP.TakeProfitPrice = p.Risk.TakeProfitPrice
		r.riskP.StopLossPrice = p.Risk.StopLossPrice
		r.riskP.OutOfRange = p.Risk.OutOfRange
		r.riskP.MaxPositionNotional = p.Risk.MaxPositionNotional
		r.riskP.MinMarginRatio = p.Risk.MinMarginRatio
		if p.Risk.PriceSource != 0 {
			r.riskP.PriceSource = p.Risk.PriceSource
		}
	}

	mkt, err := r.ex.Market(ctx, p.Symbol)
	if err != nil {
		return r.failResult(err.Error())
	}
	tick, err := r.ex.Ticker(ctx, p.Symbol)
	if err != nil {
		return r.failResult(err.Error())
	}
	acct, err := r.ex.Account(ctx)
	if err != nil {
		return r.failResult(err.Error())
	}
	pos, err := r.ex.Position(ctx, p.Symbol)
	if err != nil {
		return r.failResult(err.Error())
	}
	orders, err := r.ex.OpenOrders(ctx, p.Symbol)
	if err != nil {
		return r.failResult(err.Error())
	}

	now := time.Now().UTC()
	r.state = strategy.State{
		Market:   mkt,
		Position: pos,
		Account:  acct,
		Book:     tick.Book,
		Mark:     tick.Mark,
		Orders:   orders,
		Epoch:    r.epoch,
		Slot:     r.cfg.Slot,
		Now:      now,
	}
	if !r.state.Mark.IsPositive() {
		r.state.Mark = tick.Book.Mid()
	}

	r.guard = risk.New(r.riskP)
	r.guard.SetPosition(pos)
	r.guard.SetAccount(acct)
	r.guard.SetMark(r.state.Mark, r.state.Book, now)

	r.exec = executor.New(r.ex, executor.Options{
		Symbol:     p.Symbol,
		Slot:       r.cfg.Slot,
		Epoch:      r.epoch,
		MaxRetries: r.cfg.MaxRetries,
		RetryWait:  50 * time.Millisecond,
		Log:        r.log,
	})

	r.status = StatusStarting
	r.stopReason = 0
	r.pendingStop = 0
	r.residual = false
	r.stopping = false

	acts, err := r.strat.Init(r.state)
	if err != nil {
		r.status = StatusStopped
		return r.failResult(err.Error())
	}
	r.epoch = r.strat.View().Epoch
	r.exec.SetEpoch(r.epoch)

	if err := r.subscribe(ctx); err != nil {
		r.status = StatusStopped
		return r.failResult(err.Error())
	}

	dir := r.strat.View().Direction
	r.log.Info(fmt.Sprintf("已启动 %s %s网格，现价 %s", r.symbol, gridDirCN(dir), r.state.Mark),
		"symbol", r.symbol, "direction", dir, "mark", r.state.Mark.String())
	r.apply(ctx, acts, now)
	r.syncStatus()
	r.persist()
	return CommandResult{OK: true, View: r.View()}
}

func (r *Runner) handleStop(ctx context.Context) CommandResult {
	if r.status == StatusStopped {
		return CommandResult{OK: true, Message: "already stopped", View: r.View()}
	}
	r.finishStop(ctx, strategy.StopManual)
	r.persist()
	return CommandResult{OK: true, View: r.View()}
}

func (r *Runner) handleStrategyCommand(ctx context.Context, cmd Command) CommandResult {
	if r.status != StatusRunning && r.status != StatusPaused && r.status != StatusStarting {
		return r.failResult("当前状态不能执行 " + cmd.Kind.String())
	}
	sk, ok := toStrategyKind(cmd.Kind)
	if !ok {
		return r.failResult("命令无法转给策略")
	}
	now := time.Now().UTC()
	acts, err := r.strat.OnCommand(strategy.Command{
		Kind:    sk,
		Payload: cmd.Payload,
		Mark:    r.state.Mark,
		Now:     now,
	})
	if err != nil {
		return r.failResult(err.Error())
	}
	r.apply(ctx, acts, now)
	r.syncStatus()
	r.persist()
	return CommandResult{OK: true, View: r.View()}
}

func (r *Runner) onStream(ctx context.Context, se exchange.StreamEvent) {
	now := se.Time
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if se.Err != nil {
		r.log.Warn("stream error", "err", se.Err)
		return
	}
	if se.Resync {
		if err := r.reconcile(ctx); err != nil {
			r.log.Error("reconcile failed", "err", err)
		}
		return
	}
	if se.Ticker != nil {
		r.state.Book = se.Ticker.Book
		if se.Ticker.Mark.IsPositive() {
			r.state.Mark = se.Ticker.Mark
		}
		r.dispatch(ctx, strategy.BookEvent{Book: r.state.Book, Mark: r.state.Mark, Now: now})
	}
	if se.Order != nil {
		r.recordFill(*se.Order)
		r.dispatch(ctx, strategy.OrderEvent{Order: *se.Order, Now: now})
	}
	if se.Trade != nil {
		tr := *se.Trade
		if tr.Time.IsZero() {
			tr.Time = now
		}
		r.dispatch(ctx, strategy.TradeEvent{Trade: tr, Now: now})
	}
	if se.Position != nil {
		r.state.Position = *se.Position
		r.dispatch(ctx, strategy.PositionEvent{
			Position: r.state.Position,
			Account:  r.state.Account,
			Now:      now,
		})
	}
	if se.Account != nil {
		r.state.Account = *se.Account
	}
}

func (r *Runner) dispatch(ctx context.Context, ev strategy.Event) {
	if r.status != StatusRunning && r.status != StatusStarting {
		r.updateViewOnly(ev)
		return
	}
	if r.guard != nil && r.pendingStop == 0 {
		v := r.guard.Check(ev)
		if v.Reconnect {
			r.log.Warn("market data stale, reconnecting")
			_ = r.resubscribe(ctx)
			return
		}
		if v.Stop {
			if v.Reason == strategy.StopTakeProfit && !r.state.Position.IsFlat() {
				r.pendingStop = v.Reason
				r.apply(ctx, v.Actions, ev.At())
				return
			}
			r.apply(ctx, v.Actions, ev.At())
			r.finishStop(ctx, v.Reason)
			return
		}
	}
	r.route(ctx, ev)
	r.syncStatus()
}

func (r *Runner) route(ctx context.Context, ev strategy.Event) {
	if r.stopping {
		return
	}
	if r.entering && r.trig != nil {
		if _, isTrade := ev.(strategy.TradeEvent); isTrade {
			acts, err := r.strat.OnEvent(ev)
			if err != nil {
				r.fail(ctx, err)
				return
			}
			r.apply(ctx, acts, ev.At())
		}
		if _, isEntry := ev.(strategy.EntryDoneEvent); !isEntry {
			if _, isFail := ev.(strategy.EntryFailedEvent); !isFail {
				acts, done, failed := r.trig.OnEvent(ev)
				r.apply(ctx, acts, ev.At())
				if failed {
					r.entering = false
					if r.completePendingStop(ctx) {
						return
					}
					filled, reason := r.trig.Result()
					r.route(ctx, strategy.EntryFailedEvent{Filled: filled, Reason: reason, Now: ev.At()})
					return
				}
				if done {
					r.entering = false
					filled, _ := r.trig.Result()
					if r.completePendingStop(ctx) {
						return
					}
					r.route(ctx, strategy.EntryDoneEvent{Filled: filled, Now: ev.At()})
				}
				return
			}
		}
	}

	acts, err := r.strat.OnEvent(ev)
	if err != nil {
		r.fail(ctx, err)
		return
	}
	r.apply(ctx, acts, ev.At())
}

func (r *Runner) apply(ctx context.Context, acts []strategy.Action, now time.Time) {
	if len(acts) == 0 || r.stopping {
		return
	}
	r.syncEpoch()
	if r.guard != nil && r.guard.BlockOpens() {
		acts = risk.FilterOpens(acts)
		if len(acts) == 0 {
			return
		}
	}
	res := r.exec.Apply(ctx, acts)
	if r.guard != nil {
		if v := r.guard.RecordFailures(res.Failures, res.Fatal); v.Stop {
			r.exec.Apply(ctx, stripControl(v.Actions))
			r.finishStop(ctx, v.Reason)
			return
		}
	} else if res.Fatal != nil {
		r.fail(ctx, res.Fatal)
		return
	}

	for _, ev := range res.Events {
		r.route(ctx, ev)
	}
	if res.Ensure != nil {
		r.startEntry(ctx, *res.Ensure, now)
	}
	if res.Stop != nil && r.pendingStop == 0 {
		r.finishStop(ctx, res.Stop.Reason)
	}
}

func (r *Runner) startEntry(ctx context.Context, req strategy.EnsurePosition, now time.Time) {
	if r.entering {
		r.log.Info("entry already in progress, ignore EnsurePosition")
		return
	}
	r.syncEpoch()
	r.refreshPosition(ctx)
	params := r.entryP
	if r.pendingStop != 0 {
		params = strategy.DefaultEntryParams()
	}
	r.trig = entry.New(params, r.state.Market, r.cfg.Slot, r.epoch)
	acts := r.trig.Start(req.Target, r.state.Position.Size, r.state.Book, r.state.Mark, now)
	if !r.trig.Active() {
		filled, _ := r.trig.Result()
		r.entering = false
		if r.completePendingStop(ctx) {
			return
		}
		r.route(ctx, strategy.EntryDoneEvent{Filled: filled, Now: now})
		return
	}
	r.entering = true
	r.status = StatusStarting
	r.apply(ctx, acts, now)
}

func (r *Runner) completePendingStop(ctx context.Context) bool {
	if r.pendingStop == 0 {
		return false
	}
	reason := r.pendingStop
	r.pendingStop = 0
	r.finishStop(ctx, reason)
	return true
}

func (r *Runner) finishStop(ctx context.Context, reason strategy.StopReason) {
	if r.status == StatusStopped && r.stopping {
		return
	}
	r.stopping = true
	r.entering = false
	r.pendingStop = 0
	acts, err := r.strat.OnStop(reason)
	if err != nil {
		r.log.Error("strategy OnStop failed", "err", err)
	} else {
		r.refreshPosition(ctx)
		// 止盈若仓位还在，不能 CancelAll，否则刚挂上的 maker 平仓单会被撤掉。
		if reason != strategy.StopTakeProfit || r.state.Position.IsFlat() {
			r.execApplyQuiet(ctx, stripControl(acts))
		}
	}
	r.refreshPosition(ctx)
	r.status = StatusStopped
	r.stopReason = reason
	r.residual = !r.state.Position.IsFlat()
	r.stopping = false
	r.log.Info("instance stopped", "reason", reason, "residual", r.residual)
	r.persist()
}

func (r *Runner) fail(ctx context.Context, err error) {
	r.log.Error("instance failed", "err", err)
	r.finishStop(ctx, strategy.StopError)
	r.status = StatusError
}

func (r *Runner) execApplyQuiet(ctx context.Context, acts []strategy.Action) {
	if r.exec == nil || len(acts) == 0 {
		return
	}
	_ = r.exec.Apply(ctx, acts)
}

func (r *Runner) refreshPosition(ctx context.Context) {
	if r.symbol == "" {
		return
	}
	if pos, err := r.ex.Position(ctx, r.symbol); err == nil {
		r.state.Position = pos
	}
}

func (r *Runner) syncEpoch() {
	if r.strat == nil {
		return
	}
	ep := r.strat.View().Epoch
	if ep == r.epoch {
		return
	}
	r.log.Info("epoch advanced", "from", r.epoch, "to", ep)
	r.epoch = ep
	if r.exec != nil {
		r.exec.SetEpoch(ep)
	}
}

func (r *Runner) subscribe(ctx context.Context) error {
	if r.streamCancel != nil {
		r.streamCancel()
		r.streamCancel = nil
		r.stream = nil
	}
	sctx, cancel := context.WithCancel(ctx)
	ch, err := r.ex.Subscribe(sctx, r.symbol)
	if err != nil {
		cancel()
		return err
	}
	r.streamCancel = cancel
	r.stream = ch
	return nil
}

func (r *Runner) resubscribe(ctx context.Context) error {
	if r.symbol == "" {
		return fmt.Errorf("no symbol subscribed")
	}
	if err := r.subscribe(ctx); err != nil {
		return err
	}
	return r.reconcile(ctx)
}

func (r *Runner) maybeWatchdog(ctx context.Context, now time.Time) {
	if r.cfg.WatchdogInterval <= 0 {
		return
	}
	if !r.lastWatchdog.IsZero() && now.Sub(r.lastWatchdog) < r.cfg.WatchdogInterval {
		return
	}
	r.lastWatchdog = now
	r.syncEpoch()
	r.watchdog(ctx)
}

// watchdog 对照交易所真实挂单：本实例多余的撤掉，缺失的格子补挂。
// 只动本 slot 且属于当前交易对的订单；解不出 COID 的手工单不动。
func (r *Runner) watchdog(ctx context.Context) {
	if r.symbol == "" || r.exec == nil || r.entering {
		return
	}
	orders, err := r.ex.OpenOrders(ctx, r.symbol)
	if err != nil {
		r.log.Warn("watchdog open orders failed", "err", err)
		return
	}
	pos, err := r.ex.Position(ctx, r.symbol)
	if err != nil {
		r.log.Warn("watchdog position failed", "err", err)
		return
	}
	acct, err := r.ex.Account(ctx)
	if err != nil {
		r.log.Warn("watchdog account failed", "err", err)
		return
	}

	var cancels []strategy.Action
	var ours []order.Order
	for _, o := range orders {
		if !o.ClientOrderID.Valid() {
			continue
		}
		ref := o.ClientOrderID.Decode()
		if ref.Slot != r.cfg.Slot {
			continue
		}
		// 编排层只丢掉明显不属于本轮的单；「缺哪笔、多哪笔」由策略 Resync 裁决。
		if (r.epoch > 0 && ref.Epoch != r.epoch) || ref.Purpose == order.PurposeEntry {
			cancels = append(cancels, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
			continue
		}
		ours = append(ours, o)
	}
	if len(cancels) > 0 {
		r.log.Info("watchdog cancel extras", "n", len(cancels))
		r.execApplyQuiet(ctx, cancels)
	}
	r.state.Position = pos
	r.state.Account = acct
	r.dispatch(ctx, strategy.ResyncEvent{
		Position: pos,
		Account:  acct,
		Orders:   ours,
		Now:      time.Now().UTC(),
	})
}

func (r *Runner) reconcile(ctx context.Context) error {
	if r.symbol == "" || r.exec == nil {
		return nil
	}
	pos, err := r.ex.Position(ctx, r.symbol)
	if err != nil {
		return err
	}
	acct, err := r.ex.Account(ctx)
	if err != nil {
		return err
	}
	orders, err := r.ex.OpenOrders(ctx, r.symbol)
	if err != nil {
		return err
	}
	tick, err := r.ex.Ticker(ctx, r.symbol)
	if err != nil {
		return err
	}
	r.state.Position = pos
	r.state.Account = acct
	r.state.Book = tick.Book
	if tick.Mark.IsPositive() {
		r.state.Mark = tick.Mark
	}
	if r.guard != nil {
		r.guard.SetPosition(pos)
		r.guard.SetAccount(acct)
		r.guard.SetMark(r.state.Mark, r.state.Book, time.Now().UTC())
	}

	var cancels []strategy.Action
	var ours []order.Order
	for _, o := range orders {
		ref := o.ClientOrderID.Decode()
		if !o.ClientOrderID.Valid() || ref.Slot != r.cfg.Slot || (r.epoch > 0 && ref.Epoch != r.epoch) {
			cancels = append(cancels, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
			continue
		}
		ours = append(ours, o)
	}
	if len(cancels) > 0 {
		r.execApplyQuiet(ctx, cancels)
	}
	if r.status == StatusRunning || r.status == StatusStarting {
		r.dispatch(ctx, strategy.ResyncEvent{
			Position: pos,
			Account:  acct,
			Orders:   ours,
			Now:      time.Now().UTC(),
		})
	}
	return nil
}

func (r *Runner) updateViewOnly(ev strategy.Event) {
	switch e := ev.(type) {
	case strategy.BookEvent:
		r.state.Book = e.Book
		if e.Mark.IsPositive() {
			r.state.Mark = e.Mark
		}
	case strategy.PositionEvent:
		r.state.Position = e.Position
		r.state.Account = e.Account
	}
}

func (r *Runner) syncStatus() {
	if r.status == StatusError || r.status == StatusReconnecting || r.status == StatusStopped {
		return
	}
	switch r.strat.View().Phase {
	case strategy.PhaseOutOfRange, strategy.PhasePaused:
		r.status = StatusPaused
	case strategy.PhaseEntering:
		r.status = StatusStarting
	case strategy.PhaseRunning:
		r.status = StatusRunning
	case strategy.PhaseStopped:
		r.status = StatusStopped
	}
}

func (r *Runner) failResult(msg string) CommandResult {
	return CommandResult{OK: false, Message: msg, View: r.View()}
}

func toStrategyKind(k CommandKind) (strategy.CommandKind, bool) {
	switch k {
	case CmdAdjustRange:
		return strategy.CmdAdjustRange, true
	case CmdCancelOrders:
		return strategy.CmdCancelOrders, true
	case CmdRefill:
		return strategy.CmdRefill, true
	case CmdResetStats:
		return strategy.CmdResetStats, true
	default:
		return 0, false
	}
}

func stripControl(acts []strategy.Action) []strategy.Action {
	var out []strategy.Action
	for _, a := range acts {
		switch a.(type) {
		case strategy.Stop, strategy.EnsurePosition:
			continue
		default:
			out = append(out, a)
		}
	}
	return out
}

func (r *Runner) logOrderFill(o order.Order) {
	if !o.FilledQty.IsPositive() {
		return
	}
	if o.ClientOrderID.Valid() && o.ClientOrderID.Decode().Slot != r.cfg.Slot {
		return
	}
	if r.lastFillQty == nil {
		r.lastFillQty = map[order.ClientOrderID]decimal.Decimal{}
	}
	prev := r.lastFillQty[o.ClientOrderID]
	if !o.FilledQty.GreaterThan(prev) {
		return
	}
	delta := o.FilledQty.Sub(prev)
	r.lastFillQty[o.ClientOrderID] = o.FilledQty
	px := o.AvgFillPrice
	if !px.IsPositive() {
		px = o.Price
	}
	if !px.IsPositive() || !delta.IsPositive() {
		return
	}
	side := "买"
	if o.Side == order.Sell {
		side = "卖"
	}
	r.log.Info(fmt.Sprintf("%s成交 %s × %s", side, px, delta),
		"symbol", r.symbol, "side", o.Side.String(), "price", px.String(), "qty", delta.String())
}

func gridDirCN(dir string) string {
	switch dir {
	case "long":
		return "做多"
	case "short":
		return "做空"
	case "neutral":
		return "中性"
	default:
		return dir
	}
}
