package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// pendingExecution retains the complete executor result, including outstanding
// IOC identities and rescue batches. Recovery never resubmits an original IOC.
type pendingExecution struct {
	action            strategy.Action
	quote             spread.Quote
	beforeL, beforeR  float64
	result            execution.Result
	unwind            bool
	lastAttempt       time.Time
	stalls            int
	dustWarned        bool
	intervention      bool
	interventionTries int
	interrupted       *execution.Result // settled original orders, separate from the new flatten
}

// interventionStallLimit is how many consecutive inconsistent settlements are
// tolerated before the actor assumes external interference on the account.
const interventionStallLimit = 3

// subMinRetryInterval spaces recovery retries once a sub-minimum residual has
// been reported. Emergency and drain requests keep the normal interval.
const subMinRetryInterval = 30 * time.Second

func (a *PairActor) syncPositionState() {
	a.flatPositions.Store(math.Abs(a.posL) < math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 &&
		math.Abs(a.posR) < math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2)
}

func (a *PairActor) executionContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	a.busy.Store(true)
	a.execMu.Lock()
	a.execCancel = cancel
	a.execMu.Unlock()
	return ctx, func() {
		a.execMu.Lock()
		a.execCancel = nil
		a.execMu.Unlock()
		cancel()
		a.busy.Store(false)
	}
}

// watchRisk owns its margin monitor independently of the trading loop. It can
// interrupt a long ordinary rescue; settlement is then recovered before unwind.
func (a *PairActor) watchRisk(ctx context.Context) {
	m := &a.margin
	t := time.NewTicker(time.Second)
	defer t.Stop()
	var reported, lastBlocked bool
	var lastReason string
	for {
		now := time.Now()
		if now.Sub(m.lastRefresh) >= m.cfg.refresh {
			m.refresh(ctx, a.cfg.L, a.cfg.R, a.cfg.Pair.Left.MarketID, a.cfg.Pair.Right.MarketID)
		}
		ml, mr, fresh := a.riskMarks()
		// A stale peer must block admission without hiding a liquidation
		// trigger on the leg whose price is still known.
		assessedAt := time.Now()
		action := m.assessAt(assessedAt, ml, mr, ml > 0 || mr > 0)
		blocked := !fresh || m.armed || action == marginArm || action == marginTrigger
		a.riskBlocked.Store(blocked)
		reason := m.reason
		if !fresh && action != marginTrigger {
			reason = "book_unavailable"
		}
		if !reported || blocked != lastBlocked || blocked && reason != lastReason {
			a.logRiskState(assessedAt, m, ml, mr, blocked, reason)
			reported, lastBlocked, lastReason = true, blocked, reason
		}
		if action == marginTrigger && a.emergencyRequested.CompareAndSwap(false, true) {
			a.execMu.Lock()
			if a.execCancel != nil {
				a.execCancel()
			}
			a.execMu.Unlock()
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// logRiskState records transitions rather than repeating the same reason each
// second. These inputs explain a block without logging account identifiers.
func (a *PairActor) logRiskState(now time.Time, m *marginMonitor, markL, markR float64, blocked bool, reason string) {
	// Observational only: feed the monitor's risk-flapping anomaly rule. This
	// is called from the risk watcher goroutine, so it bypasses a.record to
	// avoid touching actor-owned state (a.state.Mode) from another goroutine.
	if a.cfg.Monitor != nil {
		a.cfg.Monitor.Record(monitor.Event{Time: now, Pair: a.key, Kind: monitor.KindRiskState, Msg: reason})
	}
	ageMS := func(t time.Time) int64 {
		if t.IsZero() {
			return -1
		}
		return now.Sub(t).Milliseconds()
	}
	lb, _ := a.book(a.cfg.L.Name(), a.cfg.Pair.Left.MarketID)
	rb, _ := a.book(a.cfg.R.Name(), a.cfg.Pair.Right.MarketID)
	args := []any{
		"blocked", blocked, "reason", reason, "assessment_reason", m.reason,
		"qty_l", m.qtyL, "qty_r", m.qtyR,
		"entry_l", m.entryL, "entry_r", m.entryR,
		"liquidation_l", m.liqL, "liquidation_r", m.liqR,
		"mark_l", markL, "mark_r", markR,
		"position_age_l_ms", ageMS(m.lastL), "position_age_r_ms", ageMS(m.lastR),
		"book_age_l_ms", ageMS(lb.Ts), "book_age_r_ms", ageMS(rb.Ts),
		"snapshot_notional_l", m.notionalL, "snapshot_notional_r", m.notionalR,
	}
	if m.crossL != nil {
		args = append(args, "cross_equity_l", m.crossL.Equity, "cross_maintenance_l", m.crossL.Maintenance)
	}
	if m.crossR != nil {
		args = append(args, "cross_equity_r", m.crossR.Equity, "cross_maintenance_r", m.crossR.Maintenance)
	}
	if blocked {
		a.log.Warn("risk state", args...)
	} else {
		a.log.Info("risk state", args...)
	}
}

func (a *PairActor) checkpointBeforeSend() bool {
	a.checkpoint = true
	a.settled.Store(false)
	return a.persist() == nil
}

func (a *PairActor) failSettlement(p *pendingExecution) {
	a.pending = p
	a.checkpoint = true
	a.settled.Store(false)
	if p.result.PositionsKnown {
		a.posL, a.posR = p.result.PosL, p.result.PosR
		a.syncPositionState()
	}
	a.state.Mode = strategy.ModePaused
	a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = fmt.Sprintf("execution unresolved: %v", p.result.Err) })
	if err := a.persist(); err != nil {
		a.log.Error("persist unresolved execution", "err", err)
	}
}

func (a *PairActor) resolvePending(ctx context.Context) {
	p := a.pending
	interval := a.cfg.ExecCfg.QueryInterval
	if interval <= 0 {
		interval = time.Second
	}
	if p.dustWarned && interval < subMinRetryInterval && !a.emergencyRequested.Load() && !a.draining.Load() {
		// A sub-minimum residual will not clear on the next tick; retrying
		// every query interval only spams rejected minimum-sized orders.
		interval = subMinRetryInterval
	}
	if time.Since(p.lastAttempt) < interval {
		return
	}
	p.lastAttempt = time.Now()
	if p.intervention {
		a.recoverExternalIntervention(ctx, p, p.result)
		return
	}
	l, r := a.legSpecs(p.action)
	if p.unwind {
		l.Side, r.Side = 0, 0
	}
	if a.emergencyRequested.Load() || a.draining.Load() {
		p.result = a.exec.EscalateOutstanding(p.result)
	}
	execCtx, finish := a.executionContext(ctx)
	res := a.exec.ResolveOutstanding(execCtx, l, r, p.result, p.action.Qty, p.beforeL, p.beforeR)
	finish()
	p.result = res
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		if res.PositionsKnown {
			a.posL, a.posR = res.PosL, res.PosR
			a.syncPositionState()
		}
		if errors.Is(res.Err, execution.ErrUnexpectedPosition) {
			p.stalls++
			if p.stalls >= interventionStallLimit {
				a.recoverExternalIntervention(ctx, p, res)
			}
		} else if errors.Is(res.Err, execution.ErrSubMinResidual) {
			// An untradable dust residual is not an external intervention: the
			// ledger stays intact and trading stays paused. Openings first try
			// a forward completion inside the executor; what remains here is
			// retried at subMinRetryInterval. Warn once so the pair does not
			// loop silently.
			if !p.dustWarned {
				p.dustWarned = true
				a.record(monitor.KindError, func(e *monitor.Event) {
					e.Msg = fmt.Sprintf("sub-minimum residual %.8f/%.8f cannot be traded; pair stays paused until it clears", res.PosL, res.PosR)
				})
				a.log.Warn("sub-minimum residual keeps pair paused", "posL", res.PosL, "posR", res.PosR, "err", res.Err)
			}
		} else {
			p.stalls = 0
		}
		return
	}
	if p.unwind {
		if err := a.finishUnwind(res); err != nil {
			p.stalls++
			if p.stalls >= interventionStallLimit {
				a.recoverExternalIntervention(ctx, p, res)
			} else {
				a.log.Error("unwind accounting", "err", err)
			}
			return
		}
	} else if err := a.accountExecution(p.action, p.quote, p.beforeL, p.beforeR, res); err != nil {
		p.stalls++
		if p.stalls >= interventionStallLimit {
			a.recoverExternalIntervention(ctx, p, res)
			return
		}
		a.state.Mode = strategy.ModePaused
		a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = err.Error() })
		return
	}
	a.pending = nil
	a.completeExecutionCheckpoint()
	if !a.draining.Load() && !a.emergencyRequested.Load() && !a.legacyPrices {
		a.state.Mode = strategy.ModeWarmup
	}
	if err := a.persist(); err != nil {
		a.log.Error("persist recovered execution", "err", err)
		return
	}
	a.settled.Store(!a.checkpoint)
	if p.unwind {
		if a.draining.Load() && !a.checkpoint {
			a.drainConfirmed.Store(true)
		}
		a.emergencyRequested.Store(false)
	}
}

// recoverExternalIntervention flattens whatever exposure is left after an
// account was modified outside this process, then clears the ledger without
// claiming any profit. Only our own confirmed fills' fees enter the books and
// daily PnL is marked uncertain.
func (a *PairActor) recoverExternalIntervention(ctx context.Context, p *pendingExecution, res execution.Result) {
	if !p.intervention {
		p.intervention = true
		a.record(monitor.KindError, func(e *monitor.Event) {
			e.Msg = "execution recovery stalled on inconsistent positions; verifying old orders before flattening the account"
		})
	}
	a.volAcc.pnlUncertain = true
	a.state.Mode = strategy.ModePaused
	execCtx, finish := a.executionContext(ctx)
	defer finish()
	l, r := a.legSpecs(p.action)
	if p.interrupted == nil {
		res = a.exec.ResolveTerminals(execCtx, l, r, res, p.action.Qty)
		p.result = res
		if res.Err != nil || !res.PositionsKnown || !execution.OrdersTerminal(res) {
			a.noteInterventionStall(p, res)
			return
		}
		// Keep the original fills intact. The new flatten has its own order
		// identities and must never replace unresolved original quantities.
		original := res
		p.interrupted = &original
		l.Side, r.Side = 0, 0
		if a.emergencyRequested.Load() {
			res, res.Err = a.exec.EmergencyFlatten(execCtx, l, r, 0, res.PosL, res.PosR)
		} else {
			res, res.Err = a.exec.FlattenToZero(execCtx, l, r)
		}
	} else {
		l.Side, r.Side = 0, 0
		if a.emergencyRequested.Load() || a.draining.Load() {
			res = a.exec.EscalateOutstanding(res)
		}
		res = a.exec.ResolveOutstanding(execCtx, l, r, res, 0, 0, 0)
	}
	p.result = res
	if res.PositionsKnown {
		a.posL, a.posR = res.PosL, res.PosR
		a.syncPositionState()
	}
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		a.noteInterventionStall(p, res)
		return
	}
	if err := a.finishIntervention(res, *p.interrupted); err != nil {
		a.log.Error("external intervention accounting", "err", err)
		return
	}
	a.pending = nil
	a.completeExecutionCheckpoint()
	// The account is confirmed flat and the ledger cleared. Draining,
	// emergency, legacy ledgers, and repeated interventions within the daily
	// window must stay paused instead of silently resuming trading.
	now := time.Now()
	a.interventions = append(a.interventions, now)
	a.pruneInterventions(now)
	if !a.draining.Load() && !a.emergencyRequested.Load() && !a.legacyPrices && a.interventionBudgetOK(now) {
		a.state.Mode = strategy.ModeWarmup
		a.log.Warn("external intervention completed; both legs confirmed flat, resuming warmup")
	} else {
		a.state.Mode = strategy.ModePaused
		a.record(monitor.KindError, func(e *monitor.Event) {
			e.Msg = fmt.Sprintf("external intervention completed; pair stays paused (auto-resume budget exhausted or exit mode active)")
		})
		a.log.Warn("external intervention completed; auto-resume suppressed", "interventions_24h", a.interventionCount(now))
	}
	if err := a.persist(); err != nil {
		a.log.Error("persist external intervention", "err", err)
		return
	}
	a.settled.Store(!a.checkpoint)
	if a.draining.Load() && !a.checkpoint {
		a.drainConfirmed.Store(true)
	}
	a.emergencyRequested.Store(false)
}

// noteInterventionStall keeps a long-running self-flatten visible: the
// recovery loop retries silently by design, so surface progress on a coarse
// cadence instead of leaving operators with only the silent anomaly.
func (a *PairActor) noteInterventionStall(p *pendingExecution, res execution.Result) {
	p.interventionTries++
	if p.interventionTries%10 == 0 {
		a.log.Warn("external intervention still unresolved", "attempts", p.interventionTries, "err", res.Err)
	}
}

// finishIntervention clears the step ledger once both legs are confirmed flat.
func (a *PairActor) finishIntervention(res execution.Result, interrupted ...execution.Result) error {
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown ||
		math.Abs(res.PosL) >= math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 ||
		math.Abs(res.PosR) >= math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2 {
		return fmt.Errorf("intervention requires confirmed flat positions")
	}
	nl, err := legNotional(res.L)
	if err != nil {
		return err
	}
	nr, err := legNotional(res.R)
	if err != nil {
		return err
	}
	for _, original := range interrupted {
		left, err := legNotional(original.L)
		if err != nil {
			return err
		}
		right, err := legNotional(original.R)
		if err != nil {
			return err
		}
		nl += left
		nr += right
	}
	fees := nl*a.cfg.Pair.Left.TakerFee + nr*a.cfg.Pair.Right.TakerFee
	closed := len(a.state.Steps)
	if closed > 0 {
		a.lastDir = a.state.Steps[0].Dir
	}
	a.state.Steps = nil
	a.syncCapEngaged()
	a.posL, a.posR = res.PosL, res.PosR
	a.syncCounter()
	a.syncPositionState()
	a.state.Mode = strategy.ModePaused
	a.addSettled(nl+nr, -fees)
	a.volAcc.pnlUncertain = true
	for i := 0; i < closed; i++ {
		a.record(monitor.KindStepClosed, nil)
	}
	a.record(monitor.KindExecuted, func(e *monitor.Event) { e.USD = nl + nr })
	a.record(monitor.KindPnL, func(e *monitor.Event) { e.USD = -fees })
	return nil
}

// pruneInterventions drops intervention timestamps outside the rolling 24h
// window so the auto-resume budget does not accumulate forever.
func (a *PairActor) pruneInterventions(now time.Time) {
	kept := a.interventions[:0]
	for _, t := range a.interventions {
		if now.Sub(t) <= 24*time.Hour {
			kept = append(kept, t)
		}
	}
	a.interventions = kept
}

// interventionCount returns the number of intervention recoveries in the
// rolling 24h window.
func (a *PairActor) interventionCount(now time.Time) int {
	n := 0
	for _, t := range a.interventions {
		if now.Sub(t) <= 24*time.Hour {
			n++
		}
	}
	return n
}

// interventionBudgetOK reports whether another automatic resume is allowed.
// With the default budget of one, a second intervention within 24 hours keeps
// the pair paused until an operator confirms the account.
func (a *PairActor) interventionBudgetOK(now time.Time) bool {
	max := a.cfg.Risk.maxAutoResume
	if max < 1 {
		max = 1
	}
	return a.interventionCount(now) <= max
}

func (a *PairActor) legSpecs(act strategy.Action) (execution.LegSpec, execution.LegSpec) {
	return execution.LegSpec{Venue: a.cfg.L, Inst: a.cfg.Pair.Left, Side: act.L.Side, Price: act.L.Price, MarginBps: act.ExpectedBps},
		execution.LegSpec{Venue: a.cfg.R, Inst: a.cfg.Pair.Right, Side: act.R.Side, Price: act.R.Price, MarginBps: act.ExpectedBps}
}

func (a *PairActor) apply(ctx context.Context, act strategy.Action, q spread.Quote) {
	if act.Kind == strategy.Open && a.cfg.MayOpen != nil && !a.cfg.MayOpen() {
		return
	}
	if a.draining.Load() || a.emergencyRequested.Load() || (act.Kind == strategy.Open && a.riskBlocked.Load()) {
		return
	}
	if a.cfg.DryRun && act.Kind == strategy.Open && a.cfg.MaxOpenNotional > 0 && act.Qty*math.Max(act.L.Price, act.R.Price) > a.cfg.MaxOpenNotional {
		act.Qty = domain.Floor(a.cfg.MaxOpenNotional/math.Max(act.L.Price, act.R.Price), a.cfg.Pair.SizeDecimals)
		if act.Qty <= 0 || act.Qty < a.cfg.Pair.MinQty || act.Qty*act.L.Price < a.cfg.Pair.Left.MinQuote || act.Qty*act.R.Price < a.cfg.Pair.Right.MinQuote {
			return
		}
	}
	if act.Forced && act.Kind == strategy.Close {
		a.forcedCloseAt = time.Now()
	}
	beforeL, beforeR := a.posL, a.posR
	a.resetVolumeIfNewDay(time.Now())
	var res execution.Result
	if a.cfg.DryRun {
		a.busy.Store(true)
		defer a.busy.Store(false)
		res = execution.Result{L: execution.LegResult{Filled: act.Qty, AvgPrice: act.L.Price, Resolved: true},
			R:    execution.LegResult{Filled: act.Qty, AvgPrice: act.R.Price, Resolved: true},
			PosL: beforeL + float64(act.L.Side)*act.Qty, PosR: beforeR + float64(act.R.Side)*act.Qty,
			PositionsKnown: true, SettlementKnown: true}
	} else {
		if !a.checkpointBeforeSend() {
			return
		}
		execCtx, finish := a.executionContext(ctx)
		// A drain/risk request may arrive between the decision and registering
		// the execution cancel function. Recheck before sending either IOC.
		if a.draining.Load() || a.emergencyRequested.Load() || (act.Kind == strategy.Open && a.riskBlocked.Load()) {
			finish()
			a.completeExecutionCheckpoint()
			if err := a.persist(); err != nil {
				a.log.Error("persist canceled decision", "err", err)
				return
			}
			a.settled.Store(!a.checkpoint)
			return
		}
		l, r := a.legSpecs(act)
		res = a.exec.ExecuteBounded(execCtx, l, r, act.Qty, act.Kind == strategy.Close, beforeL, beforeR, a.cfg.MaxOpenNotional)
		finish()
		if res.RequestedQty > 0 {
			act.Qty = res.RequestedQty
		}
		if errors.Is(res.Err, execution.ErrNotionalBudget) {
			a.completeExecutionCheckpoint()
			if err := a.persist(); err != nil {
				a.log.Error("persist declined opening budget", "err", err)
				return
			}
			a.settled.Store(!a.checkpoint)
			return
		}
	}
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		a.failSettlement(&pendingExecution{action: act, quote: q, beforeL: beforeL, beforeR: beforeR, result: res})
		return
	}
	if err := a.accountExecution(act, q, beforeL, beforeR, res); err != nil {
		a.failSettlement(&pendingExecution{action: act, quote: q, beforeL: beforeL, beforeR: beforeR, result: res})
		a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = err.Error() })
		return
	}
	a.completeExecutionCheckpoint()
	if err := a.persist(); err != nil {
		a.log.Error("persist execution", "err", err)
		return
	}
	a.settled.Store(!a.checkpoint)
}

func finitePositive(x float64) bool { return x > 0 && !math.IsNaN(x) && !math.IsInf(x, 0) }

func legNotional(lr execution.LegResult) (float64, error) {
	sum := 0.0
	for _, fill := range []struct{ qty, px float64 }{{lr.Filled, lr.AvgPrice}, {lr.RescueFilled, lr.RescuePrice}} {
		if math.IsNaN(fill.qty) || math.IsInf(fill.qty, 0) || fill.qty < 0 {
			return 0, fmt.Errorf("invalid fill quantity")
		}
		if fill.qty == 0 {
			continue
		}
		if !finitePositive(fill.px) {
			return 0, fmt.Errorf("missing or invalid price for confirmed fill")
		}
		sum += fill.qty * fill.px
	}
	return sum, nil
}

func closePrice(lr execution.LegResult, qty float64) (float64, error) {
	n, err := legNotional(lr)
	if err != nil {
		return 0, err
	}
	total := lr.Filled + lr.RescueFilled
	if total <= 0 || math.Abs(total-qty) > math.Max(1e-9, qty*1e-8) {
		return 0, fmt.Errorf("closing fills %.8f do not match position reduction %.8f", total, qty)
	}
	return n / total, nil
}

// accountExecution derives ledger quantity from confirmed final positions,
// not min/max of the original IOC fills. Fees include every direct/rescue fill.
// executionFees returns the total fee of a settled execution: the
// exchange-reported fees when every filled leg reported one, else the static
// rate estimate of the executed notional.
func (a *PairActor) executionFees(res execution.Result, nl, nr float64) float64 {
	fees := nl*a.cfg.Pair.Left.TakerFee + nr*a.cfg.Pair.Right.TakerFee
	if reported := res.L.Fee + res.L.RescueFee + res.R.Fee + res.R.RescueFee; reported > 0 &&
		(res.L.Filled+res.L.RescueFilled <= 0 || res.L.Fee+res.L.RescueFee > 0) &&
		(res.R.Filled+res.R.RescueFilled <= 0 || res.R.Fee+res.R.RescueFee > 0) {
		return reported
	}
	return fees
}

func (a *PairActor) accountExecution(act strategy.Action, q spread.Quote, beforeL, beforeR float64, res execution.Result) error {
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		return execution.ErrUnresolved
	}
	nl, err := legNotional(res.L)
	if err != nil {
		return err
	}
	nr, err := legNotional(res.R)
	if err != nil {
		return err
	}
	fees := a.executionFees(res, nl, nr)
	deltaPnL := -fees
	uncertain := res.L.PriceEstimated || res.R.PriceEstimated || res.L.RescuePriceEstimated || res.R.RescuePriceEstimated
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	steps := append([]strategy.Step(nil), a.state.Steps...)
	var opened *strategy.Step
	var removed []float64 // consumed quantities of every fully closed step
	var rescueCost, grossUSD float64
	switch act.Kind {
	case strategy.Open:
		dl, dr := res.PosL-beforeL, res.PosR-beforeR
		h := float64(act.L.Side) * dl
		if h < -eps || math.Abs(float64(act.R.Side)*dr-h) >= eps {
			return fmt.Errorf("final opening positions are not an equal hedge")
		}
		for _, leg := range []struct {
			result execution.LegResult
			side   domain.Side
		}{{res.L, act.L.Side}, {res.R, act.R.Side}} {
			if leg.result.RescueFilled > 0 {
				if !finitePositive(leg.result.AvgPrice) {
					return fmt.Errorf("cannot price an opening rollback")
				}
				cost := float64(leg.side) * leg.result.RescueFilled * (leg.result.RescuePrice - leg.result.AvgPrice)
				deltaPnL += cost
				rescueCost += cost
			}
		}
		if h >= eps {
			if !finitePositive(res.L.AvgPrice) || !finitePositive(res.R.AvgPrice) {
				return fmt.Errorf("opening prices unknown")
			}
			// Split the entry fee per leg: exchange-reported rates when present,
			// the static venue rate otherwise, so the retained step carries the
			// actual cost of its own fills.
			feeL, feeR := a.cfg.Pair.Left.TakerFee, a.cfg.Pair.Right.TakerFee
			if rep := res.L.Fee + res.L.RescueFee; rep > 0 && nl > 0 {
				feeL = rep / nl
			}
			if rep := res.R.Fee + res.R.RescueFee; rep > 0 && nr > 0 {
				feeR = rep / nr
			}
			st := strategy.Step{ID: a.next, Dir: act.Dir, Qty: domain.Round(h, a.cfg.Pair.SizeDecimals), OriginalQty: h,
				OpenL: res.L.AvgPrice, OpenR: res.R.AvgPrice, EntryBps: strategy.EntryBps(act.Dir, res.L.AvgPrice, res.R.AvgPrice),
				EntryFeesUSD:   h * (res.L.AvgPrice*feeL + res.R.AvgPrice*feeR),
				PriceEstimated: res.L.PriceEstimated || res.R.PriceEstimated, OpenedAt: time.Now()}
			steps = append(steps, st)
			opened = &st
		}
	case strategy.Close:
		cl, cr := math.Abs(beforeL)-math.Abs(res.PosL), math.Abs(beforeR)-math.Abs(res.PosR)
		if cl < -eps || math.Abs(cl-cr) >= eps {
			return fmt.Errorf("final closing positions are not an equal reduction")
		}
		if cl >= eps {
			lpx, err := closePrice(res.L, cl)
			if err != nil {
				return err
			}
			rpx, err := closePrice(res.R, cr)
			if err != nil {
				return err
			}
			sel := -1
			for i := range steps {
				if steps[i].ID == act.StepID {
					sel = i
					break
				}
			}
			if sel < 0 {
				return fmt.Errorf("selected step %d not found", act.StepID)
			}
			// The selected step is consumed first; any spillover past its
			// quantity consumes other steps in FIFO order, so an oversized
			// reduction no longer forces a whole-account intervention.
			order := []int{sel}
			for i := range steps {
				if i != sel {
					order = append(order, i)
				}
			}
			rest := order[1:]
			sort.SliceStable(rest, func(a, b int) bool {
				return steps[rest[a]].OpenedAt.Before(steps[rest[b]].OpenedAt)
			})
			order = append(order[:1], rest...)
			remaining := cl
			var spilled []int64
			for _, idx := range order {
				if remaining < eps {
					break
				}
				st := &steps[idx]
				uncertain = uncertain || st.PriceEstimated
				if st.OriginalQty <= 0 {
					st.OriginalQty = st.Qty
				}
				part := math.Min(remaining, st.Qty)
				if st.Qty-part < eps {
					if idx != sel {
						spilled = append(spilled, st.ID)
					}
					removed = append(removed, part)
				}
				gross := strategy.GrossProfitUSD(*st, lpx, rpx, part)
				if math.IsNaN(gross) || math.IsInf(gross, 0) {
					return fmt.Errorf("step %d lacks a trustworthy entry basis", st.ID)
				}
				feesPart := fees * part / cl
				entryFees := st.EntryFeesUSD * part / st.Qty
				partNet := gross - feesPart - entryFees
				st.EntryFeesUSD -= entryFees
				st.RealizedUSD += partNet
				st.Realized = st.RealizedUSD / (st.OriginalQty * (st.OpenL + st.OpenR) / 2) * 1e4
				st.PriceEstimated = st.PriceEstimated || res.L.PriceEstimated || res.R.PriceEstimated || res.L.RescuePriceEstimated || res.R.RescuePriceEstimated
				st.Qty = domain.Round(st.Qty-part, a.cfg.Pair.SizeDecimals)
				deltaPnL += gross
				remaining -= part
				if idx == sel {
					grossUSD = gross
				}
			}
			if remaining >= eps {
				return fmt.Errorf("close exceeds ledger")
			}
			kept := steps[:0]
			for i := range steps {
				if steps[i].Qty < eps {
					continue
				}
				kept = append(kept, steps[i])
			}
			steps = kept
			if len(spilled) > 0 {
				a.record(monitor.KindError, func(e *monitor.Event) {
					e.Msg = fmt.Sprintf("close spilled into steps %v", spilled)
				})
				a.log.Warn("close consumed part of other steps", "spilled", spilled)
			}
		}
	default:
		return fmt.Errorf("unsupported action %v", act.Kind)
	}
	expected := 0.0
	for _, st := range steps {
		expected += -float64(st.Dir) * st.Qty
	}
	if math.Abs(res.PosL-expected) >= math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 ||
		math.Abs(res.PosR+expected) >= math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2 {
		return fmt.Errorf("confirmed positions %.8f/%.8f do not match updated ledger %.8f", res.PosL, res.PosR, expected)
	}
	a.state.Steps = steps
	a.syncCapEngaged()
	a.posL, a.posR = res.PosL, res.PosR
	a.syncCounter()
	a.syncPositionState()
	if opened != nil {
		if a.lastDir != 0 && a.lastDir != act.Dir {
			a.record(monitor.KindFlip, func(e *monitor.Event) { e.Dir = act.Dir.String() })
			a.log.Info("direction flip", "from", a.lastDir.String(), "to", act.Dir.String())
		}
		a.lastDir = act.Dir
		a.next++
		a.record(monitor.KindStepOpened, func(e *monitor.Event) { e.Dir = act.Dir.String(); e.Qty = opened.Qty })
	}
	if len(removed) > 0 {
		a.lastDir = act.Dir
		for _, qty := range removed {
			a.record(monitor.KindStepClosed, func(e *monitor.Event) { e.Dir = act.Dir.String(); e.Qty = clampedQty(qty) })
		}
	}
	// Budgets use the UTC day on which settlement is confirmed, including
	// executions recovered after midnight.
	a.addSettled(nl+nr, deltaPnL)
	oneLeg := math.Abs(res.L.Filled-res.R.Filled) >= eps || res.RescueOrders > 0
	a.observeFailure(oneLeg)
	a.lat.add(res.L.Latency)
	a.lat.add(res.R.Latency)
	a.record(monitor.KindExecuted, func(e *monitor.Event) {
		e.USD = nl + nr
		e.OneLeg = oneLeg
		e.LatencyMs = float64(res.L.Latency+res.R.Latency) / float64(2*time.Millisecond)
		e.LatencyLMs = float64(res.L.Latency) / float64(time.Millisecond)
		e.LatencyRMs = float64(res.R.Latency) / float64(time.Millisecond)
		e.RescueCostUSD = rescueCost
		e.GrossUSD = grossUSD
	})
	a.record(monitor.KindPnL, func(e *monitor.Event) { e.USD = deltaPnL })
	if res.RescueOrders > 0 {
		a.record(monitor.KindRescue, func(e *monitor.Event) { e.Qty = res.RescueQty })
	}
	if uncertain {
		// Uncertainty is sticky: an estimated entry basis never becomes exact
		// through a later exact exit, and the marker is never silently dropped
		// by a day rollover (resetVolumeIfNewDay preserves it). Operators can
		// reset it explicitly in the state file after reviewing the books.
		a.volAcc.pnlUncertain = true
		a.log.Warn("PnL includes protective-price estimates", "pair", a.key)
	}
	a.feedLatency(res)
	return nil
}

func clampedQty(q float64) float64 {
	if q < 0 {
		return 0
	}
	return q
}

func (a *PairActor) drain(ctx context.Context) {
	if a.Flat() {
		a.drainConfirmed.Store(true)
		a.state.Mode = strategy.ModePaused
		return
	}
	a.unwind(ctx, false)
}

func (a *PairActor) emergency(ctx context.Context) { a.unwind(ctx, true) }

func (a *PairActor) unwind(ctx context.Context, emergency bool) {
	if a.pending != nil || !a.reconciled {
		return
	}
	a.state.Mode = strategy.ModePaused
	a.resetVolumeIfNewDay(time.Now())
	beforeL, beforeR := a.posL, a.posR
	var res execution.Result
	if a.cfg.DryRun {
		q, ok := a.quote(time.Now())
		if !ok {
			return
		}
		lp, rp := q.L.Bids[0].Price, q.R.Asks[0].Price
		if beforeL < 0 {
			lp, rp = q.L.Asks[0].Price, q.R.Bids[0].Price
		}
		res = execution.Result{L: execution.LegResult{RescueFilled: math.Abs(beforeL), RescuePrice: lp},
			R: execution.LegResult{RescueFilled: math.Abs(beforeR), RescuePrice: rp}, PositionsKnown: true, SettlementKnown: true}
		a.busy.Store(true)
		defer a.busy.Store(false)
	} else {
		if !a.checkpointBeforeSend() {
			return
		}
		execCtx, finish := a.executionContext(ctx)
		l, r := a.legSpecs(strategy.Action{})
		var err error
		res, err = a.exec.EmergencyFlatten(execCtx, l, r, 0, beforeL, beforeR)
		if res.Err == nil {
			res.Err = err
		}
		finish()
	}
	if emergency {
		a.record(monitor.KindEmergency, nil)
	}
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		a.failSettlement(&pendingExecution{beforeL: beforeL, beforeR: beforeR, result: res, unwind: true})
		return
	}
	if err := a.finishUnwind(res); err != nil {
		a.failSettlement(&pendingExecution{beforeL: beforeL, beforeR: beforeR, result: res, unwind: true})
		a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = err.Error() })
		return
	}
	a.completeExecutionCheckpoint()
	if err := a.persist(); err != nil {
		a.log.Error("persist unwind", "err", err)
		return
	}
	a.settled.Store(!a.checkpoint)
	if a.draining.Load() && !a.checkpoint {
		a.drainConfirmed.Store(true)
	}
	a.emergencyRequested.Store(false)
}

func (a *PairActor) finishUnwind(res execution.Result) error {
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown || math.Abs(res.PosL) >= math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 ||
		math.Abs(res.PosR) >= math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2 {
		return fmt.Errorf("unwind has not confirmed both legs flat")
	}
	nl, err := legNotional(res.L)
	if err != nil {
		return err
	}
	nr, err := legNotional(res.R)
	if err != nil {
		return err
	}
	fees := a.executionFees(res, nl, nr)
	delta := -fees
	uncertain := res.L.PriceEstimated || res.R.PriceEstimated || res.L.RescuePriceEstimated || res.R.RescuePriceEstimated
	qty := math.Abs(a.expectedL())
	ql := res.L.Filled + res.L.RescueFilled
	qr := res.R.Filled + res.R.RescueFilled
	matchingFills := math.Abs(ql-qty) < math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 &&
		math.Abs(qr-qty) < math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2
	if qty > 0 && !matchingFills {
		// A flat account without our matching closing fills was modified
		// externally. This includes the zero-fill case on both legs.
		uncertain = true
	}
	if qty > 0 && matchingFills {
		// Price each leg against the quantity it actually closed, not the
		// ledger quantity: fine-precision dust on one venue must not wedge the
		// unwind accounting in a retry loop after both legs are already flat.
		lp, err := closePrice(res.L, ql)
		if err != nil {
			return err
		}
		rp, err := closePrice(res.R, qr)
		if err != nil {
			return err
		}
		for _, st := range a.state.Steps {
			uncertain = uncertain || st.PriceEstimated
			gross := strategy.GrossProfitUSD(st, lp, rp, st.Qty)
			if math.IsNaN(gross) || math.IsInf(gross, 0) {
				uncertain = true
				a.record(monitor.KindError, func(e *monitor.Event) {
					e.Msg = fmt.Sprintf("legacy step %d exited without a known PnL basis; no profit claimed", st.ID)
				})
				continue
			}
			delta += gross
		}
	}
	closed := len(a.state.Steps)
	if closed > 0 {
		a.lastDir = a.state.Steps[0].Dir
	}
	a.state.Steps = nil
	a.syncCapEngaged()
	a.posL, a.posR = res.PosL, res.PosR
	a.syncCounter()
	a.syncPositionState()
	a.state.Mode = strategy.ModePaused
	a.addSettled(nl+nr, delta)
	if uncertain {
		a.volAcc.pnlUncertain = true
	}
	for i := 0; i < closed; i++ {
		a.record(monitor.KindStepClosed, nil)
	}
	a.record(monitor.KindExecuted, func(e *monitor.Event) { e.USD = nl + nr })
	a.record(monitor.KindPnL, func(e *monitor.Event) { e.USD = delta })
	return nil
}
