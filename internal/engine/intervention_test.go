package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

type auditInterventionVenue struct {
	*actorTestVenue
	submitOverride func(context.Context, domain.OrderReq) (domain.SubmitAck, error)
	queryOverride  func(context.Context, domain.Instrument, int64) (domain.OrderUpdate, error)
}

func (v *auditInterventionVenue) Submit(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
	if v.submitOverride != nil {
		return v.submitOverride(ctx, req)
	}
	return v.actorTestVenue.Submit(ctx, req)
}
func (v *auditInterventionVenue) QueryOrder(ctx context.Context, inst domain.Instrument, cid int64) (domain.OrderUpdate, error) {
	if v.queryOverride != nil {
		return v.queryOverride(ctx, inst, cid)
	}
	return v.actorTestVenue.QueryOrder(ctx, inst, cid)
}
func auditInterventionActor(t *testing.T) (*PairActor, *actorTestVenue, *auditInterventionVenue) {
	t.Helper()
	a := settlementReviewActor(t)
	l := a.cfg.L.(*actorTestVenue)
	r := &auditInterventionVenue{actorTestVenue: a.cfg.R.(*actorTestVenue)}
	a.cfg.R = r
	a.cfg.DryRun = false
	a.cfg.MaxBookAge = time.Minute
	cfg := execution.DefaultConfig()
	cfg.AckTimeout, cfg.QueryInterval, cfg.SettleDelay, cfg.RescueInterval = time.Millisecond, time.Millisecond, 0, 0
	cfg.QueryRetries = 1
	a.cfg.ExecCfg = cfg
	tr, err := execution.NewTracker(context.Background(), l, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	a.exec = execution.NewExecutor(cfg, tr, execution.NewIDGen(), a.book, nil, nil, func(string) {})
	a.state.Steps = []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: .01, OriginalQty: .01, OpenL: 100, OpenR: 100}}
	a.next = 2
	a.posL, a.posR = .01, -.01
	a.syncCounter()
	a.syncPositionState()
	a.reconciled = true
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 100)
	l.submit = func(context.Context, domain.OrderReq) (float64, error) { return 0, nil }
	r.actorTestVenue.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	return a, l, r
}
func auditInterventionResolveThree(a *PairActor) {
	for i := 0; i < interventionStallLimit; i++ {
		if a.pending == nil {
			return
		}
		a.pending.lastAttempt = time.Time{}
		a.resolvePending(context.Background())
	}
}

func TestInterventionWaitsForUnknownOriginalBeforeFlatten(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	l.pos, r.pos = 0, -.01
	var original domain.OrderReq
	submits, queries := 0, 0
	terminal := false
	r.submitOverride = func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
		submits++
		if submits == 1 {
			original = req
			return domain.SubmitAck{ClientID: req.ClientID}, nil
		}
		return r.actorTestVenue.Submit(ctx, req)
	}
	r.queryOverride = func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		if cid == original.ClientID {
			queries++
			if terminal {
				return domain.OrderUpdate{ClientID: cid, Status: domain.StatusFilled, FilledQty: original.Qty, FilledQuote: original.Qty * original.Price}, nil
			}
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}
	act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01, L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	if a.pending == nil || !errors.Is(a.pending.result.Err, execution.ErrUnresolved) || !errors.Is(a.pending.result.Err, execution.ErrUnexpectedPosition) {
		t.Fatalf("missing mixed inconsistent/unknown result: %+v", a.pending)
	}
	auditInterventionResolveThree(a)
	if a.pending == nil || !a.pending.intervention || a.pending.interrupted != nil || !a.checkpoint || a.Flat() || submits != 1 || queries < 4 {
		t.Fatalf("unknown IOC was abandoned: pending=%+v checkpoint=%v flat=%v submits=%d queries=%d", a.pending, a.checkpoint, a.Flat(), submits, queries)
	}
	// The old opening IOC lands while it is still retained, before any flatten.
	r.mu.Lock()
	r.pos += float64(original.Side) * original.Qty
	r.mu.Unlock()
	terminal = true
	a.pending.lastAttempt = time.Time{}
	a.resolvePending(context.Background())
	if a.pending != nil || a.checkpoint || !a.Flat() || submits != 2 || len(a.state.Steps) != 0 || !a.volAcc.pnlUncertain {
		t.Fatalf("terminal IOC did not recover safely: pending=%+v flat=%v submits=%d", a.pending, a.Flat(), submits)
	}
	if a.state.Mode != strategy.ModeWarmup {
		t.Fatalf("mode after successful intervention = %v, want warmup so trading resumes", a.state.Mode)
	}
	r.mu.Lock()
	actual := r.pos
	r.mu.Unlock()
	if actual != 0 {
		t.Fatalf("late original left exposure %g", actual)
	}
}

func TestSubMinResidualStallsEscalateToIntervention(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	instQ := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001, MinQuote: .9}
	a.cfg.Pair.Left, a.cfg.Pair.Right = instQ, instQ
	l.pos, r.pos = .012, -.008
	a.posL, a.posR = .012, -.008
	l.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	r.actorTestVenue.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	a.pending = &pendingExecution{
		action:  strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01},
		beforeL: 0, beforeR: 0,
		result: execution.Result{PosL: .012, PosR: -.008, PositionsKnown: true, SettlementKnown: true,
			Err: fmt.Errorf("%w: rescue quantity below venue minimum", execution.ErrSubMinResidual)},
		stalls: interventionStallLimit - 1,
	}
	a.resolvePending(context.Background())
	if a.pending == nil || !a.pending.intervention || a.pending.stalls != interventionStallLimit {
		t.Fatalf("sub-min residual did not escalate: pending=%+v", a.pending)
	}
}

func TestInterventionRetainsPartialUnknownFlatten(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	l.pos, r.pos = 0, -.01
	submits, queries := 0, 0
	terminal := false
	var flatten domain.OrderReq
	r.submitOverride = func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
		submits++
		if submits == 1 {
			r.actorTestVenue.submit = func(context.Context, domain.OrderReq) (float64, error) { return 0, nil }
			return r.actorTestVenue.Submit(ctx, req)
		}
		if submits == 2 {
			flatten = req
			r.mu.Lock()
			r.pos += float64(req.Side) * .004
			r.mu.Unlock()
			return domain.SubmitAck{ClientID: req.ClientID}, nil
		}
		r.actorTestVenue.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
		return r.actorTestVenue.Submit(ctx, req)
	}
	r.queryOverride = func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		if cid == flatten.ClientID {
			queries++
			if terminal {
				return domain.OrderUpdate{ClientID: cid, Status: domain.StatusCanceled, FilledQty: .004, FilledQuote: .004 * flatten.Price}, nil
			}
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: .005, L: strategy.Leg{Side: domain.Sell, Price: 100}, R: strategy.Leg{Side: domain.Buy, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	auditInterventionResolveThree(a)
	if a.pending == nil || a.pending.interrupted == nil || !a.checkpoint || a.Flat() || submits != 2 || flatten.Qty != .01 {
		t.Fatalf("flatten abandoned: pending=%+v checkpoint=%v submits=%d", a.pending, a.checkpoint, submits)
	}
	a.pending.lastAttempt = time.Time{}
	a.resolvePending(context.Background())
	if a.pending == nil || submits != 2 || queries < 2 {
		t.Fatal("unknown flatten was replaced instead of queried")
	}
	terminal = true
	a.pending.lastAttempt = time.Time{}
	a.resolvePending(context.Background())
	if a.pending != nil || !a.Flat() || submits != 3 || len(a.state.Steps) != 0 {
		t.Fatalf("partial flatten did not resume: pending=%+v flat=%v submits=%d", a.pending, a.Flat(), submits)
	}
	if math.Abs(a.volAcc.volumeUSD-.01*flatten.Price) > 1e-9 {
		t.Fatalf("flatten counted twice or lost fills: volume=%g", a.volAcc.volumeUSD)
	}
}

func TestInterventionAlreadyFlatClearsLedgerAfterTerminalOrders(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	l.pos, r.pos = 0, 0
	r.actorTestVenue.submit = func(context.Context, domain.OrderReq) (float64, error) { return 0, nil }
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: .01, L: strategy.Leg{Side: domain.Sell, Price: 100}, R: strategy.Leg{Side: domain.Buy, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	if a.pending == nil || !errors.Is(a.pending.result.Err, execution.ErrUnexpectedPosition) {
		t.Fatalf("missing position mismatch: %+v", a.pending)
	}
	auditInterventionResolveThree(a)
	if a.pending != nil || a.checkpoint || len(a.state.Steps) != 0 || !a.Flat() || !a.volAcc.pnlUncertain || a.volAcc.pnlUSD != 0 || a.volAcc.volumeUSD != 0 {
		t.Fatalf("external flat did not clear conservatively: pending=%+v checkpoint=%v steps=%d flat=%v budget=%+v", a.pending, a.checkpoint, len(a.state.Steps), a.Flat(), a.volAcc)
	}
}

func TestInterventionAccountsOriginalAndFlattenFillsOnce(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	l.pos, r.pos = 0, -.01
	var orders []domain.OrderReq
	r.submitOverride = func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
		orders = append(orders, req)
		if len(orders) == 1 {
			r.actorTestVenue.submit = func(context.Context, domain.OrderReq) (float64, error) { return .002, nil }
		} else {
			r.actorTestVenue.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
		}
		return r.actorTestVenue.Submit(ctx, req)
	}
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: .005, L: strategy.Leg{Side: domain.Sell, Price: 100}, R: strategy.Leg{Side: domain.Buy, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	auditInterventionResolveThree(a)
	if !a.Flat() || a.pending != nil || len(orders) != 2 {
		t.Fatalf("intervention incomplete: orders=%+v pending=%+v", orders, a.pending)
	}
	notional := .002*orders[0].Price + .008*orders[1].Price
	if math.Abs(a.volAcc.volumeUSD-notional) > 1e-9 || math.Abs(a.volAcc.pnlUSD+notional*a.cfg.Pair.Right.TakerFee) > 1e-9 || !a.volAcc.pnlUncertain {
		t.Fatalf("lost original or duplicate flatten fill: got %+v notional=%g", a.volAcc, notional)
	}
	persisted, err := a.cfg.Store.Load(a.key)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Unresolved || math.Abs(persisted.VolumeUSD-notional) > 1e-9 {
		t.Fatalf("incorrect persisted intervention: %+v", persisted)
	}
}

func TestUnwindExternalZeroFillMarksPnLUncertain(t *testing.T) {
	for _, rightFill := range []float64{0, .004, .01} {
		a, _, _ := auditInterventionActor(t)
		res := execution.Result{R: execution.LegResult{Filled: rightFill, AvgPrice: 100}, PositionsKnown: true, SettlementKnown: true}
		if err := a.finishUnwind(res); err != nil {
			t.Fatal(err)
		}
		fees := rightFill * 100 * a.cfg.Pair.Right.TakerFee
		if !a.volAcc.pnlUncertain || math.Abs(a.volAcc.pnlUSD+fees) > 1e-9 || len(a.state.Steps) != 0 {
			t.Fatalf("external exit claimed gross PnL for missing fills: %+v", a.volAcc)
		}
	}
}

func TestApplyDryRunRejectsOverBudgetOpen(t *testing.T) {
	a := settlementReviewActor(t)
	a.cfg.MaxOpenNotional = .0001 // too small for a single native quantity unit
	act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .02,
		L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	if len(a.state.Steps) != 0 || a.pending != nil || a.checkpoint {
		t.Fatalf("paper open exceeded reservation: %+v", a.state)
	}
}

func TestInterventionRetainsFullInferredFlattenUntilTerminal(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	l.pos, r.pos = 0, -.01
	submits := 0
	terminal := false
	var flatten domain.OrderReq
	r.submitOverride = func(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
		submits++
		if submits == 1 {
			r.actorTestVenue.submit = func(context.Context, domain.OrderReq) (float64, error) { return 0, nil }
			return r.actorTestVenue.Submit(ctx, req)
		}
		flatten = req
		// The old/external close wins the race; this new reduction is still
		// unknown and may remain executable against future inventory.
		r.mu.Lock()
		r.pos = 0
		r.mu.Unlock()
		return domain.SubmitAck{ClientID: req.ClientID}, nil
	}
	r.queryOverride = func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		if cid == flatten.ClientID && terminal {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusCanceled}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: .005, L: strategy.Leg{Side: domain.Sell, Price: 100}, R: strategy.Leg{Side: domain.Buy, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	auditInterventionResolveThree(a)
	if a.pending == nil || a.pending.interrupted == nil || a.Flat() || !a.checkpoint || a.pending.result.SettlementKnown || submits != 2 {
		t.Fatalf("flat inference released a live reduction: pending=%+v flat=%v submits=%d", a.pending, a.Flat(), submits)
	}
	terminal = true
	a.pending.lastAttempt = time.Time{}
	a.resolvePending(context.Background())
	if a.pending != nil || !a.Flat() || submits != 2 || a.volAcc.volumeUSD != 0 || a.volAcc.pnlUSD != 0 || !a.volAcc.pnlUncertain {
		t.Fatalf("zero terminal outcome did not correct inference exactly once: pending=%+v budget=%+v", a.pending, a.volAcc)
	}
}

func TestApplyBudgetShrinkRetainsActualUnknownQuantity(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	a.state.Steps = nil
	a.next = 1
	a.posL, a.posR = 0, 0
	a.syncCounter()
	a.syncPositionState()
	l.pos, r.pos = 0, 0
	a.cfg.MaxOpenNotional = 1
	l.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	terminal := false
	var original domain.OrderReq
	r.submitOverride = func(_ context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
		original = req
		r.mu.Lock()
		r.pos -= .004
		r.mu.Unlock()
		return domain.SubmitAck{ClientID: req.ClientID}, nil
	}
	r.queryOverride = func(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
		if terminal {
			return domain.OrderUpdate{ClientID: cid, Status: domain.StatusCanceled, FilledQty: .004, FilledQuote: .004 * original.Price}, nil
		}
		return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
	}
	act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01, L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 100}}
	a.apply(context.Background(), act, spread.Quote{})
	if a.pending == nil || a.pending.action.Qty != .0099 || original.Qty != .0099 || a.pending.result.RequestedQty != .0099 {
		t.Fatalf("pending retained uncapped quantity: original=%+v pending=%+v", original, a.pending)
	}
	terminal = true
	a.pending.lastAttempt = time.Time{}
	a.resolvePending(context.Background())
	if a.pending != nil || a.checkpoint || len(a.state.Steps) != 1 || math.Abs(a.state.Steps[0].Qty-.004) > 1e-9 {
		t.Fatalf("capped partial opening did not settle: pending=%+v state=%+v", a.pending, a.state)
	}
}
