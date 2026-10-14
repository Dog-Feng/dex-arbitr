package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

// rejectOpeningVenue injects a definite local failure. Normal allowance and
// market movement cannot turn this request into a real right-leg order.
type rejectOpeningVenue struct{ exchange.Venue }

func (v rejectOpeningVenue) Submit(ctx context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	if !r.ReduceOnly {
		return domain.SubmitAck{ClientID: r.ClientID}, fmt.Errorf("%w: selfcheck rescue injection", domain.ErrRejected)
	}
	return v.Venue.Submit(ctx, r)
}

type liveAction struct {
	l, r                  execution.LegSpec
	qty, beforeL, beforeR float64
	result                execution.Result
}

func settled(r execution.Result) bool {
	return r.Err == nil && r.PositionsKnown && r.SettlementKnown
}

func flat(l, r float64) bool {
	return l == 0 && r == 0
}

func readPositions(ctx context.Context, L, R exchange.Venue, pair symbols.Pair) (float64, float64, error) {
	l, errL := execution.MarketPosition(ctx, L, pair.Left.MarketID)
	r, errR := execution.MarketPosition(ctx, R, pair.Right.MarketID)
	if err := errors.Join(errL, errR); err != nil {
		return 0, 0, err
	}
	if math.IsNaN(l) || math.IsNaN(r) || math.IsInf(l, 0) || math.IsInf(r, 0) {
		return 0, 0, fmt.Errorf("nonfinite account position")
	}
	return l, r, nil
}

func liveFlow(ctx context.Context, L, R exchange.Venue, pair symbols.Pair, qty, depthMult float64, maxAge time.Duration, rescue bool, cfg execution.Config, out io.Writer) (err error) {
	if !L.CanTrade() || !R.CanTrade() {
		return fmt.Errorf("both venues need trading credentials")
	}
	if ok, why := pair.ValidQty(qty, 0); !ok {
		return fmt.Errorf("live quantity: %s", why)
	}
	if depthMult <= 0 || math.IsNaN(depthMult) || math.IsInf(depthMult, 0) || math.IsInf(qty*depthMult, 0) {
		return fmt.Errorf("invalid depth multiplier")
	}
	pl, pr, err := readPositions(ctx, L, R, pair)
	if err != nil {
		return fmt.Errorf("baseline positions: %w", err)
	}
	if !flat(pl, pr) {
		return fmt.Errorf("selfcheck requires zero baseline positions: %.8f/%.8f", pl, pr)
	}

	// Keep observations alive during bounded cleanup after the trading context
	// expires. The defer always cancels and joins our local consumers.
	streamCtx, stopStreams := context.WithCancel(context.WithoutCancel(ctx))
	defer stopStreams()
	tr, err := execution.NewTracker(streamCtx, L, R)
	if err != nil {
		return err
	}
	defer tr.Close()
	books, err := subscribeBooks(streamCtx, L, R, pair, maxAge)
	if err != nil {
		return err
	}
	bookCtx := ctx
	ex := execution.NewExecutor(cfg, tr, execution.NewIDGen(), func(venue string, marketID int) (domain.Book, bool) {
		return books.latest(bookCtx, venue, marketID)
	}, nil, nil, nil)
	var pending *liveAction
	// Once an order is attempted, every exit first resolves its outstanding
	// outcomes. Flattening while an earlier order can still fill is unsafe.
	defer func() {
		if pending == nil {
			return
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Minute)
		defer cancel()
		bookCtx = cleanupCtx
		a := pending
		if !settled(a.result) {
			recoverAction(cleanupCtx, ex, a, cfg.QueryInterval)
			if !settled(a.result) {
				err = errors.Join(err, fmt.Errorf("cleanup cannot confirm prior orders: %w", resultError(a.result)))
				return
			}
		}
		pl, pr, positionErr := awaitPositions(cleanupCtx, L, R, pair, cfg.QueryInterval)
		if positionErr != nil {
			err = errors.Join(err, fmt.Errorf("cleanup positions: %w", positionErr))
			return
		}
		if !flat(pl, pr) {
			l := execution.LegSpec{Venue: L, Inst: pair.Left}
			r := execution.LegSpec{Venue: R, Inst: pair.Right}
			res, flattenErr := ex.EmergencyFlatten(cleanupCtx, l, r, 0, pl, pr)
			if flattenErr != nil || !settled(res) {
				cleanup := liveAction{l: l, r: r, beforeL: pl, beforeR: pr, result: res}
				recoverAction(cleanupCtx, ex, &cleanup, cfg.QueryInterval)
				res = cleanup.result
			}
			if !settled(res) || !flat(res.PosL, res.PosR) {
				err = errors.Join(err, fmt.Errorf("cleanup incomplete: positions %.8f/%.8f: %w", res.PosL, res.PosR, resultError(res)))
				return
			}
		}
		pl, pr, positionErr = awaitPositions(cleanupCtx, L, R, pair, cfg.QueryInterval)
		if positionErr != nil || !flat(pl, pr) {
			err = errors.Join(err, fmt.Errorf("final positions not confirmed flat: %.8f/%.8f: %w", pl, pr, errors.Join(positionErr, nonflatError(pl, pr))))
		}
	}()

	legs := func(ctx context.Context, leftSide, rightSide domain.Side, right exchange.Venue) (execution.LegSpec, execution.LegSpec, error) {
		if err := books.refresh(ctx); err != nil {
			return execution.LegSpec{}, execution.LegSpec{}, err
		}
		if err := validPairQty(pair, qty, books.bl, books.br); err != nil {
			return execution.LegSpec{}, execution.LegSpec{}, err
		}
		makeLeg := func(v exchange.Venue, inst domain.Instrument, b domain.Book, side domain.Side) execution.LegSpec {
			px := b.Bids[0].Price
			if side == domain.Buy {
				px = b.Asks[0].Price
			}
			return execution.LegSpec{Venue: v, Inst: inst, Side: side, Price: px}
		}
		return makeLeg(L, pair.Left, books.bl, leftSide), makeLeg(right, pair.Right, books.br, rightSide), nil
	}
	if err := books.refresh(ctx); err != nil {
		return err
	}
	q, ok := spread.Compute(books.bl, books.br)
	if !ok {
		return fmt.Errorf("live direction requires two valid books")
	}
	ls, rs := domain.Buy, domain.Sell
	if q.SL > q.LS {
		ls, rs = domain.Sell, domain.Buy
	}
	if !hasDepth(books.bl, ls, qty*depthMult) || !hasDepth(books.br, rs, qty*depthMult) {
		return fmt.Errorf("insufficient top-of-book depth for live round trip")
	}
	l, r, err := legs(ctx, ls, rs, R)
	if err != nil {
		return err
	}
	fmt.Fprintln(out, "== 6. LIVE round trip ==")
	a := liveAction{l: l, r: r, qty: qty}
	a.result = ex.Execute(ctx, l, r, qty, false, 0, 0)
	pending = &a
	report(out, a.result)
	if !settled(a.result) {
		return fmt.Errorf("open outcome: %w", resultError(a.result))
	}
	hedge := math.Min(math.Abs(a.result.PosL), math.Abs(a.result.PosR))
	if a.result.L.Filled <= 0 || a.result.R.Filled <= 0 || hedge <= 0 {
		return fmt.Errorf("round trip did not open both legs")
	}
	l, r, err = legs(ctx, ls.Opposite(), rs.Opposite(), R)
	if err != nil {
		return err
	}
	closeAction := liveAction{l: l, r: r, qty: pair.AlignQty(hedge), beforeL: a.result.PosL, beforeR: a.result.PosR}
	closeAction.result = ex.Execute(ctx, l, r, closeAction.qty, true, closeAction.beforeL, closeAction.beforeR)
	pending = &closeAction
	report(out, closeAction.result)
	if !settled(closeAction.result) || !flat(closeAction.result.PosL, closeAction.result.PosR) {
		return fmt.Errorf("close outcome: %w", resultError(closeAction.result))
	}
	if !rescue {
		return nil
	}

	pl, pr, err = readPositions(ctx, L, R, pair)
	if err != nil {
		return err
	}
	if !flat(pl, pr) {
		return fmt.Errorf("rescue test requires flat positions")
	}
	l, r, err = legs(ctx, domain.Buy, domain.Sell, rejectOpeningVenue{R})
	if err != nil {
		return err
	}
	if !hasDepth(books.bl, domain.Buy, qty*depthMult) {
		return fmt.Errorf("insufficient left depth for rescue test")
	}
	fmt.Fprintln(out, "== 7. RESCUE: reject right opening order locally ==")
	rescueAction := liveAction{l: l, r: r, qty: qty}
	rescueAction.result = ex.Execute(ctx, l, r, qty, false, 0, 0)
	pending = &rescueAction
	report(out, rescueAction.result)
	res := rescueAction.result
	if !settled(res) || !flat(res.PosL, res.PosR) {
		return fmt.Errorf("rescue outcome: %w", resultError(res))
	}
	if res.L.Filled <= 0 || res.R.Filled != 0 || res.RescueOrders <= 0 || res.RescueQty <= 0 || res.L.RescueFilled <= 0 {
		return fmt.Errorf("rescue was not exercised: fills %.8f/%.8f, rescue orders %d, rescue fill %.8f", res.L.Filled, res.R.Filled, res.RescueOrders, res.L.RescueFilled)
	}
	return nil
}

func recoverAction(ctx context.Context, ex *execution.Executor, a *liveAction, interval time.Duration) {
	for !settled(a.result) {
		a.result = ex.ResolveOutstanding(ctx, a.l, a.r, a.result, a.qty, a.beforeL, a.beforeR)
		if settled(a.result) {
			return
		}
		if err := waitRecovery(ctx, interval); err != nil {
			a.result.Err = errors.Join(a.result.Err, err)
			return
		}
	}
}

func awaitPositions(ctx context.Context, L, R exchange.Venue, pair symbols.Pair, interval time.Duration) (float64, float64, error) {
	for {
		l, r, err := readPositions(ctx, L, R, pair)
		if err == nil {
			return l, r, nil
		}
		if waitErr := waitRecovery(ctx, interval); waitErr != nil {
			return 0, 0, errors.Join(err, waitErr)
		}
	}
}

func waitRecovery(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func hasDepth(b domain.Book, side domain.Side, need float64) bool {
	if !validBook(b) {
		return false
	}
	if side == domain.Buy {
		return b.Asks[0].Size >= need
	}
	return b.Bids[0].Size >= need
}

func nonflatError(l, r float64) error {
	if flat(l, r) {
		return nil
	}
	return fmt.Errorf("nonzero positions")
}

func resultError(r execution.Result) error {
	if r.Err != nil {
		return r.Err
	}
	if !r.PositionsKnown || !r.SettlementKnown {
		return execution.ErrUnresolved
	}
	return fmt.Errorf("positions %.8f/%.8f", r.PosL, r.PosR)
}

func report(out io.Writer, r execution.Result) {
	fmt.Fprintf(out, "     L: cid %d %s filled %.8f resolved=%v err=%v\n", r.L.ClientID, r.L.Status, r.L.Filled, r.L.Resolved, r.L.SubmitErr)
	fmt.Fprintf(out, "     R: cid %d %s filled %.8f resolved=%v err=%v\n", r.R.ClientID, r.R.Status, r.R.Filled, r.R.Resolved, r.R.SubmitErr)
	fmt.Fprintf(out, "     positions %.8f/%.8f known=%v settled=%v | rescue orders %d qty %.8f | %v\n", r.PosL, r.PosR, r.PositionsKnown, r.SettlementKnown, r.RescueOrders, r.RescueQty, r.Err)
}
