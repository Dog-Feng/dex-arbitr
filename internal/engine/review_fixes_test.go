package engine

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// Once a sub-minimum residual has been reported, recovery retries are
// spaced at subMinRetryInterval instead of every query interval, unless an
// emergency exit is requested.
func TestSubMinResidualRetryBackoff(t *testing.T) {
	a, l, r := auditInterventionActor(t)
	instQ := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001, MinQuote: .9}
	a.cfg.Pair.Left, a.cfg.Pair.Right = instQ, instQ
	l.pos, r.pos = .012, -.008
	a.posL, a.posR = .012, -.008
	l.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	r.actorTestVenue.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { return req.Qty, nil }
	a.pending = &pendingExecution{
		action: strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: .01},
		result: execution.Result{PosL: .012, PosR: -.008, PositionsKnown: true, SettlementKnown: true,
			Err: fmt.Errorf("%w: rescue quantity below venue minimum", execution.ErrSubMinResidual)},
	}
	a.resolvePending(context.Background())
	if a.pending == nil || !a.pending.dustWarned {
		t.Fatalf("expected a retained dust warning: %+v", a.pending)
	}
	a.pending.result.Err = fmt.Errorf("%w: again", execution.ErrSubMinResidual)
	a.pending.lastAttempt = time.Now().Add(-2 * time.Second)
	last := a.pending.lastAttempt
	a.resolvePending(context.Background())
	if a.pending == nil || !a.pending.lastAttempt.Equal(last) {
		t.Fatal("sub-minimum residual retried before the backoff elapsed")
	}
	a.emergencyRequested.Store(true)
	a.resolvePending(context.Background())
	if a.pending != nil && a.pending.lastAttempt.Equal(last) {
		t.Fatal("emergency exit must not wait for the dust backoff")
	}
}

func TestLegFeesPreferVenueReports(t *testing.T) {
	const taker = .00045
	known := execution.LegResult{Filled: .01, AvgPrice: 100, Fee: .0009,
		RescueFilled: .004, RescuePrice: 99, RescueFee: .0004}
	if got := legFees(known, taker); math.Abs(got-.0013) > 1e-12 {
		t.Fatalf("reported fees not used: %v", got)
	}
	if got := openFeeRate(known, taker); math.Abs(got-.0009/1.0) > 1e-12 {
		t.Fatalf("effective entry fee rate: %v", got)
	}
	// Missing reports fall back per component to the static estimate.
	mixed := known
	mixed.FeeUnknown = true
	want := .01*100*taker + .0004
	if got := legFees(mixed, taker); math.Abs(got-want) > 1e-12 {
		t.Fatalf("mixed fees: %v want %v", got, want)
	}
	if got := openFeeRate(mixed, taker); got != taker {
		t.Fatalf("unknown entry fee must use the taker rate: %v", got)
	}
	// A result without any fee facts (venue reports none) keeps the
	// previous static estimate exactly.
	none := execution.LegResult{Filled: .01, AvgPrice: 100, RescueFilled: .004, RescuePrice: 99}
	want = .01*100*taker + .004*99*taker
	if got := legFees(none, taker); math.Abs(got-want) > 1e-12 {
		t.Fatalf("static fallback: %v want %v", got, want)
	}
}
