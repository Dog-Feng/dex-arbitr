package engine

import (
	"context"
	"math"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// completeExecutionCheckpoint keeps a restart's unknown old transactions
// quarantined even after our own reduce-only execution has finished. Only
// recoverCheckpointExit may certify this actor as settled in that phase.
func (a *PairActor) completeExecutionCheckpoint() {
	a.checkpoint = a.checkpointExit
}

// recoverCheckpointExit permits protective reductions during restart recovery
// without treating a momentarily flat snapshot as proof that old IOC requests
// cannot still arrive. The durable exit marker survives another crash; after a
// restart its observation period begins again. All newly submitted orders keep
// the normal pending execution recovery path.
func (a *PairActor) recoverCheckpointExit(ctx context.Context, now time.Time) {
	if a.pending != nil {
		a.resolvePending(ctx)
		return
	}
	if !a.checkpointExit {
		a.checkpointExit = true
		a.checkpointExitSince = now
		a.volAcc.pnlUncertain = true
		a.settled.Store(false)
		if err := a.persist(); err != nil {
			return
		}
	}
	if a.checkpointExitSince.IsZero() {
		a.checkpointExitSince = now
	}
	a.settled.Store(false)
	a.state.Mode = strategy.ModePaused
	pl, err := execution.MarketPosition(ctx, a.cfg.L, a.cfg.Pair.Left.MarketID)
	if err != nil {
		a.log.Error("checkpoint exit left positions", "err", err)
		return
	}
	pr, err := execution.MarketPosition(ctx, a.cfg.R, a.cfg.Pair.Right.MarketID)
	if err != nil {
		a.log.Error("checkpoint exit right positions", "err", err)
		return
	}
	a.posL, a.posR = pl, pr
	a.syncPositionState()
	// A requested account exit may reduce confirmed live positions even if
	// they no longer equal the old ledger. It never submits an opening order,
	// and the quarantine prevents premature flat/slot certification.
	a.reconciled = true
	flat := math.Abs(pl) < math.Pow10(-a.cfg.Pair.Left.SizeDecimals)/2 &&
		math.Abs(pr) < math.Pow10(-a.cfg.Pair.Right.SizeDecimals)/2
	if !flat || len(a.state.Steps) != 0 {
		a.unwind(ctx, a.emergencyRequested.Load())
		return
	}
	grace := a.cfg.CheckpointGrace
	if grace <= 0 {
		grace = 15 * time.Minute
	}
	if now.Sub(a.checkpointExitSince) < grace {
		return
	}
	// Run calls this only with no pending execution. Our reduce-only orders
	// have settled, old signed transactions have aged out, and both markets
	// have just been read flat. Unknown PnL remains sticky after release.
	a.checkpointExit = false
	a.checkpoint = false
	if err := a.persist(); err != nil {
		return
	}
	a.settled.Store(true)
	a.startupRecovered.Store(true)
	if a.draining.Load() {
		a.drainConfirmed.Store(true)
	}
	a.emergencyRequested.Store(false)
	a.log.Warn("checkpoint exit quarantine completed with both markets flat")
}
