package engine

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func checkpointState() store.PairState {
	return store.PairState{NextID: 2, Unresolved: true, Steps: []strategy.Step{{
		ID: 1, Dir: strategy.DirLS, Qty: .5, OpenL: 100, OpenR: 101,
	}}}
}

func TestCheckpointClearsAfterStablePositionMatch(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	c.key = pairKey("X", "L", "R", false)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.pos, r.pos = .5, -.5
	if err := rt.store.Save(c.key, checkpointState()); err != nil {
		t.Fatal(err)
	}
	a, err := rt.buildActor(c.pair, .1, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.CheckpointCheckEvery, a.cfg.CheckpointGrace = time.Millisecond, 20*time.Millisecond
	if err := a.reconcile(context.Background()); err != nil {
		t.Fatalf("matching positions must not error a checkpointed restart: %v", err)
	}
	if !a.reconciled || !a.checkpoint || a.Flat() || a.state.Mode != strategy.ModePaused {
		t.Fatalf("checkpoint must survive until stable: reconciled=%v checkpoint=%v flat=%v mode=%s",
			a.reconciled, a.checkpoint, a.Flat(), a.state.Mode)
	}
	// The grace window has not elapsed yet, so an early re-check keeps it.
	a.recoverCheckpoint(context.Background(), time.Now())
	if !a.checkpoint {
		t.Fatal("checkpoint cleared before the stability window elapsed")
	}
	time.Sleep(25 * time.Millisecond)
	a.recoverCheckpoint(context.Background(), time.Now())
	if a.checkpoint || a.state.Mode != strategy.ModeWarmup || !a.volAcc.pnlUncertain || !a.settled.Load() {
		t.Fatalf("checkpoint not cleared after stable match: checkpoint=%v mode=%s uncertain=%v settled=%v",
			a.checkpoint, a.state.Mode, a.volAcc.pnlUncertain, a.settled.Load())
	}
	st, err := rt.store.Load(c.key)
	if err != nil || st.Unresolved {
		t.Fatalf("cleared checkpoint was not persisted: %+v, %v", st, err)
	}
}

func TestCheckpointKeepsPauseWhilePositionsDiverge(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	c.key = pairKey("X", "L", "R", false)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.pos, r.pos = .5, -.4 // one leg differs from the recorded step
	if err := rt.store.Save(c.key, checkpointState()); err != nil {
		t.Fatal(err)
	}
	a, err := rt.buildActor(c.pair, .1, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.reconcile(context.Background()); err == nil {
		t.Fatal("divergent positions must keep the pair paused")
	}
	if !a.checkpoint || a.reconciled {
		t.Fatalf("mismatch must not clear the checkpoint: checkpoint=%v reconciled=%v", a.checkpoint, a.reconciled)
	}
}

func TestRestoredExposureRespectsAccountNotionalBudget(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.Auto.TopY = 2
	// Each saved step reserves max(leg notional)=50.5; the second restored
	// pair crosses the 60 cap and must drain while the first keeps trading.
	rt.cfg.Auto.MaxTotalNotional = 60
	a, b := lifecycleCandidate("A", "L", "R", 1, 0), lifecycleCandidate("B", "L", "R", 2, 0)
	for _, c := range []*candidate{a, b} {
		if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
			t.Fatal(err)
		}
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(context.Background(), []*candidate{a, b}, active, &wg); err != nil {
		t.Fatal(err)
	}
	if active[a.key] == nil || active[a.key].draining || active[b.key] == nil || !active[b.key].draining {
		t.Fatalf("restored exposure must respect the account notional budget: active=%v", active)
	}
}

func TestUnwindAccountingToleratesFinePrecisionDust(t *testing.T) {
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	left := domain.Instrument{MarketID: 1, SizeDecimals: 2, PriceDecimals: 2, TakerFee: .001}
	right := domain.Instrument{MarketID: 2, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001}
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("TEST", left, right),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
		Store: s, Est: actorTestEstimator{}, MaxBookAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a.state.Steps = []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: .01, OriginalQty: .01, OpenL: 100, OpenR: 100.5}}
	a.syncCounter()
	// The right venue closed one unit of fine precision beyond the ledger.
	res := execution.Result{
		L:    execution.LegResult{Filled: .01, AvgPrice: 100.2, Resolved: true},
		R:    execution.LegResult{Filled: .01001, AvgPrice: 100.3, Resolved: true},
		PosL: 0, PosR: 0, PositionsKnown: true, SettlementKnown: true,
	}
	if err := a.finishUnwind(res); err != nil {
		t.Fatalf("dust unwind wedged in accounting: %v", err)
	}
	if len(a.state.Steps) != 0 || math.Abs(a.posL) > 0 || math.Abs(a.posR) > 0 {
		t.Fatalf("unwind did not clear the ledger: steps=%d posL=%f posR=%f", len(a.state.Steps), a.posL, a.posR)
	}
}

func TestLegacyLedgerWithCheckpointCanStillDrain(t *testing.T) {
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("TEST", inst, inst),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
		Store: s, Est: actorTestEstimator{}, MaxBookAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// Legacy step without entry prices, an unresolved checkpoint from the
	// previous run, and matching positions.
	a.state = strategy.State{Mode: strategy.ModePaused, Steps: []strategy.Step{{ID: 1, Dir: strategy.DirLS, Qty: .5}}}
	a.syncCounter()
	a.posL, a.posR = .5, -.5
	a.syncPositionState()
	a.reconciled, a.legacyPrices, a.checkpoint = true, true, true
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 101)
	a.draining.Store(true)
	a.drain(context.Background())
	if len(a.state.Steps) != 0 || !a.Flat() || !a.drainConfirmed.Load() {
		t.Fatalf("legacy+checkpoint pair could not drain: steps=%d flat=%v confirmed=%v",
			len(a.state.Steps), a.Flat(), a.drainConfirmed.Load())
	}
}

func TestAutoActorRefreshesSizingFromSharedView(t *testing.T) {
	rt := lifecycleRuntime(t)
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	c.notional, c.depthRatio = 10, .25
	c.bookL = bk(100, 100.1, 50, 50)
	c.bookR = bk(100.05, 100.15, 50, 50)
	a, err := rt.buildActor(c.pair, .5, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.View = c
	want, ok := c.SizedQty()
	if !ok {
		t.Fatal("sizing unavailable")
	}
	a.refreshSizing()
	if a.cfg.Params.StepQty != want || want == .5 {
		t.Fatalf("restored actor kept stale step quantity: got %v want %v", a.cfg.Params.StepQty, want)
	}
}

func TestExternalInterventionRecoveryFlattensAndClearsLedger(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	c.key = pairKey("X", "L", "R", false)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.pos, r.pos = 0, -.5 // the left leg was closed outside this process
	for _, v := range []*actorTestVenue{l, r} {
		v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
			return req.Qty, nil
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tr, err := execution.NewTracker(ctx, l, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	rt.tracker, rt.ids = tr, execution.NewIDGen()
	if err := rt.store.Save(c.key, store.PairState{NextID: 2, Steps: []strategy.Step{{
		ID: 1, Dir: strategy.DirLS, Qty: .5, OpenL: 100, OpenR: 101,
	}}}); err != nil {
		t.Fatal(err)
	}
	a, err := rt.buildActor(c.pair, .1, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.ExecCfg.QueryInterval = time.Millisecond
	a.reconciled = true
	a.checkpoint = true
	a.posL, a.posR = 0, -.5
	a.syncPositionState()
	a.pending = &pendingExecution{
		action:  strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: .5},
		beforeL: .5, beforeR: -.5,
		result: execution.Result{Err: execution.ErrUnexpectedPosition, PosL: 0, PosR: -.5,
			PositionsKnown: true, SettlementKnown: true},
		stalls: interventionStallLimit - 1,
	}
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 101)
	a.resolvePending(ctx)
	if a.pending != nil || a.checkpoint || len(a.state.Steps) != 0 || !a.volAcc.pnlUncertain {
		t.Fatalf("external intervention not recovered: pending=%v checkpoint=%v steps=%d uncertain=%v",
			a.pending != nil, a.checkpoint, len(a.state.Steps), a.volAcc.pnlUncertain)
	}
	if !a.Flat() {
		t.Fatal("intervention recovery did not certify the pair as flat")
	}
	st, err := rt.store.Load(c.key)
	if err != nil || st.Unresolved {
		t.Fatalf("intervention recovery was not persisted: %+v, %v", st, err)
	}
}

func TestDayRolloverPreservesPnLUncertainty(t *testing.T) {
	a := &PairActor{}
	a.volAcc = volumeAcc{day: "2026-10-14", volumeUSD: 123, pnlUSD: -4, pnlUncertain: true}
	a.resetVolumeIfNewDay(time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC))
	if !a.volAcc.pnlUncertain || a.volAcc.day != "2026-10-15" || a.volAcc.volumeUSD != 0 || a.volAcc.pnlUSD != 0 {
		t.Fatalf("day rollover dropped the uncertainty marker: %+v", a.volAcc)
	}
}
