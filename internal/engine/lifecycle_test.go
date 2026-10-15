package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

func lifecycleRuntime(t *testing.T) *runtime {
	t.Helper()
	st, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	cfg := &config.Config{DryRun: true, Auto: config.Auto{
		TopY: 1, MaxSteps: 3, NotionalPerLeg: 10, DepthUseRatio: .25,
		MinChurnScore: 1, PromoteMargin: .05, Dwell: time.Nanosecond, MaxFailRate: .3,
	}}
	cfg.Strategy.Kind = "arbitrage"
	cfg.Strategy.IntervalMin, cfg.Strategy.IntervalMax = time.Hour, time.Hour
	cfg.Strategy.MaxBookAge = time.Minute
	cfg.Estimator.Kind = "sma"
	return &runtime{cfg: cfg, store: st, matcher: symbols.DefaultMatcher()}
}

func lifecycleCandidate(base, lname, rname string, marketID int, score float64) *candidate {
	l, r := newActorTestVenue(lname), newActorTestVenue(rname)
	inst := domain.Instrument{MarketID: marketID, SizeDecimals: 2, PriceDecimals: 2}
	lb, rb := actorTestBook(lname, 100), actorTestBook(rname, 101)
	lb.MarketID, rb.MarketID = marketID, marketID
	return &candidate{
		base: base, key: pairKey(base, lname, rname, true), pair: symbols.NewPair(base, inst, inst), l: l, r: r,
		est: actorTestEstimator{}, vol: volatility.New(volatility.Params{}),
		volL: volatility.New(volatility.Params{}), volR: volatility.New(volatility.Params{}),
		bookL: lb, bookR: rb, healthLast: scan.Health{Score: score},
		notional: 10, depthRatio: .25, maxBookAge: time.Minute,
	}
}

func savedLifecycleStep() store.PairState {
	return store.PairState{NextID: 2, Steps: []strategy.Step{{
		ID: 1, Dir: strategy.DirLS, Qty: .5, OpenL: 100, OpenR: 101,
	}}}
}

func lifecycleActor(t *testing.T, rt *runtime, c *candidate, open bool) *PairActor {
	t.Helper()
	if open {
		if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
			t.Fatal(err)
		}
	}
	a, err := rt.buildActor(c.pair, .1, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.View = c
	return a
}

func stopLifecycleActors(t *testing.T, active map[string]*activeRun, wg *sync.WaitGroup) {
	t.Helper()
	t.Cleanup(func() {
		for _, ar := range active {
			ar.cancel()
		}
		wg.Wait()
	})
}

func TestAutoRestoresOpenPairsBeforeWarmupAndSelection(t *testing.T) {
	for _, excluded := range []bool{false, true} {
		t.Run(map[bool]string{false: "below selection threshold", true: "removed from whitelist"}[excluded], func(t *testing.T) {
			rt := lifecycleRuntime(t)
			old, fresh := lifecycleCandidate("OLD", "L", "R", 1, 0), lifecycleCandidate("NEW", "L", "R", 2, 100)
			old.est = estimator.NewSMA(estimator.Params{MinWarmup: time.Hour})
			old.excluded = excluded
			if err := rt.store.Save(old.key, savedLifecycleStep()); err != nil {
				t.Fatal(err)
			}
			active := map[string]*activeRun{}
			var wg sync.WaitGroup
			stopLifecycleActors(t, active, &wg)
			if err := rt.restorePairs(context.Background(), []*candidate{old, fresh}, active, &wg); err != nil {
				t.Fatal(err)
			}
			ar := active[old.key]
			wantBudget := 70.5 // historical exposure plus two new ten-dollar steps
			if excluded {
				wantBudget = 50.5 // draining only retains its existing exposure
			}
			if ar == nil || len(active) != 1 || ar.budget != wantBudget || ar.draining != excluded {
				t.Fatalf("saved exposure not restored: active=%v, run=%+v", active, ar)
			}
			if _, ready := old.Mean(); ready {
				t.Fatal("test must restore before estimator warmup")
			}
			promote := map[string]time.Time{fresh.key: time.Now().Add(-time.Hour)}
			if err := rt.selectPairs(context.Background(), []*candidate{old, fresh}, active, promote, map[string]time.Time{}, &wg); err != nil {
				t.Fatal(err)
			}
			if active[old.key] == nil || active[fresh.key] != nil || len(active) > rt.cfg.Auto.TopY {
				t.Fatalf("restored inventory must retain its slot: active=%v", active)
			}
		})
	}
}

func TestAutoWhitelistRetainsMatchedRecoveryCandidates(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.Auto.Candidates = []string{"NEW"}
	inst := []domain.Instrument{
		{Base: "OLD", Symbol: "OLD", MarketID: 1, SizeDecimals: 2, PriceDecimals: 2},
		{Base: "NEW", Symbol: "NEW", MarketID: 2, SizeDecimals: 2, PriceDecimals: 2},
	}
	l := &lifecycleVenue{actorTestVenue: newActorTestVenue("L"), instruments: inst}
	r := &lifecycleVenue{actorTestVenue: newActorTestVenue("R"), instruments: inst}
	rt.venues = map[string]exchange.Venue{"L": l, "R": r}
	cands, err := rt.matchAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	byBase := map[string]*candidate{}
	for _, c := range cands {
		byBase[c.base] = c
	}
	if len(cands) != 2 || byBase["OLD"] == nil || !byBase["OLD"].excluded || byBase["NEW"] == nil || byBase["NEW"].excluded {
		t.Fatalf("whitelist discarded recovery markets: %v", cands)
	}
}

func TestAutoNoMatchesReportsSavedOpenPair(t *testing.T) {
	rt := lifecycleRuntime(t)
	if err := rt.store.Save("OLD_L_R_paper", savedLifecycleStep()); err != nil {
		t.Fatal(err)
	}
	err := rt.runAuto(context.Background())
	if err == nil || !strings.Contains(err.Error(), "saved open pair OLD_L_R_paper") {
		t.Fatalf("unmatched saved exposure hidden by generic startup error: %v", err)
	}
}

func TestAutoRejectsUnrecoverableExposure(t *testing.T) {
	for _, tc := range []struct {
		name string
		live bool
		want string
	}{
		{name: "saved pair no longer matched", want: "saved open pair OLD_L_R_paper"},
		{name: "position without ledger", live: true, want: "position without a recoverable step ledger"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt := lifecycleRuntime(t)
			c := lifecycleCandidate("NEW", "L", "R", 2, 100)
			if tc.live {
				rt.cfg.DryRun = false
				c.key = pairKey(c.base, "L", "R", false)
				l := c.l.(*actorTestVenue)
				l.pos = .5
				rt.venues = map[string]exchange.Venue{"L": c.l, "R": c.r}
			} else if err := rt.store.Save("OLD_L_R_paper", savedLifecycleStep()); err != nil {
				t.Fatal(err)
			}
			active := map[string]*activeRun{}
			var wg sync.WaitGroup
			err := rt.restorePairs(context.Background(), []*candidate{c}, active, &wg)
			if err == nil || !strings.Contains(err.Error(), tc.want) || len(active) != 0 {
				t.Fatalf("restore error=%v active=%v", err, active)
			}
		})
	}
}

func TestAutoRestoresUnresolvedExecutionWithEmptyLedger(t *testing.T) {
	rt := lifecycleRuntime(t)
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	if err := rt.store.Save(c.key, store.PairState{NextID: 1, Unresolved: true}); err != nil {
		t.Fatal(err)
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(context.Background(), []*candidate{c}, active, &wg); err != nil {
		t.Fatal(err)
	}
	if active[c.key] == nil || active[c.key].budget != rt.pairBudget() {
		t.Fatalf("unresolved execution lost management and reservation: %v", active)
	}
	if active[c.key].actor.Flat() || active[c.key].actor.Drained() {
		t.Fatal("empty ledger incorrectly certified an unresolved execution as flat")
	}
}

func TestAutoRestoresExcessInventoryWithoutExceedingTradingCapacity(t *testing.T) {
	rt := lifecycleRuntime(t)
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
	trading := 0
	for _, ar := range active {
		if !ar.draining {
			trading++
		}
	}
	if len(active) != 2 || trading != rt.cfg.Auto.TopY || !active[b.key].draining {
		t.Fatalf("recovery must retain all inventory while capping opening actors: active=%v trading=%d", active, trading)
	}
}

func TestRestoredActorProtectsMarginBeforeEstimatorWarmup(t *testing.T) {
	testRestoredMarginProtection(t, false)
}

func TestRestoredActorProtectsKnownMarginWithMissingPeerBook(t *testing.T) {
	testRestoredMarginProtection(t, true)
}

func testRestoredMarginProtection(t *testing.T, missingPeerBook bool) {
	t.Helper()
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	rt.cfg.Risk.MarginFloorRatio, rt.cfg.Risk.MarginTriggerRatio = .2, .1
	rt.cfg.Risk.PositionRefresh = time.Second
	c := lifecycleCandidate("OLD", "L", "R", 1, 0)
	c.key = pairKey("OLD", "L", "R", false)
	c.est = estimator.NewSMA(estimator.Params{MinWarmup: time.Hour})
	c.bookL = actorTestBook("L", 90.4)
	peerBook := c.bookR
	if missingPeerBook {
		c.bookR = domain.Book{}
	}
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.pos, r.pos = .5, -.5
	for _, v := range []*actorTestVenue{l, r} {
		v.positions = func(context.Context) ([]domain.Position, error) {
			v.mu.Lock()
			defer v.mu.Unlock()
			liq := 90.0
			if v == r {
				liq = 110
			}
			return []domain.Position{{MarketID: 1, Qty: v.pos, AvgEntry: 100, LiquidationPrice: liq}}, nil
		}
	}
	ordersL, ordersR := make(chan domain.OrderReq, 2), make(chan domain.OrderReq, 2)
	l.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { ordersL <- req; return req.Qty, nil }
	r.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { ordersR <- req; return req.Qty, nil }
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tr, err := execution.NewTracker(ctx, l, r)
	if err != nil {
		t.Fatal(err)
	}
	rt.venues, rt.tracker, rt.ids = map[string]exchange.Venue{"L": l, "R": r}, tr, execution.NewIDGen()
	if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
		t.Fatal(err)
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(ctx, []*candidate{c}, active, &wg); err != nil {
		t.Fatal(err)
	}
	if missingPeerBook {
		awaitActorCondition(t, "known liquidation trigger despite missing peer book", func() bool {
			return active[c.key].actor.emergencyRequested.Load() && active[c.key].actor.riskBlocked.Load()
		})
		peerBook.Ts = time.Now()
		c.mu.Lock()
		c.bookR = peerBook
		c.mu.Unlock()
	}
	left, right := awaitActorOrder(t, ordersL), awaitActorOrder(t, ordersR)
	if !left.ReduceOnly || !right.ReduceOnly || left.Side != domain.Sell || right.Side != domain.Buy {
		t.Fatalf("restored liquidation risk was not reduced: left=%+v right=%+v", left, right)
	}
	flatCtx, flatCancel := context.WithTimeout(ctx, 2*time.Second)
	defer flatCancel()
	flatTick := time.NewTicker(time.Millisecond)
	defer flatTick.Stop()
	for !active[c.key].actor.Flat() {
		select {
		case <-flatCtx.Done():
			t.Fatalf("emergency close before warmup: %v", flatCtx.Err())
		case <-flatTick.C:
		}
	}
	if _, ready := c.Mean(); ready {
		t.Fatal("test unexpectedly completed estimator warmup")
	}
}

func TestActorWaitsForInitialRiskAssessmentBeforeOpening(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
	rt.cfg.Strategy.OpenBps, rt.cfg.Strategy.CloseBps, rt.cfg.Strategy.DepthMult = 1, 1, 1
	rt.cfg.Risk.MarginFloorRatio, rt.cfg.Risk.MarginTriggerRatio = .2, .1
	rt.cfg.Risk.PositionRefresh = time.Second
	c := lifecycleCandidate("X", "L", "R", 1, 100)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	started, release := make(chan struct{}), make(chan struct{})
	var reads atomic.Int64
	l.positions = func(context.Context) ([]domain.Position, error) {
		l.mu.Lock()
		defer l.mu.Unlock()
		return []domain.Position{{MarketID: 1, Qty: l.pos, AvgEntry: 100, LiquidationPrice: 90}}, nil
	}
	r.positions = func(ctx context.Context) ([]domain.Position, error) {
		// Reconciliation is the first read; the separate admission assessment
		// must finish its second read before the ready actor can open.
		if reads.Add(1) == 2 {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		return []domain.Position{{MarketID: 1, Qty: r.pos, AvgEntry: 100, LiquidationPrice: 110}}, nil
	}
	orders := make(chan domain.OrderReq, 4)
	for _, v := range []*actorTestVenue{l, r} {
		v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) { orders <- req; return req.Qty, nil }
	}
	ctx, cancel := context.WithCancel(context.Background())
	tr, err := execution.NewTracker(ctx, l, r)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	rt.tracker, rt.ids = tr, execution.NewIDGen()
	a := lifecycleActor(t, rt, c, false)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("actor stopped: %v", err)
		}
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial risk assessment never started")
	}
	select {
	case req := <-orders:
		t.Fatalf("submitted before initial risk assessment completed: %+v", req)
	case <-time.After(250 * time.Millisecond):
	}
	close(release)
	first, second := awaitActorOrder(t, orders), awaitActorOrder(t, orders)
	if first.ReduceOnly || second.ReduceOnly {
		t.Fatalf("healthy risk assessment did not permit opening: %+v %+v", first, second)
	}
	awaitActorCondition(t, "completed opening after risk assessment", func() bool {
		st, err := rt.store.Load(a.key)
		if err != nil {
			t.Fatal(err)
		}
		return len(st.Steps) == 1 && !st.Unresolved
	})
}

func TestAutoReplacementRetainsSlotAndBudgetUntilActorExits(t *testing.T) {
	rt := lifecycleRuntime(t)
	old, fresh := lifecycleCandidate("OLD", "L", "R", 1, 10), lifecycleCandidate("NEW", "L", "R", 2, 100)
	oldActor := lifecycleActor(t, rt, old, true)
	canceled := 0
	oldRun := &activeRun{actor: oldActor, cancel: func() { canceled++ }, budget: 50.5, done: make(chan error, 1)}
	active := map[string]*activeRun{old.key: oldRun}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	promote, demote := map[string]time.Time{fresh.key: time.Now().Add(-time.Hour)}, map[string]time.Time{old.key: time.Now().Add(-time.Hour)}
	if err := rt.selectPairs(context.Background(), []*candidate{old, fresh}, active, promote, demote, &wg); err != nil {
		t.Fatal(err)
	}
	if !oldRun.draining || canceled != 0 || active[fresh.key] != nil || len(active) != 1 || oldRun.budget != 50.5 {
		t.Fatalf("replacement abandoned old inventory: active=%v run=%+v canceled=%d", active, oldRun, canceled)
	}
	// Even after cancellation has been requested, the actor owns its slot
	// until Run has exited and all execution cleanup is complete.
	oldRun.stopping = true
	if err := rt.selectPairs(context.Background(), []*candidate{old, fresh}, active, promote, demote, &wg); err != nil {
		t.Fatal(err)
	}
	if active[fresh.key] != nil || active[old.key] == nil {
		t.Fatal("slot released before actor completion")
	}
	oldRun.done <- nil
	if err := rt.selectPairs(context.Background(), []*candidate{old, fresh}, active, promote, demote, &wg); err != nil {
		t.Fatal(err)
	}
	if active[old.key] != nil || active[fresh.key] == nil || len(active) != 1 {
		t.Fatalf("completed drain did not make replacement slot available: active=%v", active)
	}
}

func TestAutoFailureGateDrainsWithoutAdaptivePricingOrDwell(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.Auto.Dwell = time.Hour
	c := lifecycleCandidate("X", "L", "R", 1, 100)
	a := lifecycleActor(t, rt, c, true)
	for i := 0; i < 10; i++ {
		a.observeFailure(true)
	}
	if a.adaptive != nil || a.FailRate() != 1 {
		t.Fatalf("independent failure statistic missing: adaptive=%v rate=%v", a.adaptive, a.FailRate())
	}
	canceled := false
	ar := &activeRun{actor: a, cancel: func() { canceled = true }, budget: rt.pairBudget()}
	active := map[string]*activeRun{c.key: ar}
	var wg sync.WaitGroup
	if err := rt.selectPairs(context.Background(), []*candidate{c}, active, map[string]time.Time{}, map[string]time.Time{}, &wg); err != nil {
		t.Fatal(err)
	}
	if !ar.draining || canceled || active[c.key] == nil {
		t.Fatalf("failed actor must drain immediately while retaining inventory: run=%+v canceled=%v", ar, canceled)
	}
}

func TestAutoSelectsBestVenueCombination(t *testing.T) {
	rt := lifecycleRuntime(t)
	best, last := lifecycleCandidate("X", "A", "B", 1, 100), lifecycleCandidate("X", "B", "C", 1, 2)
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	promote := map[string]time.Time{best.key: time.Now().Add(-time.Hour), last.key: time.Now().Add(-time.Hour)}
	if err := rt.selectPairs(context.Background(), []*candidate{best, last}, active, promote, map[string]time.Time{}, &wg); err != nil {
		t.Fatal(err)
	}
	if active[best.key] == nil || active[last.key] != nil || len(active) != 1 {
		t.Fatalf("best combination not selected: active=%v", active)
	}
}

func TestAutoPreventsSharedMarketActors(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.Auto.TopY = 2
	first, overlapping := lifecycleCandidate("X", "A", "B", 1, 100), lifecycleCandidate("X", "B", "C", 1, 90)
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.promote(context.Background(), first, active, &wg); err != nil {
		t.Fatal(err)
	}
	if err := rt.promote(context.Background(), overlapping, active, &wg); err == nil || !strings.Contains(err.Error(), "market already managed") {
		t.Fatalf("overlapping promotion error=%v", err)
	}
	if len(active) != 1 {
		t.Fatalf("overlap changed active membership: %v", active)
	}
}

type lifecycleVenue struct {
	*actorTestVenue
	instruments []domain.Instrument
	bookErr     error
	bookCalls   atomic.Int64
	subContexts chan context.Context
}

func (v *lifecycleVenue) Instruments(context.Context) ([]domain.Instrument, error) {
	return v.instruments, nil
}

func (v *lifecycleVenue) SubscribeBook(ctx context.Context, inst domain.Instrument) (<-chan domain.Book, error) {
	v.bookCalls.Add(1)
	if v.subContexts != nil {
		select {
		case v.subContexts <- ctx:
		default:
		}
	}
	if v.bookErr != nil {
		return nil, v.bookErr
	}
	return v.actorTestVenue.SubscribeBook(ctx, inst)
}

func TestManualReturnsActorStartupFailure(t *testing.T) {
	rt := lifecycleRuntime(t)
	inst := []domain.Instrument{{Base: "X", Symbol: "X", MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}}
	want := errors.New("subscription failed")
	l := &lifecycleVenue{actorTestVenue: newActorTestVenue("L"), instruments: inst, bookErr: want}
	r := &lifecycleVenue{actorTestVenue: newActorTestVenue("R"), instruments: inst}
	rt.venues = map[string]exchange.Venue{"L": l, "R": r}
	rt.cfg.Pairs = []config.Pair{{Base: "X", Left: "L", Right: "R", StepQty: .1, MaxSteps: 1}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.runManual(ctx); !errors.Is(err, want) {
		t.Fatalf("startup failure not propagated: %v", err)
	}
}

func TestAutoReturnsCollectorStartupFailure(t *testing.T) {
	rt := lifecycleRuntime(t)
	inst := []domain.Instrument{{Base: "X", Symbol: "X", MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}}
	want := errors.New("subscription failed")
	l := &lifecycleVenue{actorTestVenue: newActorTestVenue("L"), instruments: inst, bookErr: want}
	r := &lifecycleVenue{actorTestVenue: newActorTestVenue("R"), instruments: inst}
	rt.venues = map[string]exchange.Venue{"L": l, "R": r}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.runAuto(ctx); !errors.Is(err, want) {
		t.Fatalf("collector startup failure not propagated: %v", err)
	}
}

func TestManualStartupFailureCancelsOtherActors(t *testing.T) {
	rt := lifecycleRuntime(t)
	instX := []domain.Instrument{{Base: "X", Symbol: "X", MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}}
	instY := []domain.Instrument{{Base: "Y", Symbol: "Y", MarketID: 2, SizeDecimals: 2, PriceDecimals: 2}}
	want := errors.New("subscription failed")
	l1 := &lifecycleVenue{actorTestVenue: newActorTestVenue("L1"), instruments: instX, subContexts: make(chan context.Context, 1)}
	r1 := &lifecycleVenue{actorTestVenue: newActorTestVenue("R1"), instruments: instX}
	l2 := &lifecycleVenue{actorTestVenue: newActorTestVenue("L2"), instruments: instY, bookErr: want}
	r2 := &lifecycleVenue{actorTestVenue: newActorTestVenue("R2"), instruments: instY}
	rt.venues = map[string]exchange.Venue{"L1": l1, "R1": r1, "L2": l2, "R2": r2}
	rt.cfg.Pairs = []config.Pair{
		{Base: "X", Left: "L1", Right: "R1", StepQty: .1, MaxSteps: 1},
		{Base: "Y", Left: "L2", Right: "R2", StepQty: .1, MaxSteps: 1},
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := rt.runManual(ctx); !errors.Is(err, want) {
		t.Fatalf("startup failure not propagated: %v", err)
	}
	select {
	case actorCtx := <-l1.subContexts:
		if actorCtx.Err() == nil {
			t.Fatal("other actor was left running after a peer startup failure")
		}
	default:
		t.Fatal("other actor never reached its subscription")
	}
}

func TestManualValidatesAllPairsBeforeStartingActors(t *testing.T) {
	rt := lifecycleRuntime(t)
	inst := []domain.Instrument{{Base: "X", Symbol: "X", MarketID: 1, SizeDecimals: 2, PriceDecimals: 2}}
	l := &lifecycleVenue{actorTestVenue: newActorTestVenue("L"), instruments: inst}
	r := &lifecycleVenue{actorTestVenue: newActorTestVenue("R"), instruments: inst}
	rt.venues = map[string]exchange.Venue{"L": l, "R": r}
	rt.cfg.Pairs = []config.Pair{
		{Base: "X", Left: "L", Right: "R", StepQty: .1, MaxSteps: 1},
		{Base: "MISSING", Left: "L", Right: "R", StepQty: .1, MaxSteps: 1},
	}
	if err := rt.runManual(context.Background()); err == nil {
		t.Fatal("unmatched pair should fail setup")
	}
	if l.bookCalls.Load() != 0 || r.bookCalls.Load() != 0 {
		t.Fatal("earlier actor started before later pair setup was validated")
	}
}

func TestCandidateCollectorWritesCompleteQuoteAndPropagatesErrors(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete quote", true: "write error"}[closed], func(t *testing.T) {
			c := lifecycleCandidate("X", "L", "R", 1, 10)
			c.health = scan.NewHealthTracker(600)
			path := filepath.Join(t.TempDir(), "samples.csv")
			sink, err := store.NewSampleSink(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := sink.Close(); err != nil {
					t.Errorf("close samples: %v", err)
				}
			})
			c.samples = sink
			if closed {
				if err := sink.Close(); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			done := make(chan struct{})
			go func() {
				defer close(done)
				err = c.collect(ctx, time.Hour, time.Millisecond, time.Minute)
			}()
			t.Cleanup(func() { cancel(); <-done })
			if closed {
				<-done
				if !errors.Is(err, os.ErrClosed) {
					t.Fatalf("sample error lost: %v", err)
				}
				return
			}
			awaitActorCondition(t, "a complete collector sample", func() bool {
				info, statErr := os.Stat(path)
				if statErr != nil {
					t.Fatal(statErr)
				}
				return info.Size() > 0
			})
			cancel()
			<-done
			if err != nil {
				t.Fatal(err)
			}
			if err := sink.Close(); err != nil {
				t.Fatal(err)
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			rows := strings.Split(strings.TrimSpace(string(raw)), "\n")
			if len(rows) == 0 || len(strings.Split(rows[0], ",")) != 15 {
				t.Fatalf("collector omitted execution prices and depth: %q", raw)
			}
		})
	}
}

func TestCandidateExpiredDataCannotRemainReady(t *testing.T) {
	c := lifecycleCandidate("X", "L", "R", 1, 100)
	c.maxBookAge = time.Second
	old := time.Now().Add(-time.Minute)
	c.bookL.Ts, c.bookR.Ts = old, old
	est := estimator.NewSMA(estimator.Params{Window: time.Hour, Interval: time.Second, MinWarmup: time.Second})
	est.Update(old, 10)
	c.est = est
	if _, ready := c.Mean(); ready || c.Stats().Ready || c.healthSnapshot().Score != 0 {
		t.Fatal("stopped collector retained eligibility")
	}
	c.bookL, c.bookR = domain.Book{Ts: time.Now()}, domain.Book{Ts: time.Now()}
	c.health = scan.NewHealthTracker(600)
	if got := c.healthSnapshot(); got.Score != 0 {
		t.Fatalf("invalid books retained health: %+v", got)
	}
}

type lifecycleClockEstimator struct {
	actorTestEstimator
	entered chan struct{}
	release chan struct{}
	calls   chan time.Time
	updates int
}

func (e *lifecycleClockEstimator) Update(now time.Time, _ float64) {
	if e.updates == 0 {
		close(e.entered)
		<-e.release
	}
	e.updates++
	select {
	case e.calls <- now:
	default:
	}
}

func TestCandidateCollectorUsesEvaluationClockAfterDelayedTick(t *testing.T) {
	c := lifecycleCandidate("X", "L", "R", 1, 100)
	c.health = scan.NewHealthTracker(600)
	est := &lifecycleClockEstimator{entered: make(chan struct{}), release: make(chan struct{}), calls: make(chan time.Time, 8)}
	c.est = est
	ctx, cancel := context.WithCancel(context.Background())
	var release sync.Once
	done := make(chan struct{})
	var collectErr error
	go func() {
		defer close(done)
		collectErr = c.collect(ctx, 5*time.Millisecond, time.Hour, time.Minute)
	}()
	t.Cleanup(func() {
		cancel()
		release.Do(func() { close(est.release) })
		<-done
		if collectErr != nil {
			t.Errorf("collector stopped: %v", collectErr)
		}
	})
	select {
	case <-est.entered:
	case <-time.After(time.Second):
		t.Fatal("first sample never started")
	}
	// Let another ticker notification queue while the first update is busy.
	timer := time.NewTimer(30 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	releasedAt := time.Now()
	release.Do(func() { close(est.release) })
	for i := 0; i < 2; i++ {
		select {
		case now := <-est.calls:
			if i == 1 && now.Before(releasedAt) {
				t.Fatalf("delayed tick stamped a current quote with an old scheduled time: sample=%v release=%v", now, releasedAt)
			}
		case <-time.After(time.Second):
			t.Fatal("collector never resumed sampling")
		}
	}
}

func TestCandidateRejectsBookIdentityAndFutureTimestamps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		modify func(*candidate)
		venue  string
	}{
		{name: "unknown venue", venue: "UNKNOWN", modify: func(*candidate) {}},
		{name: "wrong venue", venue: "L", modify: func(c *candidate) { c.bookL.Venue = "R" }},
		{name: "wrong market", venue: "L", modify: func(c *candidate) { c.bookL.MarketID++ }},
		{name: "future timestamp", venue: "L", modify: func(c *candidate) { c.bookL.Ts = time.Now().Add(time.Hour) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := lifecycleCandidate("X", "L", "R", 1, 100)
			tc.modify(c)
			if _, ok := c.Book(tc.venue); ok {
				t.Fatal("invalid book returned to execution")
			}
			if tc.venue != "UNKNOWN" {
				if _, ok := c.Quote(time.Now(), time.Minute); ok || c.healthSnapshot().Score != 0 {
					t.Fatal("invalid book retained quote or selection health")
				}
			}
		})
	}
}
