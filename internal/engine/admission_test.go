package engine

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

func TestRecoveryReservationCoversReplacementOfCheapHistoricalStep(t *testing.T) {
	st := store.PairState{Steps: []strategy.Step{
		{Qty: 1, OpenL: 100, OpenR: 100},
		{Qty: .01, OpenL: 100, OpenR: 100},
	}}
	used, reserved, priced := recoveryReservation(st, 50, 3)
	// Keep the hundred-dollar step, close the one-dollar step, then open
	// two fifty-dollar steps. Counting only the initially empty slot gives
	// 151, which does not cover this reachable 200-dollar inventory.
	if !priced || used != 101 || reserved != 200 {
		t.Fatalf("replacement exposure not reserved: used=%v reserved=%v priced=%v", used, reserved, priced)
	}
}

func TestAutoRecoveryIncludesQueriedPositionNotional(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun, rt.cfg.Auto.MaxTotalNotional = false, 160
	c := lifecycleCandidate("A", "L", "R", 1, 0)
	c.key = pairKey("A", "L", "R", false)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.positions = func(context.Context) ([]domain.Position, error) {
		return []domain.Position{{MarketID: 1, Qty: .5, AvgEntry: 300}}, nil
	}
	r.positions = func(context.Context) ([]domain.Position, error) {
		return []domain.Position{{MarketID: 1, Qty: -.5, AvgEntry: 300}}, nil
	}
	rt.venues = map[string]exchange.Venue{"L": l, "R": r}
	if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
		t.Fatal(err)
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(context.Background(), []*candidate{c}, active, &wg); err != nil {
		t.Fatal(err)
	}
	// The venue reports 150 rather than the historical 50.5. Its two
	// future ten-dollar slots no longer fit the account's 160-dollar cap.
	if ar := active[c.key]; !ar.draining || ar.budget != 150 {
		t.Fatalf("queried exposure was not counted before admission: %+v", ar)
	}
}

func TestAutoRecoveryCountsEveryRetainedPositionBeforeOpening(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.mon = monitor.New(nil)
	rt.cfg.Auto.TopY, rt.cfg.Auto.MaxTotalNotional = 2, 160
	rt.cfg.Auto.NotionalPerLeg = 51
	rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
	rt.cfg.Strategy.DepthMult = 1
	a, b := lifecycleCandidate("A", "L", "R", 1, 0), lifecycleCandidate("B", "L", "R", 2, 0)
	for _, c := range []*candidate{a, b} {
		c.notional, c.depthRatio = 51, .75
		if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
			t.Fatal(err)
		}
	}
	// B cannot finish draining. Its allocation must remain occupied while
	// A decides whether it can add otherwise correctly sized steps.
	b.bookL.Ts, b.bookR.Ts = time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(context.Background(), []*candidate{a, b}, active, &wg); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*candidate{a, b} {
		if !active[c.key].draining || active[c.key].budget != 50.5 {
			t.Fatalf("restoration granted unavailable capacity: %+v", active[c.key])
		}
	}
	time.Sleep(650 * time.Millisecond)
	for _, ar := range active {
		ar.cancel()
	}
	wg.Wait()
	if opens := rt.mon.Metrics().Opens; opens != 0 {
		t.Fatalf("recovery opened %d steps before retained exposure exited", opens)
	}
	st, err := rt.store.Load(b.key)
	if err != nil || len(st.Steps) != 1 {
		t.Fatalf("test did not retain blocked draining peer: %+v, %v", st, err)
	}
}

func TestAutoRecoveryReservesOldExposureAndNewSteps(t *testing.T) {
	for _, total := range []float64{60, 70.5} {
		t.Run(fmtBudget(total), func(t *testing.T) {
			rt := lifecycleRuntime(t)
			rt.mon = monitor.New(nil)
			rt.cfg.Auto.MaxTotalNotional = total
			rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
			rt.cfg.Strategy.DepthMult = 1
			c := lifecycleCandidate("A", "L", "R", 1, 0)
			// Warm the shared volatility view before restoration, so funded
			// recovery can exercise its allocated openings.
			now := time.Now()
			for i := 0; i <= 5; i++ {
				c.vol.AddAt(now.Add(time.Duration(i-5)*100*time.Millisecond), float64(i))
			}
			if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
				t.Fatal(err)
			}
			active := map[string]*activeRun{}
			var wg sync.WaitGroup
			stopLifecycleActors(t, active, &wg)
			if err := rt.restorePairs(context.Background(), []*candidate{c}, active, &wg); err != nil {
				t.Fatal(err)
			}
			ar := active[c.key]
			if total == 60 && (!ar.draining || ar.budget != 50.5) {
				t.Fatalf("old exposure borrowed new-step budget: %+v", ar)
			}
			if total == 70.5 && (ar.draining || ar.budget != 70.5) {
				t.Fatalf("available recovery allocation not used: %+v", ar)
			}
			time.Sleep(650 * time.Millisecond)
			ar.cancel()
			wg.Wait()
			st, err := rt.store.Load(c.key)
			if err != nil {
				t.Fatal(err)
			}
			used := 0.0
			for _, step := range st.Steps {
				used += step.Qty * step.OpenR
			}
			if used > total || used > ar.budget {
				t.Fatalf("actual %v exceeds account %v or reservation %v", used, total, ar.budget)
			}
			if total == 60 && rt.mon.Metrics().Opens != 0 {
				t.Fatal("recovery with no opening allocation created a new step")
			}
			if total == 70.5 && len(st.Steps) != 3 {
				t.Fatalf("funded recovery did not exercise new openings: %+v", st)
			}
		})
	}
}

func fmtBudget(total float64) string {
	if total == 60 {
		return "insufficient allocation"
	}
	return "funded allocation"
}

func TestAutoSizingFailureBlocksOpenButPreservesClose(t *testing.T) {
	for _, profitable := range []bool{false, true} {
		t.Run(map[bool]string{false: "no oversized replacement", true: "profitable historical close"}[profitable], func(t *testing.T) {
			rt := lifecycleRuntime(t)
			rt.mon = monitor.New(nil)
			rt.cfg.Auto.NotionalPerLeg = .1
			rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
			rt.cfg.Strategy.DepthMult = 2
			c := lifecycleCandidate("A", "L", "R", 1, 0)
			c.notional = .1
			now := time.Now()
			for i := 0; i <= 5; i++ {
				c.vol.AddAt(now.Add(time.Duration(i-5)*100*time.Millisecond), float64(i))
			}
			if profitable {
				c.bookR = actorTestBook("R", 100)
			}
			if qty, ok := c.SizedQty(); ok || qty != 0 {
				t.Fatalf("test requires unavailable current sizing, got %v/%v", qty, ok)
			}
			if err := rt.store.Save(c.key, savedLifecycleStep()); err != nil {
				t.Fatal(err)
			}
			active := map[string]*activeRun{}
			var wg sync.WaitGroup
			stopLifecycleActors(t, active, &wg)
			if err := rt.restorePairs(context.Background(), []*candidate{c}, active, &wg); err != nil {
				t.Fatal(err)
			}
			time.Sleep(650 * time.Millisecond)
			active[c.key].cancel()
			wg.Wait()
			st, err := rt.store.Load(c.key)
			if err != nil {
				t.Fatal(err)
			}
			if rt.mon.Metrics().Opens != 0 {
				t.Fatal("failed sizing reused a historical opening quantity")
			}
			want := 1
			if profitable {
				want = 0
			}
			if len(st.Steps) != want {
				t.Fatalf("sizing failure changed closing ability: got %d steps want %d", len(st.Steps), want)
			}
		})
	}
}

func TestCandidateSizingCapsBothLegNotionals(t *testing.T) {
	c := lifecycleCandidate("A", "L", "R", 1, 0)
	c.bookL, c.bookR = actorTestBook("L", 100), actorTestBook("R", 150)
	c.depthRatio, c.notional = 1, 100
	qty, ok := c.SizedQty()
	if !ok || math.Max(qty*c.bookL.Asks[0].Price, qty*c.bookR.Asks[0].Price) > c.notional {
		t.Fatalf("single-leg notional cap exceeded: qty=%v ok=%v", qty, ok)
	}
}

func TestAutoWaitsForAllRestartCheckpointsBeforeOpening(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.mon = monitor.New(nil)
	rt.cfg.Auto.TopY, rt.cfg.Auto.MaxTotalNotional = 3, 200
	rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
	rt.cfg.Strategy.DepthMult = 1
	a := lifecycleCandidate("A", "L", "R", 1, 0)
	b := lifecycleCandidate("B", "L", "R", 2, 0)
	fresh := lifecycleCandidate("NEW", "L", "R", 3, 0)
	if err := rt.store.Save(a.key, savedLifecycleStep()); err != nil {
		t.Fatal(err)
	}
	if err := rt.store.Save(b.key, store.PairState{NextID: 1, Unresolved: true}); err != nil {
		t.Fatal(err)
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	if err := rt.restorePairs(context.Background(), []*candidate{a, b, fresh}, active, &wg); err != nil {
		t.Fatal(err)
	}
	if active[a.key].draining || active[b.key].draining {
		t.Fatal("test requires enough known budget for both restored actors")
	}
	if err := rt.promote(context.Background(), fresh, active, &wg); err == nil {
		t.Fatal("promoted a new actor while an old request quantity was unknown")
	}
	time.Sleep(650 * time.Millisecond)
	for _, ar := range active {
		ar.cancel()
	}
	wg.Wait()
	if rt.mon.Metrics().Opens != 0 {
		t.Fatal("a restored peer opened while startup orders were quarantined")
	}
	// Resolve the original empty checkpoint without running its actor again.
	// The account gate is released only after successful durable recovery.
	old := active[b.key].actor
	old.cfg.CheckpointGrace = time.Nanosecond
	old.checkpointCheckedAt = time.Time{}
	old.checkpointSince = time.Now().Add(-time.Second)
	old.recoverCheckpoint(context.Background(), time.Now())
	if !rt.startupRecoveryReady() {
		t.Fatal("successful checkpoint recovery did not release startup admission")
	}
	if err := rt.promote(context.Background(), fresh, active, &wg); err != nil {
		t.Fatalf("recovered account could not promote: %v", err)
	}
}

func TestConcurrentAutoActorsRemainWithinFixedAccountAllocations(t *testing.T) {
	rt := lifecycleRuntime(t)
	rt.mon = monitor.New(nil)
	rt.cfg.Auto.TopY, rt.cfg.Auto.MaxTotalNotional = 2, 60
	rt.cfg.Strategy.IntervalMin, rt.cfg.Strategy.IntervalMax = time.Millisecond, time.Millisecond
	rt.cfg.Strategy.DepthMult = 1
	a, b := lifecycleCandidate("A", "L", "R", 1, 0), lifecycleCandidate("B", "L", "R", 2, 0)
	now := time.Now()
	for _, c := range []*candidate{a, b} {
		for i := 0; i <= 5; i++ {
			c.vol.AddAt(now.Add(time.Duration(i-5)*100*time.Millisecond), float64(i))
		}
	}
	active := map[string]*activeRun{}
	var wg sync.WaitGroup
	stopLifecycleActors(t, active, &wg)
	for _, c := range []*candidate{a, b} {
		if err := rt.promote(context.Background(), c, active, &wg); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(650 * time.Millisecond)
	for _, ar := range active {
		ar.cancel()
	}
	wg.Wait()
	total := 0.0
	for _, c := range []*candidate{a, b} {
		st, err := rt.store.Load(c.key)
		if err != nil {
			t.Fatal(err)
		}
		used := 0.0
		for _, step := range st.Steps {
			used += step.Qty * math.Max(step.OpenL, step.OpenR)
		}
		if len(st.Steps) != 3 || used > active[c.key].budget {
			t.Fatalf("actor did not use its reserved capacity safely: steps=%d used=%v reserved=%v", len(st.Steps), used, active[c.key].budget)
		}
		total += used
	}
	if total > rt.cfg.Auto.MaxTotalNotional || rt.mon.Metrics().Opens != 6 {
		t.Fatalf("concurrent opens exceeded account allocation: total=%v opens=%v", total, rt.mon.Metrics().Opens)
	}
}
