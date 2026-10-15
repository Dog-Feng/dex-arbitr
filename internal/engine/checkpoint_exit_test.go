package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
)

func newCheckpointExitActor(t *testing.T, legacy bool) (*PairActor, *actorTestVenue, *actorTestVenue) {
	t.Helper()
	rt := lifecycleRuntime(t)
	rt.cfg.DryRun = false
	c := lifecycleCandidate("X", "L", "R", 1, 0)
	c.key = pairKey("X", "L", "R", false)
	l, r := c.l.(*actorTestVenue), c.r.(*actorTestVenue)
	l.pos, r.pos = .5, -.5
	for _, v := range []*actorTestVenue{l, r} {
		v.submit = func(_ context.Context, req domain.OrderReq) (float64, error) {
			if !req.ReduceOnly {
				t.Error("checkpoint recovery submitted an opening order")
			}
			return req.Qty, nil
		}
	}
	tr, err := execution.NewTracker(context.Background(), l, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	rt.tracker, rt.ids = tr, execution.NewIDGen()
	st := checkpointState()
	st.Steps[0].OriginalQty = .5
	if legacy {
		st.Steps[0].OpenL = 0
	}
	if err := rt.store.Save(c.key, st); err != nil {
		t.Fatal(err)
	}
	a, err := rt.buildActor(c.pair, .1, 3, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		t.Fatal(err)
	}
	a.cfg.MaxBookAge = time.Minute
	a.cfg.ExecCfg.SettleDelay = time.Millisecond
	a.cfg.ExecCfg.QueryInterval = time.Millisecond
	a.cfg.ExecCfg.AckTimeout = 50 * time.Millisecond
	a.exec = execution.NewExecutor(a.cfg.ExecCfg, tr, rt.ids, a.book, nil, nil, nil)
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 101)
	return a, l, r
}

func TestCheckpointRunCanReduceBeforeGraceWithoutCertifyingFlat(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, emergency := range []bool{false, true} {
			name := map[bool]string{false: "priced", true: "legacy"}[legacy] + "/" + map[bool]string{false: "drain", true: "emergency"}[emergency]
			t.Run(name, func(t *testing.T) {
				a, l, r := newCheckpointExitActor(t, legacy)
				l.books <- a.bookL
				r.books <- a.bookR
				if emergency {
					a.emergencyRequested.Store(true)
				} else {
					a.Drain()
				}
				ctx, cancel := context.WithTimeout(context.Background(), 650*time.Millisecond)
				defer cancel()
				if err := a.Run(ctx); err != nil {
					t.Fatal(err)
				}
				if !l.traded || !r.traded || l.pos != 0 || r.pos != 0 || len(a.state.Steps) != 0 {
					t.Fatalf("requested exit was blocked: traded=%v/%v positions=%g/%g steps=%d", l.traded, r.traded, l.pos, r.pos, len(a.state.Steps))
				}
				if !a.checkpoint || !a.checkpointExit || a.Flat() || a.Drained() {
					t.Fatalf("released while old transactions can still arrive: checkpoint=%v exit=%v flat=%v drained=%v", a.checkpoint, a.checkpointExit, a.Flat(), a.Drained())
				}
				st, err := a.cfg.Store.Load(a.key)
				if err != nil || !st.Unresolved || !st.CheckpointExit || !st.PnLUncertain {
					t.Fatalf("exit quarantine was not durable: %+v, %v", st, err)
				}
			})
		}
	}
}

func TestCheckpointExitReducesLateFillAndOnlyReleasesAfterExpiry(t *testing.T) {
	a, l, r := newCheckpointExitActor(t, false)
	a.Drain()
	now := time.Now()
	a.recoverCheckpointExit(context.Background(), now)
	if l.pos != 0 || r.pos != 0 || a.pending != nil || a.Flat() {
		t.Fatalf("first reduction did not remain quarantined: positions=%g/%g pending=%v flat=%v", l.pos, r.pos, a.pending != nil, a.Flat())
	}
	// An original request whose identity was lost in the previous process
	// executes after our protective exit. Keep the actor and its budget alive.
	r.mu.Lock()
	r.pos = -.1
	r.mu.Unlock()
	a.recoverCheckpointExit(context.Background(), now.Add(time.Second))
	if r.pos != 0 || a.pending != nil || a.Flat() || a.Drained() {
		t.Fatalf("late fill escaped recovery: right=%g pending=%v flat=%v drained=%v", r.pos, a.pending != nil, a.Flat(), a.Drained())
	}
	a.checkpointExitSince = now.Add(-16 * time.Minute)
	a.recoverCheckpointExit(context.Background(), now.Add(2*time.Second))
	if a.checkpoint || a.checkpointExit || !a.Flat() || !a.Drained() || !a.volAcc.pnlUncertain {
		t.Fatalf("safe exit did not finish: checkpoint=%v exit=%v flat=%v drained=%v uncertain=%v", a.checkpoint, a.checkpointExit, a.Flat(), a.Drained(), a.volAcc.pnlUncertain)
	}
}

func TestCheckpointExitRestartsQuarantineAndHandlesMismatchedLatePosition(t *testing.T) {
	a, _, r := newCheckpointExitActor(t, false)
	a.Drain()
	a.recoverCheckpointExit(context.Background(), time.Now())
	// A new process must retain the exit intent even when the empty ledger
	// no longer matches a late order's newly visible position.
	r.mu.Lock()
	r.pos = -.1
	r.mu.Unlock()
	started := time.Now()
	b, err := NewPairActor(a.cfg)
	if err != nil {
		t.Fatal(err)
	}
	b.bookL, b.bookR = a.bookL, a.bookR
	if !b.checkpointExit || b.checkpointExitSince.Before(started) {
		t.Fatal("restart lost quarantine or reused an expired observation window")
	}
	if err := b.reconcile(context.Background()); err == nil {
		t.Fatal("late position must initially disagree with the saved empty ledger")
	}
	b.recoverCheckpointExit(context.Background(), time.Now())
	if r.pos != 0 || b.pending != nil || !b.checkpoint || !b.checkpointExit || b.Flat() {
		t.Fatalf("restart dropped late exposure or released quarantine: right=%g pending=%v checkpoint=%v exit=%v flat=%v", r.pos, b.pending != nil, b.checkpoint, b.checkpointExit, b.Flat())
	}
}

func TestCheckpointExitPersistsIntentBeforeSending(t *testing.T) {
	a, l, r := newCheckpointExitActor(t, false)
	if err := a.cfg.Store.Close(); err != nil {
		t.Fatal(err)
	}
	a.emergencyRequested.Store(true)
	a.recoverCheckpointExit(context.Background(), time.Now())
	if a.fatalErr == nil || l.traded || r.traded {
		t.Fatalf("sent before durable exit intent: err=%v traded=%v/%v", a.fatalErr, l.traded, r.traded)
	}
}

// The old process's closing IOC lands while the new protective IOC is still
// unknown. The flat position is therefore not evidence of the new IOC's fill.
type checkpointUnknownExitVenue struct {
	*actorTestVenue
	orderMu  sync.Mutex
	request  domain.OrderReq
	submits  int
	queries  int
	terminal bool
}

func (v *checkpointUnknownExitVenue) Submit(_ context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
	v.orderMu.Lock()
	v.request, v.submits = req, v.submits+1
	v.orderMu.Unlock()
	v.actorTestVenue.mu.Lock()
	v.pos = 0 // A different, old closing IOC changed the position.
	v.actorTestVenue.mu.Unlock()
	return domain.SubmitAck{ClientID: req.ClientID}, errors.New("protective IOC submission outcome unknown")
}

func (v *checkpointUnknownExitVenue) QueryOrder(_ context.Context, inst domain.Instrument, cid int64) (domain.OrderUpdate, error) {
	v.orderMu.Lock()
	defer v.orderMu.Unlock()
	if cid != v.request.ClientID {
		return domain.OrderUpdate{}, errors.New("protective IOC identity was replaced")
	}
	v.queries++
	status := domain.StatusUnknown
	if v.terminal {
		status = domain.StatusCanceled // The new IOC did not fill.
	}
	return domain.OrderUpdate{Venue: v.name, ClientID: cid, MarketID: inst.MarketID, Status: status}, nil
}

func TestCheckpointExitRetainsUnknownNewOrdersDespiteOldClose(t *testing.T) {
	for _, emergency := range []bool{false, true} {
		name := map[bool]string{false: "drain", true: "emergency"}[emergency]
		t.Run(name, func(t *testing.T) {
			a, l, r := newCheckpointExitActor(t, false)
			left := &checkpointUnknownExitVenue{actorTestVenue: l}
			right := &checkpointUnknownExitVenue{actorTestVenue: r}
			a.cfg.L, a.cfg.R = left, right
			a.cfg.ExecCfg.AckTimeout = time.Millisecond
			a.cfg.ExecCfg.QueryRetries = 1
			a.exec = execution.NewExecutor(a.cfg.ExecCfg, a.cfg.Tracker, a.cfg.IDs, a.book, nil, nil, nil)
			if emergency {
				a.emergencyRequested.Store(true)
			} else {
				a.Drain()
			}
			now := time.Now()
			// The old requests remain valid near the end of their 599s TTL.
			// A newly submitted request can remain valid past the original 15m
			// quarantine, so that original deadline cannot discard its identity.
			a.checkpointExit = true
			a.checkpointExitSince = now.Add(-590 * time.Second)
			if err := a.persist(); err != nil {
				t.Fatal(err)
			}
			a.recoverCheckpointExit(context.Background(), now)
			if a.pending == nil || !a.pending.result.PositionsKnown || a.pending.result.PosL != 0 || a.pending.result.PosR != 0 || execution.OrdersTerminal(a.pending.result) || a.Flat() || a.Drained() {
				t.Fatalf("old close was mistaken for new IOC completion: pending=%+v flat=%v drained=%v", a.pending, a.Flat(), a.Drained())
			}
			pastQuarantine := a.checkpointExitSince.Add(16 * time.Minute)
			for i := 0; i < 3; i++ {
				a.pending.lastAttempt = time.Time{}
				a.recoverCheckpointExit(context.Background(), pastQuarantine)
				if a.pending == nil || !a.checkpoint || !a.checkpointExit || a.Flat() || a.Drained() {
					t.Fatal("original quarantine deadline released an unknown new IOC")
				}
			}
			st, err := a.cfg.Store.Load(a.key)
			if err != nil || !st.Unresolved || !st.CheckpointExit {
				t.Fatalf("unknown new IOC lost durable quarantine: %+v, %v", st, err)
			}
			for _, v := range []*checkpointUnknownExitVenue{left, right} {
				v.orderMu.Lock()
				if v.submits != 1 || v.queries < 3 || !v.request.ReduceOnly {
					t.Errorf("new IOC was replaced or not queried: submits=%d queries=%d request=%+v", v.submits, v.queries, v.request)
				}
				v.terminal = true
				v.orderMu.Unlock()
			}
			for i := 0; i < 6 && a.pending != nil; i++ {
				a.pending.lastAttempt = time.Time{}
				a.recoverCheckpointExit(context.Background(), pastQuarantine)
			}
			if a.pending != nil {
				t.Fatalf("terminal new orders did not finish recovery: %+v", a.pending)
			}
			a.recoverCheckpointExit(context.Background(), pastQuarantine)
			if !a.Flat() || a.checkpoint || a.checkpointExit || (!emergency && !a.Drained()) || !a.volAcc.pnlUncertain {
				t.Fatalf("terminal flat exit did not release safely: flat=%v checkpoint=%v exit=%v drained=%v", a.Flat(), a.checkpoint, a.checkpointExit, a.Drained())
			}
			if a.volAcc.volumeUSD != 0 || a.volAcc.pnlUSD != 0 {
				t.Fatalf("old closes were accounted as new protective fills: %+v", a.volAcc)
			}
			st, err = a.cfg.Store.Load(a.key)
			if err != nil || st.Unresolved || st.CheckpointExit {
				t.Fatalf("terminal exit completion was not durable: %+v, %v", st, err)
			}
		})
	}
}

func TestCheckpointExitFinalPersistenceFailureDoesNotRelease(t *testing.T) {
	a, _, _ := newCheckpointExitActor(t, false)
	a.Drain()
	now := time.Now()
	a.recoverCheckpointExit(context.Background(), now)
	if a.pending != nil || a.Flat() {
		t.Fatal("initial exit did not reach quarantined flat state")
	}
	if err := a.cfg.Store.Close(); err != nil {
		t.Fatal(err)
	}
	a.recoverCheckpointExit(context.Background(), now.Add(16*time.Minute))
	if a.fatalErr == nil || a.Flat() || a.Drained() || a.settled.Load() {
		t.Fatalf("failed final checkpoint save released the actor: fatal=%v flat=%v drained=%v settled=%v", a.fatalErr, a.Flat(), a.Drained(), a.settled.Load())
	}
}
