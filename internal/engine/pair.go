// Package engine runs one actor goroutine per pair.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/adaptive"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// PairConfig wires one pair actor.
type PairConfig struct {
	Pair            symbols.Pair
	L, R            exchange.Venue
	Params          strategy.Params
	Strategy        strategy.Strategy
	Est             estimator.Estimator
	IntervalMin     time.Duration
	IntervalMax     time.Duration
	MaxBookAge      time.Duration
	SampleEvery     time.Duration
	DryRun          bool // paper trading: fills assumed at decision prices
	Store           *store.Store
	Tracker         *execution.Tracker // nil in dry run
	ExecCfg         execution.Config
	IDs             *execution.IDGen
	Vol             *volatility.Estimator
	VolL            *volatility.Estimator
	VolR            *volatility.Estimator
	VolEvery        time.Duration
	VolMaxAge       time.Duration
	Samples         *store.SampleSink
	Risk            marginConfig
	Adaptive        adaptive.Config
	Monitor         *monitor.Monitor
	View            PairView    // shared stats/books in auto mode; nil = actor-owned
	MaxOpenNotional float64     // per-leg auto opening cap; 0 leaves manual sizing unchanged
	MayOpen         func() bool // optional account startup recovery admission
	AdaptiveLatency adaptiveLatencyCfg
	// Checkpoint recovery: after a restart with an unresolved pre-send
	// checkpoint, positions are re-verified every CheckpointCheckEvery and the
	// checkpoint is cleared once positions match the ledger continuously for
	// CheckpointGrace (defaults 15s / 15m). PnL is then marked uncertain.
	CheckpointCheckEvery time.Duration
	CheckpointGrace      time.Duration
}

// PairView is the shared, continuously-updated market view an auto-mode actor
// reads instead of subscribing to books and sampling its own estimators.
type PairView interface {
	Book(venue string) (domain.Book, bool)
	Quote(now time.Time, maxAge time.Duration) (spread.Quote, bool)
	Mean() (float64, bool) // mean bps, ready
	VolBps() float64
	VolReady() bool // short-horizon volatility warm and fresh
	LegVol(venue string) float64
	SetVolWindows(spreadK, legLK, legRK int)
	Stats() estimator.Stats
}

// PairActor owns the trading state of one pair (single goroutine).
// A separate goroutine updates the book cache, including during execution.
type PairActor struct {
	cfg   PairConfig
	key   string
	log   *slog.Logger
	state strategy.State
	next  int64
	posL  float64
	posR  float64
	exec  *execution.Executor

	bookMu              sync.RWMutex
	bookL               domain.Book
	bookR               domain.Book
	bookWake            chan struct{}
	lat                 latencyStat
	margin              marginMonitor
	volAcc              volumeAcc
	procVolumeUSD       float64 // process-lifetime settled volume (status log)
	procPnlUSD          float64 // process-lifetime settled PnL (status log)
	adaptive            *adaptive.K
	cnt                 atomic.Int64 // signed step counter, readable by the picker
	busy                atomic.Bool
	flatPositions       atomic.Bool
	settled             atomic.Bool
	draining            atomic.Bool
	drainConfirmed      atomic.Bool
	emergencyRequested  atomic.Bool
	riskBlocked         atomic.Bool
	failBits            atomic.Uint64
	execMu              sync.Mutex
	execCancel          context.CancelFunc
	drainWake           chan struct{}
	reconciled          bool
	legacyPrices        bool
	checkpoint          bool
	pending             *pendingExecution
	fatalErr            error
	failures            []bool
	failureIndex        int
	checkpointSince     time.Time
	checkpointCheckedAt time.Time
	checkpointExit      bool
	checkpointExitSince time.Time
	startupRecovered    atomic.Bool
	volBlockLoggedAt    time.Time
	latL                *latencySeries
	latR                *latencySeries
	winUpdatedAt        time.Time
	lastDir             strategy.Dir
	forcedCloseAt       time.Time // last forced-exit close; feeds the reopen cooldown
	reconLast           reconBaseline
	reconInit           bool
	interventions       []time.Time
	// capEngaged records that the step count touched MaxSteps; opens stay
	// blocked until the count falls to MaxSteps-CapBand or below.
	capEngaged bool
}

// volumeAcc keeps a UTC-day-scoped volume/PnL audit trail and the sticky
// PnL-uncertainty marker; owned by the actor goroutine, no locking.
type volumeAcc struct {
	day          string
	volumeUSD    float64
	pnlUSD       float64
	pnlUncertain bool
}

func (a *PairActor) resetVolumeIfNewDay(now time.Time) {
	today := now.UTC().Format("2006-01-02")
	if a.volAcc.day == today {
		return
	}
	// An uncertainty marker from a recovered or estimated execution must
	// survive the UTC day rollover; the day-scoped counters reset with it.
	a.volAcc = volumeAcc{day: today, pnlUncertain: a.volAcc.pnlUncertain}
}

// addSettled books confirmed settlement into the day-scoped audit accumulator
// and the process-lifetime counters shown in the status log. The day-scoped
// counters roll over at UTC midnight and are persisted as an audit trail; the
// process counters never reset and are deliberately not persisted.
func (a *PairActor) addSettled(volumeUSD, pnlUSD float64) {
	a.resetVolumeIfNewDay(time.Now())
	a.volAcc.volumeUSD += volumeUSD
	a.volAcc.pnlUSD += pnlUSD
	a.procVolumeUSD += volumeUSD
	a.procPnlUSD += pnlUSD
}

// syncCapEngaged updates the capacity-band state from the current ledger. It
// only changes when the count touches MaxSteps or falls back into the band.
func (a *PairActor) syncCapEngaged() {
	p := a.cfg.Params
	n := absCounter(a.state.Counter())
	band := p.CapBand
	if band > p.MaxSteps {
		band = p.MaxSteps
	}
	if p.MaxSteps > 0 && n >= p.MaxSteps {
		a.capEngaged = true
	}
	if band > 0 && n <= p.MaxSteps-band {
		a.capEngaged = false
	}
}

// latencyStat aggregates per-leg submit-to-result latency for the status report.
type latencyStat struct {
	n   int
	sum time.Duration
	max time.Duration
}

func (s *latencyStat) add(d time.Duration) {
	if d <= 0 {
		return
	}
	s.n++
	s.sum += d
	if d > s.max {
		s.max = d
	}
}

func (s *latencyStat) avg() time.Duration {
	if s.n == 0 {
		return 0
	}
	return s.sum / time.Duration(s.n)
}

func (s *latencyStat) reset() { *s = latencyStat{} }

// NewPairActor builds the actor and loads persisted steps.
func NewPairActor(cfg PairConfig) (*PairActor, error) {
	if cfg.Strategy == nil {
		cfg.Strategy = strategy.Arbitrage{}
	}
	key := pairKey(cfg.Pair.Base, cfg.L.Name(), cfg.R.Name(), cfg.DryRun)
	a := &PairActor{cfg: cfg, key: key, log: slog.With("pair", cfg.Pair.Base, "L", cfg.L.Name(), "R", cfg.R.Name()), drainWake: make(chan struct{}, 1)}
	a.bookWake = make(chan struct{}, 1)
	a.margin = marginMonitor{cfg: cfg.Risk}
	ringN := cfg.AdaptiveLatency.ringN
	if ringN < 2 {
		ringN = 32
	}
	a.latL, a.latR = newLatencySeries(ringN), newLatencySeries(ringN)
	st, err := cfg.Store.Load(key)
	if err != nil {
		return nil, err
	}
	a.state = strategy.State{Mode: strategy.ModeWarmup, Steps: st.Steps}
	a.next = st.NextID
	a.volAcc = volumeAcc{day: st.Day, volumeUSD: st.VolumeUSD, pnlUSD: st.PnLUSD, pnlUncertain: st.PnLUncertain}
	a.interventions = append(a.interventions, st.Interventions...)
	a.checkpoint = st.Unresolved
	a.startupRecovered.Store(!st.Unresolved)
	a.checkpointExit = st.CheckpointExit
	if st.Estimator != nil {
		// Resume the mean estimator's warmup history: a restart skips the
		// full warmup window. Freshness gates still block readiness until a
		// new sample arrives, and the first update decays the restored mean
		// by the real elapsed time.
		if p, ok := cfg.Est.(estimator.Persistable); ok {
			if err := p.Restore(*st.Estimator); err != nil {
				a.log.Warn("estimator state unusable; cold-starting warmup", "kind", st.Estimator.Kind, "err", err)
			}
		}
	}
	if a.checkpointExit {
		// Restart the quarantine: even an order sent just before the prior
		// process stopped must expire before this actor can release its slot.
		a.checkpointExitSince = time.Now()
	}
	if st.Unresolved {
		a.state.Mode = strategy.ModePaused
	}
	for i := range a.state.Steps {
		step := &a.state.Steps[i]
		if step.OriginalQty <= 0 {
			// Older ledgers did not record original quantity or entry fees.
			// Preserve the known price basis but never present their costs as exact.
			step.PriceEstimated = true
			a.volAcc.pnlUncertain = true
			a.log.Warn("legacy step has unknown historical fee/partial-fill accounting", "step", step.ID)
		}
		if step.OpenL <= 0 || step.OpenR <= 0 {
			a.legacyPrices = true
			a.state.Mode = strategy.ModePaused
		}
	}
	a.syncCapEngaged()
	a.settled.Store(!st.Unresolved && cfg.DryRun)
	a.posL, a.posR = a.expectedL(), -a.expectedL()
	a.flatPositions.Store(cfg.DryRun && len(st.Steps) == 0)
	a.reconciled = cfg.DryRun
	a.riskBlocked.Store(!cfg.DryRun && cfg.Risk.trigger > 0)
	a.cnt.Store(int64(a.state.Counter()))
	if !cfg.DryRun {
		// Live trading stays paused until reconciliation succeeds.
		a.state.Mode = strategy.ModePaused
		var tickK execution.TickKFn
		if cfg.Adaptive.Window > 0 {
			a.adaptive = adaptive.NewK(cfg.Adaptive)
			tickK = a.adaptive.K
		}
		a.exec = execution.NewExecutor(cfg.ExecCfg, cfg.Tracker, cfg.IDs, a.book, a.legVol, tickK, nil)
	}
	return a, nil
}

// syncCounter mirrors the step ledger into the atomic counter.
func (a *PairActor) syncCounter() { a.cnt.Store(int64(a.state.Counter())) }

// Flat reports whether no steps are open, without racing the actor goroutine.
func (a *PairActor) Flat() bool {
	return !a.busy.Load() && a.settled.Load() && a.flatPositions.Load() && a.cnt.Load() == 0
}

// Busy includes execution and reconciliation, so an empty ledger is not an idle account.
func (a *PairActor) Busy() bool { return a.busy.Load() }
func (a *PairActor) Drain() {
	if a.draining.CompareAndSwap(false, true) {
		// Move an in-flight ordinary rescue back to the actor so it can be
		// recovered with the wider unwind protection before retiring.
		a.execMu.Lock()
		if a.execCancel != nil {
			a.execCancel()
		}
		a.execMu.Unlock()
	}
	select {
	case a.drainWake <- struct{}{}:
	default:
	}
}
func (a *PairActor) Drained() bool     { return a.draining.Load() && a.drainConfirmed.Load() && a.Flat() }
func (a *PairActor) FailRate() float64 { return math.Float64frombits(a.failBits.Load()) }

// mean returns the spread mean and readiness from the view or the actor's own
// estimator.
func (a *PairActor) mean() (float64, bool) {
	if a.cfg.View != nil {
		return a.cfg.View.Mean()
	}
	st := estimator.StatsAt(a.cfg.Est, time.Now())
	return st.Mean, st.Ready
}

func (a *PairActor) meanReady() bool { _, ok := a.mean(); return ok }

// statsSnapshot reports estimator state for logs, from either source.
func (a *PairActor) statsSnapshot() estimator.Stats {
	if a.cfg.View != nil {
		return a.cfg.View.Stats()
	}
	return estimator.StatsAt(a.cfg.Est, time.Now())
}

func (a *PairActor) book(venue string, marketID int) (domain.Book, bool) {
	if (venue != a.cfg.L.Name() || marketID != a.cfg.Pair.Left.MarketID) &&
		(venue != a.cfg.R.Name() || marketID != a.cfg.Pair.Right.MarketID) {
		return domain.Book{}, false
	}
	if a.cfg.View != nil {
		b, ok := a.cfg.View.Book(venue)
		age := time.Since(b.Ts)
		return b, ok && b.Valid() && b.Venue == venue && b.MarketID == marketID && age >= 0 && age <= a.cfg.MaxBookAge
	}
	a.bookMu.RLock()
	defer a.bookMu.RUnlock()
	b := a.bookR
	if venue == a.cfg.L.Name() {
		b = a.bookL
	}
	age := time.Since(b.Ts)
	return b, b.Valid() && b.Venue == venue && b.MarketID == marketID && age >= 0 && age <= a.cfg.MaxBookAge
}

// collectBooks keeps rescue pricing current while Run waits for execution.
func (a *PairActor) collectBooks(ctx context.Context, chL, chR <-chan domain.Book) {
	for chL != nil || chR != nil {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-chL:
			if !ok {
				chL = nil
			}
			a.bookMu.Lock()
			a.bookL = b
			a.bookMu.Unlock()
			a.wakeDecide()
		case b, ok := <-chR:
			if !ok {
				chR = nil
			}
			a.bookMu.Lock()
			a.bookR = b
			a.bookMu.Unlock()
			a.wakeDecide()
		}
	}
}

// wakeDecide signals the decision loop that fresh books are available. The
// channel is coalescing: pending updates collapse into one wake.
func (a *PairActor) wakeDecide() {
	select {
	case a.bookWake <- struct{}{}:
	default:
	}
}

func (a *PairActor) persist() error {
	err := a.cfg.Store.Save(a.key, store.PairState{
		NextID: a.next, Steps: a.state.Steps, Day: a.volAcc.day,
		VolumeUSD: a.volAcc.volumeUSD, PnLUSD: a.volAcc.pnlUSD,
		PnLUncertain: a.volAcc.pnlUncertain, Unresolved: a.checkpoint,
		CheckpointExit: a.checkpointExit, Interventions: a.interventions,
		Estimator: a.estimatorSnapshot(),
	})
	if err != nil {
		a.state.Mode = strategy.ModePaused
		a.settled.Store(false)
		a.fatalErr = fmt.Errorf("persist %s: %w", a.key, err)
		a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = a.fatalErr.Error() })
		return a.fatalErr
	}
	return nil
}

// estimatorSnapshot returns the mean estimator's restart state, or nil when
// the estimator does not support persistence (SMA cold-starts by design).
func (a *PairActor) estimatorSnapshot() *estimator.State {
	if a.cfg.Est == nil {
		return nil
	}
	if p, ok := a.cfg.Est.(estimator.Persistable); ok {
		s := p.Save()
		return &s
	}
	return nil
}

// sampleMean feeds the mean estimator from the freshest books. Event-driven
// sampling keeps the mean within one book update of the market; the 1s
// fallback ticker remains for quiet stretches and is a no-op for bucket-based
// estimators (SMA dedups per bucket, z-score weights by real dt). Only the
// actor goroutine owns the estimator, so sampling happens here, never in the
// collector goroutine.
func (a *PairActor) sampleMean(now time.Time) {
	if a.cfg.View != nil || a.cfg.Est == nil {
		return // auto mode: the shared view owns the estimator
	}
	if q, ok := a.quote(now); ok {
		a.cfg.Est.Update(now, q.Mid)
	}
}

func (a *PairActor) jitter() time.Duration {
	d := a.cfg.IntervalMax - a.cfg.IntervalMin
	if d <= 0 {
		return a.cfg.IntervalMin
	}
	return a.cfg.IntervalMin + time.Duration(rand.Int63n(int64(d)))
}

// reconcile checks persisted steps against live positions.
func (a *PairActor) reconcile(ctx context.Context) error {
	// Any failed query or mismatch must block both opens and closes.
	a.state.Mode = strategy.ModePaused
	a.busy.Store(true)
	defer a.busy.Store(false)
	pl, err := execution.MarketPosition(ctx, a.cfg.L, a.cfg.Pair.Left.MarketID)
	if err != nil {
		return fmt.Errorf("%s positions: %w", a.cfg.L.Name(), err)
	}
	pr, err := execution.MarketPosition(ctx, a.cfg.R, a.cfg.Pair.Right.MarketID)
	if err != nil {
		return fmt.Errorf("%s positions: %w", a.cfg.R.Name(), err)
	}
	a.posL, a.posR = pl, pr
	expL := a.expectedL()
	epsL := math.Pow10(-a.cfg.Pair.Left.SizeDecimals) / 2
	epsR := math.Pow10(-a.cfg.Pair.Right.SizeDecimals) / 2
	eps := math.Min(epsL, epsR)
	a.log.Info("positions", "posL", pl, "posR", pr, "expected_posL", expL, "steps", a.state.Counter())
	if math.Abs(pl+pr) >= eps {
		return fmt.Errorf("positions unhedged (posL %.6f + posR %.6f != 0): paused, fix manually", pl, pr)
	}
	if math.Abs(pl-expL) >= epsL || math.Abs(pr+expL) >= epsR {
		return fmt.Errorf("positions differ from recorded steps (posL %.6f posR %.6f, expected L %.6f R %.6f): paused, fix manually", pl, pr, expL, -expL)
	}
	a.reconciled = true
	a.syncPositionState()
	a.settled.Store(true)
	for _, st := range a.state.Steps {
		if st.OpenL <= 0 || st.OpenR <= 0 {
			a.legacyPrices = true
			if !a.checkpoint {
				return fmt.Errorf("step %d lacks entry prices; normal trading paused, drain remains available", st.ID)
			}
		}
	}
	if a.checkpoint {
		// A previous execution was checkpointed before sending. Positions now
		// match the ledger, but a late order could still land. Keep trading
		// paused and require a stable observation window before clearing.
		a.settled.Store(false)
		a.state.Mode = strategy.ModePaused
		if a.checkpointSince.IsZero() {
			a.checkpointSince = time.Now()
		}
		a.log.Warn("unresolved execution from a previous run; positions match the ledger, verifying stability before resuming")
		return nil
	}
	if a.legacyPrices {
		a.state.Mode = strategy.ModePaused
		a.log.Warn("ledger contains steps without entry prices; normal trading stays paused, drain and emergency remain available")
		return nil
	}
	a.state.Mode = strategy.ModeWarmup
	a.log.Info("position reconciliation passed")
	return nil
}

// recoverCheckpoint re-verifies positions after a restart left an unresolved
// pre-send checkpoint. The checkpoint is cleared only after positions match
// the ledger continuously for the configured grace period; daily PnL is then
// marked uncertain because the interrupted execution's fills are unknown.
func (a *PairActor) recoverCheckpoint(ctx context.Context, now time.Time) {
	every := a.cfg.CheckpointCheckEvery
	if every <= 0 {
		every = 15 * time.Second
	}
	if now.Sub(a.checkpointCheckedAt) < every {
		return
	}
	a.checkpointCheckedAt = now
	if err := a.reconcile(ctx); err != nil {
		// A mismatch restarts the stability window; clearing requires the
		// positions to match continuously for the whole grace period.
		a.checkpointSince = time.Time{}
		a.log.Error("checkpoint recovery re-reconcile", "err", err)
		a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = fmt.Sprintf("checkpoint recovery: %v", err) })
		return
	}
	grace := a.cfg.CheckpointGrace
	if grace <= 0 {
		// Signed transactions can remain executable well beyond a few
		// seconds; observe position stability for longer than that window.
		grace = 15 * time.Minute
	}
	if now.Sub(a.checkpointSince) < grace {
		return
	}
	a.checkpoint = false
	a.volAcc.pnlUncertain = true
	if err := a.persist(); err != nil {
		a.log.Error("persist checkpoint recovery", "err", err)
		return
	}
	a.settled.Store(true)
	if !a.legacyPrices {
		a.state.Mode = strategy.ModeWarmup
	}
	a.log.Warn("cleared unresolved checkpoint after a stable position match; daily PnL marked uncertain")
	a.startupRecovered.Store(true)
}

// Run the actor until ctx is done.
func (a *PairActor) Run(ctx context.Context) (retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer func() { retErr = errors.Join(retErr, a.persist()) }()
	if a.cfg.View == nil {
		chL, err := a.cfg.L.SubscribeBook(ctx, a.cfg.Pair.Left)
		if err != nil {
			return err
		}
		chR, err := a.cfg.R.SubscribeBook(ctx, a.cfg.Pair.Right)
		if err != nil {
			return err
		}
		done := make(chan struct{})
		go func() { defer close(done); a.collectBooks(ctx, chL, chR) }()
		defer func() { cancel(); <-done }()
	}
	if a.cfg.Samples != nil {
		defer func() { retErr = errors.Join(retErr, a.cfg.Samples.Close()) }()
	}
	if !a.cfg.DryRun {
		if err := a.reconcile(ctx); err != nil {
			a.log.Error("reconcile", "err", err)
			a.record(monitor.KindError, func(e *monitor.Event) { e.Msg = err.Error() })
		}
	}
	if a.cfg.SampleEvery <= 0 {
		a.cfg.SampleEvery = time.Second
	}
	if a.cfg.VolEvery <= 0 {
		a.cfg.VolEvery = 100 * time.Millisecond
	}
	if a.cfg.VolMaxAge <= 0 {
		a.cfg.VolMaxAge = 50 * time.Millisecond
	}
	var sample, volC <-chan time.Time
	if a.cfg.View == nil {
		t := time.NewTicker(a.cfg.SampleEvery)
		defer t.Stop()
		sample = t.C
		if a.cfg.Vol != nil {
			v := time.NewTicker(a.cfg.VolEvery)
			defer v.Stop()
			volC = v.C
		}
	}
	if !a.cfg.DryRun && a.cfg.Risk.trigger > 0 {
		done := make(chan struct{})
		go func() { defer close(done); a.watchRisk(ctx) }()
		defer func() { cancel(); <-done }()
	}
	// Decisions are event-driven: a book update (manual mode) or a shared-view
	// update (auto mode) wakes the loop immediately. The ticker is only a
	// fallback so pending/checkpoint/drain/emergency work still progresses
	// when no updates arrive for a while.
	housekeep := time.NewTicker(50 * time.Millisecond)
	defer housekeep.Stop()
	var viewUpdates <-chan struct{}
	// The shared view may additionally expose coalesced update signals; the
	// interface stays small, so opt in with a type assertion at the call site.
	if v, ok := a.cfg.View.(interface{ Updates() <-chan struct{} }); ok {
		viewUpdates = v.Updates()
	}
	report := time.NewTicker(time.Minute)
	defer report.Stop()
	nextAt := time.Now().Add(a.jitter())
	for {
		if a.fatalErr != nil {
			return a.fatalErr
		}
		select {
		case <-ctx.Done():
			return nil
		case <-a.drainWake:
			// Drain is an actor-owned transition; it cannot be overwritten by risk recovery.
			if a.reconciled {
				a.state.Mode = strategy.ModeReduceOnly
			}
		case <-sample:
			now := time.Now()
			if q, ok := a.quote(now); ok {
				a.cfg.Est.Update(now, q.Mid)
			}
		case <-volC:
			now := time.Now()
			if q, ok := a.volQuote(now); ok {
				a.cfg.Vol.AddAt(now, q.Mid)
				if a.cfg.VolL != nil {
					a.cfg.VolL.AddAt(now, q.L.Mid())
				}
				if a.cfg.VolR != nil {
					a.cfg.VolR.AddAt(now, q.R.Mid())
				}
				if a.cfg.Samples != nil {
					if err := a.cfg.Samples.AddQuote(now, q); err != nil {
						return fmt.Errorf("sample %s: %w", a.key, err)
					}
				}
			}
		case <-report.C:
			now := time.Now()
			st := a.statsSnapshot()
			a.record(monitor.KindStatus, func(e *monitor.Event) {
				e.Steps = absCounter(a.state.Counter())
				e.Mode = a.state.Mode.String()
				e.MuBps, e.SigmaBps, e.TickK, e.FailRate = st.Mean, a.volBps(), a.tickK(), a.FailRate()
				e.MaxSteps = a.cfg.Params.MaxSteps
				e.OldestStepAgeS = a.oldestStepAge(now)
			})
			a.log.Info("status", "mode", a.state.Mode, "steps", a.state.Counter(),
				"volume_usd", strconv.FormatFloat(a.procVolumeUSD, 'f', 8, 64),
				"pnl_usd", strconv.FormatFloat(a.procPnlUSD, 'f', 8, 64),
				"fail_rate", a.FailRate(), "sigma_bps", round4(a.volBps()), "vol_ready", a.volReady(time.Now()))
			a.lat.reset()
			a.logAccountRecon()
		case <-housekeep.C:
			nextAt = a.decideOnce(ctx, time.Now(), nextAt)
		case <-a.bookWake:
			now := time.Now()
			a.sampleMean(now)
			nextAt = a.decideOnce(ctx, now, nextAt)
		case <-viewUpdates:
			now := time.Now()
			a.sampleMean(now)
			nextAt = a.decideOnce(ctx, now, nextAt)
		}
	}
}

// decideOnce runs one decision pass: housekeeping (pending/checkpoint/drain/
// emergency/mode transitions) and the opportunity evaluation. nextAt is the
// action-interval cooldown deadline; a non-nil action extends it.
func (a *PairActor) decideOnce(ctx context.Context, now time.Time, nextAt time.Time) time.Time {
	if a.pending != nil {
		a.resolvePending(ctx)
		return nextAt
	}
	if a.checkpoint {
		if a.checkpointExit || a.emergencyRequested.Load() || a.draining.Load() {
			a.recoverCheckpointExit(ctx, now)
		} else {
			a.recoverCheckpoint(ctx, now)
		}
		return nextAt
	}
	if a.emergencyRequested.Load() && a.reconciled {
		a.emergency(ctx)
		return now.Add(a.jitter())
	}
	if a.draining.Load() {
		if a.reconciled {
			a.drain(ctx)
		}
		return nextAt
	}
	if a.state.Mode == strategy.ModePaused {
		return nextAt
	}
	a.applyAdaptiveWindows(now)
	if a.riskBlocked.Load() {
		a.state.Mode = strategy.ModeReduceOnly
	} else if a.state.Mode == strategy.ModeReduceOnly && a.meanReady() {
		a.state.Mode = strategy.ModeRunning
	}
	if a.state.Mode == strategy.ModeWarmup && a.meanReady() && !a.riskBlocked.Load() {
		a.state.Mode = strategy.ModeRunning
	}
	if now.Before(nextAt) {
		return nextAt
	}
	q, ok := a.quote(now)
	if !ok {
		return nextAt
	}
	canSize := a.refreshSizing()
	mu, ready := a.mean()
	st := a.state
	st.LastDir = a.lastDir
	st.CapEngaged = a.capEngaged
	st.Now = now
	st.ForcedCloseAt = a.forcedCloseAt
	vol := a.volBps()
	if _, zk := a.cfg.Strategy.(strategy.ZScore); zk {
		// The z-score strategy normalizes by the long-horizon dispersion of
		// the estimator, not the short-horizon volatility chain.
		if lv, ok := a.cfg.Est.(estimator.LongVol); ok {
			vol = lv.LongVol()
		}
	}
	act := a.cfg.Strategy.Decide(a.cfg.Params, st, q, mu, ready, vol)
	if act.Kind == strategy.None {
		return nextAt
	}
	if (act.Kind == strategy.Open || act.Kind == strategy.Close) && !a.volReady(now) {
		// Volatility is the key PnL protection: without it the open
		// threshold loses its adverse-move buffer and closes cannot be
		// priced against a trustworthy short-horizon move, so both are
		// blocked until the short-horizon sigma chain is warm again.
		if time.Since(a.volBlockLoggedAt) > 30*time.Second {
			a.log.Warn("trading blocked until short-horizon volatility is ready", "kind", act.Kind, "sigma_bps", round4(a.volBps()))
			a.volBlockLoggedAt = time.Now()
		}
		return nextAt
	}
	if act.Kind == strategy.Open && (!canSize || a.riskBlocked.Load() || a.cfg.MayOpen != nil && !a.cfg.MayOpen()) {
		return nextAt
	}
	a.apply(ctx, act, q)
	return time.Now().Add(a.jitter())
}

func absCounter(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// oldestStepAge reports the age of the oldest open step in seconds (0 when
// flat). It is observational only and never changes a trading decision.
func (a *PairActor) oldestStepAge(now time.Time) float64 {
	oldest := 0.0
	for i := range a.state.Steps {
		if a.state.Steps[i].OpenedAt.IsZero() {
			continue
		}
		if d := now.Sub(a.state.Steps[i].OpenedAt).Seconds(); d > oldest {
			oldest = d
		}
	}
	return oldest
}

func (a *PairActor) quote(now time.Time) (spread.Quote, bool) {
	if a.cfg.View != nil {
		return a.cfg.View.Quote(now, a.cfg.MaxBookAge)
	}
	a.bookMu.RLock()
	l, r := a.bookL, a.bookR
	a.bookMu.RUnlock()
	if now.Sub(l.Ts) > a.cfg.MaxBookAge || now.Sub(r.Ts) > a.cfg.MaxBookAge {
		return spread.Quote{}, false
	}
	if l.Ts.After(now) || r.Ts.After(now) || l.Venue != a.cfg.L.Name() || r.Venue != a.cfg.R.Name() ||
		l.MarketID != a.cfg.Pair.Left.MarketID || r.MarketID != a.cfg.Pair.Right.MarketID {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
}

// refreshSizing realigns the open quantity of an auto-mode actor with the
// current shared depth-based sizing (config notional and depth caps). Existing
// steps keep their own quantities; only new opens use the refreshed size, so
// a restored actor can no longer add at a stale, oversized step quantity.
func (a *PairActor) refreshSizing() bool {
	if a.cfg.View == nil {
		return true
	}
	sizer, ok := a.cfg.View.(interface{ SizedQty() (float64, bool) })
	if !ok {
		return true
	}
	if qty, ok := sizer.SizedQty(); ok && qty > 0 {
		a.cfg.Params.StepQty = qty
		return true
	}
	// Retain the prior positive parameter for closing existing steps, but
	// never treat a failed current sizing attempt as permission to open.
	return false
}

// volQuote returns a spread sample only when both books are fresh and mutually
// close in time, so short-horizon volatility is not polluted by stale reads.
func (a *PairActor) volQuote(now time.Time) (spread.Quote, bool) {
	a.bookMu.RLock()
	l, r := a.bookL, a.bookR
	a.bookMu.RUnlock()
	age := a.cfg.VolMaxAge
	if now.Sub(l.Ts) > age || now.Sub(r.Ts) > age {
		return spread.Quote{}, false
	}
	if l.Ts.After(now) || r.Ts.After(now) || l.Venue != a.cfg.L.Name() || r.Venue != a.cfg.R.Name() ||
		l.MarketID != a.cfg.Pair.Left.MarketID || r.MarketID != a.cfg.Pair.Right.MarketID {
		return spread.Quote{}, false
	}
	if d := l.Ts.Sub(r.Ts); d > age || -d > age {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
}

// volBps is the current short-horizon spread volatility in bps, or 0.
func (a *PairActor) volBps() float64 {
	if a.cfg.View != nil {
		return a.cfg.View.VolBps()
	}
	if a.cfg.Vol == nil {
		return 0
	}
	return a.cfg.Vol.VolAt(time.Now())
}

// volReady reports whether the short-horizon volatility chain is warm and
// fresh. Strategy openings and closings require it; only the safety exits
// (rescue, drain and emergency unwind) proceed without it.
func (a *PairActor) volReady(now time.Time) bool {
	if a.cfg.View != nil {
		return a.cfg.View.VolReady()
	}
	if a.cfg.Vol == nil {
		return false
	}
	return a.cfg.Vol.ReadyAt(now)
}

// applyAdaptiveWindows periodically re-derives the volatility window from the
// recent per-leg fill latencies. The spread window takes the larger leg, as
// the hedge is only complete once the slower leg fills.
func (a *PairActor) applyAdaptiveWindows(now time.Time) {
	cfg := a.cfg.AdaptiveLatency
	if !cfg.enabled || cfg.updateInterval <= 0 {
		return
	}
	if now.Sub(a.winUpdatedAt) < cfg.updateInterval {
		return
	}
	a.winUpdatedAt = now
	interval := a.cfg.VolEvery
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	kL, okL := targetWindowSlots(a.latL, cfg, interval)
	kR, okR := targetWindowSlots(a.latR, cfg, interval)
	if !okL && !okR {
		return
	}
	spread := max(kL, kR)
	a.applyVolWindows(spread, kL, kR)
}

func (a *PairActor) applyVolWindows(kSpread, kL, kR int) {
	if a.cfg.View != nil {
		a.cfg.View.SetVolWindows(kSpread, kL, kR)
		return
	}
	if a.cfg.Vol != nil && kSpread > 0 {
		a.cfg.Vol.SetWindow(kSpread)
	}
	if a.cfg.VolL != nil && kL > 0 {
		a.cfg.VolL.SetWindow(kL)
	}
	if a.cfg.VolR != nil && kR > 0 {
		a.cfg.VolR.SetWindow(kR)
	}
}

// feedLatency records the per-leg submit-to-terminal latency of a clean,
// one-shot execution. Rescue rounds and unresolved outcomes are excluded so
// the statistics describe the normal fill path only.
func (a *PairActor) feedLatency(res execution.Result) {
	if !a.cfg.AdaptiveLatency.enabled || res.RescueOrders > 0 {
		return
	}
	if res.L.Resolved && res.L.Status == domain.StatusFilled && res.L.Latency > 0 {
		a.latL.add(res.L.Latency)
	}
	if res.R.Resolved && res.R.Status == domain.StatusFilled && res.R.Latency > 0 {
		a.latR.add(res.R.Latency)
	}
}

func round4(v float64) float64 { return math.Round(v*10000) / 10000 }

// record publishes a runtime event to the optional monitor.
func (a *PairActor) record(kind monitor.Kind, fill func(*monitor.Event)) {
	if a.cfg.Monitor == nil {
		return
	}
	e := monitor.Event{Time: time.Now(), Pair: a.key, Kind: kind, Mode: a.state.Mode.String()}
	if fill != nil {
		fill(&e)
	}
	a.cfg.Monitor.Record(e)
}

// logAccountRecon prints the per-minute account reconciliation line: both
// venues' total asset value, positions, unrealized PnL, the ledger's realized
// PnL, and the funding residual. It is observational only.
func (a *PairActor) logAccountRecon() {
	snap := a.margin.reconAccount()
	markL, markR, _ := a.riskMarks()
	unrealL, unrealR, total, deltaTotal, deltaUnreal, deltaRealized, funding, book, deltaBook, next :=
		computeRecon(snap, markL, markR, a.volAcc.pnlUSD, a.reconLast, a.reconInit)
	a.reconLast, a.reconInit = next, true
	recon := monitor.AccountRecon{
		QtyL:           monitor.Fixed8(snap.QtyL),
		QtyR:           monitor.Fixed8(snap.QtyR),
		EquityL:        monitor.Fixed4(snap.EquityL),
		EquityR:        monitor.Fixed4(snap.EquityR),
		TotalEquity:    monitor.Fixed4(total),
		EquityBook:     monitor.Fixed4(book),
		UnrealL:        monitor.Fixed4(unrealL),
		UnrealR:        monitor.Fixed4(unrealR),
		UnrealTotal:    monitor.Fixed4(unrealL + unrealR),
		LedgerRealized: monitor.Fixed4(a.volAcc.pnlUSD),
		DeltaTotal:     monitor.Fixed4(deltaTotal),
		DeltaBook:      monitor.Fixed4(deltaBook),
		DeltaUnreal:    monitor.Fixed4(deltaUnreal),
		DeltaRealized:  monitor.Fixed4(deltaRealized),
		FundingEst:     monitor.Fixed4(funding),
	}
	a.record(monitor.KindAccountRecon, func(e *monitor.Event) { e.Recon = &recon })
	a.log.Info("account_recon",
		"qty_l", strconv.FormatFloat(snap.QtyL, 'f', 8, 64),
		"qty_r", strconv.FormatFloat(snap.QtyR, 'f', 8, 64),
		"equity_l", strconv.FormatFloat(snap.EquityL, 'f', 4, 64),
		"equity_r", strconv.FormatFloat(snap.EquityR, 'f', 4, 64),
		"total_equity", strconv.FormatFloat(total, 'f', 4, 64),
		"equity_book", strconv.FormatFloat(book, 'f', 4, 64),
		"unreal_l", strconv.FormatFloat(unrealL, 'f', 4, 64),
		"unreal_r", strconv.FormatFloat(unrealR, 'f', 4, 64),
		"unreal_total", strconv.FormatFloat(unrealL+unrealR, 'f', 4, 64),
		"ledger_realized", strconv.FormatFloat(a.volAcc.pnlUSD, 'f', 4, 64),
		"delta_total", strconv.FormatFloat(deltaTotal, 'f', 4, 64),
		"delta_book", strconv.FormatFloat(deltaBook, 'f', 4, 64),
		"delta_unreal", strconv.FormatFloat(deltaUnreal, 'f', 4, 64),
		"delta_realized", strconv.FormatFloat(deltaRealized, 'f', 4, 64),
		"funding_est", strconv.FormatFloat(funding, 'f', 4, 64))
}

// legVol is the short-horizon leg-price volatility (price units) of a venue.
func (a *PairActor) legVol(venue string) float64 {
	if a.cfg.View != nil {
		return a.cfg.View.LegVol(venue)
	}
	if venue == a.cfg.L.Name() && a.cfg.VolL != nil {
		return a.cfg.VolL.VolAt(time.Now())
	}
	if venue == a.cfg.R.Name() && a.cfg.VolR != nil {
		return a.cfg.VolR.VolAt(time.Now())
	}
	return 0
}

// tickK returns the current allowance coefficient (dynamic or configured).
func (a *PairActor) tickK() float64 {
	if a.adaptive != nil {
		return a.adaptive.K()
	}
	return a.cfg.ExecCfg.TickK
}

func (a *PairActor) observeFailure(failed bool) {
	window := a.cfg.Adaptive.Window
	if window <= 0 {
		window = 50
	}
	if len(a.failures) < window {
		a.failures = append(a.failures, failed)
	} else {
		a.failures[a.failureIndex] = failed
		a.failureIndex = (a.failureIndex + 1) % window
	}
	n := 0
	for _, f := range a.failures {
		if f {
			n++
		}
	}
	a.failBits.Store(math.Float64bits(float64(n) / float64(len(a.failures))))
	if a.adaptive != nil {
		a.adaptive.Observe(failed)
	}
}

// riskMarks returns fresh book mids for both legs.
func (a *PairActor) riskMarks() (float64, float64, bool) {
	lb, lok := a.book(a.cfg.L.Name(), a.cfg.Pair.Left.MarketID)
	rb, rok := a.book(a.cfg.R.Name(), a.cfg.Pair.Right.MarketID)
	var ml, mr float64
	if lok {
		ml = lb.Mid()
	}
	if rok {
		mr = rb.Mid()
	}
	return ml, mr, lok && rok
}

// Trading settlement and dollar accounting live in settlement.go.

// expectedL is the left-leg position implied by the step ledger.
func (a *PairActor) expectedL() float64 {
	var exp float64
	for _, s := range a.state.Steps {
		exp += -float64(s.Dir) * s.Qty
	}
	return exp
}
