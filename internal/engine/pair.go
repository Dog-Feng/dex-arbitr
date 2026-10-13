// Package engine runs one actor goroutine per pair.
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// PairConfig wires one pair actor.
type PairConfig struct {
	Pair          symbols.Pair
	L, R          exchange.Venue
	Params        strategy.Params
	Strategy      strategy.Strategy
	Est           estimator.Estimator
	IntervalMin   time.Duration
	IntervalMax   time.Duration
	MaxBookAge    time.Duration
	SampleEvery   time.Duration
	DryRun        bool // paper trading: fills assumed at decision prices
	Store         *store.Store
	Tracker       *execution.Tracker // nil in dry run
	ExecCfg       execution.Config
	IDs           *execution.IDGen
	Vol           *volatility.Estimator
	VolEvery      time.Duration
	VolMaxAge     time.Duration
	Samples       *store.SampleSink
	Risk          marginConfig
	VolTarget     float64 // daily volume target (USD); 0 = unlimited
	VolCostBudget float64 // max daily realized loss (USD); 0 = unlimited
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

	bookMu sync.RWMutex
	bookL  domain.Book
	bookR  domain.Book
	lat    latencyStat
	margin marginMonitor
	volAcc volumeAcc
}

// volumeAcc accumulates daily traded volume and realized PnL for the volume
// budget gate; owned by the actor goroutine, no locking.
type volumeAcc struct {
	day       string
	volumeUSD float64
	pnlUSD    float64
}

func (a *PairActor) resetVolumeIfNewDay(now time.Time) {
	today := now.UTC().Format("2006-01-02")
	if a.volAcc.day != today {
		a.volAcc = volumeAcc{day: today}
	}
}

// canOpenVolume reports whether the daily volume/cost budget allows another open.
func (a *PairActor) canOpenVolume() bool {
	if a.cfg.VolTarget > 0 && a.volAcc.volumeUSD >= a.cfg.VolTarget {
		return false
	}
	if a.cfg.VolCostBudget > 0 && a.volAcc.pnlUSD <= -a.cfg.VolCostBudget {
		return false
	}
	return true
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
	key := fmt.Sprintf("%s_%s_%s", cfg.Pair.Base, cfg.L.Name(), cfg.R.Name())
	if cfg.DryRun {
		key += "_paper"
	}
	a := &PairActor{cfg: cfg, key: key, log: slog.With("pair", cfg.Pair.Base, "L", cfg.L.Name(), "R", cfg.R.Name())}
	a.margin = marginMonitor{cfg: cfg.Risk}
	st, err := cfg.Store.Load(key)
	if err != nil {
		return nil, err
	}
	a.state = strategy.State{Mode: strategy.ModeWarmup, Steps: st.Steps}
	a.next = st.NextID
	if !cfg.DryRun {
		// Live trading stays paused until reconciliation succeeds.
		a.state.Mode = strategy.ModePaused
		a.exec = execution.NewExecutor(cfg.ExecCfg, cfg.Tracker, cfg.IDs, a.book, a.volBps, nil)
	}
	return a, nil
}

func (a *PairActor) book(venue string, _ int) (domain.Book, bool) {
	a.bookMu.RLock()
	defer a.bookMu.RUnlock()
	b := a.bookR
	if venue == a.cfg.L.Name() {
		b = a.bookL
	}
	return b, b.Valid() && time.Since(b.Ts) <= a.cfg.MaxBookAge
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
		case b, ok := <-chR:
			if !ok {
				chR = nil
			}
			a.bookMu.Lock()
			a.bookR = b
			a.bookMu.Unlock()
		}
	}
}

func (a *PairActor) persist() {
	if err := a.cfg.Store.Save(a.key, store.PairState{NextID: a.next, Steps: a.state.Steps}); err != nil {
		a.log.Error("persist failed", "err", err)
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
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	a.log.Info("positions", "posL", pl, "posR", pr, "expected_posL", expL, "steps", a.state.Counter())
	if math.Abs(pl+pr) >= eps {
		return fmt.Errorf("positions unhedged (posL %.6f + posR %.6f != 0): paused, fix manually", pl, pr)
	}
	if math.Abs(pl-expL) >= eps || math.Abs(pr+expL) >= eps {
		return fmt.Errorf("positions differ from recorded steps (posL %.6f posR %.6f, expected L %.6f R %.6f): paused, fix manually", pl, pr, expL, -expL)
	}
	a.state.Mode = strategy.ModeWarmup
	a.log.Info("position reconciliation passed")
	return nil
}

// Run the actor until ctx is done.
func (a *PairActor) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	chL, err := a.cfg.L.SubscribeBook(ctx, a.cfg.Pair.Left)
	if err != nil {
		return err
	}
	chR, err := a.cfg.R.SubscribeBook(ctx, a.cfg.Pair.Right)
	if err != nil {
		return err
	}
	booksDone := make(chan struct{})
	go func() {
		defer close(booksDone)
		a.collectBooks(ctx, chL, chR)
	}()
	defer func() {
		cancel()
		<-booksDone
	}()
	if a.cfg.Samples != nil {
		defer a.cfg.Samples.Close()
	}
	if !a.cfg.DryRun {
		if err := a.reconcile(ctx); err != nil {
			a.log.Error("reconcile", "err", err)
		}
	}
	if a.cfg.SampleEvery == 0 {
		a.cfg.SampleEvery = time.Second
	}
	if a.cfg.Vol != nil && a.cfg.VolEvery == 0 {
		a.cfg.VolEvery = 100 * time.Millisecond
	}
	if a.cfg.Vol != nil && a.cfg.VolMaxAge == 0 {
		a.cfg.VolMaxAge = 50 * time.Millisecond
	}
	sample := time.NewTicker(a.cfg.SampleEvery)
	defer sample.Stop()
	var volC <-chan time.Time
	if a.cfg.Vol != nil {
		vt := time.NewTicker(a.cfg.VolEvery)
		defer vt.Stop()
		volC = vt.C
	}
	var riskC <-chan time.Time
	if !a.cfg.DryRun && a.cfg.Risk.trigger > 0 {
		rt := time.NewTicker(time.Second)
		defer rt.Stop()
		riskC = rt.C
	}
	decide := time.NewTicker(200 * time.Millisecond)
	defer decide.Stop()
	report := time.NewTicker(time.Minute)
	defer report.Stop()
	nextAt := time.Now().Add(a.jitter())
	a.log.Info("pair actor started", "dry_run", a.cfg.DryRun, "step_qty", a.cfg.Params.StepQty, "max_steps", a.cfg.Params.MaxSteps,
		"size_decimals", a.cfg.Pair.SizeDecimals, "recorded_steps", len(a.state.Steps))

	for {
		select {
		case <-ctx.Done():
			a.persist()
			return nil
		case now := <-sample.C:
			if q, ok := a.quote(now); ok {
				a.cfg.Est.Update(now, q.Mid)
			}
			if a.state.Mode == strategy.ModeWarmup && a.cfg.Est.Ready() {
				a.state.Mode = strategy.ModeRunning
				a.log.Info("mean ready -> RUNNING", "stats", a.cfg.Est.Stats())
			}
		case now := <-volC:
			if q, ok := a.volQuote(now); ok {
				a.cfg.Vol.Add(q.Mid)
				if a.cfg.Samples != nil {
					a.cfg.Samples.Add(now, q.Mid, q.LS, q.SL)
				}
			}
		case now := <-riskC:
			a.risk(ctx, now)
		case <-report.C:
			st := a.cfg.Est.Stats()
			q, _ := a.quote(time.Now())
			vol := 0.0
			if a.cfg.Vol != nil {
				vol = a.cfg.Vol.Vol()
			}
			a.log.Info("status", "mode", a.state.Mode, "steps", a.state.Counter(), "mu_bps", round2(st.Mean),
				"samples", st.Samples, "span", st.Span.Round(time.Second), "coverage", round2(st.Coverage),
				"LS_bps", round2(q.LS), "SL_bps", round2(q.SL), "mid_bps", round2(q.Mid),
				"vol_500ms_bps", round4(vol), "lat_n", a.lat.n, "lat_avg_ms", a.lat.avg().Milliseconds(), "lat_max_ms", a.lat.max.Milliseconds(),
				"day_volume_usd", round2(a.volAcc.volumeUSD), "day_pnl_usd", round4(a.volAcc.pnlUSD))
			a.lat.reset()
		case now := <-decide.C:
			if now.Before(nextAt) || a.state.Mode == strategy.ModePaused {
				continue
			}
			q, ok := a.quote(now)
			if !ok {
				continue
			}
			act := a.cfg.Strategy.Decide(a.cfg.Params, a.state, q, a.cfg.Est.Mean(), a.cfg.Est.Ready(), a.volBps())
			if act.Kind == strategy.None {
				continue
			}
			a.resetVolumeIfNewDay(now)
			if act.Kind == strategy.Open && !a.canOpenVolume() {
				a.log.Warn("volume budget reached; skipping open", "day_volume_usd", a.volAcc.volumeUSD, "day_pnl_usd", a.volAcc.pnlUSD)
				continue
			}
			a.apply(ctx, act, q)
			nextAt = time.Now().Add(a.jitter())
		}
	}
}

func (a *PairActor) quote(now time.Time) (spread.Quote, bool) {
	a.bookMu.RLock()
	l, r := a.bookL, a.bookR
	a.bookMu.RUnlock()
	if now.Sub(l.Ts) > a.cfg.MaxBookAge || now.Sub(r.Ts) > a.cfg.MaxBookAge {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
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
	if d := l.Ts.Sub(r.Ts); d > age || -d > age {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
}

// volBps is the current short-horizon spread volatility in bps, or 0.
func (a *PairActor) volBps() float64 {
	if a.cfg.Vol == nil {
		return 0
	}
	return a.cfg.Vol.Vol()
}

// risk refreshes liquidation data on its interval, then applies the margin
// monitor's decision (arm/disarm/trigger) to the actor state.
func (a *PairActor) risk(ctx context.Context, now time.Time) {
	if a.cfg.DryRun || a.cfg.Risk.trigger <= 0 {
		return
	}
	if now.Sub(a.margin.lastRefresh) >= a.margin.cfg.refresh {
		a.margin.refresh(ctx, a.cfg.L, a.cfg.R, a.cfg.Pair.Left.MarketID, a.cfg.Pair.Right.MarketID)
	}
	markL, markR, fresh := a.riskMarks()
	switch a.margin.assess(markL, markR, fresh) {
	case marginArm:
		if a.state.Mode == strategy.ModeRunning {
			a.state.Mode = strategy.ModeReduceOnly
			a.log.Warn("margin floor reached; reduce only", "markL", round2(markL), "markR", round2(markR))
		}
	case marginDisarm:
		if a.state.Mode == strategy.ModeReduceOnly {
			a.state.Mode = strategy.ModeRunning
			a.log.Info("margin recovered; resuming", "markL", round2(markL), "markR", round2(markR))
		}
	case marginTrigger:
		a.emergency(ctx)
	}
}

// riskMarks returns fresh book mids for both legs.
func (a *PairActor) riskMarks() (float64, float64, bool) {
	lb, lok := a.book(a.cfg.L.Name(), a.cfg.Pair.Left.MarketID)
	rb, rok := a.book(a.cfg.R.Name(), a.cfg.Pair.Right.MarketID)
	if !lok || !rok {
		return 0, 0, false
	}
	return lb.Mid(), rb.Mid(), true
}

// emergency market-flattens both legs to zero and pauses the pair.
func (a *PairActor) emergency(ctx context.Context) {
	a.log.Error("ALERT: liquidation risk, emergency unwind")
	res, err := a.exec.EmergencyFlatten(ctx,
		execution.LegSpec{Venue: a.cfg.L, Inst: a.cfg.Pair.Left},
		execution.LegSpec{Venue: a.cfg.R, Inst: a.cfg.Pair.Right},
		0, a.posL, a.posR)
	if err != nil {
		a.log.Error("emergency flatten failed; will retry", "err", err)
		return
	}
	a.posL, a.posR = res.PosL, res.PosR
	a.state.Steps = nil
	a.state.Mode = strategy.ModePaused
	a.log.Error("emergency unwind done", "rescue_orders", res.RescueOrders, "posL", res.PosL, "posR", res.PosR,
		"took", res.Duration.Round(time.Millisecond))
	a.persist()
}

func (a *PairActor) apply(ctx context.Context, act strategy.Action, q spread.Quote) {
	a.log.Info("action", "kind", act.Kind, "dir", act.Dir, "qty", act.Qty, "L", fmt.Sprintf("%s@%.4f", act.L.Side, act.L.Price),
		"R", fmt.Sprintf("%s@%.4f", act.R.Side, act.R.Price), "expected_bps", round2(act.ExpectedBps), "reason", act.Reason)

	fillL, fillR, pxL, pxR := act.Qty, act.Qty, act.L.Price, act.R.Price
	rescueLPx, rescueRPx := 0.0, 0.0
	if !a.cfg.DryRun {
		res := a.exec.Execute(ctx,
			execution.LegSpec{Venue: a.cfg.L, Inst: a.cfg.Pair.Left, Side: act.L.Side, Price: act.L.Price},
			execution.LegSpec{Venue: a.cfg.R, Inst: a.cfg.Pair.Right, Side: act.R.Side, Price: act.R.Price},
			act.Qty, act.Kind == strategy.Close, a.posL, a.posR)
		fillL, fillR, pxL, pxR = res.L.Filled, res.R.Filled, res.L.AvgPrice, res.R.AvgPrice
		rescueLPx, rescueRPx = res.L.RescuePrice, res.R.RescuePrice
		a.posL, a.posR = res.PosL, res.PosR
		a.lat.add(res.L.Latency)
		a.lat.add(res.R.Latency)
		a.log.Info("executed", "fillL", fillL, "fillR", fillR, "pxL", pxL, "pxR", pxR, "statusL", res.L.Status, "statusR", res.R.Status,
			"rescue_orders", res.RescueOrders, "rescue_qty", res.RescueQty, "posL", res.PosL, "posR", res.PosR, "took", res.Duration.Round(time.Millisecond),
			"latL_ms", res.L.Latency.Milliseconds(), "latR_ms", res.R.Latency.Milliseconds())
	}
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	switch act.Kind {
	case strategy.Open:
		h := math.Min(fillL, fillR)
		if h < eps {
			a.log.Warn("open produced no hedged qty")
			break
		}
		st := strategy.Step{ID: a.next, Dir: act.Dir, Qty: domain.Round(h, a.cfg.Pair.SizeDecimals),
			EntryBps: strategy.EntryBps(act.Dir, pxL, pxR), OpenL: pxL, OpenR: pxR, OpenedAt: time.Now()}
		a.next++
		a.state.Steps = append(a.state.Steps, st)
		a.volAcc.volumeUSD += 2 * st.Qty * q.RefMid
		a.log.Info("step opened", "id", st.ID, "entry_bps", round2(st.EntryBps), "counter", a.state.Counter())
	case strategy.Close:
		closed := math.Max(fillL, fillR) // the lagging leg is completed by rescue
		fee := a.roundTripFeeBps()
		for i := range a.state.Steps {
			if a.state.Steps[i].ID != act.StepID {
				continue
			}
			if closed >= eps {
				lpx, rpx := pxL, pxR
				if fillL <= eps && rescueLPx > 0 {
					lpx = rescueLPx
				}
				if fillR <= eps && rescueRPx > 0 {
					rpx = rescueRPx
				}
				gross := a.state.Steps[i].EntryBps + strategy.EntryBps(-act.Dir, lpx, rpx)
				net := gross - fee
				a.state.Steps[i].Realized += net
				a.volAcc.volumeUSD += 2 * closed * q.RefMid
				a.volAcc.pnlUSD += closed * q.RefMid * net / 1e4
				a.log.Info("step pnl", "id", act.StepID, "gross_bps", round2(gross), "fee_bps", round2(fee),
					"net_bps", round2(net), "realized_bps", round2(a.state.Steps[i].Realized), "lpx", lpx, "rpx", rpx)
			}
			a.state.Steps[i].Qty = domain.Round(a.state.Steps[i].Qty-closed, a.cfg.Pair.SizeDecimals)
			if a.state.Steps[i].Qty < eps {
				a.state.Steps = append(a.state.Steps[:i], a.state.Steps[i+1:]...)
				a.log.Info("step closed", "id", act.StepID, "profit_bps", round2(act.ExpectedBps), "counter", a.state.Counter())
			}
			break
		}
	}
	if !a.cfg.DryRun {
		if err := a.checkHedge(); err != nil {
			a.state.Mode = strategy.ModePaused
			a.log.Error("hedge invariant broken; pausing", "err", err)
		}
	}
	a.persist()
}

// expectedL is the left-leg position implied by the step ledger.
func (a *PairActor) expectedL() float64 {
	var exp float64
	for _, s := range a.state.Steps {
		exp += -float64(s.Dir) * s.Qty
	}
	return exp
}

// roundTripFeeBps is the total taker fee (bps) of a full open+close round trip.
func (a *PairActor) roundTripFeeBps() float64 {
	return a.cfg.Params.FeeBps
}

// checkHedge verifies the step ledger still matches venue positions after
// execution; drift is an invariant violation (e.g. an incomplete rescue).
func (a *PairActor) checkHedge() error {
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	expL := a.expectedL()
	if math.Abs(a.posL-expL) >= eps || math.Abs(a.posR+expL) >= eps {
		return fmt.Errorf("posL %.6f posR %.6f vs expected L %.6f R %.6f", a.posL, a.posR, expL, -expL)
	}
	return nil
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }

func round4(v float64) float64 { return math.Round(v*10000) / 10000 }
