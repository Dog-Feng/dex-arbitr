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
)

// PairConfig wires one pair actor.
type PairConfig struct {
	Pair        symbols.Pair
	L, R        exchange.Venue
	Params      strategy.Params
	Est         estimator.Estimator
	IntervalMin time.Duration
	IntervalMax time.Duration
	MaxBookAge  time.Duration
	SampleEvery time.Duration
	DryRun      bool // paper trading: fills assumed at decision prices
	Store       *store.Store
	Tracker     *execution.Tracker // nil in dry run
	ExecCfg     execution.Config
	IDs         *execution.IDGen
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
}

// NewPairActor builds the actor and loads persisted steps.
func NewPairActor(cfg PairConfig) (*PairActor, error) {
	key := fmt.Sprintf("%s_%s_%s", cfg.Pair.Base, cfg.L.Name(), cfg.R.Name())
	if cfg.DryRun {
		key += "_paper"
	}
	a := &PairActor{cfg: cfg, key: key, log: slog.With("pair", cfg.Pair.Base, "L", cfg.L.Name(), "R", cfg.R.Name())}
	st, err := cfg.Store.Load(key)
	if err != nil {
		return nil, err
	}
	a.state = strategy.State{Mode: strategy.ModeWarmup, Steps: st.Steps}
	a.next = st.NextID
	if !cfg.DryRun {
		a.exec = execution.NewExecutor(cfg.ExecCfg, cfg.Tracker, cfg.IDs, a.book, nil)
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
	pl, err := execution.MarketPosition(ctx, a.cfg.L, a.cfg.Pair.Left.MarketID)
	if err != nil {
		return err
	}
	pr, err := execution.MarketPosition(ctx, a.cfg.R, a.cfg.Pair.Right.MarketID)
	if err != nil {
		return err
	}
	a.posL, a.posR = pl, pr
	var expL float64
	for _, s := range a.state.Steps {
		expL += -float64(s.Dir) * s.Qty
	}
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	a.log.Info("positions", "posL", pl, "posR", pr, "expected_posL", expL, "steps", a.state.Counter())
	if math.Abs(pl+pr) >= eps {
		a.state.Mode = strategy.ModePaused
		return fmt.Errorf("positions unhedged (posL %.6f + posR %.6f != 0): paused, fix manually", pl, pr)
	}
	if math.Abs(pl-expL) >= eps {
		a.log.Warn("position differs from recorded steps (manual trades?)", "posL", pl, "expected", expL)
	}
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
	if !a.cfg.DryRun {
		if err := a.reconcile(ctx); err != nil {
			a.log.Error("reconcile", "err", err)
		}
	}
	if a.cfg.SampleEvery == 0 {
		a.cfg.SampleEvery = time.Second
	}
	sample := time.NewTicker(a.cfg.SampleEvery)
	defer sample.Stop()
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
		case <-report.C:
			st := a.cfg.Est.Stats()
			q, _ := a.quote(time.Now())
			a.log.Info("status", "mode", a.state.Mode, "steps", a.state.Counter(), "mu_bps", round2(st.Mean),
				"samples", st.Samples, "span", st.Span.Round(time.Second), "coverage", round2(st.Coverage),
				"LS_bps", round2(q.LS), "SL_bps", round2(q.SL), "mid_bps", round2(q.Mid))
		case now := <-decide.C:
			if now.Before(nextAt) || a.state.Mode == strategy.ModePaused {
				continue
			}
			q, ok := a.quote(now)
			if !ok {
				continue
			}
			act := strategy.Decide(a.cfg.Params, a.state, q, a.cfg.Est.Mean(), a.cfg.Est.Ready())
			if act.Kind == strategy.None {
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

func (a *PairActor) apply(ctx context.Context, act strategy.Action, q spread.Quote) {
	a.log.Info("action", "kind", act.Kind, "dir", act.Dir, "qty", act.Qty, "L", fmt.Sprintf("%s@%.4f", act.L.Side, act.L.Price),
		"R", fmt.Sprintf("%s@%.4f", act.R.Side, act.R.Price), "expected_bps", round2(act.ExpectedBps), "reason", act.Reason)

	fillL, fillR, pxL, pxR := act.Qty, act.Qty, act.L.Price, act.R.Price
	if !a.cfg.DryRun {
		res := a.exec.Execute(ctx,
			execution.LegSpec{Venue: a.cfg.L, Inst: a.cfg.Pair.Left, Side: act.L.Side, Price: act.L.Price},
			execution.LegSpec{Venue: a.cfg.R, Inst: a.cfg.Pair.Right, Side: act.R.Side, Price: act.R.Price},
			act.Qty, act.Kind == strategy.Close, a.posL, a.posR)
		fillL, fillR, pxL, pxR = res.L.Filled, res.R.Filled, res.L.AvgPrice, res.R.AvgPrice
		a.posL, a.posR = res.PosL, res.PosR
		a.log.Info("executed", "fillL", fillL, "fillR", fillR, "pxL", pxL, "pxR", pxR, "statusL", res.L.Status, "statusR", res.R.Status,
			"rescue_orders", res.RescueOrders, "rescue_qty", res.RescueQty, "posL", res.PosL, "posR", res.PosR, "took", res.Duration.Round(time.Millisecond))
	}
	eps := math.Pow10(-a.cfg.Pair.SizeDecimals) / 2
	switch act.Kind {
	case strategy.Open:
		h := math.Min(fillL, fillR)
		if h < eps {
			a.log.Warn("open produced no hedged qty")
			return
		}
		st := strategy.Step{ID: a.next, Dir: act.Dir, Qty: domain.Round(h, a.cfg.Pair.SizeDecimals),
			EntryBps: strategy.EntryBps(act.Dir, pxL, pxR, q.RefMid), OpenedAt: time.Now()}
		a.next++
		a.state.Steps = append(a.state.Steps, st)
		a.log.Info("step opened", "id", st.ID, "entry_bps", round2(st.EntryBps), "counter", a.state.Counter())
	case strategy.Close:
		closed := math.Max(fillL, fillR) // the lagging leg is completed by rescue
		for i := range a.state.Steps {
			if a.state.Steps[i].ID != act.StepID {
				continue
			}
			a.state.Steps[i].Qty = domain.Round(a.state.Steps[i].Qty-closed, a.cfg.Pair.SizeDecimals)
			if a.state.Steps[i].Qty < eps {
				a.state.Steps = append(a.state.Steps[:i], a.state.Steps[i+1:]...)
				a.log.Info("step closed", "id", act.StepID, "profit_bps", round2(act.ExpectedBps), "counter", a.state.Counter())
			}
			break
		}
	}
	a.persist()
}

func round2(v float64) float64 { return math.Round(v*100) / 100 }
