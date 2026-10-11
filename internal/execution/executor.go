package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// Config of the executor.
type Config struct {
	AckTimeout       time.Duration // wait for WS terminal state
	QueryRetries     int           // REST fallback attempts
	QueryInterval    time.Duration
	SettleDelay      time.Duration // before reading positions
	RescueInterval   time.Duration
	RescueAlertAfter time.Duration
	Rescue           RescueParams
	// PriceTicks widens the IOC limit price of each leg by this many ticks
	// beyond the decision price (buy: +N ticks, sell: -N ticks). Fills still
	// match from the best level, so this only adds a worst-case allowance.
	PriceTicks int
}

// DefaultConfig values.
func DefaultConfig() Config {
	return Config{AckTimeout: 3 * time.Second, QueryRetries: 3, QueryInterval: time.Second,
		SettleDelay: 700 * time.Millisecond, RescueInterval: 400 * time.Millisecond,
		RescueAlertAfter: 10 * time.Second, Rescue: DefaultRescue, PriceTicks: 3}
}

// LegSpec is one side of a hedged action.
type LegSpec struct {
	Venue exchange.Venue
	Inst  domain.Instrument
	Side  domain.Side
	Price float64
}

// LegResult is the resolved outcome of a leg.
type LegResult struct {
	ClientID  int64
	Status    domain.OrderStatus
	Filled    float64 // unsigned
	AvgPrice  float64
	Resolved  bool // status came from venue (WS/REST), else inferred from positions
	SubmitErr error
}

// Result of a two-leg execution.
type Result struct {
	L, R         LegResult
	PosL, PosR   float64 // final positions on the pair markets
	RescueOrders int
	RescueQty    float64
	Duration     time.Duration
}

// BookFn returns the latest book for a venue/market (for rescue pricing).
type BookFn func(venue string, marketID int) (domain.Book, bool)

// Executor runs hedged two-leg actions.
type Executor struct {
	cfg     Config
	tracker *Tracker
	ids     *IDGen
	book    BookFn
	alert   func(msg string)
}

// NewExecutor builds an executor.
func NewExecutor(cfg Config, t *Tracker, ids *IDGen, book BookFn, alert func(string)) *Executor {
	if alert == nil {
		alert = func(m string) { slog.Error("ALERT", "msg", m) }
	}
	return &Executor{cfg: cfg, tracker: t, ids: ids, book: book, alert: alert}
}

// MarketPosition returns the signed position on one market.
func MarketPosition(ctx context.Context, v exchange.Venue, marketID int) (float64, error) {
	ps, err := v.Positions(ctx)
	if err != nil {
		return 0, err
	}
	for _, p := range ps {
		if p.MarketID == marketID {
			return p.Qty, nil
		}
	}
	return 0, nil
}

// Execute submits both legs concurrently (IOC), resolves each leg
// (WS -> REST fallback -> positions), then flattens any net exposure.
// before = positions (L, R) on the pair markets prior to the action; the
// invariant enforced is posL+posR == before.L+before.R after completion.
func (e *Executor) Execute(ctx context.Context, l, r LegSpec, qty float64, reduceOnly bool, beforeL, beforeR float64) Result {
	start := time.Now()
	res := Result{}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); res.L = e.runLeg(ctx, l, qty, reduceOnly) }()
	go func() { defer wg.Done(); res.R = e.runLeg(ctx, r, qty, reduceOnly) }()
	wg.Wait()

	// Positions are the source of truth for exposure.
	time.Sleep(e.cfg.SettleDelay)
	pl, pr, err := e.positions(ctx, l, r)
	for err != nil && ctx.Err() == nil {
		slog.Warn("positions read failed, retrying", "err", err)
		time.Sleep(e.cfg.QueryInterval)
		pl, pr, err = e.positions(ctx, l, r)
	}
	inferLeg(&res.L, pl-beforeL, l.Price)
	inferLeg(&res.R, pr-beforeR, r.Price)

	target := beforeL + beforeR
	res.PosL, res.PosR = pl, pr
	if eps := halfUnit(l.Inst, r.Inst); math.Abs(pl+pr-target) >= eps {
		n, q, fl, fr := e.flatten(ctx, l, r, target, pl, pr)
		res.RescueOrders, res.RescueQty, res.PosL, res.PosR = n, q, fl, fr
	}
	res.Duration = time.Since(start)
	return res
}

func inferLeg(lr *LegResult, delta, px float64) {
	if lr.Resolved {
		return
	}
	lr.Filled = math.Abs(delta)
	if lr.Filled > 0 {
		lr.AvgPrice = px // estimate; exact price unknown
		lr.Status = domain.StatusFilled
	}
}

func halfUnit(a, b domain.Instrument) float64 {
	d := a.SizeDecimals
	if b.SizeDecimals > d {
		d = b.SizeDecimals
	}
	return math.Pow10(-d) / 2
}

func (e *Executor) positions(ctx context.Context, l, r LegSpec) (float64, float64, error) {
	var pl, pr float64
	var el, er error
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); pl, el = MarketPosition(ctx, l.Venue, l.Inst.MarketID) }()
	go func() { defer wg.Done(); pr, er = MarketPosition(ctx, r.Venue, r.Inst.MarketID) }()
	wg.Wait()
	return pl, pr, errors.Join(el, er)
}

// runLeg submits one IOC order and resolves its final state.
func (e *Executor) runLeg(ctx context.Context, leg LegSpec, qty float64, reduceOnly bool) LegResult {
	return e.submitAndResolve(ctx, domain.OrderReq{
		ClientID: e.ids.Next(), Inst: leg.Inst, Side: leg.Side, Qty: qty, Price: e.limitPrice(leg),
		Type: domain.Limit, ReduceOnly: reduceOnly,
	}, leg.Venue)
}

// limitPrice returns the IOC limit price for a leg: the decision price moved
// PriceTicks ticks in the aggressive direction. leg.Price itself is left
// untouched so fill estimates and expected_bps keep using the decision price.
func (e *Executor) limitPrice(leg LegSpec) float64 {
	n := e.cfg.PriceTicks
	if n <= 0 {
		return leg.Price
	}
	off := float64(n) * leg.Inst.Tick()
	px := leg.Price + off
	if leg.Side == domain.Sell {
		px = leg.Price - off
		if px <= 0 {
			return leg.Price
		}
	}
	return domain.Round(px, leg.Inst.PriceDecimals)
}

func (e *Executor) submitAndResolve(ctx context.Context, req domain.OrderReq, v exchange.Venue) LegResult {
	lr := LegResult{ClientID: req.ClientID}
	w := e.tracker.Watch(v.Name(), req.ClientID)
	defer e.tracker.Done(v.Name(), req.ClientID)

	_, err := v.Submit(ctx, req)
	lr.SubmitErr = err
	if errors.Is(err, domain.ErrRejected) || errors.Is(err, domain.ErrNoCredentials) {
		lr.Status, lr.Resolved = domain.StatusRejected, true
		slog.Warn("order rejected", "venue", v.Name(), "cid", req.ClientID, "err", err)
		return lr
	}
	// phase 2: WS terminal state
	timer := time.NewTimer(e.cfg.AckTimeout)
	defer timer.Stop()
	for {
		select {
		case u := <-w:
			if u.Status.Terminal() {
				return fill(lr, u)
			}
			continue
		case <-timer.C:
		case <-ctx.Done():
			return lr
		}
		break
	}
	// REST fallback; unknown is never a failure.
	for i := 0; i < e.cfg.QueryRetries && ctx.Err() == nil; i++ {
		u, qerr := v.QueryOrder(ctx, req.Inst, req.ClientID)
		if qerr == nil && u.Status.Terminal() {
			return fill(lr, u)
		}
		select {
		case u := <-w:
			if u.Status.Terminal() {
				return fill(lr, u)
			}
		case <-time.After(e.cfg.QueryInterval):
		}
	}
	slog.Warn("order state unresolved; will infer from positions", "venue", v.Name(), "cid", req.ClientID)
	lr.Status = domain.StatusUnknown
	return lr
}

func fill(lr LegResult, u domain.OrderUpdate) LegResult {
	lr.Status, lr.Filled, lr.AvgPrice, lr.Resolved = u.Status, u.FilledQty, u.AvgPrice(), true
	return lr
}

// flatten loops reduce-only orders until posL+posR == target.
// Positions are re-read before every retry.
func (e *Executor) flatten(ctx context.Context, l, r LegSpec, target, pl, pr float64) (int, float64, float64, float64) {
	start := time.Now()
	alerted := false
	orders, total := 0, 0.0
	eps := halfUnit(l.Inst, r.Inst)
	for attempt := 0; ctx.Err() == nil; attempt++ {
		if attempt > 0 {
			time.Sleep(e.cfg.SettleDelay)
			var err error
			if pl, pr, err = e.positions(ctx, l, r); err != nil {
				slog.Warn("rescue: positions read failed", "err", err)
				time.Sleep(e.cfg.RescueInterval)
				attempt--
				continue
			}
		}
		resid := pl + pr - target
		if math.Abs(resid) < eps {
			slog.Info("rescue complete", "orders", orders, "qty", total, "took", time.Since(start))
			return orders, total, pl, pr
		}
		if !alerted && time.Since(start) > e.cfg.RescueAlertAfter {
			alerted = true
			e.alert(fmt.Sprintf("rescue running >%s: residual %.6f (posL %.6f posR %.6f)", e.cfg.RescueAlertAfter, resid, pl, pr))
		}
		side := domain.Sell
		if resid < 0 {
			side = domain.Buy
		}
		// reduce-only on the venue holding a position opposite to the side we need
		leg, pos := l, pl
		if (side == domain.Sell && pr > pl) || (side == domain.Buy && pr < pl) {
			leg, pos = r, pr
		}
		q := math.Min(math.Abs(resid), math.Abs(pos))
		q = domain.Floor(q, leg.Inst.SizeDecimals)
		if q <= 0 {
			e.alert(fmt.Sprintf("rescue cannot size order: residual %.6f, pos %.6f on %s", resid, pos, leg.Venue.Name()))
			time.Sleep(e.cfg.RescueInterval * 5)
			continue
		}
		b, ok := e.book(leg.Venue.Name(), leg.Inst.MarketID)
		if !ok || !b.Valid() {
			slog.Warn("rescue: no valid book, waiting", "venue", leg.Venue.Name())
			time.Sleep(e.cfg.RescueInterval)
			attempt--
			continue
		}
		px, typ := RescuePrice(e.cfg.Rescue, attempt, b, side, leg.Inst.PriceDecimals)
		slog.Warn("rescue order", "attempt", attempt+1, "venue", leg.Venue.Name(), "side", side, "qty", q, "price", px, "type", typ)
		lr := e.submitAndResolve(ctx, domain.OrderReq{ClientID: e.ids.Next(), Inst: leg.Inst, Side: side, Qty: q,
			Price: px, Type: typ, ReduceOnly: true}, leg.Venue)
		orders++
		total += lr.Filled
		time.Sleep(e.cfg.RescueInterval)
	}
	return orders, total, pl, pr
}
