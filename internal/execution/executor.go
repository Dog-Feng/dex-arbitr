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
	// beyond the decision price (buy: +N ticks, sell: -N ticks). It is the
	// fixed allowance, and the fallback while volatility is not ready.
	// DynamicTicks replaces it with ticks = clamp(ceil(price*TickK*sigmaBps/1e4/tick),
	// TickMin, TickMax). Fills still match from the best level, so this only
	// adds a worst-case allowance.
	PriceTicks   int
	DynamicTicks bool
	TickK        float64
	TickMin      int
	TickMax      int // 0 = no upper clamp
	// EmergencyMarketBps bounds the protective market price of an emergency
	// unwind (liquidation-risk flatten), wider than the normal rescue bound.
	EmergencyMarketBps float64
}

// DefaultConfig values.
func DefaultConfig() Config {
	return Config{AckTimeout: 3 * time.Second, QueryRetries: 3, QueryInterval: time.Second,
		SettleDelay: 700 * time.Millisecond, RescueInterval: 400 * time.Millisecond,
		RescueAlertAfter: 10 * time.Second, Rescue: DefaultRescue, PriceTicks: 30,
		TickK: 2.5, TickMin: 1, EmergencyMarketBps: 100}
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
	Latency   time.Duration // submit -> terminal state (WS/REST) or give-up
	// RescueFilled/RescuePrice are the quantity and volume-weighted price this
	// leg completed via rescue orders (opposite side for opens, same side for
	// closes). They let callers reconstruct the effective close price.
	RescueFilled float64
	RescuePrice  float64
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

// VolFn returns the current short-horizon leg-price volatility (bps) of a venue
// (0 if not ready). The allowance covers the leg's own price move, not the
// cross-venue spread move.
type VolFn func(venue string) float64

// TickKFn returns the current allowance coverage coefficient (dynamic tuning);
// nil falls back to Config.TickK.
type TickKFn func() float64

// Executor runs hedged two-leg actions.
type Executor struct {
	cfg     Config
	tracker *Tracker
	ids     *IDGen
	book    BookFn
	vol     VolFn
	tickK   TickKFn
	alert   func(msg string)
}

// NewExecutor builds an executor.
func NewExecutor(cfg Config, t *Tracker, ids *IDGen, book BookFn, vol VolFn, tickK TickKFn, alert func(string)) *Executor {
	if alert == nil {
		alert = func(m string) { slog.Error("ALERT", "msg", m) }
	}
	return &Executor{cfg: cfg, tracker: t, ids: ids, book: book, vol: vol, tickK: tickK, alert: alert}
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
		n, q, fl, fr := e.flatten(ctx, l, r, target, pl, pr, &res, e.cfg.Rescue)
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
		ClientID: e.ids.Next(), Inst: leg.Inst, Side: leg.Side, Qty: qty, Price: e.limitPrice(leg, qty),
		Type: domain.Limit, ReduceOnly: reduceOnly,
	}, leg.Venue)
}

// limitPrice returns the IOC limit price for a leg: the decision price moved
// allowance ticks in the aggressive direction. leg.Price itself is left
// untouched so fill estimates and expected_bps keep using the decision price.
func (e *Executor) limitPrice(leg LegSpec, qty float64) float64 {
	n := e.allowance(leg, qty)
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

// allowance is the IOC tick allowance for a leg: the fixed PriceTicks, or when
// DynamicTicks is enabled and volatility is ready, a volatility-scaled value
// covering TickK one-sided standard deviations of the horizon move.
func (e *Executor) allowance(leg LegSpec, qty float64) int {
	if !e.cfg.DynamicTicks || e.vol == nil {
		return e.cfg.PriceTicks
	}
	sigma := 0.0
	if leg.Venue != nil {
		sigma = e.vol(leg.Venue.Name())
	}
	if sigma <= 0 {
		return e.cfg.PriceTicks
	}
	k := e.cfg.TickK
	if e.tickK != nil {
		k = e.tickK()
	}
	if k <= 0 {
		k = e.cfg.TickK
	}
	ticks := int(math.Ceil(leg.Price*(k*sigma/1e4)/leg.Inst.Tick() - 1e-9))
	// Cover the depth the quantity actually consumes (IOC walks past level 1).
	if e.book != nil && leg.Venue != nil {
		if b, ok := e.book(leg.Venue.Name(), leg.Inst.MarketID); ok {
			if d := depthTicks(leg, b, qty); d > ticks {
				ticks = d
			}
		}
	}
	if ticks < e.cfg.TickMin {
		ticks = e.cfg.TickMin
	}
	if e.cfg.TickMax > 0 && ticks > e.cfg.TickMax {
		ticks = e.cfg.TickMax
	}
	return ticks
}

// depthTicks is the number of ticks from the decision price to the level where
// qty fills (adverse direction), or to the deepest level if depth is short.
func depthTicks(leg LegSpec, b domain.Book, qty float64) int {
	levels := b.Asks
	if leg.Side == domain.Sell {
		levels = b.Bids
	}
	if len(levels) == 0 {
		return 0
	}
	rem := qty
	for _, lv := range levels {
		rem -= lv.Size
		if rem > 0 {
			continue
		}
		return ticksTo(leg, lv.Price)
	}
	return ticksTo(leg, levels[len(levels)-1].Price)
}

func ticksTo(leg LegSpec, price float64) int {
	d := price - leg.Price
	if leg.Side == domain.Sell {
		d = leg.Price - price
	}
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d/leg.Inst.Tick() - 1e-9))
}

func (e *Executor) submitAndResolve(ctx context.Context, req domain.OrderReq, v exchange.Venue) LegResult {
	lr := LegResult{ClientID: req.ClientID}
	start := time.Now()
	w := e.tracker.Watch(v.Name(), req.ClientID)
	defer e.tracker.Done(v.Name(), req.ClientID)

	_, err := v.Submit(ctx, req)
	lr.SubmitErr = err
	if errors.Is(err, domain.ErrRejected) || errors.Is(err, domain.ErrNoCredentials) {
		lr.Status, lr.Resolved = domain.StatusRejected, true
		lr.Latency = time.Since(start)
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
				return fill(lr, u, start)
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
			return fill(lr, u, start)
		}
		select {
		case u := <-w:
			if u.Status.Terminal() {
				return fill(lr, u, start)
			}
		case <-time.After(e.cfg.QueryInterval):
		}
	}
	slog.Warn("order state unresolved; will infer from positions", "venue", v.Name(), "cid", req.ClientID)
	lr.Status = domain.StatusUnknown
	lr.Latency = time.Since(start)
	return lr
}

func fill(lr LegResult, u domain.OrderUpdate, start time.Time) LegResult {
	lr.Status, lr.Filled, lr.AvgPrice, lr.Resolved = u.Status, u.FilledQty, u.AvgPrice(), true
	lr.Latency = time.Since(start)
	return lr
}

// flatten loops reduce-only orders until posL+posR == target.
// Positions are re-read before every retry.
// EmergencyFlatten reduces both legs to a flat position using immediate market
// orders (liquidation-risk unwind). It reuses the flatten loop with a wider
// protective bound and no price relaxation ladder.
func (e *Executor) EmergencyFlatten(ctx context.Context, l, r LegSpec, target, pl, pr float64) (Result, error) {
	start := time.Now()
	res := Result{}
	pl, pr, err := e.positions(ctx, l, r)
	if err != nil {
		return res, err
	}
	bps := e.cfg.EmergencyMarketBps
	if bps <= 0 {
		bps = 100
	}
	rescue := RescueParams{Immediate: true, MarketBps: bps}
	n, q, fl, fr := e.flatten(ctx, l, r, target, pl, pr, &res, rescue)
	res.RescueOrders, res.RescueQty, res.PosL, res.PosR = n, q, fl, fr
	res.Duration = time.Since(start)
	return res, nil
}

func (e *Executor) flatten(ctx context.Context, l, r LegSpec, target, pl, pr float64, res *Result, rescue RescueParams) (int, float64, float64, float64) {
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
		px, typ := RescuePrice(rescue, attempt, b, side, leg.Inst.PriceDecimals)
		slog.Warn("rescue order", "attempt", attempt+1, "venue", leg.Venue.Name(), "side", side, "qty", q, "price", px, "type", typ)
		lr := e.submitAndResolve(ctx, domain.OrderReq{ClientID: e.ids.Next(), Inst: leg.Inst, Side: side, Qty: q,
			Price: px, Type: typ, ReduceOnly: true}, leg.Venue)
		orders++
		total += lr.Filled
		if lr.Filled > 0 {
			if leg.Venue.Name() == l.Venue.Name() {
				mergeRescue(&res.L, lr.Filled, lr.AvgPrice)
			} else {
				mergeRescue(&res.R, lr.Filled, lr.AvgPrice)
			}
		}
		time.Sleep(e.cfg.RescueInterval)
	}
	return orders, total, pl, pr
}

// mergeRescue folds a rescue fill into the leg's volume-weighted rescue price.
func mergeRescue(lr *LegResult, qty, px float64) {
	if qty <= 0 || px <= 0 {
		return
	}
	if lr.RescueFilled <= 0 {
		lr.RescueFilled, lr.RescuePrice = qty, px
		return
	}
	total := lr.RescueFilled + qty
	lr.RescuePrice = (lr.RescuePrice*lr.RescueFilled + px*qty) / total
	lr.RescueFilled = total
}
