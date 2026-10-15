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
	// PriceTicks is the fixed allowance (fallback while volatility is not
	// ready). DynamicTicks replaces it with a volatility-scaled allowance:
	// ticks = ceil(TickK * legVol / tick * depthScale), where legVol is the
	// leg-price volatility in price units and depthScale >= 1 grows when the
	// top-of-book is thin relative to the quantity.
	PriceTicks    int
	DynamicTicks  bool
	TickK         float64
	TickMin       int
	TickMax       int     // 0 = no upper clamp
	DepthMinMult  float64 // depth multiple below which the allowance widens
	DepthMaxScale float64 // max multiplier applied to the allowance
	// EmergencyMarketBps bounds the protective market price of an emergency
	// unwind (liquidation-risk flatten), wider than the normal rescue bound.
	EmergencyMarketBps float64
}

// DefaultConfig values.
func DefaultConfig() Config {
	return Config{AckTimeout: 3 * time.Second, QueryRetries: 3, QueryInterval: time.Second,
		SettleDelay: 700 * time.Millisecond, RescueInterval: 400 * time.Millisecond,
		RescueAlertAfter: 10 * time.Second, Rescue: DefaultRescue, PriceTicks: 30,
		TickK: 2.5, TickMin: 1, DepthMinMult: 5, DepthMaxScale: 4, EmergencyMarketBps: 100}
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
	ClientID       int64
	Status         domain.OrderStatus
	Filled         float64 // unsigned
	AvgPrice       float64
	Resolved       bool // terminal venue outcome or definitively never submitted
	SubmitErr      error
	Latency        time.Duration // submit -> terminal state (WS/REST) or give-up
	PriceEstimated bool          // AvgPrice is a protective/decision price estimate
	// RescueFilled/RescuePrice are the quantity and volume-weighted price this
	// leg completed via rescue orders (opposite side for opens, same side for
	// closes). They let callers reconstruct the effective close price.
	RescueFilled         float64
	RescuePrice          float64
	RescuePriceEstimated bool
	request              domain.OrderReq
}

// Result of a two-leg execution.
type Result struct {
	L, R            LegResult
	PosL, PosR      float64 // final positions on the pair markets
	RescueOrders    int
	RescueQty       float64
	Duration        time.Duration
	PositionsKnown  bool  // positions were successfully read after the last order
	SettlementKnown bool  // all submitted quantities have a terminal or fully-filled outcome
	Err             error // cancellation, unavailable positions, or an unresolved order
	pending         *pendingRescue
	targetL         float64
	targetR         float64
	targetsKnown    bool
	rescue          RescueParams
	emergency       bool
}

// pendingRescue retains a submitted batch until its terminal outcomes and
// position changes agree. Copies of Result must be recovered sequentially.
type pendingRescue struct {
	reqs             [2]domain.OrderReq
	results          [2]LegResult
	beforeL, beforeR float64
	accountedQty     [2]float64
	accountedQuote   [2]float64
}

// ErrUnresolved means a submitted order may still change the account position.
var ErrUnresolved = errors.New("order outcome unresolved")

// ErrIncomplete means orders have settled, but a rescue target has not yet
// been reached. Retain Result and retry recovery without releasing the actor.
var ErrIncomplete = errors.New("rescue targets not reached")

// ErrUnexpectedPosition means confirmed fills and the resulting position
// change disagree in a way that cannot self-heal (e.g. an external close).
var ErrUnexpectedPosition = errors.New("unexpected position change")

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
			if math.IsNaN(p.Qty) || math.IsInf(p.Qty, 0) {
				return 0, fmt.Errorf("invalid position quantity on %s market %d", v.Name(), marketID)
			}
			return p.Qty, nil
		}
	}
	return 0, nil
}

// Execute submits both IOC legs on an existing hedge, resolves their outcomes,
// and reduces unequal fills to a hedge representable on both markets.
func (e *Executor) Execute(ctx context.Context, l, r LegSpec, qty float64, reduceOnly bool, beforeL, beforeR float64) (res Result) {
	start := time.Now()
	defer func() { res.Duration = time.Since(start) }()
	res.PosL, res.PosR = beforeL, beforeR
	res.rescue = e.cfg.Rescue
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}
	if math.IsNaN(beforeL) || math.IsInf(beforeL, 0) || math.IsNaN(beforeR) || math.IsInf(beforeR, 0) || (l.Side != domain.Buy && l.Side != domain.Sell) || l.Side != -r.Side {
		res.Err = fmt.Errorf("invalid pre-execution position or hedge sides")
		return res
	}
	if math.Abs(beforeL+beforeR) >= halfUnit(l.Inst, r.Inst) {
		res.Err = fmt.Errorf("cannot execute on unhedged positions %.8f/%.8f", beforeL, beforeR)
		return res
	}
	if qty <= 0 || math.IsNaN(qty) || math.IsInf(qty, 0) || l.Price <= 0 || r.Price <= 0 || math.IsNaN(l.Price) || math.IsNaN(r.Price) || math.IsInf(l.Price, 0) || math.IsInf(r.Price, 0) {
		res.Err = fmt.Errorf("invalid execution quantity or decision price")
		return res
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); res.L = e.runLeg(ctx, l, qty, reduceOnly) }()
	go func() { defer wg.Done(); res.R = e.runLeg(ctx, r, qty, reduceOnly) }()
	wg.Wait()

	pl, pr, known, err := e.confirmPositions(ctx, l, r, beforeL, beforeR, &res.L, &res.R, qty, qty, l.Side, r.Side)
	if known {
		res.PosL, res.PosR, res.PositionsKnown = pl, pr, true
	}
	if err != nil {
		res.Err = err
		return res
	}
	res.SettlementKnown = true
	// Keep only a hedge representable on both markets. Reducing both legs is
	// necessary when a fine-precision partial fill leaves dust on the coarse leg.
	tl, tr := hedgeTargets(pl, pr, l.Inst, r.Inst)
	res.targetL, res.targetR, res.targetsKnown = tl, tr, true
	res.Err = e.flatten(ctx, l, r, tl, tr, &res, e.cfg.Rescue)
	return res
}

// ResolveOutstanding continues a canceled execution with a live parent
// context. It queries existing orders before confirming positions and resumes
// reduce-only rescue only after every outstanding quantity is settled. Callers
// must retain Result, the original legs, quantity and pre-execution positions,
// and keep the actor paused while Err is non-nil.
func (e *Executor) ResolveOutstanding(ctx context.Context, l, r LegSpec, res Result, qty, beforeL, beforeR float64) Result {
	start := time.Now()
	previousDuration := res.Duration
	res.Err = nil
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}
	// Work on a private batch copy so an older Result remains a valid snapshot.
	if res.pending != nil {
		batch := *res.pending
		res.pending = &batch
		res.Err = e.resolveRescue(ctx, l, r, &res, true)
	} else if res.emergency {
		pl, pr, err := e.positions(ctx, l, r)
		res.PositionsKnown, res.SettlementKnown = err == nil, err == nil
		if err == nil {
			res.PosL, res.PosR = pl, pr
		}
		res.Err = err
	} else if !res.SettlementKnown {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); res.L = e.resolveExisting(ctx, l, qty, res.L) }()
		go func() { defer wg.Done(); res.R = e.resolveExisting(ctx, r, qty, res.R) }()
		wg.Wait()
		pl, pr, known, err := e.confirmPositions(ctx, l, r, beforeL, beforeR, &res.L, &res.R, qty, qty, l.Side, r.Side)
		res.PositionsKnown = known
		if known {
			res.PosL, res.PosR = pl, pr
		}
		res.Err = err
		res.SettlementKnown = err == nil
	}
	if res.Err == nil {
		if !res.targetsKnown {
			res.targetL, res.targetR = hedgeTargets(res.PosL, res.PosR, l.Inst, r.Inst)
			res.targetsKnown = true
		}
		res.Err = e.flatten(ctx, l, r, res.targetL, res.targetR, &res, res.rescue)
	}
	res.Duration = previousDuration + time.Since(start)
	return res
}

// EscalateOutstanding widens only future rescue orders to the emergency price
// bound. Outstanding IOC batches must still settle first, and the original
// hedge targets and accounting remain intact for the caller's normal action.
func (e *Executor) EscalateOutstanding(res Result) Result {
	bps := e.cfg.EmergencyMarketBps
	if bps <= 0 {
		bps = 100
	}
	res.rescue = RescueParams{Immediate: true, MarketBps: bps}
	return res
}

func inferLeg(lr *LegResult, delta, px, qty float64) {
	if lr.Resolved {
		return
	}
	lr.Filled = math.Abs(delta)
	if lr.Filled > 0 {
		// For an IOC with no execution quote, use its original price bound:
		// buy costs are bounded above and sell proceeds below. A top-of-book
		// decision estimate could invent profit inside the allowed slippage.
		if lr.request.Price > 0 && !math.IsNaN(lr.request.Price) && !math.IsInf(lr.request.Price, 0) {
			px = lr.request.Price
		}
		lr.AvgPrice = px // estimate; exact price unknown
		lr.PriceEstimated = true
		lr.Status = domain.StatusUnknown
		if math.Abs(delta-qty) < 1e-9 {
			lr.Status = domain.StatusFilled
		}
	}
}

func hedgeTargets(pl, pr float64, a, b domain.Instrument) (float64, float64) {
	if pl*pr >= 0 {
		return 0, 0
	}
	d := a.SizeDecimals
	if b.SizeDecimals < d {
		d = b.SizeDecimals
	}
	q := domain.Floor(math.Min(math.Abs(pl), math.Abs(pr)), d)
	if pl < 0 {
		q = -q
	}
	return q, -q
}

func pause(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// confirmPositions never interprets a failed read as a zero position. Known
// fills must be reflected in the snapshot; an unknown order is safe to infer
// only when its entire requested quantity has already changed the position.
func (e *Executor) confirmPositions(ctx context.Context, l, r LegSpec, beforeL, beforeR float64, fl, fr *LegResult, qtyL, qtyR float64, sideL, sideR domain.Side) (float64, float64, bool, error) {
	if err := pause(ctx, e.cfg.SettleDelay); err != nil {
		return 0, 0, false, err
	}
	var lastErr error
	var lastL, lastR float64
	known := false
	for i := 0; i <= e.cfg.QueryRetries; i++ {
		pl, pr, err := e.positions(ctx, l, r)
		if err == nil {
			lastL, lastR, known = pl, pr, true
			dl, dr := pl-beforeL, pr-beforeR
			if sideL != 0 {
				dl *= float64(sideL)
			}
			if sideR != 0 {
				dr *= float64(sideR)
			}
			errL, errR := verifyDelta(fl, dl, qtyL, l.Inst), verifyDelta(fr, dr, qtyR, r.Inst)
			if errL == nil || errors.Is(errL, ErrUnresolved) {
				inferLeg(fl, dl, l.Price, qtyL)
			}
			if errR == nil || errors.Is(errR, ErrUnresolved) {
				inferLeg(fr, dr, r.Price, qtyR)
			}
			err = errors.Join(errL, errR)
			if err == nil {
				return pl, pr, true, nil
			}
		}
		lastErr = err
		if i < e.cfg.QueryRetries {
			if err := pause(ctx, e.cfg.QueryInterval); err != nil {
				return lastL, lastR, known, errors.Join(lastErr, err)
			}
		}
	}
	return lastL, lastR, known, fmt.Errorf("confirm positions: %w", lastErr)
}

func verifyDelta(lr *LegResult, delta, qty float64, inst domain.Instrument) error {
	eps := math.Pow10(-inst.SizeDecimals) / 2
	if math.IsNaN(delta) || math.IsInf(delta, 0) || math.IsNaN(lr.Filled) || math.IsInf(lr.Filled, 0) || lr.Filled < 0 {
		return fmt.Errorf("invalid position or filled quantity")
	}
	if delta < -eps || delta-qty >= eps {
		return fmt.Errorf("%w: %.8f for quantity %.8f", ErrUnexpectedPosition, delta, qty)
	}
	if lr.Resolved {
		if lr.Filled-qty >= eps || lr.Status == domain.StatusRejected && lr.Filled >= eps || lr.Status == domain.StatusFilled && math.Abs(lr.Filled-qty) >= eps {
			return fmt.Errorf("invalid terminal fill %.8f for status %s and quantity %.8f", lr.Filled, lr.Status, qty)
		}
		if math.Abs(delta-lr.Filled) >= eps {
			return fmt.Errorf("%w: %.8f does not reflect confirmed fill %.8f", ErrUnexpectedPosition, delta, lr.Filled)
		}
		return nil
	}
	if lr.Filled-delta >= eps {
		return fmt.Errorf("%w: %.8f is behind observed fill %.8f", ErrUnexpectedPosition, delta, lr.Filled)
	}
	if math.Abs(delta-qty) >= eps {
		return fmt.Errorf("%w: confirmed %.8f of requested %.8f", ErrUnresolved, delta, qty)
	}
	return nil
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
		return boundPrice(leg)
	}
	off := float64(n) * leg.Inst.Tick()
	px := leg.Price + off
	if leg.Side == domain.Sell {
		px = leg.Price - off
		if px <= 0 {
			return leg.Price
		}
	}
	// Round in the tightening direction so the native-precision price can
	// never widen the worst-acceptable bound by half a tick.
	rounded := domain.Floor(px, leg.Inst.PriceDecimals)
	if leg.Side == domain.Sell {
		rounded = domain.Ceil(px, leg.Inst.PriceDecimals)
	}
	return rounded
}

// boundPrice rounds the decision price itself in the tightening direction.
func boundPrice(leg LegSpec) float64 {
	if leg.Side == domain.Sell {
		return domain.Ceil(leg.Price, leg.Inst.PriceDecimals)
	}
	return domain.Floor(leg.Price, leg.Inst.PriceDecimals)
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
	ticks := int(math.Ceil(k*sigma/leg.Inst.Tick()*e.depthScale(leg, qty) - 1e-9))
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

// depthScale returns a multiplier >= 1 for the volatility allowance: the
// thinner the top-of-book relative to qty, the larger the multiplier (a thin
// book is swept by a small move, so the allowance must reach further).
func (e *Executor) depthScale(leg LegSpec, qty float64) float64 {
	if e.cfg.DepthMinMult <= 0 || e.cfg.DepthMaxScale <= 1 || qty <= 0 || e.book == nil || leg.Venue == nil {
		return 1
	}
	b, ok := e.book(leg.Venue.Name(), leg.Inst.MarketID)
	if !ok || len(b.Bids) == 0 || len(b.Asks) == 0 {
		return 1
	}
	top := b.Asks[0].Size
	if leg.Side == domain.Sell {
		top = b.Bids[0].Size
	}
	if top <= 0 {
		return 1
	}
	m := top / qty
	if m >= e.cfg.DepthMinMult {
		return 1
	}
	s := e.cfg.DepthMinMult / m
	if s > e.cfg.DepthMaxScale {
		s = e.cfg.DepthMaxScale
	}
	return s
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

func (e *Executor) submitAndResolve(ctx context.Context, req domain.OrderReq, v exchange.Venue) (lr LegResult) {
	lr = LegResult{ClientID: req.ClientID, request: req}
	start := time.Now()
	defer func() { lr.Latency = time.Since(start) }()
	if err := ctx.Err(); err != nil {
		lr.SubmitErr = err
		lr.Status, lr.Resolved = domain.StatusCanceled, true // no submission attempted
		return lr
	}
	if req.Price <= 0 || math.IsNaN(req.Price) || math.IsInf(req.Price, 0) {
		lr.SubmitErr = fmt.Errorf("%w: invalid order price", domain.ErrRejected)
		lr.Status, lr.Resolved = domain.StatusRejected, true
		return lr
	}
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
	return e.awaitOrder(ctx, req, v, lr, w, start)
}

// resolveExisting never resubmits an order whose submission was uncertain.
func (e *Executor) resolveExisting(ctx context.Context, leg LegSpec, qty float64, lr LegResult) LegResult {
	if lr.Resolved {
		return lr
	}
	if lr.ClientID == 0 {
		lr.Status, lr.Resolved = domain.StatusCanceled, true // Execute stopped before submission
		return lr
	}
	req := lr.request
	if req.ClientID == 0 {
		req = domain.OrderReq{ClientID: lr.ClientID, Inst: leg.Inst, Side: leg.Side, Qty: qty, Price: e.limitPrice(leg, qty)}
	}
	return e.queryExisting(ctx, req, leg.Venue, lr)
}

func (e *Executor) queryExisting(ctx context.Context, req domain.OrderReq, v exchange.Venue, lr LegResult) LegResult {
	if lr.Resolved || ctx.Err() != nil {
		return lr
	}
	start := time.Now()
	w := e.tracker.Watch(v.Name(), req.ClientID)
	defer e.tracker.Done(v.Name(), req.ClientID)
	for i := 0; i < max(1, e.cfg.QueryRetries); i++ {
		u, err := v.QueryOrder(ctx, req.Inst, req.ClientID)
		if err == nil && u.Status.Terminal() {
			return fill(lr, u, start, req.Price)
		}
		timer := time.NewTimer(e.cfg.QueryInterval)
		select {
		case u, ok := <-w:
			timer.Stop()
			if !ok {
				w = nil
			} else if u.Status.Terminal() {
				return fill(lr, u, start, req.Price)
			}
		case <-ctx.Done():
			timer.Stop()
			return lr
		case <-timer.C:
		}
	}
	return lr
}

func (e *Executor) awaitOrder(ctx context.Context, req domain.OrderReq, v exchange.Venue, lr LegResult, w <-chan domain.OrderUpdate, start time.Time) LegResult {
	// phase 2: WS terminal state
	timer := time.NewTimer(e.cfg.AckTimeout)
	defer timer.Stop()
	for {
		select {
		case u, ok := <-w:
			if !ok {
				w = nil
				break
			}
			if u.Status.Terminal() {
				return fill(lr, u, start, req.Price)
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
			return fill(lr, u, start, req.Price)
		}
		select {
		case u, ok := <-w:
			if !ok {
				w = nil
				continue
			}
			if u.Status.Terminal() {
				return fill(lr, u, start, req.Price)
			}
		case <-ctx.Done():
			return lr
		case <-time.After(e.cfg.QueryInterval):
		}
	}
	slog.Warn("order state unresolved; will infer from positions", "venue", v.Name(), "cid", req.ClientID)
	lr.Status = domain.StatusUnknown
	lr.Latency = time.Since(start)
	return lr
}

func fill(lr LegResult, u domain.OrderUpdate, start time.Time, estimate float64) LegResult {
	lr.Status, lr.Filled, lr.AvgPrice, lr.Resolved = u.Status, u.FilledQty, u.AvgPrice(), true
	lr.PriceEstimated = false
	if lr.Filled > 0 && (lr.AvgPrice <= 0 || math.IsNaN(lr.AvgPrice) || math.IsInf(lr.AvgPrice, 0)) {
		lr.AvgPrice, lr.PriceEstimated = estimate, true
	}
	lr.Latency = time.Since(start)
	return lr
}

// EmergencyFlatten reduces both market positions to zero and reports an error
// unless every submitted order and both final positions have been confirmed.
func (e *Executor) EmergencyFlatten(ctx context.Context, l, r LegSpec, target, pl, pr float64) (res Result, err error) {
	start := time.Now()
	defer func() { res.Duration = time.Since(start); res.Err = err }()
	res.PosL, res.PosR = pl, pr
	if target != 0 {
		return res, fmt.Errorf("emergency target must be zero")
	}
	res.targetL, res.targetR, res.targetsKnown, res.emergency = 0, 0, true, true
	bps := e.cfg.EmergencyMarketBps
	if bps <= 0 {
		bps = 100
	}
	res.rescue = RescueParams{Immediate: true, MarketBps: bps}
	if err = ctx.Err(); err != nil {
		return res, err
	}
	pl, pr, err = e.positions(ctx, l, r)
	if err != nil {
		return res, fmt.Errorf("emergency positions: %w", err)
	}
	res.PosL, res.PosR, res.PositionsKnown, res.SettlementKnown = pl, pr, true, true
	err = e.flatten(ctx, l, r, 0, 0, &res, res.rescue)
	return res, err
}

// FlattenToZero reduces both market positions to zero using the normal rescue
// price ladder (not emergency prices), for post-intervention recovery where
// the priority is a confirmed flat account rather than immediacy.
func (e *Executor) FlattenToZero(ctx context.Context, l, r LegSpec) (res Result, err error) {
	start := time.Now()
	defer func() { res.Duration = time.Since(start); res.Err = err }()
	res.targetL, res.targetR, res.targetsKnown = 0, 0, true
	res.rescue = e.cfg.Rescue
	if err = ctx.Err(); err != nil {
		return res, err
	}
	var pl, pr float64
	if pl, pr, err = e.positions(ctx, l, r); err != nil {
		return res, fmt.Errorf("recovery positions: %w", err)
	}
	res.PosL, res.PosR, res.PositionsKnown, res.SettlementKnown = pl, pr, true, true
	err = e.flatten(ctx, l, r, 0, 0, &res, res.rescue)
	return res, err
}

// flatten reduces each leg to its own target. Urgent orders can reduce the leg
// with an available book independently. Another batch is never submitted until
// all previous outcomes and their resulting position snapshot are confirmed.
func (e *Executor) flatten(ctx context.Context, l, r LegSpec, targetL, targetR float64, res *Result, rescue RescueParams) error {
	start := time.Now()
	alerted := false
	epsL := math.Pow10(-l.Inst.SizeDecimals) / 2
	epsR := math.Pow10(-r.Inst.SizeDecimals) / 2
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		dl, dr := res.PosL-targetL, res.PosR-targetR
		if math.Abs(dl) < epsL && math.Abs(dr) < epsR {
			return nil
		}
		if !alerted && time.Since(start) > e.cfg.RescueAlertAfter {
			alerted = true
			e.alert(fmt.Sprintf("rescue running >%s: positions %.8f/%.8f, targets %.8f/%.8f", e.cfg.RescueAlertAfter, res.PosL, res.PosR, targetL, targetR))
		}
		legs := [2]LegSpec{l, r}
		deltas := [2]float64{dl, dr}
		positions := [2]float64{res.PosL, res.PosR}
		var reqs [2]domain.OrderReq
		var results [2]LegResult
		ready := true
		for i, leg := range legs {
			results[i].Resolved = true // no order on this leg is already settled
			if math.Abs(deltas[i]) < math.Pow10(-leg.Inst.SizeDecimals)/2 {
				continue
			}
			if deltas[i]*positions[i] <= 0 {
				return fmt.Errorf("rescue target cannot be reached reduce-only on %s", leg.Venue.Name())
			}
			q := domain.Floor(math.Abs(deltas[i]), leg.Inst.SizeDecimals)
			if q <= 0 {
				return fmt.Errorf("rescue quantity below native precision on %s", leg.Venue.Name())
			}
			if e.book == nil {
				return fmt.Errorf("rescue has no book source")
			}
			b, ok := e.book(leg.Venue.Name(), leg.Inst.MarketID)
			if !ok || !b.Valid() {
				if rescue.Immediate {
					// A missing peer book must not block reducing a leg whose
					// protective price is available during an urgent unwind.
					continue
				}
				ready = false
				break
			}
			side := domain.Sell
			if deltas[i] < 0 {
				side = domain.Buy
			}
			px, typ := RescuePrice(rescue, attempt, b, side, leg.Inst.PriceDecimals)
			if px <= 0 || math.IsNaN(px) || math.IsInf(px, 0) {
				return fmt.Errorf("invalid rescue price on %s", leg.Venue.Name())
			}
			reqs[i] = domain.OrderReq{ClientID: e.ids.Next(), Inst: leg.Inst, Side: side, Qty: q, Price: px, Type: typ, ReduceOnly: true}
		}
		if !ready || reqs[0].Qty == 0 && reqs[1].Qty == 0 {
			if rescue.Immediate {
				return fmt.Errorf("%w: no protective book available for remaining positions %.8f/%.8f", ErrIncomplete, res.PosL, res.PosR)
			}
			if err := pause(ctx, e.cfg.RescueInterval); err != nil {
				return err
			}
			attempt--
			continue
		}
		beforeL, beforeR := res.PosL, res.PosR
		res.PositionsKnown, res.SettlementKnown = false, false
		var wg sync.WaitGroup
		for i := range reqs {
			if reqs[i].Qty == 0 {
				continue
			}
			res.RescueOrders++
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				results[i] = e.submitAndResolve(ctx, reqs[i], legs[i].Venue)
			}(i)
		}
		wg.Wait()
		res.pending = &pendingRescue{reqs: reqs, results: results, beforeL: beforeL, beforeR: beforeR}
		if err := e.resolveRescue(ctx, l, r, res, false); err != nil {
			return err
		}
		if rescue.Immediate {
			if math.Abs(res.PosL-targetL) < epsL && math.Abs(res.PosR-targetR) < epsR {
				return nil
			}
			return fmt.Errorf("%w: positions %.8f/%.8f, targets %.8f/%.8f", ErrIncomplete, res.PosL, res.PosR, targetL, targetR)
		}
		if err := pause(ctx, e.cfg.RescueInterval); err != nil {
			return err
		}
	}
}

func (e *Executor) resolveRescue(ctx context.Context, l, r LegSpec, res *Result, query bool) error {
	p := res.pending
	legs := [2]LegSpec{l, r}
	if query {
		var wg sync.WaitGroup
		for i, req := range p.reqs {
			if req.Qty == 0 || p.results[i].Resolved {
				continue
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				p.results[i] = e.queryExisting(ctx, p.reqs[i], legs[i].Venue, p.results[i])
			}(i)
		}
		wg.Wait()
	}
	// Protective prices are explicit estimates for position-inferred fills.
	pricedL, pricedR := l, r
	pricedL.Price, pricedR.Price = p.reqs[0].Price, p.reqs[1].Price
	pl, pr, known, err := e.confirmPositions(ctx, pricedL, pricedR, p.beforeL, p.beforeR, &p.results[0], &p.results[1], p.reqs[0].Qty, p.reqs[1].Qty, p.reqs[0].Side, p.reqs[1].Side)
	res.PositionsKnown = known
	if known {
		res.PosL, res.PosR = pl, pr
	}
	for i, lr := range p.results {
		if lr.Filled <= 0 || lr.AvgPrice <= 0 || math.IsNaN(lr.Filled) || math.IsInf(lr.Filled, 0) || math.IsNaN(lr.AvgPrice) || math.IsInf(lr.AvgPrice, 0) || lr.Filled-p.reqs[i].Qty >= math.Pow10(-legs[i].Inst.SizeDecimals)/2 {
			continue
		}
		legResult := &res.L
		if i == 1 {
			legResult = &res.R
		}
		// Replace this batch's cumulative contribution as additional fills or
		// an exact quote arrive; recovery must not count a fill twice.
		quote := lr.Filled * lr.AvgPrice
		res.RescueQty += lr.Filled - p.accountedQty[i]
		if p.accountedQty[i] == 0 {
			mergeRescue(legResult, lr.Filled, lr.AvgPrice)
		} else {
			totalQty := legResult.RescueFilled + lr.Filled - p.accountedQty[i]
			totalQuote := legResult.RescueFilled*legResult.RescuePrice + quote - p.accountedQuote[i]
			legResult.RescueFilled, legResult.RescuePrice = totalQty, totalQuote/totalQty
		}
		legResult.RescuePriceEstimated = legResult.RescuePriceEstimated || lr.PriceEstimated
		p.accountedQty[i], p.accountedQuote[i] = lr.Filled, quote
	}
	res.SettlementKnown = err == nil
	if err == nil {
		res.pending = nil
	}
	return err
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
