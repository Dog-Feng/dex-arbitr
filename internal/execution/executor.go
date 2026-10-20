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
	// TopUpBps caps the price allowance of the step-completion order that
	// restores a partially filled opening leg to the full step size. The
	// completion is attempted once; failure falls back to shrinking the
	// over-filled leg. 0 disables completion.
	TopUpBps float64
}

// DefaultConfig values.
func DefaultConfig() Config {
	return Config{AckTimeout: 3 * time.Second, QueryRetries: 3, QueryInterval: time.Second,
		SettleDelay: 700 * time.Millisecond, RescueInterval: 400 * time.Millisecond,
		RescueAlertAfter: 10 * time.Second, Rescue: DefaultRescue, PriceTicks: 30,
		TickK: 2.5, TickMin: 1, DepthMinMult: 5, DepthMaxScale: 4, EmergencyMarketBps: 100, TopUpBps: 25}
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
	RequestedQty    float64 // actual original IOC quantity after an opening budget cap
	PositionsKnown  bool    // positions were successfully read after the last order
	SettlementKnown bool    // all submitted quantities have a terminal or fully-filled outcome
	Err             error   // cancellation, unavailable positions, or an unresolved order
	pending         *pendingRescue
	unconfirmed     []pendingRescue // full position-inferred batches still lacking terminal reports
	topUp           *topUpState     // step-completion order on the under-filled leg
	targetL         float64
	targetR         float64
	targetsKnown    bool
	rescue          RescueParams
	emergency       bool
	opening         bool // retained so recovery can roll an opening back to before
}

// topUpState retains the step-completion order for recovery.
type topUpState struct {
	req    domain.OrderReq
	result LegResult
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

// ErrNotionalBudget means neither IOC was submitted because its decision or
// fixed IOC price would exceed the caller's per-leg opening reservation.
var ErrNotionalBudget = errors.New("opening notional budget exceeded")

// ErrSubMinResidual means reaching a rescue target would require trading less
// than one leg's venue minimum. The engine escalates it like an unexpected
// position change instead of retrying silently forever.
var ErrSubMinResidual = errors.New("residual below venue minimum")

// lateFillRecoveryLimit bounds consecutive flatten re-sizes caused by late
// fills on legs without a matching request. Persistent movement past this
// limit is reported as an unexpected position change for the engine to handle.
const lateFillRecoveryLimit = 3

// subMinAttemptLimit bounds consecutive minimum-sized reduce-only attempts
// used to clear a sub-minimum residual. Repeated rejections past this limit
// surface as ErrSubMinResidual for the engine to escalate.
const subMinAttemptLimit = 3

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
	return e.ExecuteBounded(ctx, l, r, qty, reduceOnly, beforeL, beforeR, 0)
}

// ExecuteBounded freezes both IOC prices before checking an opening budget.
// The same prices are submitted, so changing books or dynamic tick allowances
// cannot widen the reserved opening notional after admission. Zero disables it.
func (e *Executor) ExecuteBounded(ctx context.Context, l, r LegSpec, qty float64, reduceOnly bool, beforeL, beforeR, maxOpenNotional float64) (res Result) {
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
	priceL, priceR := e.limitPrice(l, qty), e.limitPrice(r, qty)
	reservationPrice := math.Max(math.Max(l.Price, r.Price), math.Max(priceL, priceR))
	if !reduceOnly && maxOpenNotional > 0 && qty*reservationPrice > maxOpenNotional {
		d := min(l.Inst.SizeDecimals, r.Inst.SizeDecimals)
		qty = domain.Floor(maxOpenNotional/reservationPrice, d)
		if qty*reservationPrice > maxOpenNotional {
			qty = domain.Round(qty-math.Pow10(-d), d)
		}
		if qty <= 0 || qty < math.Max(l.Inst.MinBase, r.Inst.MinBase) || qty*priceL < l.Inst.MinQuote || qty*priceR < r.Inst.MinQuote {
			res.Err = ErrNotionalBudget
			return res
		}
	}
	res.RequestedQty = qty
	res.opening = !reduceOnly
	reqL := domain.OrderReq{ClientID: e.ids.Next(), Inst: l.Inst, Side: l.Side, Qty: qty, Price: priceL, Type: domain.Limit, ReduceOnly: reduceOnly}
	reqR := domain.OrderReq{ClientID: e.ids.Next(), Inst: r.Inst, Side: r.Side, Qty: qty, Price: priceR, Type: domain.Limit, ReduceOnly: reduceOnly}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); res.L = e.submitAndResolve(ctx, reqL, l.Venue) }()
	go func() { defer wg.Done(); res.R = e.submitAndResolve(ctx, reqR, r.Venue) }()
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
	if !reduceOnly && e.cfg.TopUpBps > 0 {
		completed, terr := e.completeStep(ctx, l, r, qty, beforeL, beforeR, pl, pr, &res)
		if terr != nil {
			res.Err = terr
			return res
		}
		if completed {
			// The completion changed one leg; re-read authoritative positions.
			pl2, pr2, perr := e.positions(ctx, l, r)
			if perr != nil {
				res.Err = perr
				return res
			}
			pl, pr = pl2, pr2
			res.PosL, res.PosR = pl, pr
		}
	}
	// Keep only a hedge representable on both markets. Reducing both legs is
	// necessary when a fine-precision partial fill leaves dust on the coarse leg.
	tl, tr := hedgeTargets(pl, pr, l.Inst, r.Inst)
	if e.unwindSmallResidual(l, r, tl, tr, pl, pr) {
		if reduceOnly {
			// The residual cannot be traded on either leg: unwind the whole
			// step to zero, where every reduction equals a full position size.
			tl, tr = 0, 0
		} else {
			// The opening cannot be equalized: undo only this step back to the
			// pre-open positions so previously recorded steps stay intact.
			tl, tr = beforeL, beforeR
		}
	}
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
	if res.emergency {
		return e.ResolveFlattenToZero(ctx, l, r, res)
	}
	if res.topUp != nil && !res.topUp.result.Resolved {
		leg := l
		if res.topUp.req.Inst.MarketID == r.Inst.MarketID && res.topUp.req.Side == r.Side {
			leg = r
		}
		res.topUp.result = e.queryExisting(ctx, res.topUp.req, leg.Venue, res.topUp.result)
		if res.topUp.result.Resolved && res.topUp.result.Filled > 0 {
			if leg.Inst.MarketID == l.Inst.MarketID && leg.Side == l.Side {
				mergeTopUp(&res.L, res.topUp.result)
			} else {
				mergeTopUp(&res.R, res.topUp.result)
			}
		}
	}
	if res.topUp != nil && !res.topUp.result.Resolved {
		res.Err = fmt.Errorf("%w: completion order %d unresolved", ErrUnresolved, res.topUp.req.ClientID)
		return res
	}
	// Work on a private batch copy so an older Result remains a valid snapshot.
	if res.pending != nil {
		batch := *res.pending
		res.pending = &batch
		res.Err = e.resolveRescue(ctx, l, r, &res, true)
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
		if res.opening && e.unwindSmallResidual(l, r, res.targetL, res.targetR, res.PosL, res.PosR) {
			// The opening cannot be equalized: undo only this step back to the
			// pre-open positions so previously recorded steps stay intact.
			res.targetL, res.targetR = beforeL, beforeR
		}
		res.Err = e.flatten(ctx, l, r, res.targetL, res.targetR, &res, res.rescue)
	}
	res.Duration = previousDuration + time.Since(start)
	return res
}

// OrdersTerminal reports whether every retained submitted order has an explicit
// terminal outcome. A full fill inferred from positions is deliberately not a
// terminal report: external account changes can invalidate that inference.
func OrdersTerminal(res Result) bool {
	for _, lr := range []LegResult{res.L, res.R} {
		if lr.ClientID != 0 && !terminalOrder(lr.request, lr) {
			return false
		}
	}
	if res.pending != nil {
		if !batchTerminal(res.pending) {
			return false
		}
	}
	for i := range res.unconfirmed {
		if !batchTerminal(&res.unconfirmed[i]) {
			return false
		}
	}
	if res.topUp != nil {
		if !terminalOrder(res.topUp.req, res.topUp.result) {
			return false
		}
	}
	return true
}

func batchTerminal(p *pendingRescue) bool {
	for i, req := range p.reqs {
		if req.Qty > 0 && !terminalOrder(req, p.results[i]) {
			return false
		}
	}
	return true
}

func terminalOrder(req domain.OrderReq, lr LegResult) bool {
	return lr.Resolved && lr.Status.Terminal() && verifyDelta(&lr, lr.Filled, req.Qty, req.Inst) == nil
}

// ResolveTerminals queries retained identities without submitting any orders.
// It reads positions independently of the original fill/position comparison so
// callers can safely recover an externally modified account once all previous
// orders have terminated. The original and rescue fill accounting is retained.
func (e *Executor) ResolveTerminals(ctx context.Context, l, r LegSpec, res Result, qty float64) (out Result) {
	start := time.Now()
	defer func() { out.Duration += time.Since(start) }()
	if err := ctx.Err(); err != nil {
		res.Err = err
		return res
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); res.L = e.resolveExisting(ctx, l, qty, res.L) }()
	go func() { defer wg.Done(); res.R = e.resolveExisting(ctx, r, qty, res.R) }()
	wg.Wait()
	res.unconfirmed = append([]pendingRescue(nil), res.unconfirmed...)
	for i := range res.unconfirmed {
		p := &res.unconfirmed[i]
		e.queryRescue(ctx, l, r, p)
		accountRescue(l, r, &res, p)
	}
	if res.pending != nil {
		batch := *res.pending
		res.pending = &batch
		e.queryRescue(ctx, l, r, &batch)
		accountRescue(l, r, &res, &batch)
	}
	pl, pr, err := e.positions(ctx, l, r)
	res.PositionsKnown = err == nil
	if err == nil {
		res.PosL, res.PosR = pl, pr
	}
	res.SettlementKnown = OrdersTerminal(res)
	if !res.SettlementKnown {
		err = errors.Join(err, ErrUnresolved)
	}
	res.Err = err
	return res
}

// ResolveFlattenToZero keeps every old reduction order until its explicit
// terminal outcome, then reads positions independently before reducing again.
// A previous order may have been helped by an external close; its later zero
// terminal report corrects the inferred fees without blocking a confirmed exit.
func (e *Executor) ResolveFlattenToZero(ctx context.Context, l, r LegSpec, res Result) Result {
	res = e.ResolveTerminals(ctx, l, r, res, 0)
	if res.Err != nil {
		return res
	}
	res.pending, res.unconfirmed = nil, nil
	res.targetL, res.targetR, res.targetsKnown, res.emergency = 0, 0, true, true
	start := time.Now()
	res.Err = e.flatten(ctx, l, r, 0, 0, &res, res.rescue)
	res.Duration += time.Since(start)
	requireTerminalFlatten(&res)
	return res
}

func requireTerminalFlatten(res *Result) {
	if !OrdersTerminal(*res) {
		res.SettlementKnown = false
		res.Err = errors.Join(res.Err, ErrUnresolved)
	}
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
	// Positions live on each market's native quantity lattice; restore that
	// lattice after floating-point additions before applying the common floor.
	q := domain.Floor(math.Min(domain.Round(math.Abs(pl), a.SizeDecimals), domain.Round(math.Abs(pr), b.SizeDecimals)), d)
	if pl < 0 {
		q = -q
	}
	return q, -q
}

// legMinQty is the smallest tradable quantity on one leg at the given price,
// derived from the venue's minimum base size and minimum order notional.
func legMinQty(inst domain.Instrument, price float64) float64 {
	m := inst.MinBase
	if price > 0 {
		if q := inst.MinQuote / price; q > m {
			m = q
		}
	}
	if m < 0 {
		return 0
	}
	return m
}

// completeStep tops up the under-filled leg of an opening when exactly one
// leg filled short of the requested quantity. A single IOC completes the
// step; any failure falls back to the ordinary shrink path. The completion
// price is the decision price widened by at most TopUpBps in the aggressive
// direction, so chasing the book is hard-bounded.
func (e *Executor) completeStep(ctx context.Context, l, r LegSpec, qty, beforeL, beforeR, pl, pr float64, res *Result) (bool, error) {
	dl := float64(l.Side) * (pl - beforeL)
	dr := float64(r.Side) * (pr - beforeR)
	epsL := math.Pow10(-l.Inst.SizeDecimals) / 2
	epsR := math.Pow10(-r.Inst.SizeDecimals) / 2
	var short LegSpec
	var s float64
	switch {
	case dl < qty-epsL && dr >= qty-epsR:
		short, s = l, qty-dl
	case dr < qty-epsR && dl >= qty-epsL:
		short, s = r, qty-dr
	default:
		return false, nil // both legs full, or both short: shrink aligns them
	}
	if s <= 0 || s < legMinQty(short.Inst, short.Price) {
		return false, nil // below the venue minimum: cannot complete
	}
	s = domain.Round(s, short.Inst.SizeDecimals)
	if s <= 0 || s < legMinQty(short.Inst, short.Price) {
		return false, nil
	}
	req := domain.OrderReq{
		ClientID: e.ids.Next(), Inst: short.Inst, Side: short.Side, Qty: s,
		Price: topUpPrice(short, e.cfg.TopUpBps), Type: domain.Limit, ReduceOnly: false,
	}
	up := e.submitAndResolve(ctx, req, short.Venue)
	res.topUp = &topUpState{req: req, result: up}
	if !up.Resolved {
		res.SettlementKnown = false
		return false, fmt.Errorf("%w: completion order %d unresolved", ErrUnresolved, req.ClientID)
	}
	if up.Filled <= 0 {
		return false, nil // rejected or empty: fall back to shrinking
	}
	if short.Inst.MarketID == l.Inst.MarketID && short.Side == l.Side {
		mergeTopUp(&res.L, up)
	} else {
		mergeTopUp(&res.R, up)
	}
	return true, nil
}

// topUpPrice widens the decision price by bps in the aggressive direction and
// rounds in the tightening direction so the bound is never exceeded.
func topUpPrice(leg LegSpec, bps float64) float64 {
	px := leg.Price
	if leg.Side == domain.Buy {
		px *= 1 + bps/1e4
		px = domain.Floor(px, leg.Inst.PriceDecimals)
	} else {
		px *= 1 - bps/1e4
		px = domain.Ceil(px, leg.Inst.PriceDecimals)
	}
	if px <= 0 || math.IsNaN(px) || math.IsInf(px, 0) {
		return boundPrice(leg)
	}
	return px
}

// mergeTopUp folds completion fills into the leg's cumulative fill facts.
func mergeTopUp(leg *LegResult, up LegResult) {
	if up.Filled <= 0 || up.AvgPrice <= 0 {
		return
	}
	total := leg.Filled + up.Filled
	leg.AvgPrice = (leg.AvgPrice*leg.Filled + up.AvgPrice*up.Filled) / total
	leg.Filled = total
	leg.PriceEstimated = leg.PriceEstimated || up.PriceEstimated
}

// unwindSmallResidual reports whether reaching the aligned hedge target would
// require a reduction below one leg's venue minimum. Such dust cannot be
// traded directly: the caller re-targets (undo the step for openings, zero for
// closes) so every remaining reduction is tradable, or flatten closes the
// residual with a minimum-sized reduce-only order.
func (e *Executor) unwindSmallResidual(l, r LegSpec, tl, tr, pl, pr float64) bool {
	legs := [2]LegSpec{l, r}
	pos := [2]float64{pl, pr}
	tgt := [2]float64{tl, tr}
	for i := range legs {
		need := pos[i] - tgt[i]
		if math.Abs(need) < math.Pow10(-legs[i].Inst.SizeDecimals)/2 {
			continue
		}
		if math.Abs(need) < legMinQty(legs[i].Inst, legs[i].Price) {
			return true
		}
	}
	return false
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
			return boundPrice(leg)
		}
	}
	if domain.Round(leg.Price, leg.Inst.PriceDecimals) == leg.Price {
		px = domain.Round(px, leg.Inst.PriceDecimals)
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
	requireTerminalFlatten(&res)
	err = errors.Join(err, res.Err)
	return res, err
}

// FlattenToZero reduces both market positions to zero using the normal rescue
// price ladder (not emergency prices), for post-intervention recovery where
// the priority is a confirmed flat account rather than immediacy.
func (e *Executor) FlattenToZero(ctx context.Context, l, r LegSpec) (res Result, err error) {
	start := time.Now()
	defer func() { res.Duration = time.Since(start); res.Err = err }()
	// Like emergency recovery, a zero-target recovery must refresh positions
	// after an initial read fails. Its normal price ladder remains unchanged.
	res.targetL, res.targetR, res.targetsKnown, res.emergency = 0, 0, true, true
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
	requireTerminalFlatten(&res)
	err = errors.Join(err, res.Err)
	return res, err
}

// flatten reduces each leg to its own target. Urgent orders can reduce the leg
// with an available book independently. Another batch is never submitted until
// all previous outcomes and their resulting position snapshot are confirmed.
func (e *Executor) flatten(ctx context.Context, l, r LegSpec, targetL, targetR float64, res *Result, rescue RescueParams) error {
	start := time.Now()
	alerted := false
	lateRecovers := 0
	subMinTries := 0
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
		roundUp := false
		for i, leg := range legs {
			results[i].Resolved = true // no order on this leg is already settled
			if math.Abs(deltas[i]) < math.Pow10(-leg.Inst.SizeDecimals)/2 {
				continue
			}
			if deltas[i]*positions[i] <= 0 {
				return fmt.Errorf("rescue target cannot be reached reduce-only on %s", leg.Venue.Name())
			}
			q := domain.Round(math.Abs(deltas[i]), leg.Inst.SizeDecimals)
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
			if minQ := legMinQty(leg.Inst, px); q < minQ {
				target := targetL
				if i == 1 {
					target = targetR
				}
				if target != 0 {
					return fmt.Errorf("%w: rescue quantity %.8f below venue minimum %.8f on %s", ErrSubMinResidual, q, minQ, leg.Venue.Name())
				}
				// A zero target makes the remaining position the whole
				// residual. Reduce-only caps the fill at that position, so a
				// minimum-sized order still closes the dust.
				q = domain.Ceil(minQ, leg.Inst.SizeDecimals)
				roundUp = true
			}
			reqs[i] = domain.OrderReq{ClientID: e.ids.Next(), Inst: leg.Inst, Side: side, Qty: q, Price: px, Type: typ, ReduceOnly: true}
		}
		if roundUp {
			subMinTries++
			if subMinTries > subMinAttemptLimit {
				return fmt.Errorf("%w: repeated minimum-sized reductions did not clear the residual", ErrSubMinResidual)
			}
		} else {
			subMinTries = 0
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
			if errors.Is(err, ErrUnexpectedPosition) && res.PositionsKnown && lateRecovers < lateFillRecoveryLimit &&
				res.pending != nil && batchTerminal(res.pending) && idleLegMoved(res.pending, res) {
				lateRecovers++
				// A late fill from an earlier order moved a leg beyond this
				// batch's request while every ordered leg settled. Observed
				// positions are authoritative: keep the settled batch for
				// recovery and re-size from the fresh snapshot instead of
				// failing the whole execution.
				res.unconfirmed = append(res.unconfirmed, *res.pending)
				res.pending = nil
				continue
			}
			return err
		}
		lateRecovers = 0
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

// idleLegMoved reports whether a position moved on a leg this batch never
// ordered. Such movement is a late fill from an earlier order; the flatten
// loop can re-size from the fresh snapshot. Movement on an ordered leg keeps
// the strict verification so live or unknown orders are never abandoned.
func idleLegMoved(p *pendingRescue, res *Result) bool {
	if p == nil {
		return false
	}
	legs := [2]struct{ before, now float64 }{{p.beforeL, res.PosL}, {p.beforeR, res.PosR}}
	for i := range p.reqs {
		if p.reqs[i].Qty == 0 && math.Abs(legs[i].now-legs[i].before) > 1e-12 {
			return true
		}
	}
	return false
}

func (e *Executor) resolveRescue(ctx context.Context, l, r LegSpec, res *Result, query bool) error {
	p := res.pending
	if query {
		e.queryRescue(ctx, l, r, p)
	}
	// Protective prices are explicit estimates for position-inferred fills.
	pricedL, pricedR := l, r
	pricedL.Price, pricedR.Price = p.reqs[0].Price, p.reqs[1].Price
	pl, pr, known, err := e.confirmPositions(ctx, pricedL, pricedR, p.beforeL, p.beforeR, &p.results[0], &p.results[1], p.reqs[0].Qty, p.reqs[1].Qty, p.reqs[0].Side, p.reqs[1].Side)
	res.PositionsKnown = known
	if known {
		res.PosL, res.PosR = pl, pr
	}
	accountRescue(l, r, res, p)
	res.SettlementKnown = err == nil
	if err == nil {
		if !batchTerminal(p) {
			res.unconfirmed = append(res.unconfirmed, *p)
		}
		res.pending = nil
	}
	return err
}

func (e *Executor) queryRescue(ctx context.Context, l, r LegSpec, p *pendingRescue) {
	legs := [2]LegSpec{l, r}
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

func accountRescue(l, r LegSpec, res *Result, p *pendingRescue) {
	legs := [2]LegSpec{l, r}
	for i, lr := range p.results {
		if lr.Filled < 0 || math.IsNaN(lr.Filled) || math.IsInf(lr.Filled, 0) || lr.Filled-p.reqs[i].Qty >= math.Pow10(-legs[i].Inst.SizeDecimals)/2 {
			continue
		}
		if lr.Filled > 0 && (lr.AvgPrice <= 0 || math.IsNaN(lr.AvgPrice) || math.IsInf(lr.AvgPrice, 0)) || lr.Filled == 0 && p.accountedQty[i] == 0 {
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
			legResult.RescueFilled, legResult.RescuePrice = totalQty, 0
			if totalQty > 0 {
				legResult.RescuePrice = totalQuote / totalQty
			}
		}
		legResult.RescuePriceEstimated = legResult.RescuePriceEstimated || lr.PriceEstimated
		p.accountedQty[i], p.accountedQuote[i] = lr.Filled, quote
	}
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
