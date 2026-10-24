// Package strategy holds the pure step-decision function.
package strategy

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

// Dir of a step.
type Dir int8

const (
	DirLS Dir = -1 // long Left / short Right  (counter goes negative)
	DirSL Dir = 1  // short Left / long Right  (counter goes positive)
)

func (d Dir) String() string {
	if d == DirLS {
		return "LONG_L/SHORT_R"
	}
	return "SHORT_L/LONG_R"
}

// LegSides returns (left side, right side) used to OPEN a step in this direction.
func (d Dir) LegSides() (domain.Side, domain.Side) {
	if d == DirLS {
		return domain.Buy, domain.Sell
	}
	return domain.Sell, domain.Buy
}

// Step is one open hedged unit.
type Step struct {
	ID             int64     `json:"id"`
	Dir            Dir       `json:"dir"`
	Qty            float64   `json:"qty"`
	EntryBps       float64   `json:"entry_bps"` // entry spread from open fills, fill-time mid
	OpenL          float64   `json:"open_l,omitempty"`
	OpenR          float64   `json:"open_r,omitempty"`
	Realized       float64   `json:"realized_bps,omitempty"` // cumulative net realized PnL
	RealizedUSD    float64   `json:"realized_usd,omitempty"`
	EntryFeesUSD   float64   `json:"entry_fees_usd,omitempty"`
	OriginalQty    float64   `json:"original_qty,omitempty"`
	PriceEstimated bool      `json:"price_estimated,omitempty"`
	OpenedAt       time.Time `json:"opened_at"`
}

// Mode of a pair.
type Mode int8

const (
	ModeWarmup Mode = iota
	ModeRunning
	ModeReduceOnly
	ModePaused
)

func (m Mode) String() string {
	return [...]string{"WARMUP", "RUNNING", "REDUCE_ONLY", "PAUSED"}[m]
}

// Params of the strategy.
type Params struct {
	StepQty      float64
	MaxSteps     int
	OpenBps      float64 // deviation from mean to open
	CloseBps     float64 // gross round-trip profit to close
	DepthMult    float64 // top-of-book qty must be >= StepQty*DepthMult on both legs
	CloseOrder   string  // best | lifo | fifo
	VolK         float64 // one-sided sigma buffer on top of OpenBps for opening
	FeeBps       float64 // round-trip taker fees (per pair), subtracted from thresholds
	TakerFeeL    float64 // per-fill fee rates; close fees use actual entry and exit notionals
	TakerFeeR    float64
	MinInventory int // minimum steps to hold (volume churn floor); 0 = none
	// FlipHysteresisSigma is the extra deviation, in units of the current
	// spread volatility, required before opening the opposite direction after
	// a flat position. Same-direction additions are unaffected.
	FlipHysteresisSigma float64
	// CapBand is the capacity hysteresis band: once the step count touches
	// MaxSteps, opens stay blocked until the count falls to MaxSteps-CapBand
	// or below. 0 disables the band.
	CapBand int
	// CapHysteresisSigma is the extra deviation, in units of the current
	// spread volatility, required to open inside the top CapBand steps. It
	// prices the marginal cost of re-churning near the capacity line.
	CapHysteresisSigma float64
	// ZOpen is the z-score threshold (units of the long-horizon dispersion)
	// used by the zscore strategy instead of OpenBps/VolK. 0 applies the
	// default of 2.
	ZOpen float64
	// StepZ is the z-unit ladder spacing for the zscore strategy: each open
	// step requires z >= ZOpen + StepZ*|cnt| so steps spread over the
	// deviation band instead of stacking at one level. 0 disables the ladder.
	StepZ float64
	// ZClose is the zscore reversion exit: a held step is force closed when
	// its same-direction z falls to ZClose or below, booking the reversion
	// whatever profit remains (may be a small loss). 0 disables it.
	ZClose float64
	// CloseHold force closes any step held at least this long. 0 disables it.
	CloseHold time.Duration
	// ForcedCloseCooldown suppresses same-direction re-opens after a forced
	// close for this long. 0 disables it.
	ForcedCloseCooldown time.Duration
}

// Strategy decides an action from the pair state and market view.
type Strategy interface {
	Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action
}

// Arbitrage is the mean-reversion step strategy.
type Arbitrage struct{}

// Volume uses the same mean-reversion mechanics with zero additional open and
// close edge. Fees, volatility, depth and inventory limits still apply.
type Volume struct{}

// Parameters applies a registered strategy's parameter policy. Drivers use it
// for both scanning and execution so their effective thresholds agree.
func Parameters(s Strategy, p Params) Params {
	if policy, ok := s.(interface{ Parameters(Params) Params }); ok {
		return policy.Parameters(p)
	}
	return p
}

func (Volume) Parameters(p Params) Params {
	p.OpenBps, p.CloseBps = 0, 0
	return p
}

func (v Volume) Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action {
	return (Arbitrage{}).Decide(v.Parameters(p), s, q, mu, muReady, vol)
}

// State is the pair state the strategy needs.
type State struct {
	Mode          Mode
	Steps         []Step    // all same direction (single signed counter)
	LastDir       Dir       // direction last traded; 0 = unknown (e.g. after restart)
	CapEngaged    bool      // capacity band engaged (steps touched MaxSteps since last reset)
	Now           time.Time // decision wall clock; zero falls back to time.Now()
	ForcedCloseAt time.Time // last forced close; re-opens suppressed during cooldown
}

// Counter is the signed step count (negative = long left / short right).
func (s State) Counter() int {
	c := 0
	for _, st := range s.Steps {
		c += int(st.Dir)
	}
	return c
}

// Kind of action.
type Kind int8

const (
	None Kind = iota
	Open
	Close
)

func (k Kind) String() string { return [...]string{"NONE", "OPEN", "CLOSE"}[k] }

// Leg of an action.
type Leg struct {
	Side  domain.Side
	Price float64 // top-of-book decision price; executor adds the configured IOC allowance
}

// Action produced by Decide.
type Action struct {
	Kind        Kind
	Dir         Dir   // direction of the step opened / closed
	StepID      int64 // for Close
	Qty         float64
	L, R        Leg
	ExpectedBps float64 // open: deviation from mean; close: gross profit
	Reason      string
	Forced      bool // close came from a forced-exit rule, not the profit rule
}

// GrossProfitUSD computes price cash flows for the specified closed quantity.
// Missing entry prices cannot be reconstructed exactly from EntryBps.
func GrossProfitUSD(st Step, closeL, closeR, qty float64) float64 {
	if !validPrice(st.OpenL) || !validPrice(st.OpenR) || !validPrice(closeL) || !validPrice(closeR) || qty < 0 || math.IsNaN(qty) || math.IsInf(qty, 0) || (st.Dir != DirLS && st.Dir != DirSL) {
		return math.NaN()
	}
	return -float64(st.Dir) * qty * ((closeL - st.OpenL) + (st.OpenR - closeR))
}

// ProfitBps normalizes both cash-flow terms by the fixed entry notional.
func ProfitBps(st Step, closeL, closeR float64) float64 {
	return GrossProfitUSD(st, closeL, closeR, 1) / ((st.OpenL + st.OpenR) / 2) * 1e4
}

// RoundTripFeeUSD estimates all four taker fees at their actual fill prices.
func RoundTripFeeUSD(st Step, closeL, closeR, qty, feeL, feeR float64) float64 {
	if math.IsNaN(GrossProfitUSD(st, closeL, closeR, qty)) || feeL < 0 || feeR < 0 || math.IsNaN(feeL) || math.IsNaN(feeR) || math.IsInf(feeL, 0) || math.IsInf(feeR, 0) {
		return math.NaN()
	}
	return qty * ((st.OpenL+closeL)*feeL + (st.OpenR+closeR)*feeR)
}

func validPrice(x float64) bool { return x > 0 && !math.IsNaN(x) && !math.IsInf(x, 0) }

func closePrices(st Step, q spread.Quote) (float64, float64) {
	if len(q.L.Bids) == 0 || len(q.L.Asks) == 0 || len(q.R.Bids) == 0 || len(q.R.Asks) == 0 {
		return math.NaN(), math.NaN()
	}
	if st.Dir == DirLS {
		return q.L.Bids[0].Price, q.R.Asks[0].Price
	}
	return q.L.Asks[0].Price, q.R.Bids[0].Price
}

// QuoteProfitBps uses the executable reverse sides of the current books.
func QuoteProfitBps(st Step, q spread.Quote) float64 {
	l, r := closePrices(st, q)
	return ProfitBps(st, l, r)
}

func profit(st Step, q spread.Quote) float64 { return QuoteProfitBps(st, q) }

func closeFeeBps(p Params, st Step, q spread.Quote) float64 {
	if st.OriginalQty > 0 {
		if !validPrice(st.Qty) || !finite(st.EntryFeesUSD) || st.EntryFeesUSD < 0 {
			return math.NaN()
		}
		l, r := closePrices(st, q)
		feeL, feeR := p.TakerFeeL, p.TakerFeeR
		if feeL == 0 && feeR == 0 {
			feeL, feeR = p.FeeBps/40000, p.FeeBps/40000
		}
		// Entry fees are retained for the remaining quantity, so a restart or
		// fee-tier change must not reprice them using today's fee rates.
		return (st.EntryFeesUSD/st.Qty + l*feeL + r*feeR) / ((st.OpenL + st.OpenR) / 2) * 1e4
	}
	if p.TakerFeeL == 0 && p.TakerFeeR == 0 {
		return p.FeeBps
	}
	l, r := closePrices(st, q)
	return RoundTripFeeUSD(st, l, r, 1, p.TakerFeeL, p.TakerFeeR) / ((st.OpenL + st.OpenR) / 2) * 1e4
}

func openFeeBps(p Params, q spread.Quote, d Dir) float64 {
	if p.TakerFeeL == 0 && p.TakerFeeR == 0 {
		return p.FeeBps
	}
	l, r := d.LegSides()
	la, ra := legs(q, l, r)
	st := Step{Dir: d, OpenL: la.Price, OpenR: ra.Price}
	lpx, rpx := closePrices(st, q)
	// Future closing prices are unknown: reserve four fees at today's
	// executable prices, using the same quote reference as the opening signal.
	return RoundTripFeeUSD(st, lpx, rpx, 1, p.TakerFeeL, p.TakerFeeR) / q.RefMid * 1e4
}

func openingThreshold(p Params, q spread.Quote, d Dir, vol float64) float64 {
	return p.OpenBps + openFeeBps(p, q, d) + p.VolK*vol
}

// OpeningMargins reports each direction's remaining edge after the same fee
// and volatility reservation used by Decide. It does not bypass warmup, sizing
// or inventory gates; a scanner score alone is never a trading instruction.
func OpeningMargins(p Params, q spread.Quote, mu, vol float64) (ls, sl float64) {
	if _, ok := spread.Compute(q.L, q.R); !ok || !validPrice(q.RefMid) || !finite(q.LS) || !finite(q.SL) ||
		!finite(mu) || !finite(vol) || vol < 0 || !finite(p.OpenBps) || !finite(p.VolK) || p.VolK < 0 ||
		!finite(p.FeeBps) || p.FeeBps < 0 || !finite(p.TakerFeeL) || p.TakerFeeL < 0 || !finite(p.TakerFeeR) || p.TakerFeeR < 0 {
		return math.NaN(), math.NaN()
	}
	return q.LS - mu - openingThreshold(p, q, DirLS, vol), q.SL + mu - openingThreshold(p, q, DirSL, vol)
}

func depthOK(q spread.Quote, l, r domain.Side, need float64) bool {
	top := func(b domain.Book, s domain.Side) float64 {
		if s == domain.Buy {
			return b.Asks[0].Size
		}
		return b.Bids[0].Size
	}
	return top(q.L, l)+1e-12 >= need && top(q.R, r)+1e-12 >= need
}

func legs(q spread.Quote, l, r domain.Side) (Leg, Leg) {
	px := func(b domain.Book, s domain.Side) float64 {
		if s == domain.Buy {
			return b.Asks[0].Price
		}
		return b.Bids[0].Price
	}
	return Leg{l, px(q.L, l)}, Leg{r, px(q.R, r)}
}

// gate validates the preconditions every strategy variant shares. ok=false
// returns a blocking action.
func gate(p Params, s State, q spread.Quote) (Action, bool) {
	if s.Mode == ModePaused {
		return Action{Reason: "paused"}, false
	}
	if _, ok := spread.Compute(q.L, q.R); !ok || !validPrice(q.RefMid) || !finite(q.LS) || !finite(q.SL) || !finite(q.Mid) {
		return Action{Reason: "invalid quote"}, false
	}
	if s.Mode < ModeWarmup || s.Mode > ModePaused || !validPrice(p.StepQty) || p.MaxSteps <= 0 || !validPrice(p.DepthMult) || !finite(p.OpenBps) || !finite(p.CloseBps) || !finite(p.VolK) || p.VolK < 0 || !finite(p.FeeBps) || p.FeeBps < 0 || !finite(p.TakerFeeL) || p.TakerFeeL < 0 || !finite(p.TakerFeeR) || p.TakerFeeR < 0 || p.MinInventory < 0 || !finite(p.FlipHysteresisSigma) || p.FlipHysteresisSigma < 0 || p.CapBand < 0 || !finite(p.CapHysteresisSigma) || p.CapHysteresisSigma < 0 || !finite(p.ZOpen) || p.ZOpen < 0 || !finite(p.StepZ) || p.StepZ < 0 || !finite(p.ZClose) || p.ZClose < 0 || p.CloseHold < 0 || p.ForcedCloseCooldown < 0 {
		return Action{Reason: "invalid strategy parameters"}, false
	}
	return Action{}, true
}

// decideClose evaluates the close-first rule shared by every strategy. ok
// reports that the close path produced either a close or a blocking reason.
func decideClose(p Params, s State, q spread.Quote) (Action, bool) {
	cnt := s.Counter()
	if abs(cnt) <= p.MinInventory {
		return Action{}, false
	}
	type cand struct {
		st Step
		pf float64
		ix int
	}
	var cs []cand
	for i, st := range s.Steps {
		if pf := profit(st, q); pf >= p.CloseBps+closeFeeBps(p, st, q) {
			cs = append(cs, cand{st, pf, i})
		}
	}
	if len(cs) == 0 {
		return Action{}, false
	}
	switch p.CloseOrder {
	case "lifo":
		sort.Slice(cs, func(i, j int) bool { return cs[i].ix > cs[j].ix })
	case "fifo":
		sort.Slice(cs, func(i, j int) bool { return cs[i].ix < cs[j].ix })
	default: // best
		sort.SliceStable(cs, func(i, j int) bool { return cs[i].pf > cs[j].pf })
	}
	// Try candidates in order so a depth-starved large step cannot
	// block a smaller, profitable step that has enough liquidity.
	for _, c := range cs {
		ol, or := c.st.Dir.LegSides()
		l, r := ol.Opposite(), or.Opposite()
		if !depthOK(q, l, r, c.st.Qty*p.DepthMult) {
			continue
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Close, Dir: c.st.Dir, StepID: c.st.ID, Qty: c.st.Qty, L: la, R: ra, ExpectedBps: c.pf,
			Reason: fmt.Sprintf("close step %d profit %.2fbps >= %.2f+%.2f (net)", c.st.ID, c.pf, p.CloseBps, closeFeeBps(p, c.st, q))}, true
	}
	return Action{Reason: fmt.Sprintf("%d profitable close candidate(s) but none has top-of-book depth for its size", len(cs))}, true
}

// stateNow returns the decision wall clock, falling back to the process clock
// for callers that do not set State.Now.
func stateNow(s State) time.Time {
	if s.Now.IsZero() {
		return time.Now()
	}
	return s.Now
}

// decideHoldClose force closes steps held at least CloseHold. It runs after
// the normal profit close, so profitable steps always exit first. Steps with
// no recorded open time cannot be aged and are left alone.
func decideHoldClose(p Params, s State, q spread.Quote, now time.Time) (Action, bool) {
	if p.CloseHold <= 0 || len(s.Steps) == 0 {
		return Action{}, false
	}
	type cand struct {
		st Step
		ix int
	}
	var cs []cand
	for i, st := range s.Steps {
		if st.OpenedAt.IsZero() || now.Sub(st.OpenedAt) < p.CloseHold {
			continue
		}
		cs = append(cs, cand{st, i})
	}
	if len(cs) == 0 {
		return Action{}, false
	}
	sort.SliceStable(cs, func(i, j int) bool { return cs[i].st.OpenedAt.Before(cs[j].st.OpenedAt) })
	for _, c := range cs {
		ol, or := c.st.Dir.LegSides()
		l, r := ol.Opposite(), or.Opposite()
		if !depthOK(q, l, r, c.st.Qty*p.DepthMult) {
			continue
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Close, Forced: true, Dir: c.st.Dir, StepID: c.st.ID, Qty: c.st.Qty, L: la, R: ra,
			ExpectedBps: QuoteProfitBps(c.st, q),
			Reason:      fmt.Sprintf("force close step %d: held %s >= %s", c.st.ID, now.Sub(c.st.OpenedAt).Round(time.Second), p.CloseHold)}, true
	}
	return Action{Reason: "steps exceed close_hold but none has top-of-book depth"}, true
}

// zScoreClose is the zscore reversion exit: when the held direction's z has
// fallen to ZClose or below, the deviation has reverted and the step is force
// closed, booking whatever profit remains (possibly a small loss). It runs
// after the normal profit close and the holding-time limit.
func zScoreClose(p Params, s State, q spread.Quote, zLS, zSL float64) (Action, bool) {
	cnt := s.Counter()
	if p.ZClose <= 0 || cnt == 0 || !finite(zLS) || !finite(zSL) {
		return Action{}, false
	}
	z := zLS
	if cnt > 0 {
		z = zSL
	}
	if z > p.ZClose {
		return Action{}, false
	}
	type cand struct {
		st Step
		pf float64
		ix int
	}
	var cs []cand
	for i, st := range s.Steps {
		cs = append(cs, cand{st, profit(st, q), i})
	}
	switch p.CloseOrder {
	case "lifo":
		sort.Slice(cs, func(i, j int) bool { return cs[i].ix > cs[j].ix })
	case "fifo":
		sort.Slice(cs, func(i, j int) bool { return cs[i].ix < cs[j].ix })
	default: // best
		sort.SliceStable(cs, func(i, j int) bool { return cs[i].pf > cs[j].pf })
	}
	for _, c := range cs {
		ol, or := c.st.Dir.LegSides()
		l, r := ol.Opposite(), or.Opposite()
		if !depthOK(q, l, r, c.st.Qty*p.DepthMult) {
			continue
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Close, Forced: true, Dir: c.st.Dir, StepID: c.st.ID, Qty: c.st.Qty, L: la, R: ra, ExpectedBps: c.pf,
			Reason: fmt.Sprintf("force close step %d: z %.2f <= %.2f (reversion exit)", c.st.ID, z, p.ZClose)}, true
	}
	return Action{Reason: "reversion exit due but no step has top-of-book depth"}, true
}

// forcedCloseCooldown reports whether a same-direction open is still inside
// the post-forced-close cooldown window.
func forcedCloseCooldown(p Params, s State, d Dir) (time.Duration, bool) {
	if p.ForcedCloseCooldown <= 0 || s.ForcedCloseAt.IsZero() || s.LastDir != d {
		return 0, false
	}
	if rem := p.ForcedCloseCooldown - stateNow(s).Sub(s.ForcedCloseAt); rem > 0 {
		return rem, true
	}
	return 0, false
}

// Decide is pure: same inputs => same action. mu is the spread mean in bps
// (only used for opening; muReady=false blocks opens). vol is the short-horizon
// spread volatility in bps; openings require the deviation to exceed
// OpenBps + VolK*vol so the expected 500ms adverse move is pre-reserved.
func (Arbitrage) Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action {
	if act, ok := gate(p, s, q); !ok {
		return act
	}
	if act, ok := decideClose(p, s, q); ok {
		return act
	}
	if act, ok := decideHoldClose(p, s, q, stateNow(s)); ok {
		return act
	}
	need := p.StepQty * p.DepthMult
	cnt := s.Counter()
	// A band at or beyond the capacity itself must degrade to "release at
	// flat" instead of a negative unlock threshold that never clears.
	band := p.CapBand
	if band > p.MaxSteps {
		band = p.MaxSteps
	}

	// open
	if s.Mode != ModeRunning {
		return Action{Reason: "mode " + s.Mode.String() + ": no opens"}
	}
	if !muReady {
		return Action{Reason: "mean not ready"}
	}
	devLS := q.LS - mu // buy L / sell R is rich vs mean
	devSL := q.SL + mu // buy R / sell L is rich vs mean
	if math.IsNaN(mu) || math.IsInf(mu, 0) || math.IsNaN(vol) || math.IsInf(vol, 0) || vol < 0 {
		return Action{Reason: "invalid statistics"}
	}
	try := func(d Dir, dev float64) (Action, bool) {
		feeBps := openFeeBps(p, q, d)
		needDev := openingThreshold(p, q, d, vol)
		if math.IsNaN(dev) || math.IsInf(dev, 0) || math.IsNaN(needDev) || math.IsInf(needDev, 0) {
			return Action{Reason: "invalid threshold"}, true
		}
		if rem, cool := forcedCloseCooldown(p, s, d); cool {
			return Action{Reason: fmt.Sprintf("%s blocked: forced-close cooldown %.0fs remaining", d, rem.Seconds())}, true
		}
		// A reversal after a flat position needs one extra sigma of deviation
		// so marginal noise around the mean cannot flip the book back and
		// forth. Same-direction continuation is never taxed.
		if cnt == 0 && s.LastDir != 0 && s.LastDir != d && p.FlipHysteresisSigma > 0 {
			needDev += p.FlipHysteresisSigma * vol
		}
		// The top CapBand steps are the most expensive re-churn: price the
		// marginal open there with one extra sigma so a close-then-reopen at
		// the capacity line requires a genuinely stronger signal.
		if band > 0 && p.CapHysteresisSigma > 0 && abs(cnt) >= p.MaxSteps-band {
			needDev += p.CapHysteresisSigma * vol
		}
		// Capacity band: once the count touched MaxSteps, holds open until it
		// falls to MaxSteps-CapBand or below. This breaks the 15->14->15
		// close-one/open-one cycle at the capacity line.
		if band > 0 && s.CapEngaged && abs(cnt) > p.MaxSteps-band {
			return Action{Reason: fmt.Sprintf("%s signal %.2fbps but capacity band engaged (reopen below %d steps)", d, dev, p.MaxSteps-band)}, true
		}
		if dev < needDev {
			return Action{}, false
		}
		if cnt != 0 && Dir(sign(cnt)) != d {
			return Action{Reason: fmt.Sprintf("%s signal %.2fbps but holding opposite steps", d, dev)}, true
		}
		if abs(cnt) >= p.MaxSteps {
			return Action{Reason: fmt.Sprintf("%s signal %.2fbps but max steps %d reached", d, dev, p.MaxSteps)}, true
		}
		l, r := d.LegSides()
		if !depthOK(q, l, r, need) {
			return Action{Reason: fmt.Sprintf("%s signal %.2fbps but top-of-book depth < %.4f", d, dev, need)}, true
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Open, Dir: d, Qty: p.StepQty, L: la, R: ra, ExpectedBps: dev,
			Reason: fmt.Sprintf("open %s dev %.2fbps >= %.2f fee %.2f +%.2f*vol %.2f (mu %.2f)", d, dev, p.OpenBps, feeBps, p.VolK, vol, mu)}, true
	}
	// prefer the larger deviation
	first, second := DirLS, DirSL
	d1, d2 := devLS, devSL
	if devSL > devLS {
		first, second, d1, d2 = DirSL, DirLS, devSL, devLS
	}
	if a, ok := try(first, d1); ok && a.Kind != None {
		return a
	} else if b, ok2 := try(second, d2); ok2 && b.Kind != None {
		return b
	} else if ok {
		return a
	} else if ok2 {
		return b
	}
	return Action{Reason: fmt.Sprintf("no signal: LS dev %.2f, SL dev %.2f (mu %.2f)", devLS, devSL, mu)}
}

// ZScore is the mean-reversion strategy whose opening gate is the spread's
// long-horizon z-score instead of a fixed bps deviation. Closing is unchanged.
type ZScore struct{}

// Parameters applies the z-score defaults.
func (ZScore) Parameters(p Params) Params {
	if p.ZOpen <= 0 {
		p.ZOpen = 2
	}
	return p
}

// Decide opens when the long-horizon z-score exceeds ZOpen in one direction
// and the deviation still covers the round-trip fees. mu is the fast mean,
// vol is the long-horizon dispersion. Closing uses the same rule as
// Arbitrage: gross profit must cover the entry-fee amortization plus exit
// fees. Forced reduction is deliberately not implemented here.
func (ZScore) Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action {
	p = (ZScore{}).Parameters(p)
	if act, ok := gate(p, s, q); !ok {
		return act
	}
	if act, ok := decideClose(p, s, q); ok {
		return act
	}
	if act, ok := decideHoldClose(p, s, q, stateNow(s)); ok {
		return act
	}
	if p.ZClose > 0 && s.Counter() != 0 && muReady && !math.IsNaN(mu) && !math.IsInf(mu, 0) && vol > 0 {
		zLS := (q.LS - mu) / vol
		zSL := (q.SL + mu) / vol
		if act, ok := zScoreClose(p, s, q, zLS, zSL); ok {
			return act
		}
	}
	need := p.StepQty * p.DepthMult
	cnt := s.Counter()
	band := p.CapBand
	if band > p.MaxSteps {
		band = p.MaxSteps
	}
	if s.Mode != ModeRunning {
		return Action{Reason: "mode " + s.Mode.String() + ": no opens"}
	}
	if !muReady {
		return Action{Reason: "mean not ready"}
	}
	if math.IsNaN(mu) || math.IsInf(mu, 0) || math.IsNaN(vol) || math.IsInf(vol, 0) || vol <= 0 {
		return Action{Reason: "invalid statistics"}
	}
	zLS := (q.LS - mu) / vol
	zSL := (q.SL + mu) / vol
	try := func(d Dir, z, dev float64) (Action, bool) {
		if math.IsNaN(z) || math.IsInf(z, 0) {
			return Action{Reason: "invalid z"}, true
		}
		if rem, cool := forcedCloseCooldown(p, s, d); cool {
			return Action{Reason: fmt.Sprintf("%s blocked: forced-close cooldown %.0fs remaining", d, rem.Seconds())}, true
		}
		needZ := p.ZOpen
		feeBps := openFeeBps(p, q, d)
		// A reversal after a flat position needs one extra sigma of z.
		if cnt == 0 && s.LastDir != 0 && s.LastDir != d && p.FlipHysteresisSigma > 0 {
			needZ += p.FlipHysteresisSigma
		}
		// The top CapBand steps price one extra sigma of z.
		if band > 0 && p.CapHysteresisSigma > 0 && abs(cnt) >= p.MaxSteps-band {
			needZ += p.CapHysteresisSigma
		}
		// The ladder prices each additional step one StepZ of dispersion
		// higher, spreading entries over the deviation band.
		needZ += p.StepZ * float64(abs(cnt))
		if band > 0 && s.CapEngaged && abs(cnt) > p.MaxSteps-band {
			return Action{Reason: fmt.Sprintf("%s z %.2f but capacity band engaged (reopen below %d steps)", d, z, p.MaxSteps-band)}, true
		}
		if z < needZ || dev < feeBps {
			return Action{}, false
		}
		if cnt != 0 && Dir(sign(cnt)) != d {
			return Action{Reason: fmt.Sprintf("%s z %.2f but holding opposite steps", d, z)}, true
		}
		if abs(cnt) >= p.MaxSteps {
			return Action{Reason: fmt.Sprintf("%s z %.2f but max steps %d reached", d, z, p.MaxSteps)}, true
		}
		l, r := d.LegSides()
		if !depthOK(q, l, r, need) {
			return Action{Reason: fmt.Sprintf("%s z %.2f but top-of-book depth < %.4f", d, z, need)}, true
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Open, Dir: d, Qty: p.StepQty, L: la, R: ra, ExpectedBps: dev,
			Reason: fmt.Sprintf("open %s z %.2f >= %.2f dev %.2fbps >= fee %.2f (mu %.2f sigma %.2f)", d, z, needZ, dev, feeBps, mu, vol)}, true
	}
	// prefer the larger z
	first, second := DirLS, DirSL
	z1, z2 := zLS, zSL
	if zSL > zLS {
		first, second, z1, z2 = DirSL, DirLS, zSL, zLS
	}
	devLS := q.LS - mu
	devSL := q.SL + mu
	dev1, dev2 := devLS, devSL
	if zSL > zLS {
		dev1, dev2 = devSL, devLS
	}
	if a, ok := try(first, z1, dev1); ok && a.Kind != None {
		return a
	} else if b, ok2 := try(second, z2, dev2); ok2 && b.Kind != None {
		return b
	} else if ok {
		return a
	} else if ok2 {
		return b
	}
	return Action{Reason: fmt.Sprintf("no signal: LS z %.2f, SL z %.2f (mu %.2f sigma %.2f)", zLS, zSL, mu, vol)}
}

// EntryBps computes the executed spread (bps) of a leg pair in direction d,
// referencing the fill-time mid (lPx+rPx)/2.
func EntryBps(d Dir, lPx, rPx float64) float64 {
	if !validPrice(lPx) || !validPrice(rPx) || (d != DirLS && d != DirSL) {
		return math.NaN()
	}
	ref := (lPx + rPx) / 2
	if d == DirLS {
		return (rPx - lPx) / ref * 1e4
	}
	return (lPx - rPx) / ref * 1e4
}

func sign(x int) int {
	if x < 0 {
		return -1
	}
	return 1
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// Factory builds a strategy.
type Factory func() Strategy

var (
	stratMu    sync.RWMutex
	strategies = map[string]Factory{}
)

// Register a strategy kind.
func Register(name string, f Factory) {
	stratMu.Lock()
	strategies[name] = f
	stratMu.Unlock()
}

// New builds a registered strategy.
func New(name string) (Strategy, error) {
	stratMu.RLock()
	f, ok := strategies[name]
	stratMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown strategy %q", name)
	}
	return f(), nil
}

func init() {
	Register("arbitrage", func() Strategy { return Arbitrage{} })
	Register("volume", func() Strategy { return Volume{} })
	Register("zscore", func() Strategy { return ZScore{} })
}
