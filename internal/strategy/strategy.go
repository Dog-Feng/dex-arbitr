// Package strategy holds the pure step-decision function.
package strategy

import (
	"fmt"
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
	ID       int64     `json:"id"`
	Dir      Dir       `json:"dir"`
	Qty      float64   `json:"qty"`
	EntryBps float64   `json:"entry_bps"` // entry spread from open fills, fill-time mid
	OpenL    float64   `json:"open_l,omitempty"`
	OpenR    float64   `json:"open_r,omitempty"`
	Realized float64   `json:"realized_bps,omitempty"` // cumulative net realized PnL
	OpenedAt time.Time `json:"opened_at"`
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
	StepQty    float64
	MaxSteps   int
	OpenBps    float64 // deviation from mean to open
	CloseBps   float64 // gross round-trip profit to close
	DepthMult  float64 // top-of-book qty must be >= StepQty*DepthMult on both legs
	CloseOrder string  // best | lifo | fifo
	VolK       float64 // one-sided sigma buffer on top of OpenBps for opening
	FeeBps     float64 // round-trip taker fees (per pair), subtracted from thresholds
}

// Strategy decides an action from the pair state and market view.
type Strategy interface {
	Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action
}

// Arbitrage is the mean-reversion step strategy.
type Arbitrage struct{}

// State is the pair state the strategy needs.
type State struct {
	Mode  Mode
	Steps []Step // all same direction (single signed counter)
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
}

func profit(st Step, q spread.Quote) float64 {
	if st.Dir == DirLS { // close: sell L @bid, buy R @ask
		return st.EntryBps + q.SL
	}
	return st.EntryBps + q.LS // close: buy L @ask, sell R @bid
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

// Decide is pure: same inputs => same action. mu is the spread mean in bps
// (only used for opening; muReady=false blocks opens). vol is the short-horizon
// spread volatility in bps; openings require the deviation to exceed
// OpenBps + VolK*vol so the expected 500ms adverse move is pre-reserved.
func (Arbitrage) Decide(p Params, s State, q spread.Quote, mu float64, muReady bool, vol float64) Action {
	if s.Mode == ModePaused {
		return Action{Reason: "paused"}
	}
	need := p.StepQty * p.DepthMult

	// 1) close first (allowed in every non-paused mode)
	type cand struct {
		st Step
		pf float64
		ix int
	}
	var cs []cand
	for i, st := range s.Steps {
		if pf := profit(st, q); pf >= p.CloseBps+p.FeeBps {
			cs = append(cs, cand{st, pf, i})
		}
	}
	if len(cs) > 0 {
		switch p.CloseOrder {
		case "lifo":
			sort.Slice(cs, func(i, j int) bool { return cs[i].ix > cs[j].ix })
		case "fifo":
			sort.Slice(cs, func(i, j int) bool { return cs[i].ix < cs[j].ix })
		default: // best
			sort.SliceStable(cs, func(i, j int) bool { return cs[i].pf > cs[j].pf })
		}
		c := cs[0]
		ol, or := c.st.Dir.LegSides()
		l, r := ol.Opposite(), or.Opposite()
		if !depthOK(q, l, r, c.st.Qty*p.DepthMult) {
			return Action{Reason: fmt.Sprintf("close step %d profitable (%.2fbps) but top-of-book depth < %.4f", c.st.ID, c.pf, c.st.Qty*p.DepthMult)}
		}
		la, ra := legs(q, l, r)
		return Action{Kind: Close, Dir: c.st.Dir, StepID: c.st.ID, Qty: c.st.Qty, L: la, R: ra, ExpectedBps: c.pf,
			Reason: fmt.Sprintf("close step %d profit %.2fbps >= %.2f+%.2f (net)", c.st.ID, c.pf, p.CloseBps, p.FeeBps)}
	}

	// 2) open
	if s.Mode != ModeRunning {
		return Action{Reason: "mode " + s.Mode.String() + ": no opens"}
	}
	if !muReady {
		return Action{Reason: "mean not ready"}
	}
	cnt := s.Counter()
	devLS := q.LS - mu // buy L / sell R is rich vs mean
	devSL := q.SL + mu // buy R / sell L is rich vs mean
	needDev := p.OpenBps + p.FeeBps + p.VolK*vol
	try := func(d Dir, dev float64) (Action, bool) {
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
			Reason: fmt.Sprintf("open %s dev %.2fbps >= %.2f fee %.2f +%.2f*vol %.2f (mu %.2f)", d, dev, p.OpenBps, p.FeeBps, p.VolK, vol, mu)}, true
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
	return Action{Reason: fmt.Sprintf("no signal: LS dev %.2f, SL dev %.2f (need %.2f, mu %.2f)", devLS, devSL, needDev, mu)}
}

// EntryBps computes the executed spread (bps) of a leg pair in direction d,
// referencing the fill-time mid (lPx+rPx)/2.
func EntryBps(d Dir, lPx, rPx float64) float64 {
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
}
