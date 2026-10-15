// Package volatility estimates the short-horizon volatility of a spread series.
package volatility

import (
	"math"
	"time"
)

// Params configure a realized-variance volatility estimator.
type Params struct {
	Interval time.Duration // nominal sampling interval
	Horizon  time.Duration // increment horizon whose volatility is estimated
	HalfLife time.Duration // EWMA half-life of the realized variance
}

// Estimator tracks sigma of the horizon-length increment of a series using
// realized variance: every accepted sample contributes a return against the
// previous one, the squared returns of the trailing horizon are summed, then
// exponentially smoothed. Readiness is count-based: once the horizon's worth
// of increments has accumulated, the estimator stays ready regardless of how
// far apart samples arrive. A long disconnect inflates the next return and
// therefore sigma, which raises the opening threshold; it never restarts the
// chain.
type Estimator struct {
	step  int     // Horizon / Interval increments
	alpha float64 // EWMA weight per sample

	rets   []float64 // ring of per-sample returns
	idx    int
	cnt    int // number of returns in the ring
	sum    float64
	rv     float64
	haveRV bool
	last   float64
	have   bool
}

// New builds an estimator; zero params fall back to 100ms / 500ms / 1m.
func New(p Params) *Estimator {
	if p.Interval <= 0 {
		p.Interval = 100 * time.Millisecond
	}
	if p.Horizon <= 0 {
		p.Horizon = 500 * time.Millisecond
	}
	if p.HalfLife <= 0 {
		p.HalfLife = time.Minute
	}
	step := int(p.Horizon / p.Interval)
	if step < 1 {
		step = 1
	}
	return &Estimator{
		step:  step,
		alpha: 1 - math.Exp(-math.Ln2*float64(p.Interval)/float64(p.HalfLife)),
		rets:  make([]float64, step),
	}
}

// Reset discards both the previous price and the variance history.
func (e *Estimator) Reset() {
	e.rets = make([]float64, e.step)
	e.rv, e.sum, e.idx, e.cnt = 0, 0, 0, 0
	e.last, e.have, e.haveRV = 0, false, false
}

// Add is the compatibility entry point for a regular sample series.
func (e *Estimator) Add(x float64) bool {
	return e.AddAt(time.Time{}, x)
}

// AddAt feeds the next sample and reports readiness. The timestamp is not
// used: samples count as increments regardless of how far apart they arrive,
// matching the original count-based semantics. A gap produces a large return
// and therefore a large sigma, which the opening threshold then blocks.
func (e *Estimator) AddAt(_ time.Time, x float64) bool {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return false
	}
	if e.have {
		r := x - e.last
		old := e.rets[e.idx]
		e.sum += r*r - old*old
		e.rets[e.idx] = r
		e.idx = (e.idx + 1) % len(e.rets)
		if e.cnt < len(e.rets) {
			e.cnt++
		}
		if e.cnt >= len(e.rets) {
			if !e.haveRV {
				e.rv, e.haveRV = e.sum, true
			} else {
				e.rv += e.alpha * (e.sum - e.rv)
			}
		}
	}
	e.last = x
	e.have = true
	return e.Ready()
}

// Ready reports whether a full horizon of increments has been observed. It
// never expires: once warm, the chain stays warm across any gap.
func (e *Estimator) Ready() bool { return e.cnt >= len(e.rets) }

// ReadyAt is a pure read retained for interface compatibility. Readiness does
// not depend on real time.
func (e *Estimator) ReadyAt(time.Time) bool { return e.Ready() }

// Vol returns sigma of the horizon increment, or 0 before Ready.
func (e *Estimator) Vol() float64 {
	if !e.Ready() || e.rv <= 0 {
		return 0
	}
	return math.Sqrt(e.rv)
}

// VolAt is a pure read; sigma stays available across gaps.
func (e *Estimator) VolAt(time.Time) float64 { return e.Vol() }
