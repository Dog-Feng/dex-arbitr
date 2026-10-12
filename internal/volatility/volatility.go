// Package volatility estimates the short-horizon volatility of a spread series.
package volatility

import (
	"math"
	"time"
)

// Params configure a realized-variance volatility estimator.
type Params struct {
	Interval time.Duration // sampling interval
	Horizon  time.Duration // increment horizon whose volatility is estimated
	HalfLife time.Duration // EWMA half-life of the realized variance
}

// Estimator tracks sigma of the horizon-length increment of a series using
// realized variance: per-interval returns are squared and summed over the
// trailing horizon, then exponentially smoothed. Vol is in the same units as
// the samples (bps for spreads).
type Estimator struct {
	step  int     // Horizon / Interval
	alpha float64 // EWMA weight per sample

	rets []float64 // ring of per-interval returns
	idx  int
	cnt  int     // number of returns in the ring
	sum  float64 // trailing sum of squared returns
	rv   float64 // smoothed realized variance
	last float64
	have bool
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

// Add feeds the next sample and reports readiness.
func (e *Estimator) Add(x float64) bool {
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
			e.rv += e.alpha * (e.sum - e.rv)
		}
	}
	e.last = x
	e.have = true
	return e.Ready()
}

// Ready reports whether a full horizon of returns has been observed.
func (e *Estimator) Ready() bool { return e.cnt >= len(e.rets) }

// Vol returns sigma of the horizon increment, or 0 before Ready.
func (e *Estimator) Vol() float64 {
	if !e.Ready() || e.rv <= 0 {
		return 0
	}
	return math.Sqrt(e.rv)
}
