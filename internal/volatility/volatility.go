// Package volatility estimates the short-horizon volatility of a spread series.
package volatility

import (
	"math"
	"time"
)

type Params struct {
	Interval time.Duration
	Horizon  time.Duration
	HalfLife time.Duration
}

type increment struct {
	start, end time.Time
	square     float64
}

// Estimator tracks realized variance over an actual time horizon. Disconnected
// samples cannot be treated as short-horizon returns.
type Estimator struct {
	p             Params
	rets          []increment
	rv            float64
	last          float64
	first, lastAt time.Time
	ready         bool
	haveRV        bool
}

func New(p Params) *Estimator {
	if p.Interval <= 0 {
		p.Interval = 100 * time.Millisecond
	}
	if p.Horizon <= 0 {
		p.Horizon = 500 * time.Millisecond
	}
	if p.Horizon < p.Interval {
		p.Horizon = p.Interval
	}
	if p.HalfLife <= 0 {
		p.HalfLife = time.Minute
	}
	return &Estimator{p: p}
}

// Reset discards both the previous price and the variance history.
func (e *Estimator) Reset() {
	e.rets = nil
	e.rv = 0
	e.last = 0
	e.first = time.Time{}
	e.lastAt = time.Time{}
	e.ready = false
	e.haveRV = false
}

// Add is the compatibility entry point for an explicitly regular sample series.
// Runtime and replay callers should use AddAt with the actual sample timestamp.
func (e *Estimator) Add(x float64) bool {
	t := e.lastAt.Add(e.p.Interval)
	if e.lastAt.IsZero() {
		t = time.Unix(0, 1)
	}
	return e.AddAt(t, x)
}

// AddAt accepts increasing timestamps and finite prices. A gap larger than one
// and a half sampling intervals restarts warmup instead of inventing a return.
func (e *Estimator) AddAt(t time.Time, x float64) bool {
	if t.IsZero() || math.IsNaN(x) || math.IsInf(x, 0) {
		return false
	}
	if !e.lastAt.IsZero() && !t.After(e.lastAt) {
		return e.ReadyAt(t)
	}
	if e.lastAt.IsZero() || t.Sub(e.lastAt) > e.p.Interval+e.p.Interval/2 {
		e.Reset()
		e.first, e.lastAt, e.last = t, t, x
		return false
	}
	dt := t.Sub(e.lastAt)
	r := x - e.last
	e.rets = append(e.rets, increment{start: e.lastAt, end: t, square: r * r})
	cutoff := t.Add(-e.p.Horizon)
	first := 0
	for first < len(e.rets) && !e.rets[first].end.After(cutoff) {
		first++
	}
	if first > 0 {
		copy(e.rets, e.rets[first:])
		e.rets = e.rets[:len(e.rets)-first]
	}
	e.ready = t.Sub(e.first) >= e.p.Horizon
	if e.ready {
		sum := 0.0
		for _, ret := range e.rets {
			weight := 1.0
			if ret.start.Before(cutoff) {
				weight = float64(ret.end.Sub(cutoff)) / float64(ret.end.Sub(ret.start))
			}
			sum += weight * ret.square
		}
		if !e.haveRV {
			e.rv = sum
			e.haveRV = true
		} else {
			alpha := -math.Expm1(-math.Ln2 * float64(dt) / float64(e.p.HalfLife))
			e.rv += alpha * (sum - e.rv)
		}
	}
	e.lastAt, e.last = t, x
	return e.ready
}

func (e *Estimator) Ready() bool { return e.ready }

// ReadyAt is a pure read: the most recent sample must still be fresh.
func (e *Estimator) ReadyAt(t time.Time) bool {
	return e.ready && !t.Before(e.lastAt) && t.Sub(e.lastAt) <= e.p.Interval+e.p.Interval/2
}

func (e *Estimator) Vol() float64 {
	if !e.ready || e.rv <= 0 {
		return 0
	}
	return math.Sqrt(e.rv)
}

// VolAt returns zero for an expired or warming estimator. Use ReadyAt to
// distinguish a valid zero volatility from missing data.
func (e *Estimator) VolAt(t time.Time) float64 {
	if !e.ReadyAt(t) {
		return 0
	}
	return e.Vol()
}
