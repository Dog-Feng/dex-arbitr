package estimator

import (
	"fmt"
	"math"
	"time"
)

func init() {
	Register("zscore", func(p Params) (Estimator, error) {
		if p.MuHalfLife <= 0 || p.SigmaHalfLife <= 0 {
			return nil, fmt.Errorf("zscore estimator requires positive mu_half_life and sigma_half_life")
		}
		return NewZScore(p), nil
	})
}

// ZScore estimates the spread center with a fast EMA and its long-horizon
// dispersion with a slow EWMA of x². The mean follows local drifts so
// deviation stays small during trends, while the dispersion scales the
// opening threshold to the pair's own variability.
type ZScore struct {
	p          Params
	muHalf     float64 // seconds
	sigmaHalf  float64 // seconds
	sigmaFloor float64
	mean       float64
	mx2        float64
	n          int
	started    time.Time
	last       time.Time
}

// NewZScore builds a z-score estimator with sensible defaults for any
// half-life or floor left unset by the driver.
func NewZScore(p Params) *ZScore {
	if p.Interval <= 0 {
		p.Interval = time.Second
	}
	if p.MinWarmup <= 0 {
		p.MinWarmup = 15 * time.Minute
	}
	if p.MuHalfLife <= 0 {
		p.MuHalfLife = 15 * time.Minute
	}
	if p.SigmaHalfLife <= 0 {
		p.SigmaHalfLife = 3 * time.Hour
	}
	if p.SigmaFloor <= 0 {
		p.SigmaFloor = 0.5
	}
	return &ZScore{
		p: p, muHalf: p.MuHalfLife.Seconds(), sigmaHalf: p.SigmaHalfLife.Seconds(),
		sigmaFloor: p.SigmaFloor,
	}
}

// Update folds one sample into the EWMA mean and second moment.
func (z *ZScore) Update(t time.Time, x float64) {
	if t.IsZero() || math.IsNaN(x) || math.IsInf(x, 0) || (!z.last.IsZero() && t.Before(z.last)) {
		return
	}
	if z.n == 0 {
		z.started, z.last, z.mean, z.mx2, z.n = t, t, x, x*x, 1
		return
	}
	dt := t.Sub(z.last).Seconds()
	if dt <= 0 {
		return
	}
	a := 1 - math.Exp(-dt/z.muHalf)
	b := 1 - math.Exp(-dt/z.sigmaHalf)
	z.mean += a * (x - z.mean)
	z.mx2 += b * (x*x - z.mx2)
	z.n++
	z.last = t
}

// Mean is the EWMA center of the spread series.
func (z *ZScore) Mean() float64 { return z.mean }

// LongVol is the long-horizon spread dispersion, floored so a quiet market
// cannot inflate z unboundedly.
func (z *ZScore) LongVol() float64 {
	if z.n < 2 {
		return z.sigmaFloor
	}
	v := z.mx2 - z.mean*z.mean
	if v < 0 {
		v = 0
	}
	s := math.Sqrt(v)
	if s < z.sigmaFloor {
		s = z.sigmaFloor
	}
	return s
}

// Ready reports whether the estimator has warmed up and stays fresh.
func (z *ZScore) Ready() bool { return z.Stats().Ready }

// Stats snapshots the current state.
func (z *ZScore) Stats() Stats {
	st := Stats{Kind: "zscore", Mean: z.mean, Samples: z.n, Span: z.last.Sub(z.started), LastSample: z.last}
	st.Ready = z.ready(time.Now())
	return st
}

// StatsAt returns a pure snapshot relative to now without mutating state.
func (z *ZScore) StatsAt(now time.Time) Stats {
	st := Stats{Kind: "zscore", Mean: z.mean, Samples: z.n, LastSample: z.last}
	if z.n == 0 || z.last.IsZero() || now.Before(z.last) {
		return st
	}
	st.Span = now.Sub(z.started)
	st.Ready = z.ready(now)
	return st
}

func (z *ZScore) ready(now time.Time) bool {
	return z.n > 0 && now.Sub(z.started) >= z.p.MinWarmup &&
		now.Sub(z.last) <= 2*z.p.Interval
}
