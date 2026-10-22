// Package estimator provides pluggable spread-mean estimators.
package estimator

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Stats describes estimator state for logging / readiness.
type Stats struct {
	Kind       string
	Mean       float64
	Samples    int
	Span       time.Duration // time since first sample in window
	Coverage   float64       // samples / expected samples over span
	Ready      bool
	LastSample time.Time // latest accepted sample; zero for estimators without timestamps
}

// StatsAt reads an estimator against the evaluation clock. Implementations that
// have no time-aware snapshot retain their existing Stats contract.
func StatsAt(e Estimator, now time.Time) Stats {
	if timed, ok := e.(interface{ StatsAt(time.Time) Stats }); ok {
		return timed.StatsAt(now)
	}
	return e.Stats()
}

// ReadyAt reports whether the current window is fresh and warmed up.
func ReadyAt(e Estimator, now time.Time) bool { return StatsAt(e, now).Ready }

// MeanAt returns the current time window's mean.
func MeanAt(e Estimator, now time.Time) float64 { return StatsAt(e, now).Mean }

// Estimator estimates the "center" of the spread series.
type Estimator interface {
	Update(t time.Time, x float64)
	Mean() float64
	Ready() bool
	Stats() Stats
}

// Params are generic estimator params.
type Params struct {
	Window      time.Duration
	MinWarmup   time.Duration
	MinCoverage float64
	Interval    time.Duration // sampling interval (1s)
	// Z-score estimator only: mean and dispersion half-lives plus a floor on
	// the dispersion so a perfectly quiet market cannot inflate z.
	MuHalfLife    time.Duration
	SigmaHalfLife time.Duration
	SigmaFloor    float64
}

// LongVol exposes the long-horizon spread dispersion used by z-score
// strategies. The short-horizon volatility chain is a different quantity.
type LongVol interface {
	LongVol() float64
}

// Factory builds an estimator.
type Factory func(p Params) (Estimator, error)

var (
	mu  sync.RWMutex
	reg = map[string]Factory{}
)

// Register an estimator kind.
func Register(kind string, f Factory) {
	mu.Lock()
	reg[kind] = f
	mu.Unlock()
}

// New builds a registered estimator.
func New(kind string, p Params) (Estimator, error) {
	mu.RLock()
	f, ok := reg[kind]
	mu.RUnlock()
	if !ok {
		ks := make([]string, 0)
		for k := range reg {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		return nil, fmt.Errorf("unknown estimator %q (have %v)", kind, ks)
	}
	return f(p)
}
