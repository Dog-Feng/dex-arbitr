package estimator

import (
	"errors"
	"math"
	"time"
)

// State is the restart-safe snapshot of an estimator's warmup history.
// Estimators that do not implement Persistable cold-start on every restart.
type State struct {
	Kind    string    `json:"kind"`
	Mean    float64   `json:"mean"`
	MX2     float64   `json:"mx2"`
	N       int       `json:"n"`
	Started time.Time `json:"started"`
	Last    time.Time `json:"last"`
}

// Persistable is implemented by estimators whose state survives restarts.
// Restore resumes the history; readiness gates that depend on sample
// freshness still block until a new sample arrives, and the first Update
// decays the restored values by the real elapsed time.
type Persistable interface {
	Save() State
	Restore(State) error
}

var (
	errInvalidState = errors.New("invalid estimator state")
	// ErrStaleState means the saved history has decayed too far to trust:
	// the stop gap outlived the dispersion half-life, so readiness gates
	// must cold-start instead of resuming a warmup clock whose dispersion
	// no longer exists.
	ErrStaleState = errors.New("estimator state stale")
)

func validState(s State) bool {
	return s.N >= 1 &&
		!math.IsNaN(s.Mean) && !math.IsInf(s.Mean, 0) &&
		!math.IsNaN(s.MX2) && !math.IsInf(s.MX2, 0) && s.MX2 >= 0 &&
		!s.Started.IsZero() && !s.Last.IsZero() && !s.Last.Before(s.Started)
}
