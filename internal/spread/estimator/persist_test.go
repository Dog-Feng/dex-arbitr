package estimator

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestZScoreSaveRestoreRoundtrip(t *testing.T) {
	params := Params{Interval: time.Second, MinWarmup: 10 * time.Second, MuHalfLife: time.Minute, SigmaHalfLife: time.Hour, SigmaFloor: 0.5}
	z := NewZScore(params)
	start := time.Now().Add(-40 * time.Second)
	for i := 1; i <= 30; i++ {
		z.Update(start.Add(time.Duration(i)*time.Second), float64(i))
	}
	z2 := NewZScore(params)
	if err := z2.Restore(z.Save()); err != nil {
		t.Fatal(err)
	}
	if z2.Mean() != z.Mean() || z2.Save().N != 30 {
		t.Fatalf("roundtrip mismatch: mean %v vs %v, n %d", z2.Mean(), z.Mean(), z2.Save().N)
	}
	// The warmup clock carries over: without any new sample the restored
	// estimator is ready right after the last saved sample.
	if st := z2.StatsAt(start.Add(31 * time.Second)); !st.Ready {
		t.Fatalf("restored estimator lost its warmup: %+v", st)
	}
	// And it goes stale again once the freshness window (2 intervals) passes.
	if st := z2.StatsAt(start.Add(35 * time.Second)); st.Ready {
		t.Fatal("restored estimator stayed fresh beyond the freshness gate")
	}
}

func TestZScoreRestoreRejectsBadState(t *testing.T) {
	z := NewZScore(Params{Interval: time.Second, MinWarmup: time.Minute, MuHalfLife: time.Minute, SigmaHalfLife: time.Hour, SigmaFloor: 0.5})
	now := time.Now()
	if err := z.Restore(State{Kind: "sma", N: 5, Mean: 1, MX2: 1, Started: now.Add(-time.Hour), Last: now}); err == nil {
		t.Fatal("kind mismatch accepted")
	}
	if err := z.Restore(State{Kind: "zscore", N: 0}); err == nil {
		t.Fatal("zero samples accepted")
	}
	if err := z.Restore(State{Kind: "zscore", N: 5, Mean: math.NaN()}); err == nil {
		t.Fatal("NaN mean accepted")
	}
	if err := z.Restore(State{Kind: "zscore", N: 5, Mean: 1, MX2: 1, Started: now, Last: now.Add(-time.Second)}); err == nil {
		t.Fatal("last before started accepted")
	}
	// A history that never completed its warmup window is not resumable.
	short := State{Kind: "zscore", N: 3, Mean: 1, MX2: 1, Started: now.Add(-5 * time.Second), Last: now}
	if err := z.Restore(short); err == nil {
		t.Fatal("incomplete warmup history accepted")
	}
	// A stop gap beyond 2x the dispersion half-life decays the history too
	// far: restoring would trade on a floor dispersion.
	stale := State{Kind: "zscore", N: 30, Mean: 1, MX2: 1, Started: now.Add(-4 * time.Hour), Last: now.Add(-3 * time.Hour)}
	if err := z.Restore(stale); !errors.Is(err, ErrStaleState) {
		t.Fatalf("stale history accepted: %v", err)
	}
}
