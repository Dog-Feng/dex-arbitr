package estimator

import (
	"math"
	"testing"
	"time"
)

func TestZScoreMeanTracksDrift(t *testing.T) {
	// A short mean half-life follows the local level, so a linear drift leaves
	// only a small gap between the mean and the last sample.
	z := NewZScore(Params{Interval: time.Second, MinWarmup: time.Minute,
		MuHalfLife: 2 * time.Second, SigmaHalfLife: time.Hour, SigmaFloor: 0.5})
	start := time.Unix(1700000000, 0)
	last := 0.0
	for i := 1; i <= 60; i++ {
		last = float64(i)
		z.Update(start.Add(time.Duration(i)*time.Second), last)
	}
	if math.Abs(z.Mean()-last) > 3 {
		t.Fatalf("mean %.2f too far behind the drifting level %.2f", z.Mean(), last)
	}
}

func TestZScoreDispersionAndFloor(t *testing.T) {
	z := NewZScore(Params{Interval: time.Second, MinWarmup: time.Minute,
		MuHalfLife: time.Hour, SigmaHalfLife: time.Hour, SigmaFloor: 0.5})
	start := time.Unix(1700000000, 0)
	for i := 1; i <= 60; i++ {
		z.Update(start.Add(time.Duration(i)*time.Second), 0.0)
	}
	if got := z.LongVol(); math.Abs(got-0.5) > 1e-12 {
		t.Fatalf("quiet series must sit at the floor: %v", got)
	}
	// An alternating ±10 series has a dispersion of about 10.
	z = NewZScore(Params{Interval: time.Second, MinWarmup: time.Minute,
		MuHalfLife: time.Minute, SigmaHalfLife: time.Minute, SigmaFloor: 0.5})
	for i := 1; i <= 200; i++ {
		x := 10.0
		if i%2 == 0 {
			x = -10
		}
		z.Update(start.Add(time.Duration(i)*time.Second), x)
	}
	if s := z.LongVol(); math.Abs(s-10) > 0.5 {
		t.Fatalf("alternating series dispersion %.2f, want ~10", s)
	}
}

func TestZScoreReadinessGating(t *testing.T) {
	z := NewZScore(Params{Interval: time.Second, MinWarmup: 60 * time.Second,
		MuHalfLife: time.Minute, SigmaHalfLife: time.Hour, SigmaFloor: 0.5})
	start := time.Unix(1700000000, 0)
	for i := 1; i <= 10; i++ {
		z.Update(start.Add(time.Duration(i)*time.Second), 1.0)
	}
	if ReadyAt(z, start.Add(10*time.Second)) {
		t.Fatal("ready before min_warmup elapsed")
	}
	z.Update(start.Add(70*time.Second), 1.0)
	if !ReadyAt(z, start.Add(70*time.Second)) {
		t.Fatal("not ready after min_warmup with a fresh sample")
	}
	if ReadyAt(z, start.Add(70*time.Second+10*time.Second)) {
		t.Fatal("ready despite a stale last sample")
	}
}

func TestZScoreFactoryValidation(t *testing.T) {
	if _, err := New("zscore", Params{MuHalfLife: time.Minute, SigmaHalfLife: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := New("zscore", Params{}); err == nil {
		t.Fatal("missing half-lives must be rejected")
	}
}
