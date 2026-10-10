package estimator

import (
	"math"
	"testing"
	"time"
)

func TestSMAMeanAndWindow(t *testing.T) {
	s := NewSMA(Params{Window: 10 * time.Second, MinWarmup: 5 * time.Second, MinCoverage: 0.8, Interval: time.Second})
	t0 := time.Unix(1_000_000, 0)
	for i := 0; i < 10; i++ {
		s.Update(t0.Add(time.Duration(i)*time.Second), float64(i))
	}
	if m := s.Mean(); math.Abs(m-4.5) > 1e-9 {
		t.Fatalf("mean=%v", m)
	}
	// slide: add 10..14, window keeps last 10 => 5..14 mean 9.5
	for i := 10; i < 15; i++ {
		s.Update(t0.Add(time.Duration(i)*time.Second), float64(i))
	}
	if m := s.Mean(); math.Abs(m-9.5) > 1e-9 {
		t.Fatalf("slid mean=%v", m)
	}
	if !s.Ready() {
		t.Fatalf("should be ready: %+v", s.Stats())
	}
}

func TestSMAReadinessCoverage(t *testing.T) {
	s := NewSMA(Params{Window: 100 * time.Second, MinWarmup: 20 * time.Second, MinCoverage: 0.8, Interval: time.Second})
	t0 := time.Unix(2_000_000, 0)
	for i := 0; i < 30; i += 2 { // 50% coverage
		s.Update(t0.Add(time.Duration(i)*time.Second), 1)
	}
	if s.Ready() {
		t.Fatalf("50%% coverage must not be ready: %+v", s.Stats())
	}
	s2 := NewSMA(Params{Window: 100 * time.Second, MinWarmup: 20 * time.Second, MinCoverage: 0.8, Interval: time.Second})
	for i := 0; i < 10; i++ {
		s2.Update(t0.Add(time.Duration(i)*time.Second), 1)
	}
	if s2.Ready() {
		t.Fatal("span 10s < warmup 20s must not be ready")
	}
}

func TestSMASameBucketOverwrite(t *testing.T) {
	s := NewSMA(Params{Window: 10 * time.Second, Interval: time.Second})
	t0 := time.Unix(3_000_000, 0)
	s.Update(t0, 1)
	s.Update(t0.Add(300*time.Millisecond), 3)
	if s.Mean() != 3 || s.Stats().Samples != 1 {
		t.Fatalf("%+v", s.Stats())
	}
}
