package volatility

import (
	"math"
	"testing"
	"time"
)

func TestRealizedVolConstantSeries(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Hour})
	// a flat series produces zero returns, hence zero realized variance
	for i := 0; i <= 5; i++ {
		e.Add(0)
	}
	if !e.Ready() {
		t.Fatal("should be ready after a full horizon")
	}
	if e.Vol() != 0 {
		t.Fatalf("constant series should have zero vol, got %v", e.Vol())
	}
}

func TestRealizedVolRandomWalk(t *testing.T) {
	e := New(Params{Interval: time.Second, Horizon: 3 * time.Second, HalfLife: time.Second})
	// +1/-1 alternating steps => each 1s return is ±1, trailing sum of 3 = 3,
	// so the smoothed realized variance converges to 3 and sigma to sqrt(3).
	for i := 0; i < 40; i++ {
		e.Add(float64(i % 2))
	}
	if !e.Ready() {
		t.Fatal("should be ready")
	}
	if got := e.Vol(); math.Abs(got-math.Sqrt(3)) > 1e-3 {
		t.Fatalf("vol=%v, want sqrt(3)", got)
	}
}
