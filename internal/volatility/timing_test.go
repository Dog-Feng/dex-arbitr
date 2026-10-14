package volatility

import (
	"math"
	"testing"
	"time"
)

func TestReadyVolSeedsFullHorizonVariance(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	start := time.Unix(1000, 0)
	for i := 0; i <= 5; i++ {
		e.AddAt(start.Add(time.Duration(i)*100*time.Millisecond), float64(i))
	}
	if !e.ReadyAt(start.Add(500*time.Millisecond)) || math.Abs(e.Vol()-math.Sqrt(5)) > 1e-12 {
		t.Fatalf("ready=%t sigma=%v want sqrt(5)", e.Ready(), e.Vol())
	}
}

func TestSamplingGapResetsReturnHistory(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	start := time.Unix(1000, 0)
	for i := 0; i <= 5; i++ {
		e.AddAt(start.Add(time.Duration(i)*100*time.Millisecond), float64(i))
	}
	if e.ReadyAt(start.Add(time.Second)) || e.VolAt(start.Add(time.Second)) != 0 {
		t.Fatal("expired sigma remains live")
	}
	if !e.ReadyAt(start.Add(500 * time.Millisecond)) {
		t.Fatal("expiry read must not mutate history")
	}
	restart := start.Add(time.Hour)
	if e.AddAt(restart, 10000) || e.Ready() || e.Vol() != 0 {
		t.Fatal("long gap must restart warmup")
	}
	for i := 1; i <= 5; i++ {
		e.AddAt(restart.Add(time.Duration(i)*100*time.Millisecond), 10000+float64(i))
	}
	if math.Abs(e.Vol()-math.Sqrt(5)) > 1e-12 {
		t.Fatalf("disconnected price jump invented short return: %v", e.Vol())
	}
	e.Reset()
	if e.Ready() || e.Vol() != 0 {
		t.Fatal("reset retained state")
	}
}

func TestRealizedVolUsesActualTimeWindow(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	start := time.Unix(1000, 0)
	// Four valid 125ms intervals span exactly 500ms, not five sample counts.
	for i := 0; i <= 4; i++ {
		e.AddAt(start.Add(time.Duration(i)*125*time.Millisecond), float64(i))
	}
	if !e.Ready() || e.Vol() != 2 {
		t.Fatalf("ready=%t sigma=%v want 2", e.Ready(), e.Vol())
	}
	before := e.Vol()
	e.AddAt(start, 10000)
	e.AddAt(start.Add(time.Second), math.NaN())
	if e.Vol() != before {
		t.Fatal("invalid sample changed estimator")
	}
}
