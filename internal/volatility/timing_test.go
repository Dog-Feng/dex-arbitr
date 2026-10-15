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
	if !e.ReadyAt(start.Add(time.Hour)) || math.Abs(e.Vol()-math.Sqrt(5)) > 1e-12 {
		t.Fatalf("ready=%t sigma=%v want sqrt(5)", e.Ready(), e.Vol())
	}
}

func TestReadinessIsCountBasedAndIgnoresGaps(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	start := time.Unix(1000, 0)
	// Six samples spaced hours apart still warm the estimator: readiness
	// counts increments, never real time.
	for i := 0; i <= 5; i++ {
		e.AddAt(start.Add(time.Duration(i)*time.Hour), float64(i)*1000)
	}
	if !e.Ready() || e.Vol() == 0 {
		t.Fatalf("count-based readiness failed: ready=%t sigma=%v", e.Ready(), e.Vol())
	}
	// Readiness and sigma remain available arbitrarily long after the last
	// sample, matching the original count-based semantics.
	if !e.ReadyAt(start.Add(100*time.Hour)) || e.VolAt(start.Add(100*time.Hour)) != e.Vol() {
		t.Fatal("sigma expired despite count-based readiness")
	}
}

func TestGapProducesReturnAndRaisesSigma(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	for i := 0; i <= 5; i++ {
		e.AddAt(time.Unix(int64(i), 0), float64(i))
	}
	before := e.Vol()
	// A far-apart, large sample is an ordinary return: it enters the ring and
	// inflates sigma. The opening threshold is what then blocks trading.
	e.AddAt(time.Unix(1000, 0), 10000)
	if e.Vol() <= before {
		t.Fatalf("gap sample did not raise sigma: %v -> %v", before, e.Vol())
	}
	if !e.Ready() {
		t.Fatal("gap invalidated a warmed estimator")
	}
}

func TestRealizedVolUsesTrailingHorizon(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 400 * time.Millisecond, HalfLife: time.Minute})
	// Four consecutive +1 returns fill the four-slot horizon exactly.
	for i := 0; i <= 4; i++ {
		e.Add(float64(i))
	}
	if !e.Ready() || e.Vol() != 2 {
		t.Fatalf("ready=%t sigma=%v want 2", e.Ready(), e.Vol())
	}
}

func TestInvalidSamplesDoNotMutate(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	for i := 0; i <= 5; i++ {
		e.Add(float64(i))
	}
	before := e.Vol()
	e.AddAt(time.Unix(1, 0), math.NaN())
	e.AddAt(time.Unix(2, 0), math.Inf(1))
	if e.Vol() != before {
		t.Fatal("invalid sample mutated the estimator")
	}
	e.Reset()
	if e.Ready() || e.Vol() != 0 {
		t.Fatal("reset retained state")
	}
}

func TestSetWindowWidensAndShrinksWithoutReset(t *testing.T) {
	e := New(Params{Interval: 100 * time.Millisecond, Horizon: 200 * time.Millisecond, HalfLife: time.Minute, MaxHorizon: 2 * time.Second})
	for i := 0; i <= 4; i++ {
		e.Add(float64(i))
	}
	if !e.Ready() {
		t.Fatal("warmup failed")
	}
	before := e.Vol()
	// Widening beyond the stored history keeps the chain; the window uses the
	// history that exists and grows as samples arrive.
	e.SetWindow(5)
	if !e.Ready() {
		t.Fatal("window change restarted the warmup chain")
	}
	if e.Vol() != before {
		t.Fatal("window change mutated sigma without new samples")
	}
	e.Add(5)
	if e.Vol() <= before {
		t.Fatal("widened window did not include the new increment")
	}
	// Shrinking below the warmup size still leaves the estimator ready.
	e.SetWindow(2)
	if !e.Ready() || e.Window() != 2 {
		t.Fatalf("shrink failed: ready=%t window=%d", e.Ready(), e.Window())
	}
}
