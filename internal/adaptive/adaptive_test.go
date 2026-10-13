package adaptive

import (
	"math"
	"testing"
)

func TestKWidens(t *testing.T) {
	k := NewK(Config{Init: 2.5, Window: 10, High: 0.1, Low: 0.02, Up: 1.3, Down: 0.95, Min: 0.5, Max: 10})
	// 3 failures out of 10 = 30% > high => widen once
	for i := 0; i < 10; i++ {
		k.Observe(i < 3)
	}
	if math.Abs(k.K()-2.5*1.3) > 1e-9 {
		t.Fatalf("widened K=%v, want %v", k.K(), 2.5*1.3)
	}
}

func TestKTightens(t *testing.T) {
	k := NewK(Config{Init: 2.5, Window: 10, High: 0.1, Low: 0.02, Up: 1.3, Down: 0.95, Min: 0.5, Max: 10})
	// 0 failures out of 10 => tighten once
	for i := 0; i < 10; i++ {
		k.Observe(false)
	}
	if math.Abs(k.K()-2.5*0.95) > 1e-9 {
		t.Fatalf("tightened K=%v, want %v", k.K(), 2.5*0.95)
	}
}

func TestKClamps(t *testing.T) {
	k := NewK(Config{Init: 1, Window: 5, High: 0.1, Low: 0.02, Up: 2, Down: 0.5, Min: 0.5, Max: 2})
	for i := 0; i < 5; i++ {
		k.Observe(true)
	}
	if k.K() != 2 {
		t.Fatalf("clamped K=%v, want 2", k.K())
	}
}
