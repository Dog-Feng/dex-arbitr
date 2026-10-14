package scan

import (
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func TestHealthUsesRollingDepthAndReversions(t *testing.T) {
	h := NewHealthTracker(10)
	q := spread.Quote{L: book(100, 100.1, 10, 10), R: book(100, 100.1, 10, 10)}
	start := time.Unix(1000, 0)
	for i := 0; i < 10; i++ {
		q.Mid = float64(i % 2)
		h.AddAt(start.Add(time.Duration(i)*time.Second), q, 1, 1, 0)
	}
	q.L = book(100, 100.1, 0.1, 0.1)
	q.R = q.L
	var health Health
	for i := 10; i < 30; i++ {
		q.Mid = float64(i)
		health = h.AddAt(start.Add(time.Duration(i)*time.Second), q, 1, 1, 0)
	}
	if health.RevertsPerMin != 0 || health.DepthQuality != 0 || health.Score != 0 {
		t.Fatalf("old depth/flips leaked into window: %+v", health)
	}
}

func TestHealthMonotoneAndPlateauDoNotCreateReversions(t *testing.T) {
	for _, xs := range [][]float64{{0, 1, 2}, {0, 1, 1, 2, 2, 3}, {3, 2, 2, 1, 1, 0}} {
		h := NewHealthTracker(10)
		q := spread.Quote{L: book(100, 100.1, 10, 10), R: book(100, 100.1, 10, 10)}
		var health Health
		for _, x := range xs {
			q.Mid = x
			health = h.Add(q, 1, 1, 0)
		}
		if health.RevertsPerMin != 0 || health.Score != 0 {
			t.Fatalf("monotone sequence %v produced %+v", xs, health)
		}
	}
}

func TestHealthExpiresWithoutMutatingTracker(t *testing.T) {
	h := NewHealthTracker(10)
	q := spread.Quote{L: book(100, 100.1, 10, 10), R: book(100, 100.1, 10, 10)}
	start := time.Unix(1000, 0)
	for i, x := range []float64{0, 1, 0} {
		q.Mid = x
		h.AddAt(start.Add(time.Duration(i)*time.Second), q, 1, 1, 0)
	}
	if st := h.SnapshotAt(start.Add(time.Minute), 0); st.Score != 0 || !st.UpdatedAt.IsZero() {
		t.Fatalf("expired=%+v", st)
	}
	if st := h.SnapshotAt(start.Add(2*time.Second), 0); st.Score <= 0 || st.UpdatedAt.IsZero() {
		t.Fatalf("read mutated historical health: %+v", st)
	}
}
