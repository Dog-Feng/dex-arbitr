package scan

import (
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func book(bid, ask, bidSize, askSize float64) domain.Book {
	return domain.Book{
		Bids: []domain.Level{{Price: bid, Size: bidSize}},
		Asks: []domain.Level{{Price: ask, Size: askSize}},
	}
}

func TestHealthTrackerDepthQuality(t *testing.T) {
	cases := []struct {
		name  string
		sizes [4]float64
		need  float64
		want  float64
	}{
		{"all deep", [4]float64{2, 2, 2, 2}, 1, 1},
		{"one leg thin", [4]float64{2, 2, 0.1, 2}, 1, 0},
		{"no depth samples", [4]float64{2, 2, 2, 2}, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealthTracker(10)
			q := spread.Quote{L: book(100, 100.1, tc.sizes[0], tc.sizes[1]), R: book(100.05, 100.15, tc.sizes[2], tc.sizes[3])}
			got := h.Add(q, 1, tc.need, 0)
			if got.DepthQuality != tc.want {
				t.Fatalf("DepthQuality=%v, want %v", got.DepthQuality, tc.want)
			}
		})
	}
}

func TestHealthTrackerTickQuality(t *testing.T) {
	cases := []struct {
		name     string
		sigmaBps float64
		tickBps  float64
		want     float64
	}{
		{"plenty of ticks", 2, 0.06, 1},
		{"too few ticks", 0.5, 0.48, 0.208333},
		{"no tick constraint", 2, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHealthTracker(10)
			q := spread.Quote{L: book(100, 100.1, 1, 1), R: book(100.05, 100.15, 1, 1)}
			got := h.Add(q, tc.sigmaBps, 0.1, tc.tickBps)
			if diff := got.TickQuality - tc.want; diff < -1e-4 || diff > 1e-4 {
				t.Fatalf("TickQuality=%v, want %v", got.TickQuality, tc.want)
			}
		})
	}
}

func TestHealthTrackerRevertsAndScore(t *testing.T) {
	h := NewHealthTracker(10)
	q := spread.Quote{L: book(100, 100.1, 1, 1), R: book(100.05, 100.15, 1, 1)}
	mids := []float64{1, 2, 1, 2, 1, 2, 1, 2, 1, 2}
	var last Health
	for i, m := range mids {
		q.Mid = m
		last = h.Add(q, 1.5, 0.1, 0)
		_ = i
	}
	if last.RevertsPerMin <= 0 {
		t.Fatalf("expected oscillation to produce flips, got RevertsPerMin=%v", last.RevertsPerMin)
	}
	if last.Score <= 0 {
		t.Fatalf("expected positive score, got %v", last.Score)
	}
	if last.SigmaBps != 1.5 {
		t.Fatalf("SigmaBps=%v, want 1.5", last.SigmaBps)
	}
}
