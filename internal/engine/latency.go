package engine

import (
	"math"
	"time"
)

// adaptiveLatencyCfg configures the runtime volatility-window adaptation.
// Latency statistics drive the per-leg windows and the spread window, which
// is the larger of the two legs because the hedge only completes when the
// slower leg fills.
type adaptiveLatencyCfg struct {
	enabled        bool
	minSamples     int
	ringN          int
	windowMin      time.Duration
	windowMax      time.Duration
	coverageK      float64
	updateInterval time.Duration
}

// latencySeries keeps a fixed-size ring of recent per-leg fill latencies.
// It is owned by the pair actor goroutine and never cleared.
type latencySeries struct {
	ring []time.Duration
	idx  int
	cnt  int
}

func newLatencySeries(n int) *latencySeries {
	if n < 2 {
		n = 2
	}
	return &latencySeries{ring: make([]time.Duration, n)}
}

func (s *latencySeries) add(d time.Duration) {
	if d <= 0 {
		return
	}
	s.ring[s.idx] = d
	s.idx = (s.idx + 1) % len(s.ring)
	if s.cnt < len(s.ring) {
		s.cnt++
	}
}

// stats reports the sample mean and standard deviation of the stored ring.
// ok is false while fewer than two samples exist.
func (s *latencySeries) stats() (mean, std time.Duration, ok bool) {
	if s.cnt < 2 {
		return 0, 0, false
	}
	var sum float64
	for _, d := range s.ring[:s.cnt] {
		sum += float64(d)
	}
	m := sum / float64(s.cnt)
	var sq float64
	for _, d := range s.ring[:s.cnt] {
		diff := float64(d) - m
		sq += diff * diff
	}
	return time.Duration(m), time.Duration(math.Sqrt(sq / float64(s.cnt-1))), true
}

// targetWindowSlots derives a window size in sampling intervals from one
// leg's latency series: mean plus coverageK one-sided standard deviations,
// clamped to the configured bounds and rounded to whole intervals. ok is
// false while the series has fewer than minSamples samples (cold start).
func targetWindowSlots(s *latencySeries, cfg adaptiveLatencyCfg, interval time.Duration) (int, bool) {
	if !cfg.enabled || s.cnt < cfg.minSamples {
		return 0, false
	}
	mean, std, ok := s.stats()
	if !ok {
		return 0, false
	}
	target := mean + time.Duration(cfg.coverageK*float64(std))
	if target < cfg.windowMin {
		target = cfg.windowMin
	}
	if target > cfg.windowMax {
		target = cfg.windowMax
	}
	slots := int(math.Round(float64(target) / float64(interval)))
	if slots < 1 {
		slots = 1
	}
	return slots, true
}
