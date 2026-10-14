package scan

import (
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"math"
	"time"
)

// Health summarizes suitability for fast churn over a rolling time window.
type Health struct {
	SigmaBps      float64   `json:"sigma_bps"`
	RevertsPerMin float64   `json:"reverts_per_min"`
	DepthQuality  float64   `json:"depth_quality"`
	TickQuality   float64   `json:"tick_quality"`
	Stability     float64   `json:"stability"`
	Score         float64   `json:"score"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type healthSample struct {
	at               time.Time
	mid, sigma       float64
	depth, haveDepth bool
}

type HealthTracker struct {
	window  time.Duration
	samples []healthSample
}

// NewHealthTracker sets the window length in seconds, matching the normal 1s
// sampling cadence. AddAt keeps its time meaning if the cadence changes.
func NewHealthTracker(window int) *HealthTracker {
	if window < 2 {
		window = 2
	}
	return &HealthTracker{window: time.Duration(window) * time.Second}
}

func (h *HealthTracker) Add(q spread.Quote, sigma, need, tick float64) Health {
	at := time.Unix(0, 1)
	if len(h.samples) > 0 {
		at = h.samples[len(h.samples)-1].at.Add(time.Second)
	}
	return h.AddAt(at, q, sigma, need, tick)
}

// AddAt records one current sample. Equal spread values carry the previous
// direction; only two nonzero, opposing increments count as a reversion.
func (h *HealthTracker) AddAt(at time.Time, q spread.Quote, sigma, need, tick float64) Health {
	if at.IsZero() || math.IsNaN(q.Mid) || math.IsInf(q.Mid, 0) || math.IsNaN(sigma) || math.IsInf(sigma, 0) || sigma < 0 {
		return Health{}
	}
	if len(h.samples) > 0 && !at.After(h.samples[len(h.samples)-1].at) {
		return h.SnapshotAt(at, tick)
	}
	s := healthSample{at: at, mid: q.Mid, sigma: sigma, haveDepth: need > 0}
	if s.haveDepth && len(q.L.Bids) > 0 && len(q.L.Asks) > 0 && len(q.R.Bids) > 0 && len(q.R.Asks) > 0 {
		s.depth = min4(q.L.Bids[0].Size, q.L.Asks[0].Size, q.R.Bids[0].Size, q.R.Asks[0].Size)+1e-12 >= need
	}
	h.samples = append(h.samples, s)
	cutoff := at.Add(-h.window)
	first := 0
	for first < len(h.samples) && !h.samples[first].at.After(cutoff) {
		first++
	}
	h.samples = h.samples[first:]
	return h.SnapshotAt(at, tick)
}

// SnapshotAt is a pure read. A stopped collector cannot leave a live score.
func (h *HealthTracker) SnapshotAt(now time.Time, tick float64) Health {
	out := Health{}
	if len(h.samples) == 0 {
		return out
	}
	last := h.samples[len(h.samples)-1]
	if now.Before(last.at) || now.Sub(last.at) > 2*time.Second {
		return out
	}
	out.SigmaBps, out.UpdatedAt = last.sigma, last.at
	cutoff := now.Add(-h.window)
	n, depthN, depthOK, flips := 0, 0, 0, 0
	var first time.Time
	var prev, sum, squares float64
	direction := 0
	for _, s := range h.samples {
		if !s.at.After(cutoff) || s.at.After(now) {
			continue
		}
		if n == 0 {
			first = s.at
		} else {
			d := 0
			if s.mid > prev {
				d = 1
			} else if s.mid < prev {
				d = -1
			}
			if d != 0 {
				if direction != 0 && d != direction {
					flips++
				}
				direction = d
			}
		}
		prev = s.mid
		n++
		if s.haveDepth {
			depthN++
			if s.depth {
				depthOK++
			}
		}
		sum += s.sigma
		squares += s.sigma * s.sigma
	}
	if n >= 2 {
		seconds := last.at.Sub(first).Seconds()
		if seconds > 0 {
			out.RevertsPerMin = float64(flips) * 60 / seconds
		}
	}
	if depthN > 0 {
		out.DepthQuality = float64(depthOK) / float64(depthN)
	}
	if tick > 0 && last.sigma > 0 {
		out.TickQuality = clamp01(last.sigma / tick / 5)
	} else {
		out.TickQuality = 1
	}
	if n >= 2 {
		mean := sum / float64(n)
		variance := math.Max(0, squares/float64(n)-mean*mean)
		if mean > 0 {
			out.Stability = clamp01(mean / (mean + 2*math.Sqrt(variance)))
		}
	}
	out.Score = out.SigmaBps * out.DepthQuality * out.TickQuality * out.Stability * clamp01(out.RevertsPerMin/10)
	return out
}

func min4(a, b, c, d float64) float64 {
	return math.Min(math.Min(a, b), math.Min(c, d))
}
func clamp01(v float64) float64 {
	return math.Max(0, math.Min(1, v))
}
