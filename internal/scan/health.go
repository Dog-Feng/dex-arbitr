package scan

import (
	"math"

	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

// Health summarizes how suitable a pair is for fast churn volume. Every field
// except Score is an input into Score, which is a multiplicative quality index:
// a zero in any component zeroes the pair out of contention.
type Health struct {
	SigmaBps      float64 `json:"sigma_bps"`       // fast spread volatility (bps)
	RevertsPerMin float64 `json:"reverts_per_min"` // direction flips per minute
	DepthQuality  float64 `json:"depth_quality"`   // [0,1] fraction with depth >= need
	TickQuality   float64 `json:"tick_quality"`    // [0,1] sigma expressed in price ticks
	Stability     float64 `json:"stability"`       // [0,1] inverse of sigma variation
	Score         float64 `json:"score"`
}

// HealthTracker accumulates rolling churn health from 1s spread samples. It is
// read-only market analysis: it holds no orders and imports no trading engine.
type HealthTracker struct {
	window   int
	mid      []float64
	idx, n   int
	prev     float64
	prevUp   bool
	havePrev bool
	flips    int
	depthOK  int
	depthN   int
	sigmas   []float64
	sIdx, sN int
	sigmaSum float64
	sigmaSq  float64
}

// NewHealthTracker builds a tracker over a 1s-sample rolling window.
func NewHealthTracker(window int) *HealthTracker {
	if window < 2 {
		window = 2
	}
	return &HealthTracker{window: window, mid: make([]float64, window), sigmas: make([]float64, window)}
}

// Add feeds one sample: the spread quote, the current fast spread sigma (bps),
// the quantity the strategy wants to trade (for the depth check), and one
// price tick expressed in bps (0 disables the tick-quality penalty). It
// returns the current Health snapshot.
func (h *HealthTracker) Add(q spread.Quote, sigmaBps, needQty, tickBps float64) Health {
	h.recordMid(q.Mid)
	h.recordDepth(q, needQty)
	h.recordSigma(sigmaBps)
	return h.snapshot(sigmaBps, tickBps)
}

func (h *HealthTracker) recordMid(mid float64) {
	if h.havePrev {
		up := mid > h.prev
		if up != h.prevUp {
			h.flips++
		}
		h.prevUp = up
	}
	h.prev, h.havePrev = mid, true
	if h.n == h.window {
		h.mid[h.idx] = mid
		h.idx = (h.idx + 1) % h.window
	} else {
		h.mid[h.n] = mid
		h.n++
	}
}

func (h *HealthTracker) recordDepth(q spread.Quote, needQty float64) {
	if needQty <= 0 {
		return
	}
	h.depthN++
	if min4(q.L.Bids[0].Size, q.L.Asks[0].Size, q.R.Bids[0].Size, q.R.Asks[0].Size)+1e-12 >= needQty {
		h.depthOK++
	}
}

func (h *HealthTracker) recordSigma(sigmaBps float64) {
	if sigmaBps <= 0 {
		return
	}
	old := h.sigmas[h.sIdx]
	if h.sN == h.window {
		h.sigmaSum += sigmaBps - old
		h.sigmaSq += sigmaBps*sigmaBps - old*old
	} else {
		h.sigmaSum += sigmaBps
		h.sigmaSq += sigmaBps * sigmaBps
		h.sN++
	}
	h.sigmas[h.sIdx] = sigmaBps
	h.sIdx = (h.sIdx + 1) % h.window
}

func (h *HealthTracker) snapshot(sigmaBps, tickBps float64) Health {
	out := Health{SigmaBps: sigmaBps}
	if h.n >= 2 {
		out.RevertsPerMin = float64(h.flips) / (float64(h.n) / 60)
	}
	if h.depthN > 0 {
		out.DepthQuality = float64(h.depthOK) / float64(h.depthN)
	}
	if tickBps > 0 && sigmaBps > 0 {
		out.TickQuality = clamp01(sigmaBps / tickBps / 5)
	} else {
		out.TickQuality = 1
	}
	if h.sN >= 2 {
		mean := h.sigmaSum / float64(h.sN)
		variance := h.sigmaSq/float64(h.sN) - mean*mean
		if variance > 0 {
			out.Stability = clamp01(mean / (mean + 2*math.Sqrt(variance)))
		} else {
			out.Stability = 1
		}
	}
	freq := clamp01(out.RevertsPerMin / 10)
	out.Score = out.SigmaBps * out.DepthQuality * out.TickQuality * out.Stability * freq
	return out
}

func min4(a, b, c, d float64) float64 {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	if d < m {
		m = d
	}
	return m
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}
