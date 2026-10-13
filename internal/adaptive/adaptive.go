// Package adaptive tunes a trading coefficient (e.g. the IOC allowance tick_k)
// from observed outcomes using AIMD: widen on too many one-leg failures,
// tighten on too few, so the coefficient tracks the market instead of staying
// a fixed config value.
package adaptive

// Config of one adaptively tuned coefficient.
type Config struct {
	Init   float64 // starting value (cold start)
	Window int     // number of outcomes before adjusting
	High   float64 // failure rate above which the coefficient widens
	Low    float64 // failure rate below which the coefficient tightens
	Up     float64 // widen factor (>1)
	Down   float64 // tighten factor (<1)
	Min    float64
	Max    float64
}

// K is one adaptively tuned coefficient. It is owned by a single goroutine
// (the pair actor), so it needs no locking.
type K struct {
	cfg  Config
	val  float64
	hist []bool
	idx  int
}

// NewK builds a coefficient starting at cfg.Init, clamped to [Min, Max].
func NewK(cfg Config) *K {
	k := &K{cfg: cfg, val: cfg.Init}
	if k.val < cfg.Min {
		k.val = cfg.Min
	}
	if k.val > cfg.Max {
		k.val = cfg.Max
	}
	return k
}

// K returns the current coefficient.
func (k *K) K() float64 { return k.val }

// Observe records one outcome; failed=true means the allowance was too tight
// (one leg missed). The coefficient adjusts once the window is full.
func (k *K) Observe(failed bool) {
	if len(k.hist) < k.cfg.Window {
		k.hist = append(k.hist, failed)
	} else {
		k.hist[k.idx] = failed
		k.idx = (k.idx + 1) % k.cfg.Window
	}
	if len(k.hist) < k.cfg.Window {
		return
	}
	rate := k.Rate()
	switch {
	case rate > k.cfg.High:
		k.val = clamp(k.val*k.cfg.Up, k.cfg.Min, k.cfg.Max)
	case rate < k.cfg.Low:
		k.val = clamp(k.val*k.cfg.Down, k.cfg.Min, k.cfg.Max)
	}
}

// Rate returns the observed failure rate (0 before the window is full).
func (k *K) Rate() float64 {
	if len(k.hist) == 0 {
		return 0
	}
	fails := 0
	for _, b := range k.hist {
		if b {
			fails++
		}
	}
	return float64(fails) / float64(len(k.hist))
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
