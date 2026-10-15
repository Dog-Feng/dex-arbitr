package estimator

import (
	"math"
	"time"
)

func init() {
	Register("sma", func(p Params) (Estimator, error) { return NewSMA(p), nil })
}

// SMA is a time-bucketed simple moving average: one slot per Interval,
// ring buffer + running sum, O(1) per update.
type SMA struct {
	p       Params
	slots   []slot
	sum     float64
	n       int
	first   int64 // first bucket observed (for span)
	lastBk  int64
	last    time.Time
	started time.Time // actual first sample of this window history, for warmup
}

type slot struct {
	bucket int64
	val    float64
	ok     bool
}

// NewSMA builds an SMA estimator.
func NewSMA(p Params) *SMA {
	if p.Interval <= 0 {
		p.Interval = time.Second
	}
	if p.Window <= 0 {
		p.Window = 2 * time.Hour
	}
	if p.MinCoverage <= 0 {
		p.MinCoverage = 0.8
	}
	size := int(p.Window / p.Interval)
	if size < 1 {
		size = 1
	}
	return &SMA{p: p, slots: make([]slot, size)}
}

func (s *SMA) bucket(t time.Time) int64 { return t.UnixNano() / int64(s.p.Interval) }

// Update adds a sample; at most one sample per bucket (last write wins).
func (s *SMA) Update(t time.Time, x float64) {
	if t.IsZero() || math.IsNaN(x) || math.IsInf(x, 0) || (!s.last.IsZero() && t.Before(s.last)) {
		return
	}
	bk := s.bucket(t)
	s.evict(bk)
	if s.n == 0 {
		// A single-slot window replaces its only sample on every ordinary
		// tick. Preserve elapsed warmup across that replacement, including
		// ticker jitter of up to half an interval. Missing a whole sampling
		// tick, or emptying a multi-slot window, still starts a new history.
		continuous := len(s.slots) == 1 && !s.last.IsZero() && t.Sub(s.last)-s.p.Interval <= s.p.Interval/2
		if !continuous {
			s.started = t
		}
	}
	i := int(bk % int64(len(s.slots)))
	if i < 0 {
		i += len(s.slots)
	}
	sl := &s.slots[i]
	if sl.ok && sl.bucket == bk {
		s.sum += x - sl.val
		sl.val = x
	} else {
		if sl.ok { // stale slot (should be evicted already)
			s.sum -= sl.val
			s.n--
		}
		*sl = slot{bucket: bk, val: x, ok: true}
		s.sum += x
		s.n++
	}
	if s.n == 1 || s.first <= bk-int64(len(s.slots)) {
		s.first = s.oldest(bk)
	}
	s.lastBk = bk
	s.last = t
}

// evict drops slots older than the window relative to bucket bk.
func (s *SMA) evict(bk int64) {
	lim := bk - int64(len(s.slots))
	if s.n == 0 {
		return
	}
	// Only buckets crossed since the last update can have become stale. A
	// long gap costs one window scan; ordinary updates visit a single slot.
	count := bk - s.lastBk
	if count > int64(len(s.slots)) {
		count = int64(len(s.slots))
	}
	for offset := int64(1); offset <= count; offset++ {
		i := int((s.lastBk + offset) % int64(len(s.slots)))
		if i < 0 {
			i += len(s.slots)
		}
		sl := &s.slots[i]
		if sl.ok && sl.bucket <= lim {
			s.sum -= sl.val
			s.n--
			sl.ok = false
		}
	}
}

func (s *SMA) oldest(bk int64) int64 {
	first := s.first
	lim := bk - int64(len(s.slots)) + 1
	if first < lim {
		first = lim
	}
	for first <= bk {
		i := int(first % int64(len(s.slots)))
		if i < 0 {
			i += len(s.slots)
		}
		if sl := s.slots[i]; sl.ok && sl.bucket == first {
			return first
		}
		first++
	}
	return bk
}

// Mean of samples in window.
func (s *SMA) Mean() float64 {
	if s.n == 0 {
		return 0
	}
	return s.sum / float64(s.n)
}

// Ready: span >= MinWarmup and coverage >= MinCoverage.
func (s *SMA) Ready() bool { return s.Stats().Ready }

// Stats snapshot.
func (s *SMA) Stats() Stats {
	st := Stats{Kind: "sma", Mean: s.Mean(), Samples: s.n, LastSample: s.last}
	if s.n == 0 {
		return st
	}
	spanBk := s.lastBk - s.first + 1
	st.Span = time.Duration(spanBk) * s.p.Interval
	st.Coverage = float64(s.n) / float64(spanBk)
	st.Ready = s.last.Sub(s.started) >= s.p.MinWarmup && st.Span >= s.p.MinWarmup && st.Coverage >= s.p.MinCoverage
	return st
}

// StatsAt returns a pure snapshot relative to now, including missing sampling
// buckets. It never evicts or mutates state, so callers may use a read lock.
func (s *SMA) StatsAt(now time.Time) Stats {
	st := Stats{Kind: "sma", LastSample: s.last}
	if s.last.IsZero() || now.Before(s.last) {
		return st
	}
	bk := s.bucket(now)
	lim := bk - int64(len(s.slots))
	first := bk
	var sum float64
	for _, sl := range s.slots {
		if !sl.ok || sl.bucket <= lim || sl.bucket > bk {
			continue
		}
		st.Samples++
		sum += sl.val
		if sl.bucket < first {
			first = sl.bucket
		}
	}
	if st.Samples == 0 {
		return st
	}
	st.Mean = sum / float64(st.Samples)
	spanBk := bk - first + 1
	st.Span = time.Duration(spanBk) * s.p.Interval
	st.Coverage = float64(st.Samples) / float64(spanBk)
	st.Ready = now.Sub(s.last) <= 2*s.p.Interval && now.Sub(s.started) >= s.p.MinWarmup && st.Span >= s.p.MinWarmup && st.Coverage >= s.p.MinCoverage
	return st
}

func (s *SMA) ReadyAt(now time.Time) bool   { return s.StatsAt(now).Ready }
func (s *SMA) MeanAt(now time.Time) float64 { return s.StatsAt(now).Mean }
