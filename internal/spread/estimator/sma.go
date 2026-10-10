package estimator

import (
	"time"
)

func init() {
	Register("sma", func(p Params) (Estimator, error) { return NewSMA(p), nil })
}

// SMA is a time-bucketed simple moving average: one slot per Interval,
// ring buffer + running sum, O(1) per update.
type SMA struct {
	p      Params
	slots  []slot
	sum    float64
	n      int
	first  int64 // first bucket observed (for span)
	lastBk int64
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
	return &SMA{p: p, slots: make([]slot, size), first: -1}
}

func (s *SMA) bucket(t time.Time) int64 { return t.UnixNano() / int64(s.p.Interval) }

// Update adds a sample; at most one sample per bucket (last write wins).
func (s *SMA) Update(t time.Time, x float64) {
	bk := s.bucket(t)
	s.evict(bk)
	i := int(bk % int64(len(s.slots)))
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
	if s.first < 0 || s.first <= bk-int64(len(s.slots)) {
		s.first = s.oldest(bk)
	}
	s.lastBk = bk
}

// evict drops slots older than the window relative to bucket bk.
func (s *SMA) evict(bk int64) {
	lim := bk - int64(len(s.slots))
	if s.n == 0 {
		return
	}
	for i := range s.slots {
		sl := &s.slots[i]
		if sl.ok && sl.bucket <= lim {
			s.sum -= sl.val
			s.n--
			sl.ok = false
		}
	}
}

func (s *SMA) oldest(bk int64) int64 {
	min := bk
	for _, sl := range s.slots {
		if sl.ok && sl.bucket < min {
			min = sl.bucket
		}
	}
	return min
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
	st := Stats{Kind: "sma", Mean: s.Mean(), Samples: s.n}
	if s.n == 0 {
		return st
	}
	spanBk := s.lastBk - s.first + 1
	st.Span = time.Duration(spanBk) * s.p.Interval
	st.Coverage = float64(s.n) / float64(spanBk)
	st.Ready = st.Span >= s.p.MinWarmup && st.Coverage >= s.p.MinCoverage
	return st
}
