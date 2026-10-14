package estimator

import (
	"math"
	"testing"
	"time"
)

func TestStatsAtExpiresWithoutUpdateAndDoesNotMutate(t *testing.T) {
	s := NewSMA(Params{Window: 10 * time.Second, MinWarmup: 5 * time.Second, MinCoverage: 0.8, Interval: time.Second})
	start := time.Unix(1000, 0)
	for i := 0; i < 10; i++ {
		s.Update(start.Add(time.Duration(i)*time.Second), float64(i))
	}
	before := s.Stats()
	if !s.StatsAt(start.Add(9 * time.Second)).Ready {
		t.Fatal("historical evaluation clock must work")
	}
	expired := StatsAt(s, start.Add(13*time.Second))
	if expired.Ready || expired.Samples != 6 || math.Abs(expired.Coverage-0.6) > 1e-12 || expired.Mean != 6.5 {
		t.Fatalf("expired=%+v", expired)
	}
	empty := s.StatsAt(start.Add(time.Hour))
	if empty.Ready || empty.Samples != 0 || empty.Mean != 0 {
		t.Fatalf("empty=%+v", empty)
	}
	if after := s.Stats(); after != before {
		t.Fatalf("read mutated stats: before=%+v after=%+v", before, after)
	}
	if s.StatsAt(start.Add(8 * time.Second)).Ready {
		t.Fatal("evaluation before latest sample must reject future observations")
	}
}

func TestSMARejectsInvalidAndOutOfOrderSamples(t *testing.T) {
	s := NewSMA(Params{Window: 5 * time.Second, Interval: time.Second})
	start := time.Unix(1000, 0)
	s.Update(start, 2)
	for _, tc := range []struct {
		at time.Time
		x  float64
	}{{start.Add(-time.Second), 3}, {start.Add(time.Second), math.NaN()}, {start.Add(time.Second), math.Inf(1)}, {time.Time{}, 3}} {
		s.Update(tc.at, tc.x)
	}
	if st := s.Stats(); st.Samples != 1 || st.Mean != 2 || !st.LastSample.Equal(start) {
		t.Fatalf("stats=%+v", st)
	}
	s.Update(start.Add(time.Hour), 4)
	if st := s.Stats(); st.Samples != 1 || st.Mean != 4 || st.Span != time.Second {
		t.Fatalf("gap must evict window: %+v", st)
	}
}

func TestSMAAcceptsPreEpochTimestamps(t *testing.T) {
	s := NewSMA(Params{Window: 5 * time.Second, Interval: time.Second})
	start := time.Unix(-10, 0)
	for i := 0; i < 10; i++ {
		s.Update(start.Add(time.Duration(i)*time.Second), float64(i))
	}
	if st := s.StatsAt(start.Add(9 * time.Second)); st.Samples != 5 || st.Mean != 7 {
		t.Fatalf("pre-epoch stats=%+v", st)
	}
}
