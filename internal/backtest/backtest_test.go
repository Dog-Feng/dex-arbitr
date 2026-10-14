package backtest

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

func replayParams() Params {
	return Params{StepQty: 1, MaxSteps: 1, OpenBps: 1, CloseBps: 1, Estimator: estimator.Params{Interval: time.Second, Window: 10 * time.Second, MinWarmup: 2 * time.Second, MinCoverage: 0.8}}
}

func legacySeries(n int) []Sample {
	samples := make([]Sample, n)
	for i := range samples {
		ls := 3.0
		if (i/2)%2 == 1 {
			ls = -3
		}
		samples[i] = Sample{Ts: time.Unix(1000, 0).Add(time.Duration(i) * 100 * time.Millisecond).UnixNano(), Mid: ls, LS: ls, SL: -ls}
	}
	return samples
}

func pricedSample(at time.Time, lbid, lask, rbid, rask, size float64) Sample {
	l := domain.Book{Ts: at, Bids: []domain.Level{{Price: lbid, Size: size}}, Asks: []domain.Level{{Price: lask, Size: size}}}
	r := domain.Book{Ts: at, Bids: []domain.Level{{Price: rbid, Size: size}}, Asks: []domain.Level{{Price: rask, Size: size}}}
	q, ok := spread.Compute(l, r)
	if !ok {
		panic("invalid test books")
	}
	return Sample{Ts: at.UnixNano(), Mid: q.Mid, LS: q.LS, SL: q.SL, RefMid: q.RefMid, L: l, R: r}
}

func TestReplayCapturesOscillation(t *testing.T) {
	r, err := ReplayChecked(legacySeries(100), replayParams())
	if err != nil {
		t.Fatal(err)
	}
	if r.RoundTrips == 0 || r.NetBps <= 0 || !r.Approximate {
		t.Fatalf("expected profitable approximate proxy: %+v", r)
	}
}

func TestReplayRequiresActualWarmupDuration(t *testing.T) {
	r, err := ReplayChecked(legacySeries(20), replayParams()) // 1.9s, below the configured 2s
	if err != nil {
		t.Fatal(err)
	}
	if r.MeanReady || r.Opens != 0 {
		t.Fatalf("replay traded before elapsed warmup: %+v", r)
	}
}

func TestReplayPreservesInventoryAndDollarLossAcrossPriceDrift(t *testing.T) {
	start := time.Unix(1000, 0)
	var samples []Sample
	for i := 0; i <= 20; i++ {
		samples = append(samples, pricedSample(start.Add(time.Duration(i)*100*time.Millisecond), 100, 100.02, 100, 100.02, 100))
	}
	entry := pricedSample(start.Add(2200*time.Millisecond), 99.98, 100, 102, 102.02, 100)
	exit := pricedSample(start.Add(2400*time.Millisecond), 200, 200.02, 202.98, 203, 100)
	samples = append(samples, pricedSample(start.Add(2100*time.Millisecond), 100, 100.02, 100, 100.02, 100), entry, pricedSample(start.Add(2300*time.Millisecond), 99.98, 100, 102, 102.02, 100), exit)
	p := replayParams()
	r, err := ReplayChecked(samples, p)
	if err != nil {
		t.Fatal(err)
	}
	if r.RoundTrips != 0 || r.EndInventory != 1 || !r.MTMValid || r.Approximate || math.Abs(r.MTMUSD+1) > 1e-9 {
		t.Fatalf("price loss must remain open and marked -1 USD: %+v", r)
	}
	// True four-fill fees grow with actual closing notionals, not exit bps.
	p.TakerFeeL, p.TakerFeeR = 0.0001, 0.0002
	r, err = ReplayChecked(samples, p)
	if err != nil {
		t.Fatal(err)
	}
	want := -1 - (100+200)*p.TakerFeeL - (102+203)*p.TakerFeeR
	if math.Abs(r.MTMUSD-want) > 1e-9 {
		t.Fatalf("mark=%v want %v", r.MTMUSD, want)
	}
}

func TestReplayUsesDepthAndMarksWithoutForcedClose(t *testing.T) {
	start := time.Unix(1000, 0)
	makeSeries := func(size float64) []Sample {
		var samples []Sample
		for i := 0; i <= 24; i++ {
			s := pricedSample(start.Add(time.Duration(i)*100*time.Millisecond), 100, 100.02, 100, 100.02, 100)
			if i >= 22 {
				s = pricedSample(time.Unix(0, s.Ts), 99.98, 100, 102, 102.02, size)
			}
			samples = append(samples, s)
		}
		return samples
	}
	for _, tc := range []struct {
		name  string
		size  float64
		steps int
	}{{"thin", 1, 0}, {"deep", 100, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := ReplayChecked(makeSeries(tc.size), replayParams())
			if err != nil {
				t.Fatal(err)
			}
			if r.EndInventory != tc.steps || r.RoundTrips != 0 {
				t.Fatalf("result=%+v", r)
			}
			if tc.steps == 1 && math.Abs(r.UnrealizedUSD+0.04) > 1e-9 {
				t.Fatalf("mark=%v", r.UnrealizedUSD)
			}
		})
	}
	samples := makeSeries(100)
	last := &samples[len(samples)-1]
	last.L.Ts = last.L.Ts.Add(-time.Minute)
	r, err := ReplayChecked(samples, replayParams())
	if err != nil {
		t.Fatal(err)
	}
	if r.MTMValid || r.EndInventory != 1 || r.SkippedBooks != 1 {
		t.Fatalf("stale final books must leave unavailable mark: %+v", r)
	}
}

func TestReplayChecksInputsAndGaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Params, []Sample)
	}{
		{"zero quantity", func(p *Params, s []Sample) { p.StepQty = 0 }},
		{"nan threshold", func(p *Params, s []Sample) { p.OpenBps = math.NaN() }},
		{"negative interval", func(p *Params, s []Sample) { p.DecisionInterval = -time.Second }},
		{"out of order", func(p *Params, s []Sample) { s[1].Ts = s[0].Ts }},
		{"gap", func(p *Params, s []Sample) { s[2].Ts += int64(time.Second) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := replayParams()
			s := legacySeries(3)
			tc.change(&p, s)
			if _, err := ReplayChecked(s, p); err == nil {
				t.Fatal("expected explicit error")
			}
		})
	}
	p := replayParams()
	p.AllowGaps = true
	s := legacySeries(3)
	s[2].Ts += int64(time.Second)
	r, err := ReplayChecked(s, p)
	if err != nil || r.GapCount != 1 {
		t.Fatalf("allowed gap=%+v err=%v", r, err)
	}
	if r := Replay(nil, p); r.Error == "" {
		t.Fatal("compatibility API must surface errors")
	}
}

func TestReadSamplesLegacyAndBooks(t *testing.T) {
	at := time.Unix(1000, 0)
	s := pricedSample(at, 100, 100.02, 102, 102.02, 3)
	line := fmt.Sprintf("%d,%g,%g,%g,100,100.02,102,102.02,%g,3,3,3,3,%d,%d\n", s.Ts, s.Mid, s.LS, s.SL, s.RefMid, s.Ts, s.Ts)
	got, err := readSamples(strings.NewReader(line))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || legacy(got[0]) || got[0].L.Asks[0].Price != 100.02 {
		t.Fatalf("samples=%+v", got)
	}
	got, err = readSamples(strings.NewReader("1,3,3,-3\n"))
	if err != nil || len(got) != 1 || !legacy(got[0]) {
		t.Fatalf("legacy=%+v err=%v", got, err)
	}
	for _, line := range []string{"", "1,0,3\n", "1,NaN,3,-3\n", "1,0,3,-3\n", "1,3,3,-3\n1,3,3,-3\n", "2,3,3,-3\n1,3,3,-3\n", strings.Repeat("x", 100000)} {
		if _, err := readSamples(strings.NewReader(line)); err == nil {
			t.Fatalf("accepted invalid input %q", line[:min(len(line), 40)])
		}
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func TestReadSamplesPropagatesReadError(t *testing.T) {
	if _, err := readSamples(brokenReader{}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err=%v", err)
	}
}

func TestLegacyProxyHasRecordedEntryPrices(t *testing.T) {
	q := sampleQuote(Sample{Ts: 1, Mid: 3, LS: 3, SL: -3}, 100)
	a := (strategy.Arbitrage{}).Decide(strategy.Params{StepQty: 1, MaxSteps: 1, DepthMult: 2, OpenBps: 1}, strategy.State{Mode: strategy.ModeRunning}, q, 0, true, 0)
	if a.Kind != strategy.Open || a.L.Price <= 0 || a.R.Price <= 0 {
		t.Fatalf("legacy prices must support cashflow math: %+v", a)
	}
}

func TestReplayRejectsMixedPriceModels(t *testing.T) {
	s := legacySeries(2)
	s[1] = pricedSample(time.Unix(0, s[1].Ts), 100, 100.02, 102, 102.02, 10)
	if _, err := ReplayChecked(s, replayParams()); err == nil || !strings.Contains(err.Error(), "cannot mix") {
		t.Fatalf("err=%v", err)
	}
}

func TestSamplingDeadlinesKeepTheirPhaseThroughJitter(t *testing.T) {
	start := time.Unix(1000, 0)
	var nextVol, nextMean, nextDecision time.Time
	volSamples, meanSamples, decisions := 0, 0, 0
	for i := 0; i < 20; i++ {
		jitter := time.Millisecond
		if i == 0 {
			jitter = 0
		} else if i%2 == 0 {
			jitter = -time.Millisecond
		}
		now := start.Add(time.Duration(i)*100*time.Millisecond + jitter)
		if sampleDue(now, &nextVol, 100*time.Millisecond, 50*time.Millisecond) {
			volSamples++
		}
		if sampleDue(now, &nextMean, time.Second, 50*time.Millisecond) {
			meanSamples++
		}
		if sampleDue(now, &nextDecision, 200*time.Millisecond, 50*time.Millisecond) {
			decisions++
		}
	}
	if volSamples != 20 || meanSamples != 2 || decisions != 10 || !nextVol.Equal(start.Add(2*time.Second)) {
		t.Fatalf("jitter drifted deadlines: vol=%d mean=%d decisions=%d next=%s", volSamples, meanSamples, decisions, nextVol)
	}
}
