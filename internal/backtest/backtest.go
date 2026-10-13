// Package backtest replays the pure strategy over persisted spread samples to
// calibrate parameters and estimate net edge. It is offline and read-only: it
// never imports the engine and never touches an exchange.
package backtest

import (
	"bufio"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// Sample is one spread snapshot in bps (line: unixNano,midBps,lsBps,slBps).
type Sample struct {
	Ts          int64
	Mid, LS, SL float64
}

// Params of a replay.
type Params struct {
	StepQty   float64
	MaxSteps  int
	DepthMult float64
	OpenBps   float64
	CloseBps  float64
	VolK      float64
	FeeBps    float64
}

// Result of a replay.
type Result struct {
	RoundTrips int
	NetBps     float64
	AvgBps     float64
}

// Load reads a samples file (see store.SampleSink).
func Load(path string) ([]Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []Sample
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		if len(p) < 4 {
			continue
		}
		ts, e1 := strconv.ParseInt(p[0], 10, 64)
		mid, e2 := strconv.ParseFloat(p[1], 64)
		ls, e3 := strconv.ParseFloat(p[2], 64)
		sl, e4 := strconv.ParseFloat(p[3], 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			continue
		}
		out = append(out, Sample{Ts: ts, Mid: mid, LS: ls, SL: sl})
	}
	return out, sc.Err()
}

// Replay runs the arbitrage strategy over samples with simulated fills at the
// executable spread (no depth checks, no rescue) and returns the realized edge.
func Replay(samples []Sample, p Params) Result {
	if p.DepthMult <= 0 {
		p.DepthMult = 2
	}
	mu := estimator.NewSMA(estimator.Params{Window: 2 * time.Hour, MinWarmup: time.Hour, MinCoverage: 0.8, Interval: 100 * time.Millisecond})
	vol := volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	sp := strategy.Params{StepQty: p.StepQty, MaxSteps: p.MaxSteps, OpenBps: p.OpenBps, CloseBps: p.CloseBps,
		DepthMult: p.DepthMult, CloseOrder: "best", VolK: p.VolK, FeeBps: p.FeeBps}
	book := domain.Book{Bids: []domain.Level{{Size: 1e18}}, Asks: []domain.Level{{Size: 1e18}}}

	st := strategy.State{Mode: strategy.ModeRunning}
	var nextID int64 = 1
	var res Result
	for _, s := range samples {
		ts := time.Unix(0, s.Ts)
		mu.Update(ts, s.Mid)
		vol.Add(s.Mid)
		q := spread.Quote{LS: s.LS, SL: s.SL, Mid: s.Mid, L: book, R: book}
		act := (strategy.Arbitrage{}).Decide(sp, st, q, mu.Mean(), mu.Ready(), vol.Vol())
		switch act.Kind {
		case strategy.Open:
			entry := s.LS
			if act.Dir == strategy.DirSL {
				entry = s.SL
			}
			st.Steps = append(st.Steps, strategy.Step{ID: nextID, Dir: act.Dir, Qty: act.Qty, EntryBps: entry})
			nextID++
		case strategy.Close:
			for i := range st.Steps {
				if st.Steps[i].ID != act.StepID {
					continue
				}
				exit := s.SL
				if st.Steps[i].Dir == strategy.DirSL {
					exit = s.LS
				}
				res.NetBps += st.Steps[i].EntryBps + exit - p.FeeBps
				res.RoundTrips++
				st.Steps = append(st.Steps[:i], st.Steps[i+1:]...)
				break
			}
		}
	}
	if res.RoundTrips > 0 {
		res.AvgBps = res.NetBps / float64(res.RoundTrips)
	}
	return res
}
