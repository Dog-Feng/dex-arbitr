// Miss analysis classifies one-leg failures from the persisted trade log and
// the 100ms book samples, and estimates what the "unwind the filled leg"
// policy would have cost instead of the recorded rescue. It is fully offline:
// it reads only files and never touches the trading engine.
package analytics

import (
	"bufio"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
)

// MissRow is one classified one-leg failure.
type MissRow struct {
	Time        time.Time
	Pair        string
	Kind        string // relative | common | ambiguous | no_samples
	CommonBps   float64
	RelativeBps float64
	RescueUSD   float64
	UnwindUSD   float64 // estimated cost of unwinding the filled leg
	SavingUSD   float64 // RescueUSD - UnwindUSD (positive = unwind cheaper)
}

// MissSummary aggregates the classification results.
type MissSummary struct {
	Total             int
	Relative          int
	Common            int
	Ambiguous         int
	NoSamples         int
	RescueUSD         float64
	UnwindUSD         float64
	SavingUSD         float64
	AvgRescueRelative float64
	AvgRescueCommon   float64
}

type sample struct {
	t    time.Time
	midL float64
	midR float64
}

// AnalyzeMisses reads the trade log and the per-pair sample files, classifies
// every one-leg execution and estimates the alternative unwind cost.
func AnalyzeMisses(trades []monitor.Event, samplesDir string, relBps, comBps float64) ([]MissRow, MissSummary, error) {
	if relBps <= 0 {
		relBps = 0.26 // ~2 ticks of 0.13 bps
	}
	if comBps <= 0 {
		comBps = 0.26
	}
	var rows []MissRow
	for _, e := range trades {
		if e.Kind != monitor.KindExecuted || !e.OneLeg {
			continue
		}
		row := MissRow{Time: e.Time, Pair: e.Pair, RescueUSD: e.RescueCostUSD}
		samples, err := loadSamples(filepath.Join(samplesDir, e.Pair+".jsonl"), e.Time, 10*time.Second)
		if err != nil || len(samples) < 2 {
			row.Kind = "no_samples"
			rows = append(rows, row)
			continue
		}
		row.CommonBps, row.RelativeBps = classify(samples)
		switch {
		case row.RelativeBps >= relBps && row.RelativeBps >= row.CommonBps:
			row.Kind = "relative"
		case row.CommonBps >= comBps:
			row.Kind = "common"
		default:
			row.Kind = "ambiguous"
		}
		if row.Kind != "no_samples" {
			row.UnwindUSD = unwindEstimate(samples, e.USD)
			row.SavingUSD = row.RescueUSD - row.UnwindUSD
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Time.Before(rows[j].Time) })
	return rows, summarize(rows), nil
}

// classify returns (common, relative) mid-move magnitudes in bps at the moment
// of maximum divergence inside the execution window. A miss where one venue
// moved far more than the other is a relative dislocation; similar moves in
// the same direction are a common jump.
func classify(s []sample) (common, relative float64) {
	baseL, baseR := s[0].midL, s[0].midR
	bestCommon, bestRel := 0.0, 0.0
	for _, x := range s[1:] {
		dl := bpsMove(baseL, x.midL)
		dr := bpsMove(baseR, x.midR)
		c := (dl + dr) / 2
		r := (dl - dr) / 2
		if math.Abs(r) > math.Abs(bestRel) {
			bestRel = r
			bestCommon = c
		}
	}
	return bestCommon, bestRel
}

// unwindEstimate approximates the cost of flattening the filled leg: the
// half-spread of the venue that moved least, applied to half the recorded
// two-leg notional. It is a bound, not a fill simulation.
func unwindEstimate(_ []sample, twoLegUSD float64) float64 {
	halfBps := 0.13 // ~1 tick half-spread proxy for the unchanged venue
	return halfBps / 1e4 * (twoLegUSD / 2)
}

func bpsMove(base, now float64) float64 {
	if base <= 0 || now <= 0 {
		return 0
	}
	return (now - base) / base * 1e4
}

// loadSamples reads the sample file around the execution time, keeping rows in
// [t-window, t+2s]. Files may be legacy 4-column (no leg prices) or the full
// 15-column quote format.
func loadSamples(path string, around time.Time, window time.Duration) ([]sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	from := around.Add(-window)
	to := around.Add(2 * time.Second)
	var out []sample
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		parts := strings.Split(line, ",")
		if len(parts) < 4 {
			continue
		}
		ns, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}
		t := time.Unix(0, ns)
		if t.Before(from) || t.After(to) {
			continue
		}
		if len(parts) < 15 {
			continue // legacy format carries no leg prices
		}
		lbid, e1 := strconv.ParseFloat(parts[3], 64)
		lask, e2 := strconv.ParseFloat(parts[4], 64)
		rbid, e3 := strconv.ParseFloat(parts[5], 64)
		rask, e4 := strconv.ParseFloat(parts[6], 64)
		ref, e5 := strconv.ParseFloat(parts[7], 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || ref <= 0 {
			continue
		}
		out = append(out, sample{
			t:    t,
			midL: (lbid + lask) / 2,
			midR: (rbid + rask) / 2,
		})
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func summarize(rows []MissRow) MissSummary {
	var s MissSummary
	var relRescue, relN, comRescue, comN float64
	for _, r := range rows {
		s.Total++
		switch r.Kind {
		case "relative":
			s.Relative++
			relRescue += r.RescueUSD
			relN++
		case "common":
			s.Common++
			comRescue += r.RescueUSD
			comN++
		case "ambiguous":
			s.Ambiguous++
		default:
			s.NoSamples++
			continue
		}
		s.RescueUSD += r.RescueUSD
		s.UnwindUSD += r.UnwindUSD
		s.SavingUSD += r.SavingUSD
	}
	if relN > 0 {
		s.AvgRescueRelative = relRescue / relN
	}
	if comN > 0 {
		s.AvgRescueCommon = comRescue / comN
	}
	return s
}
