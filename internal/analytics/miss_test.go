package analytics

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
)

func writeSampleLine(t *testing.T, f *os.File, at time.Time, lbid, lask, rbid, rask float64) {
	t.Helper()
	ref := (lask + rbid) / 2
	cols := []string{
		strconv.FormatInt(at.UnixNano(), 10),
		strconv.FormatFloat(0, 'f', -1, 64),
		strconv.FormatFloat(0, 'f', -1, 64),
		strconv.FormatFloat(0, 'f', -1, 64),
		strconv.FormatFloat(lbid, 'f', -1, 64),
		strconv.FormatFloat(lask, 'f', -1, 64),
		strconv.FormatFloat(rbid, 'f', -1, 64),
		strconv.FormatFloat(rask, 'f', -1, 64),
		strconv.FormatFloat(ref, 'f', -1, 64),
		strconv.FormatFloat(1, 'f', -1, 64),
		strconv.FormatFloat(1, 'f', -1, 64),
		strconv.FormatFloat(1, 'f', -1, 64),
		strconv.FormatFloat(1, 'f', -1, 64),
		strconv.FormatInt(at.UnixNano(), 10),
		strconv.FormatInt(at.UnixNano(), 10),
	}
	line := cols[0]
	for _, c := range cols[1:] {
		line += "," + c
	}
	if _, err := f.WriteString(line + "\n"); err != nil {
		t.Fatal(err)
	}
}

func TestClassifyRelativeVersusCommon(t *testing.T) {
	base := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	rel := []sample{
		{t: base, midL: 763.60, midR: 763.65},
		{t: base.Add(100 * time.Millisecond), midL: 763.60, midR: 763.55},
	}
	c, r := classify(rel)
	if r < 0.5 || c > 0.5 {
		t.Fatalf("relative case misclassified: common=%v relative=%v", c, r)
	}
	com := []sample{
		{t: base, midL: 763.60, midR: 763.65},
		{t: base.Add(100 * time.Millisecond), midL: 763.50, midR: 763.55},
	}
	c, r = classify(com)
	if c > -0.5 || r > 0.5 {
		t.Fatalf("common case misclassified: common=%v relative=%v", c, r)
	}
}

func TestAnalyzeMissesEndToEnd(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 17, 13, 0, 0, 0, time.UTC)
	f, err := os.Create(filepath.Join(dir, "SPY_lighter_rhlighter.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	writeSampleLine(t, f, base, 763.59, 763.61, 763.64, 763.66)
	writeSampleLine(t, f, base.Add(100*time.Millisecond), 763.59, 763.61, 763.54, 763.56)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	events := []monitor.Event{{
		Time: base.Add(500 * time.Millisecond), Pair: "SPY_lighter_rhlighter",
		Kind: monitor.KindExecuted, OneLeg: true, USD: 30.5, RescueCostUSD: 0.0014,
	}}
	rows, sum, err := AnalyzeMisses(events, dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != "relative" {
		t.Fatalf("rows=%+v", rows)
	}
	if sum.Total != 1 || sum.Relative != 1 || sum.RescueUSD != 0.0014 {
		t.Fatalf("summary=%+v", sum)
	}
	if rows[0].UnwindUSD <= 0 || rows[0].SavingUSD <= 0 {
		t.Fatalf("unwind estimate wrong: %+v", rows[0])
	}
}
