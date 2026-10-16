package analytics

import (
	"strings"
	"testing"
	"time"
)

func TestParseAndReplayWithSessions(t *testing.T) {
	lines := []string{
		`{"time":"2026-10-16T13:30:00Z","pair":"A","kind":"step_opened"}`,
		`{"time":"2026-10-16T13:30:10Z","pair":"A","kind":"executed","usd":31,"latency_l_ms":300,"latency_r_ms":340}`,
		`{"time":"2026-10-16T13:30:20Z","pair":"A","kind":"pnl","usd":-0.005}`,
		`{"time":"2026-10-16T13:31:00Z","pair":"A","kind":"status","mu_bps":0.5,"sigma_bps":0.2,"tick_k":2.5}`,
		`{"time":"2026-10-16T09:00:00Z","pair":"A","kind":"step_opened"}`, // 17:00 Beijing = us_premarket
	}
	events, err := ParseTrades(strings.NewReader(strings.Join(lines, "\n") + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 5 {
		t.Fatalf("parsed %d events, want 5", len(events))
	}
	sessions := map[string]string{"us_cash": "21:30-04:00", "us_premarket": "16:00-21:30"}
	rows, err := Replay(events, 15*time.Minute, sessions, Range{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("rows=%d, want 4 (2 buckets x pair+global)", len(rows))
	}
	got := map[string]bool{}
	for _, r := range rows {
		got[r.Session] = true
	}
	if !got["us_cash"] || !got["us_premarket"] {
		t.Fatalf("session labels wrong: %+v", rows)
	}
	sums := SessionSummaries(rows, 15*time.Minute)
	if len(sums) != 2 {
		t.Fatalf("summaries=%d, want 2", len(sums))
	}
	for _, s := range sums {
		if s.Session == "us_cash" {
			if s.Opens != 1 || s.Executions != 1 || s.MuBps != 0.5 || s.LatencyAvgMs != 320 {
				t.Fatalf("us_cash summary wrong: %+v", s)
			}
		}
	}
}
