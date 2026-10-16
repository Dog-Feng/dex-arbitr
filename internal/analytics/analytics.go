// Package analytics turns the persisted trade-event JSONL into offline bucket
// rows and session summaries. It reuses the runtime monitor's aggregation so
// offline numbers always match the /metrics time_buckets semantics. It is
// strictly read-only and never issues trading instructions.
package analytics

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
)

// ParseTrades reads one JSON event per line from the monitor's trade log.
func ParseTrades(r io.Reader) ([]monitor.Event, error) {
	var events []monitor.Event
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	lineNo := 0
	first := true
	for sc.Scan() {
		lineNo++
		line := sc.Bytes()
		if first {
			line = bytes.TrimPrefix(line, []byte{0xEF, 0xBB, 0xBF})
			first = false
		}
		if len(line) == 0 {
			continue
		}
		var e monitor.Event
		if err := json.Unmarshal(line, &e); err != nil {
			return nil, fmt.Errorf("trade log line %d: %w", lineNo, err)
		}
		if e.Time.IsZero() {
			return nil, fmt.Errorf("trade log line %d: missing time", lineNo)
		}
		events = append(events, e)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read trade log: %w", err)
	}
	return events, nil
}

// Range bounds a replay report; zero times mean unbounded.
type Range struct {
	From, To time.Time // inclusive from, exclusive to
}

// Replay aggregates events into the same per-bucket shape the runtime monitor
// exposes in /metrics, restricted to [from, to). Sessions are labeled by the
// Beijing wall clock via monitor.SessionOf.
func Replay(events []monitor.Event, bucket time.Duration, sessions map[string]string, r Range) ([]monitor.BucketMetrics, error) {
	if bucket <= 0 {
		return nil, fmt.Errorf("bucket must be positive")
	}
	m := monitor.New(nil)
	// Retain and expose every bucket of the replay; the runtime defaults are
	// irrelevant for a one-shot offline pass.
	m.ConfigureAnalytics(monitor.AnalyticsCfg{
		Bucket:         bucket,
		RetainBuckets:  1 << 20,
		MetricsBuckets: 1 << 20,
		Sessions:       sessions,
	}, nil)
	for _, e := range events {
		if !r.From.IsZero() && e.Time.Before(r.From) {
			continue
		}
		if !r.To.IsZero() && !e.Time.Before(r.To) {
			continue
		}
		m.Record(e)
	}
	rows := m.Metrics().TimeBuckets
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].StartUTC != rows[j].StartUTC {
			return rows[i].StartUTC < rows[j].StartUTC
		}
		return rows[i].Pair < rows[j].Pair
	})
	return rows, nil
}

// SessionSummary aggregates bucket rows per session label. Weighted means use
// the status-sample and latency counts so averages are exact, not averages of
// averages.
type SessionSummary struct {
	Session        string  `json:"session"`
	Buckets        int     `json:"buckets"`
	Opens          uint64  `json:"opens"`
	Closes         uint64  `json:"closes"`
	Flips          uint64  `json:"flips"`
	Executions     uint64  `json:"executions"`
	OneLegFails    uint64  `json:"one_leg_fails"`
	Rescues        uint64  `json:"rescues"`
	VolumeUSD      float64 `json:"volume_usd"`
	RealizedUSD    float64 `json:"realized_usd"`
	RealizedBps    float64 `json:"realized_bps"`
	FailRate       float64 `json:"fail_rate"`
	LatencyAvgMs   float64 `json:"latency_avg_ms"`
	LatencyMaxMs   float64 `json:"latency_max_ms"`
	MuBps          float64 `json:"mu_bps"`
	SigmaBps       float64 `json:"sigma_bps"`
	TickK          float64 `json:"tick_k"`
	RescueCostUSD  float64 `json:"rescue_cost_usd"`
	OpenStepsPeak  int     `json:"open_steps_peak"`
	OldestStepAgeS float64 `json:"oldest_step_age_s"`
	Hours          float64 `json:"hours"`
}

// SessionSummaries groups rows by session; rows without a session label into
// "uncategorized".
func SessionSummaries(rows []monitor.BucketMetrics, bucket time.Duration) []SessionSummary {
	order := []string{}
	agg := map[string]*SessionSummary{}
	for _, b := range rows {
		if b.Pair == "" {
			continue // skip global aggregate rows; summaries are per pair
		}
		sess := b.Session
		if sess == "" {
			sess = "uncategorized"
		}
		s, ok := agg[sess]
		if !ok {
			s = &SessionSummary{Session: sess}
			agg[sess] = s
			order = append(order, sess)
		}
		s.Buckets++
		s.Opens += b.Opens
		s.Closes += b.Closes
		s.Flips += b.Flips
		s.Executions += b.Executions
		s.OneLegFails += b.OneLegFails
		s.Rescues += b.Rescues
		s.VolumeUSD += float64(b.VolumeUSD)
		s.RealizedUSD += float64(b.RealizedUSD)
		s.RescueCostUSD += float64(b.RescueCostUSD)
		s.LatencyAvgMs += b.LatencyAvgMs * float64(b.LatencyN)
		if b.LatencyMaxMs > s.LatencyMaxMs {
			s.LatencyMaxMs = b.LatencyMaxMs
		}
		s.MuBps += b.MuBps * float64(b.StatusSamples)
		s.SigmaBps += b.SigmaBps * float64(b.StatusSamples)
		s.TickK += b.TickK * float64(b.StatusSamples)
		if b.OpenStepsPeak > s.OpenStepsPeak {
			s.OpenStepsPeak = b.OpenStepsPeak
		}
		if b.OldestStepAgeS > s.OldestStepAgeS {
			s.OldestStepAgeS = b.OldestStepAgeS
		}
		s.Hours = float64(s.Buckets) * bucket.Hours()
	}
	out := make([]SessionSummary, 0, len(order))
	for _, name := range order {
		s := agg[name]
		latN := 0.0
		statusN := 0.0
		for _, b := range rows {
			if b.Pair == "" {
				continue
			}
			sess := b.Session
			if sess == "" {
				sess = "uncategorized"
			}
			if sess != name {
				continue
			}
			latN += float64(b.LatencyN)
			statusN += float64(b.StatusSamples)
		}
		if latN > 0 {
			s.LatencyAvgMs /= latN
		}
		if statusN > 0 {
			s.MuBps /= statusN
			s.SigmaBps /= statusN
			s.TickK /= statusN
		}
		if s.Executions > 0 {
			s.FailRate = float64(s.OneLegFails) / float64(s.Executions)
		}
		if s.VolumeUSD > 0 {
			s.RealizedBps = s.RealizedUSD / s.VolumeUSD * 1e4
		}
		out = append(out, *s)
	}
	return out
}
