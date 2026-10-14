// Package monitor aggregates runtime events from the trading system into
// metrics and alerts. It is strictly read-only: it never issues trading
// commands and never imports the trading engine.
package monitor

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Kind classifies a runtime event.
type Kind string

const (
	KindPnL          Kind = "pnl"
	KindStepOpened   Kind = "step_opened"
	KindStepClosed   Kind = "step_closed"
	KindExecuted     Kind = "executed"
	KindRescue       Kind = "rescue"
	KindMargin       Kind = "margin"
	KindEmergency    Kind = "emergency"
	KindError        Kind = "error"
	KindStatus       Kind = "status"
	KindPairPromoted Kind = "pair_promoted"
	KindPairDemoted  Kind = "pair_demoted"
	KindPairHealth   Kind = "pair_health"
	KindPairWarmup   Kind = "pair_warmup"
)

// Event is a structured runtime event. Fields are interpreted by Kind.
type Event struct {
	Time      time.Time
	Pair      string
	Kind      Kind
	Dir       string
	Msg       string
	Qty       float64
	Bps       float64
	USD       float64
	LatencyMs float64
	OneLeg    bool
	Steps     int

	MuBps    float64
	SigmaBps float64
	TickK    float64
	FailRate float64

	Mode      string
	Score     float64
	DepthQ    float64
	TickQ     float64
	Stability float64
	Reverts   float64
	Coverage  float64
	Samples   int
	Ready     bool
}

// AlertFunc receives an alert message (log, webhook, ...).
type AlertFunc func(msg string)

// Metrics is a snapshot of the aggregated counters and gauges.
type Metrics struct {
	UptimeSec   float64 `json:"uptime_sec"`
	Opens       uint64  `json:"opens"`
	Closes      uint64  `json:"closes"`
	Executions  uint64  `json:"executions"`
	OneLegFails uint64  `json:"one_leg_fails"`
	Rescues     uint64  `json:"rescues"`
	Emergencies uint64  `json:"emergencies"`
	Errors      uint64  `json:"errors"`

	OpenSteps   int     `json:"open_steps"`
	VolumeUSD   float64 `json:"volume_usd"`
	RealizedUSD float64 `json:"realized_usd"`

	LatencyN     uint64  `json:"latency_n"`
	LatencyAvgMs float64 `json:"latency_avg_ms"`
	LatencyMaxMs float64 `json:"latency_max_ms"`

	MuBps    float64 `json:"mu_bps"`
	SigmaBps float64 `json:"sigma_bps"`
	TickK    float64 `json:"tick_k"`
	FailRate float64 `json:"fail_rate"`

	Alerts []string               `json:"alerts"`
	Pairs  map[string]PairMetrics `json:"pairs"`
}

// PairMetrics is the per-pair slice of the snapshot.
type PairMetrics struct {
	Base            string  `json:"base"`
	Active          bool    `json:"active"`
	Mode            string  `json:"mode"`
	Opens           uint64  `json:"opens"`
	Closes          uint64  `json:"closes"`
	Executions      uint64  `json:"executions"`
	OneLegFails     uint64  `json:"one_leg_fails"`
	Rescues         uint64  `json:"rescues"`
	Errors          uint64  `json:"errors"`
	OpenSteps       int     `json:"open_steps"`
	VolumeUSD       float64 `json:"volume_usd"`
	RealizedUSD     float64 `json:"realized_usd"`
	MuBps           float64 `json:"mu_bps"`
	SigmaBps        float64 `json:"sigma_bps"`
	TickK           float64 `json:"tick_k"`
	FailRate        float64 `json:"fail_rate"`
	RollingFailRate float64 `json:"rolling_fail_rate"` // latest actor window; fail_rate is lifetime
	Score           float64 `json:"score"`
	DepthQ          float64 `json:"depth_q"`
	TickQ           float64 `json:"tick_q"`
	Stability       float64 `json:"stability"`
	Reverts         float64 `json:"reverts_per_min"`
	Coverage        float64 `json:"coverage"`
	Samples         int     `json:"samples"`
	Ready           bool    `json:"ready"`
}

// Monitor consumes events, aggregates metrics and fires alerts.
type Monitor struct {
	mu    sync.Mutex
	start time.Time
	alert AlertFunc

	opens, closes, executions, oneLeg, rescues, emergencies, errors uint64
	openSteps                                                       int
	volumeUSD, realizedUSD                                          float64
	latN, latSum, latMax                                            float64
	muBps, sigmaBps, tickK, failRate                                float64
	alerts                                                          []string
	pairs                                                           map[string]*pairAcc
}

// pairAcc is one pair's rolling counters and latest gauges.
type pairAcc struct {
	base                                               string
	active                                             bool
	mode                                               string
	opens, closes, executions, oneLeg, rescues, errors uint64
	openSteps                                          int
	volumeUSD, realizedUSD                             float64
	muBps, sigmaBps, tickK                             float64
	failRate                                           float64
	score, depthQ, tickQ, stability, reverts           float64
	coverage                                           float64
	samples                                            int
	ready                                              bool
}

// New builds a monitor; alert may be nil.
func New(alert AlertFunc) *Monitor {
	return &Monitor{start: time.Now(), alert: alert, pairs: map[string]*pairAcc{}}
}

func (m *Monitor) acc(pair string) *pairAcc {
	a, ok := m.pairs[pair]
	if !ok {
		a = &pairAcc{base: pair}
		m.pairs[pair] = a
	}
	return a
}

// RollingFailRate returns the latest rolling one-leg failure rate reported for
// a pair (0 before any status), for use by the pair picker.
func (m *Monitor) RollingFailRate(pair string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.acc(pair).failRate
}

// Record applies one event.
func (m *Monitor) Record(e Event) {
	m.mu.Lock()
	alertMsg := ""
	switch e.Kind {
	case KindStepOpened:
		m.opens++
		a := m.acc(e.Pair)
		a.opens++
		a.openSteps++
	case KindPnL:
		m.realizedUSD += e.USD
		m.acc(e.Pair).realizedUSD += e.USD
	case KindStepClosed:
		m.closes++
		a := m.acc(e.Pair)
		a.closes++
		if a.openSteps > 0 {
			a.openSteps--
		}
	case KindExecuted:
		m.executions++
		m.volumeUSD += e.USD
		if e.OneLeg {
			m.oneLeg++
		}
		a := m.acc(e.Pair)
		a.executions++
		a.volumeUSD += e.USD
		if e.OneLeg {
			a.oneLeg++
		}
		if e.LatencyMs > 0 {
			m.latN++
			m.latSum += e.LatencyMs
			if e.LatencyMs > m.latMax {
				m.latMax = e.LatencyMs
			}
		}
	case KindRescue:
		m.rescues++
		m.acc(e.Pair).rescues++
	case KindEmergency:
		m.emergencies++
		alertMsg = fmt.Sprintf("emergency unwind on %s", e.Pair)
	case KindError:
		m.errors++
		m.acc(e.Pair).errors++
		alertMsg = fmt.Sprintf("error on %s: %s", e.Pair, e.Msg)
	case KindStatus:
		m.muBps, m.sigmaBps, m.tickK, m.failRate = e.MuBps, e.SigmaBps, e.TickK, e.FailRate
		a := m.acc(e.Pair)
		a.openSteps = e.Steps
		if a.openSteps < 0 {
			a.openSteps = -a.openSteps
		}
		a.mode = e.Mode
		a.muBps, a.sigmaBps, a.tickK, a.failRate = e.MuBps, e.SigmaBps, e.TickK, e.FailRate
	case KindPairPromoted:
		a := m.acc(e.Pair)
		a.active = true
		a.score = e.Score
	case KindPairDemoted:
		m.acc(e.Pair).active = false
	case KindPairHealth:
		a := m.acc(e.Pair)
		a.score = e.Score
		a.depthQ, a.tickQ, a.stability, a.reverts = e.DepthQ, e.TickQ, e.Stability, e.Reverts
	case KindPairWarmup:
		a := m.acc(e.Pair)
		a.samples, a.coverage, a.ready = e.Samples, e.Coverage, e.Ready
	}
	m.openSteps = 0
	for _, a := range m.pairs {
		m.openSteps += a.openSteps
	}
	if alertMsg != "" {
		m.alerts = append(m.alerts, alertMsg)
		if len(m.alerts) > 64 {
			m.alerts = m.alerts[len(m.alerts)-64:]
		}
	}
	alert := m.alert
	m.mu.Unlock()
	if alert != nil && alertMsg != "" {
		alert(alertMsg)
	}
}

// Metrics returns a snapshot.
func (m *Monitor) Metrics() Metrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	latAvg := 0.0
	if m.latN > 0 {
		latAvg = m.latSum / float64(m.latN)
	}
	fail := 0.0
	if m.executions > 0 {
		fail = float64(m.oneLeg) / float64(m.executions)
	}
	pairs := make(map[string]PairMetrics, len(m.pairs))
	for key, a := range m.pairs {
		pfail := 0.0
		if a.executions > 0 {
			pfail = float64(a.oneLeg) / float64(a.executions)
		}
		pairs[key] = PairMetrics{
			Base: a.base, Active: a.active, Mode: a.mode,
			Opens: a.opens, Closes: a.closes, Executions: a.executions,
			OneLegFails: a.oneLeg, Rescues: a.rescues, Errors: a.errors,
			OpenSteps: a.openSteps, VolumeUSD: a.volumeUSD, RealizedUSD: a.realizedUSD,
			MuBps: a.muBps, SigmaBps: a.sigmaBps, TickK: a.tickK, FailRate: pfail,
			Score: a.score, DepthQ: a.depthQ, TickQ: a.tickQ, Stability: a.stability, Reverts: a.reverts,
			RollingFailRate: a.failRate,
			Coverage:        a.coverage, Samples: a.samples, Ready: a.ready,
		}
	}
	return Metrics{
		UptimeSec:    time.Since(m.start).Seconds(),
		Opens:        m.opens,
		Closes:       m.closes,
		Executions:   m.executions,
		OneLegFails:  m.oneLeg,
		Rescues:      m.rescues,
		Emergencies:  m.emergencies,
		Errors:       m.errors,
		OpenSteps:    m.openSteps,
		VolumeUSD:    m.volumeUSD,
		RealizedUSD:  m.realizedUSD,
		LatencyN:     uint64(m.latN),
		LatencyAvgMs: latAvg,
		LatencyMaxMs: m.latMax,
		MuBps:        m.muBps,
		SigmaBps:     m.sigmaBps,
		TickK:        m.tickK,
		FailRate:     fail,
		Alerts:       append([]string(nil), m.alerts...),
		Pairs:        pairs,
	}
}

// Handler serves the metrics snapshot as JSON. When token is non-empty, every
// request must carry it as an Authorization Bearer header or a ?token= query.
func (m *Monitor) Handler(token string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token != "" && !authorized(r, token) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="monitor"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(m.Metrics()); err != nil {
			slog.Error("write monitor metrics", "err", err)
		}
	})
}

// authorized reports whether r carries the expected token (constant-time).
func authorized(r *http.Request, token string) bool {
	if hs := r.Header.Values("Authorization"); len(hs) > 0 {
		if len(hs) != 1 {
			return false
		}
		scheme, value, ok := strings.Cut(hs[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || value == "" || strings.ContainsAny(value, " \t\r\n") {
			return false
		}
		return subtle.ConstantTimeCompare([]byte(value), []byte(token)) == 1
	}
	if ts := r.URL.Query()["token"]; len(ts) == 1 {
		return subtle.ConstantTimeCompare([]byte(ts[0]), []byte(token)) == 1
	}
	return false
}
