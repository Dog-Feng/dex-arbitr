// Package monitor aggregates runtime events from the trading system into
// metrics and alerts. It is strictly read-only: it never issues trading
// commands and never imports the trading engine.
package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Kind classifies a runtime event.
type Kind string

const (
	KindStepOpened Kind = "step_opened"
	KindStepClosed Kind = "step_closed"
	KindExecuted   Kind = "executed"
	KindRescue     Kind = "rescue"
	KindMargin     Kind = "margin"
	KindEmergency  Kind = "emergency"
	KindError      Kind = "error"
	KindStatus     Kind = "status"
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

	Alerts []string `json:"alerts"`
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
}

// New builds a monitor; alert may be nil.
func New(alert AlertFunc) *Monitor {
	return &Monitor{start: time.Now(), alert: alert}
}

// Record applies one event.
func (m *Monitor) Record(e Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch e.Kind {
	case KindStepOpened:
		m.opens++
		m.openSteps++
	case KindStepClosed:
		m.closes++
		m.openSteps--
		m.realizedUSD += e.USD
	case KindExecuted:
		m.executions++
		m.volumeUSD += e.USD
		if e.OneLeg {
			m.oneLeg++
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
	case KindEmergency:
		m.emergencies++
		m.alertf("emergency unwind on %s", e.Pair)
	case KindError:
		m.errors++
		m.alertf("error on %s: %s", e.Pair, e.Msg)
	case KindStatus:
		m.openSteps = e.Steps
		m.muBps, m.sigmaBps, m.tickK, m.failRate = e.MuBps, e.SigmaBps, e.TickK, e.FailRate
	}
}

func (m *Monitor) alertf(format string, args ...any) {
	if m.alert == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	m.alerts = append(m.alerts, msg)
	if len(m.alerts) > 64 {
		m.alerts = m.alerts[len(m.alerts)-64:]
	}
	m.alert(msg)
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
	}
}

// Handler serves the metrics snapshot as JSON.
func (m *Monitor) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(m.Metrics())
	})
}
