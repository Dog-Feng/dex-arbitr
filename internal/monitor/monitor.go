// Package monitor aggregates runtime events from the trading system into
// metrics and alerts. It is strictly read-only: it never issues trading
// commands and never imports the trading engine.
package monitor

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
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
	KindFlip         Kind = "flip"
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
	KindRiskState    Kind = "risk_state"
	KindAccountRecon Kind = "account_recon"
)

// Event is a structured runtime event. Fields are interpreted by Kind.
type Event struct {
	Time      time.Time `json:"time,omitempty"`
	Pair      string    `json:"pair,omitempty"`
	Kind      Kind      `json:"kind"`
	Dir       string    `json:"dir,omitempty"`
	Msg       string    `json:"msg,omitempty"`
	Qty       float64   `json:"qty,omitempty"`
	Bps       float64   `json:"bps,omitempty"`
	USD       float64   `json:"usd,omitempty"`
	LatencyMs float64   `json:"latency_ms,omitempty"`
	OneLeg    bool      `json:"one_leg,omitempty"`
	Steps     int       `json:"steps,omitempty"`

	MuBps    float64 `json:"mu_bps,omitempty"`
	SigmaBps float64 `json:"sigma_bps,omitempty"`
	TickK    float64 `json:"tick_k,omitempty"`
	FailRate float64 `json:"fail_rate,omitempty"`

	Mode      string  `json:"mode,omitempty"`
	Score     float64 `json:"score,omitempty"`
	DepthQ    float64 `json:"depth_q,omitempty"`
	TickQ     float64 `json:"tick_q,omitempty"`
	Stability float64 `json:"stability,omitempty"`
	Reverts   float64 `json:"reverts_per_min,omitempty"`
	Coverage  float64 `json:"coverage,omitempty"`
	Samples   int     `json:"samples,omitempty"`
	Ready     bool    `json:"ready,omitempty"`

	// Observational extras consumed by the analytics layer; never used by
	// trading decisions.
	RescueCostUSD  float64 `json:"rescue_cost_usd,omitempty"`
	LatencyLMs     float64 `json:"latency_l_ms,omitempty"`
	LatencyRMs     float64 `json:"latency_r_ms,omitempty"`
	GrossUSD       float64 `json:"gross_usd,omitempty"`
	MaxSteps       int     `json:"max_steps,omitempty"`
	OldestStepAgeS float64 `json:"oldest_step_age_s,omitempty"`

	// Recon carries the latest per-minute account reconciliation snapshot.
	Recon *AccountRecon `json:"recon,omitempty"`
}

// AccountRecon is one per-minute account reconciliation snapshot. It is
// observational: the equity and unrealized figures reconcile the venues'
// account value with the ledger, funding_est is the unexplained residual.
type AccountRecon struct {
	QtyL           Fixed8 `json:"qty_l,omitempty"`
	QtyR           Fixed8 `json:"qty_r,omitempty"`
	EquityL        Fixed4 `json:"equity_l,omitempty"`
	EquityR        Fixed4 `json:"equity_r,omitempty"`
	TotalEquity    Fixed4 `json:"total_equity,omitempty"`
	UnrealL        Fixed4 `json:"unreal_l,omitempty"`
	UnrealR        Fixed4 `json:"unreal_r,omitempty"`
	UnrealTotal    Fixed4 `json:"unreal_total,omitempty"`
	LedgerRealized Fixed4 `json:"ledger_realized,omitempty"`
	DeltaTotal     Fixed4 `json:"delta_total,omitempty"`
	DeltaUnreal    Fixed4 `json:"delta_unreal,omitempty"`
	DeltaRealized  Fixed4 `json:"delta_realized,omitempty"`
	FundingEst     Fixed4 `json:"funding_est,omitempty"`
}

// AlertFunc receives an alert message (log, webhook, ...).
type AlertFunc func(msg string)

// Fixed8 marshals a USD amount as a fixed-point decimal with eight digits so
// snapshots never render floating-point dust in scientific notation.
type Fixed8 float64

// MarshalJSON implements json.Marshaler with fixed 8-decimal output.
func (f Fixed8) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(f), 'f', 8, 64)), nil
}

// Fixed4 marshals a USD amount as a fixed-point decimal with four digits so
// equity and reconciliation figures match the console log precision.
type Fixed4 float64

// MarshalJSON implements json.Marshaler with fixed 4-decimal output.
func (f Fixed4) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(f), 'f', 4, 64)), nil
}

// AnalyticsCfg configures time-bucketed aggregation and anomaly detection.
// It is strictly observational: nothing here changes trading behavior.
type AnalyticsCfg struct {
	Bucket         time.Duration     // aggregation granularity; <= 0 disables buckets
	RetainBuckets  int               // in-memory bucket retention; <= 0 = default 672
	MetricsBuckets int               // buckets exposed in /metrics; <= 0 = default 96
	Cooldown       time.Duration     // per-rule/pair anomaly cooldown; <= 0 = 10m
	Sessions       map[string]string // name -> "HH:MM-HH:MM" Beijing wall clock (display only)
	Anomalies      AnomalyCfg
}

// TradeSink persists serialized trade-event lines for offline analysis.
type TradeSink interface {
	WriteTrade(line []byte) error
}

// AnomalyCfg holds anomaly-rule thresholds; a non-positive threshold disables
// the rule.
type AnomalyCfg struct {
	ConsecutiveFails  int
	FlipsWindowMin    int
	FlipsThreshold    int
	FullLoadHeadroom  int
	FullLoadMinutes   int
	OldestStepMinutes int
	LossBucketCount   int
	LossPerBucketUSD  float64
	RescueSlipUSD     float64
	LatencyP95Ms      float64
	SilentMinutes     int
	RiskFlapsPerHour  int
	MuJumpSigma       float64
}

// BucketMetrics is one time-bucket aggregate (global or per pair).
type BucketMetrics struct {
	StartUTC       string  `json:"start_utc"`
	Pair           string  `json:"pair,omitempty"`
	Session        string  `json:"session,omitempty"`
	Opens          uint64  `json:"opens"`
	Closes         uint64  `json:"closes"`
	Flips          uint64  `json:"flips"`
	Executions     uint64  `json:"executions"`
	OneLegFails    uint64  `json:"one_leg_fails"`
	Rescues        uint64  `json:"rescues"`
	VolumeUSD      Fixed8  `json:"volume_usd"`
	RealizedUSD    Fixed8  `json:"realized_usd"`
	LatencyAvgMs   float64 `json:"latency_avg_ms"`
	LatencyMaxMs   float64 `json:"latency_max_ms"`
	LatencyP95Ms   float64 `json:"latency_p95_ms"`
	LatencyN       uint64  `json:"latency_n,omitempty"`
	MuBps          float64 `json:"mu_bps"`
	SigmaBps       float64 `json:"sigma_bps"`
	TickK          float64 `json:"tick_k"`
	FailRate       float64 `json:"fail_rate"`
	StatusSamples  int     `json:"status_samples,omitempty"`
	OpenStepsPeak  int     `json:"open_steps_peak"`
	OldestStepAgeS float64 `json:"oldest_step_age_s"`
	RescueCostUSD  Fixed8  `json:"rescue_cost_usd,omitempty"`
}

// Anomaly is one fired anomaly observation.
type Anomaly struct {
	Time  string `json:"time"`
	Pair  string `json:"pair,omitempty"`
	Rule  string `json:"rule"`
	Level string `json:"level"`
	Msg   string `json:"msg"`
}

// Metrics is a snapshot of the aggregated counters and gauges.
type Metrics struct {
	UptimeSec   float64 `json:"uptime_sec"`
	Opens       uint64  `json:"opens"`
	Closes      uint64  `json:"closes"`
	Flips       uint64  `json:"flips"`
	Executions  uint64  `json:"executions"`
	OneLegFails uint64  `json:"one_leg_fails"`
	Rescues     uint64  `json:"rescues"`
	Emergencies uint64  `json:"emergencies"`
	Errors      uint64  `json:"errors"`

	OpenSteps   int    `json:"open_steps"`
	VolumeUSD   Fixed8 `json:"volume_usd"`
	RealizedUSD Fixed8 `json:"realized_usd"`

	LatencyN     uint64  `json:"latency_n"`
	LatencyAvgMs float64 `json:"latency_avg_ms"`
	LatencyMaxMs float64 `json:"latency_max_ms"`

	MuBps    float64 `json:"mu_bps"`
	SigmaBps float64 `json:"sigma_bps"`
	TickK    float64 `json:"tick_k"`
	FailRate float64 `json:"fail_rate"`

	Alerts []string               `json:"alerts"`
	Pairs  map[string]PairMetrics `json:"pairs"`
	// Account is the summed latest reconciliation snapshot across pairs.
	Account *AccountRecon `json:"account,omitempty"`
	// TimeBuckets are recent per-bucket aggregates; the global row has no pair.
	TimeBuckets []BucketMetrics `json:"time_buckets"`
	// Anomalies are the most recent fired anomaly observations.
	Anomalies []Anomaly `json:"anomalies"`
}

// PairMetrics is the per-pair slice of the snapshot.
type PairMetrics struct {
	Base            string        `json:"base"`
	Active          bool          `json:"active"`
	Mode            string        `json:"mode"`
	Opens           uint64        `json:"opens"`
	Closes          uint64        `json:"closes"`
	Flips           uint64        `json:"flips"`
	Executions      uint64        `json:"executions"`
	OneLegFails     uint64        `json:"one_leg_fails"`
	Rescues         uint64        `json:"rescues"`
	Errors          uint64        `json:"errors"`
	OpenSteps       int           `json:"open_steps"`
	VolumeUSD       Fixed8        `json:"volume_usd"`
	RealizedUSD     Fixed8        `json:"realized_usd"`
	MuBps           float64       `json:"mu_bps"`
	SigmaBps        float64       `json:"sigma_bps"`
	TickK           float64       `json:"tick_k"`
	FailRate        float64       `json:"fail_rate"`
	RollingFailRate float64       `json:"rolling_fail_rate"` // latest actor window; fail_rate is lifetime
	Score           float64       `json:"score"`
	DepthQ          float64       `json:"depth_q"`
	TickQ           float64       `json:"tick_q"`
	Stability       float64       `json:"stability"`
	Reverts         float64       `json:"reverts_per_min"`
	Coverage        float64       `json:"coverage"`
	Samples         int           `json:"samples"`
	Ready           bool          `json:"ready"`
	Account         *AccountRecon `json:"account,omitempty"` // latest per-minute reconciliation
}

// Monitor consumes events, aggregates metrics and fires alerts.
type Monitor struct {
	mu    sync.Mutex
	start time.Time
	alert AlertFunc

	opens, closes, flips, executions, oneLeg, rescues, emergencies, errors uint64
	openSteps                                                              int
	volumeUSD, realizedUSD                                                 float64
	latN, latSum, latMax                                                   float64
	muBps, sigmaBps, tickK, failRate                                       float64
	alerts                                                                 []string
	pairs                                                                  map[string]*pairAcc
	analytics                                                              AnalyticsCfg
	nowFn                                                                  func() time.Time
	buckets                                                                map[string]map[int64]*bucketAcc
	anomalies                                                              []Anomaly
	anomalyLast                                                            map[string]time.Time
	anomalyAlertMsg                                                        string
	trades                                                                 TradeSink
}

// pairAcc is one pair's rolling counters and latest gauges.
type pairAcc struct {
	base                                                      string
	active                                                    bool
	mode                                                      string
	opens, closes, flips, executions, oneLeg, rescues, errors uint64
	openSteps                                                 int
	volumeUSD, realizedUSD                                    float64
	muBps, sigmaBps, tickK                                    float64
	failRate                                                  float64
	score, depthQ, tickQ, stability, reverts                  float64
	coverage                                                  float64
	samples                                                   int
	ready                                                     bool
	recon                                                     AccountRecon
	reconSet                                                  bool
	lastExec                                                  time.Time
	consecFails                                               int
	fullLoadSince                                             time.Time
	flipTimes                                                 []time.Time
	riskTransitions                                           []time.Time
	lossStreak                                                int
	prevBucketMu, prevBucketSigma                             float64
}

// bucketAcc aggregates one time bucket of events.
type bucketAcc struct {
	opens, closes, flips, executions, oneLeg, rescues uint64
	volumeUSD, realizedUSD                            float64
	latN                                              uint64
	latSum, latMax                                    float64
	lats                                              []float64
	muSum, sigmaSum, tickKSum                         float64
	statusN                                           int
	openStepsPeak                                     int
	oldestStepAgeMax                                  float64
	lastMu, lastSigma                                 float64
	rescueCostUSD                                     float64
}

// New builds a monitor; alert may be nil.
func New(alert AlertFunc) *Monitor {
	return &Monitor{start: time.Now(), alert: alert, pairs: map[string]*pairAcc{},
		buckets: map[string]map[int64]*bucketAcc{}, anomalyLast: map[string]time.Time{}}
}

// ConfigureAnalytics enables time-bucketed aggregation, anomaly rules and the
// optional trade sink. It is strictly observational and never influences
// trading decisions.
func (m *Monitor) ConfigureAnalytics(cfg AnalyticsCfg, sink TradeSink) {
	if cfg.RetainBuckets <= 0 {
		cfg.RetainBuckets = 672
	}
	if cfg.MetricsBuckets <= 0 {
		cfg.MetricsBuckets = 96
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 10 * time.Minute
	}
	// A typed nil (e.g. a nil *store.TradeLog passed through an interface)
	// must normalize to a nil sink or Record would dereference it.
	if sink != nil {
		v := reflect.ValueOf(sink)
		if v.Kind() == reflect.Ptr && v.IsNil() {
			sink = nil
		}
	}
	m.mu.Lock()
	m.analytics = cfg
	m.trades = sink
	m.mu.Unlock()
}

// Beijing is the fixed UTC+8 display zone used for session labels.
var Beijing = time.FixedZone("CST", 8*3600)

// ValidSessionSpec reports whether spec parses as "HH:MM-HH:MM".
func ValidSessionSpec(spec string) bool {
	_, _, ok := parseSessionSpec(spec)
	return ok
}

func parseSessionSpec(spec string) (startMin, endMin int, ok bool) {
	parts := strings.Split(spec, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	startMin, ok = parseHHMM(parts[0])
	if !ok {
		return 0, 0, false
	}
	endMin, ok = parseHHMM(parts[1])
	if !ok || startMin == endMin {
		return 0, 0, false
	}
	return startMin, endMin, true
}

func parseHHMM(s string) (int, bool) {
	parts := strings.Split(s, ":")
	if len(parts) != 2 || len(parts[0]) != 2 || len(parts[1]) != 2 {
		return 0, false
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || h < 0 || h > 23 || m < 0 || m > 59 {
		return 0, false
	}
	return h*60 + m, true
}

// SessionOf labels t by its Beijing wall clock against the configured session
// windows (wraparound supported, e.g. "21:30-04:00"). It returns "" when the
// time falls outside every window or no sessions are configured.
func SessionOf(t time.Time, sessions map[string]string) string {
	if len(sessions) == 0 {
		return ""
	}
	min := t.In(Beijing).Hour()*60 + t.In(Beijing).Minute()
	for name, spec := range sessions {
		start, end, ok := parseSessionSpec(spec)
		if !ok {
			continue
		}
		if start < end {
			if min >= start && min < end {
				return name
			}
		} else if min >= start || min < end {
			return name
		}
	}
	return ""
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
	if e.Time.IsZero() {
		e.Time = m.now()
	}
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
	case KindFlip:
		m.flips++
		m.acc(e.Pair).flips++
	case KindExecuted:
		m.executions++
		m.volumeUSD += e.USD
		if e.OneLeg {
			m.oneLeg++
		}
		a := m.acc(e.Pair)
		a.lastExec = e.Time
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
	case KindRiskState:
		a := m.acc(e.Pair)
		a.riskTransitions = append(a.riskTransitions, e.Time)
		if len(a.riskTransitions) > 64 {
			a.riskTransitions = a.riskTransitions[len(a.riskTransitions)-64:]
		}
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
	case KindAccountRecon:
		if e.Recon != nil {
			a := m.acc(e.Pair)
			a.recon = *e.Recon
			a.reconSet = true
		}
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
	m.updateBuckets(e)
	m.checkAnomalies(e)
	if m.anomalyAlertMsg != "" && alertMsg == "" {
		alertMsg = m.anomalyAlertMsg
	}
	m.anomalyAlertMsg = ""
	if m.trades != nil && persistableKind(e.Kind) {
		if line, err := json.Marshal(e); err == nil {
			if writeErr := m.trades.WriteTrade(append(line, '\n')); writeErr != nil {
				slog.Warn("trade log write", "err", writeErr)
			}
		}
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

func persistableKind(k Kind) bool {
	switch k {
	case KindStepOpened, KindStepClosed, KindExecuted, KindFlip, KindRescue,
		KindStatus, KindPnL, KindRiskState, KindError, KindEmergency:
		return true
	}
	return false
}

func (m *Monitor) now() time.Time {
	if m.nowFn != nil {
		return m.nowFn()
	}
	return time.Now()
}

// updateBuckets routes one event into the current bucket for its pair and for
// the global aggregate. It is observational only.
func (m *Monitor) updateBuckets(e Event) {
	key, ok := m.bucketKey(e.Time)
	if !ok {
		return
	}
	pairs := []string{e.Pair, ""}
	if e.Pair == "" {
		pairs = []string{""}
	}
	for _, pair := range pairs {
		m.bucketAdd(pair, key, e)
	}
}

func (m *Monitor) bucketKey(t time.Time) (int64, bool) {
	size := int64(m.analytics.Bucket / time.Second)
	if size <= 0 {
		return 0, false
	}
	return t.Unix() / size * size, true
}

func (m *Monitor) bucketAdd(pair string, key int64, e Event) {
	ring := m.buckets[pair]
	if ring == nil {
		ring = map[int64]*bucketAcc{}
		m.buckets[pair] = ring
	}
	acc, ok := ring[key]
	if !ok {
		m.finalizeBucket(pair, key)
		acc = &bucketAcc{}
		ring[key] = acc
		m.pruneBuckets(pair, key, ring)
	}
	switch e.Kind {
	case KindStepOpened:
		acc.opens++
	case KindStepClosed:
		acc.closes++
	case KindFlip:
		acc.flips++
	case KindExecuted:
		acc.executions++
		acc.volumeUSD += e.USD
		acc.rescueCostUSD += e.RescueCostUSD
		if e.OneLeg {
			acc.oneLeg++
		}
		latencies := []float64{e.LatencyLMs, e.LatencyRMs}
		if latencies[0] <= 0 && latencies[1] <= 0 && e.LatencyMs > 0 {
			latencies = []float64{e.LatencyMs}
		}
		for _, l := range latencies {
			if l <= 0 {
				continue
			}
			acc.latN++
			acc.latSum += l
			if l > acc.latMax {
				acc.latMax = l
			}
			acc.lats = append(acc.lats, l)
			if len(acc.lats) > 4096 {
				acc.lats = acc.lats[len(acc.lats)-4096:]
			}
		}
	case KindRescue:
		acc.rescues++
	case KindPnL:
		acc.realizedUSD += e.USD
	case KindStatus:
		acc.statusN++
		acc.muSum += e.MuBps
		acc.sigmaSum += e.SigmaBps
		acc.tickKSum += e.TickK
		steps := e.Steps
		if steps < 0 {
			steps = -steps
		}
		if steps > acc.openStepsPeak {
			acc.openStepsPeak = steps
		}
		if e.OldestStepAgeS > acc.oldestStepAgeMax {
			acc.oldestStepAgeMax = e.OldestStepAgeS
		}
		acc.lastMu, acc.lastSigma = e.MuBps, e.SigmaBps
	}
}

func (m *Monitor) pruneBuckets(pair string, key int64, ring map[int64]*bucketAcc) {
	if m.analytics.RetainBuckets <= 0 {
		return
	}
	size := int64(m.analytics.Bucket / time.Second)
	limit := key - size*int64(m.analytics.RetainBuckets)
	for k := range ring {
		if k < limit {
			delete(ring, k)
		}
	}
}

// finalizeBucket runs bucket-close anomaly rules for the previous bucket once
// a newer bucket opens.
func (m *Monitor) finalizeBucket(pair string, key int64) {
	if pair == "" {
		// The aggregate bucket mirrors the same data its pair buckets carry;
		// closing it must not run pair-level rules or create a phantom pair.
		return
	}
	ring := m.buckets[pair]
	prevKey := int64(math.MinInt64)
	for k := range ring {
		if k < key && k > prevKey {
			prevKey = k
		}
	}
	if prevKey == math.MinInt64 {
		return
	}
	b := ring[prevKey]
	a := m.acc(pair)
	ax := m.analytics.Anomalies
	now := m.now()
	if ax.LossBucketCount > 0 && ax.LossPerBucketUSD > 0 {
		if b.realizedUSD < -ax.LossPerBucketUSD {
			a.lossStreak++
			if a.lossStreak >= ax.LossBucketCount {
				m.fireAnomaly(pair, "loss_buckets", "WARN",
					fmt.Sprintf("%d consecutive buckets with realized loss (latest %.6f USD)", a.lossStreak, b.realizedUSD), now)
			}
		} else {
			a.lossStreak = 0
		}
	}
	if ax.LatencyP95Ms > 0 {
		if p := p95(b.lats); p > ax.LatencyP95Ms {
			m.fireAnomaly(pair, "latency_p95", "WARN",
				fmt.Sprintf("bucket latency p95 %.1fms above threshold %.1fms", p, ax.LatencyP95Ms), now)
		}
	}
	if ax.MuJumpSigma > 0 && b.statusN > 0 && a.prevBucketSigma > 0 {
		if math.Abs(b.lastMu-a.prevBucketMu) > ax.MuJumpSigma*a.prevBucketSigma {
			m.fireAnomaly(pair, "mu_jump", "INFO",
				fmt.Sprintf("mean spread moved %.3f bps vs previous bucket (sigma %.3f)", b.lastMu-a.prevBucketMu, a.prevBucketSigma), now)
		}
	}
	if b.statusN > 0 {
		a.prevBucketMu, a.prevBucketSigma = b.lastMu, b.lastSigma
	}
}

// checkAnomalies evaluates event-driven anomaly rules. Pair-level rules need a
// pair; bucket-close rules live in finalizeBucket.
func (m *Monitor) checkAnomalies(e Event) {
	if e.Pair == "" {
		return
	}
	ax := m.analytics.Anomalies
	a := m.acc(e.Pair)
	now := e.Time
	switch e.Kind {
	case KindExecuted:
		if e.OneLeg {
			a.consecFails++
			if ax.ConsecutiveFails > 0 && a.consecFails >= ax.ConsecutiveFails {
				m.fireAnomaly(e.Pair, "consecutive_fails", "WARN",
					fmt.Sprintf("%d consecutive one-leg failures", a.consecFails), now)
			}
		} else {
			a.consecFails = 0
		}
		if ax.RescueSlipUSD > 0 && e.RescueCostUSD > ax.RescueSlipUSD {
			m.fireAnomaly(e.Pair, "rescue_slippage", "WARN",
				fmt.Sprintf("rescue slippage %.6f USD above threshold %.6f", e.RescueCostUSD, ax.RescueSlipUSD), now)
		}
	case KindFlip:
		if ax.FlipsWindowMin > 0 && ax.FlipsThreshold > 0 {
			cut := now.Add(-time.Duration(ax.FlipsWindowMin) * time.Minute)
			a.flipTimes = keepAfter(append(a.flipTimes, now), cut)
			if len(a.flipTimes) >= ax.FlipsThreshold {
				m.fireAnomaly(e.Pair, "flip_friction", "WARN",
					fmt.Sprintf("%d direction flips in %d minutes", len(a.flipTimes), ax.FlipsWindowMin), now)
			}
		}
	case KindRiskState:
		if ax.RiskFlapsPerHour > 0 {
			a.riskTransitions = keepAfter(a.riskTransitions, now.Add(-time.Hour))
			if len(a.riskTransitions) >= ax.RiskFlapsPerHour {
				m.fireAnomaly(e.Pair, "risk_flapping", "WARN",
					fmt.Sprintf("%d risk-state transitions in the last hour", len(a.riskTransitions)), now)
			}
		}
	case KindStatus:
		steps := e.Steps
		if steps < 0 {
			steps = -steps
		}
		if ax.FullLoadHeadroom > 0 && ax.FullLoadMinutes > 0 && e.MaxSteps > 0 && steps >= e.MaxSteps-ax.FullLoadHeadroom {
			if a.fullLoadSince.IsZero() {
				a.fullLoadSince = now
			}
			if now.Sub(a.fullLoadSince) >= time.Duration(ax.FullLoadMinutes)*time.Minute {
				m.fireAnomaly(e.Pair, "full_load", "WARN",
					fmt.Sprintf("open steps %d at max_steps %d for over %d minutes", steps, e.MaxSteps, ax.FullLoadMinutes), now)
			}
		} else {
			a.fullLoadSince = time.Time{}
		}
		if ax.OldestStepMinutes > 0 && e.OldestStepAgeS > float64(ax.OldestStepMinutes)*60 {
			m.fireAnomaly(e.Pair, "oldest_step_age", "WARN",
				fmt.Sprintf("oldest open step age %.0fs above %d minutes", e.OldestStepAgeS, ax.OldestStepMinutes), now)
		}
		if ax.SilentMinutes > 0 && !a.lastExec.IsZero() &&
			now.Sub(a.lastExec) >= time.Duration(ax.SilentMinutes)*time.Minute {
			m.fireAnomaly(e.Pair, "silent", "INFO",
				fmt.Sprintf("no execution for over %d minutes", ax.SilentMinutes), now)
		}
		if int(a.oneLeg)-int(a.rescues) > 1 {
			m.fireAnomaly(e.Pair, "unrescued", "WARN",
				fmt.Sprintf("%d one-leg failures not yet rescued", a.oneLeg-a.rescues), now)
		}
	}
}

func keepAfter(ts []time.Time, cut time.Time) []time.Time {
	kept := ts[:0]
	for _, t := range ts {
		if t.After(cut) {
			kept = append(kept, t)
		}
	}
	return kept
}

func (m *Monitor) fireAnomaly(pair, rule, level, msg string, now time.Time) {
	if m.analytics.Cooldown > 0 {
		key := pair + "\x00" + rule
		if last, ok := m.anomalyLast[key]; ok && now.Sub(last) < m.analytics.Cooldown {
			return
		}
		m.anomalyLast[key] = now
	}
	m.anomalies = append(m.anomalies, Anomaly{Time: now.UTC().Format(time.RFC3339), Pair: pair, Rule: rule, Level: level, Msg: msg})
	if len(m.anomalies) > 64 {
		m.anomalies = m.anomalies[len(m.anomalies)-64:]
	}
	m.anomalyAlertMsg = fmt.Sprintf("anomaly %s on %s: %s", rule, pair, msg)
}

func p95(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	rank := int(math.Ceil(0.95 * float64(len(s))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(s) {
		rank = len(s)
	}
	return s[rank-1]
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
	var total AccountRecon
	totalSet := false
	for key, a := range m.pairs {
		pfail := 0.0
		if a.executions > 0 {
			pfail = float64(a.oneLeg) / float64(a.executions)
		}
		pm := PairMetrics{
			Base: a.base, Active: a.active, Mode: a.mode,
			Opens: a.opens, Closes: a.closes, Flips: a.flips, Executions: a.executions,
			OneLegFails: a.oneLeg, Rescues: a.rescues, Errors: a.errors,
			OpenSteps: a.openSteps, VolumeUSD: Fixed8(a.volumeUSD), RealizedUSD: Fixed8(a.realizedUSD),
			MuBps: a.muBps, SigmaBps: a.sigmaBps, TickK: a.tickK, FailRate: pfail,
			Score: a.score, DepthQ: a.depthQ, TickQ: a.tickQ, Stability: a.stability, Reverts: a.reverts,
			RollingFailRate: a.failRate,
			Coverage:        a.coverage, Samples: a.samples, Ready: a.ready,
		}
		if a.reconSet {
			rc := a.recon
			pm.Account = &rc
			total.QtyL += rc.QtyL
			total.QtyR += rc.QtyR
			total.EquityL += rc.EquityL
			total.EquityR += rc.EquityR
			total.TotalEquity += rc.TotalEquity
			total.UnrealL += rc.UnrealL
			total.UnrealR += rc.UnrealR
			total.UnrealTotal += rc.UnrealTotal
			total.LedgerRealized += rc.LedgerRealized
			total.DeltaTotal += rc.DeltaTotal
			total.DeltaUnreal += rc.DeltaUnreal
			total.DeltaRealized += rc.DeltaRealized
			total.FundingEst += rc.FundingEst
			totalSet = true
		}
		pairs[key] = pm
	}
	limit := m.analytics.MetricsBuckets
	if limit <= 0 {
		limit = 96
	}
	type bucketRow struct {
		pair string
		key  int64
	}
	var rows []bucketRow
	for pair, ring := range m.buckets {
		for k := range ring {
			rows = append(rows, bucketRow{pair: pair, key: k})
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].key != rows[j].key {
			return rows[i].key > rows[j].key
		}
		return rows[i].pair < rows[j].pair
	})
	timeBuckets := make([]BucketMetrics, 0, limit)
	for _, r := range rows {
		if len(timeBuckets) >= limit {
			break
		}
		b := m.buckets[r.pair][r.key]
		latAvg := 0.0
		if b.latN > 0 {
			latAvg = b.latSum / float64(b.latN)
		}
		mu, sigma, tickK := 0.0, 0.0, 0.0
		if b.statusN > 0 {
			mu = b.muSum / float64(b.statusN)
			sigma = b.sigmaSum / float64(b.statusN)
			tickK = b.tickKSum / float64(b.statusN)
		}
		failRate := 0.0
		if b.executions > 0 {
			failRate = float64(b.oneLeg) / float64(b.executions)
		}
		timeBuckets = append(timeBuckets, BucketMetrics{
			StartUTC:       time.Unix(r.key, 0).UTC().Format(time.RFC3339),
			Pair:           r.pair,
			Session:        SessionOf(time.Unix(r.key, 0), m.analytics.Sessions),
			Opens:          b.opens,
			Closes:         b.closes,
			Flips:          b.flips,
			Executions:     b.executions,
			OneLegFails:    b.oneLeg,
			Rescues:        b.rescues,
			VolumeUSD:      Fixed8(b.volumeUSD),
			RealizedUSD:    Fixed8(b.realizedUSD),
			LatencyAvgMs:   latAvg,
			LatencyMaxMs:   b.latMax,
			LatencyP95Ms:   p95(b.lats),
			LatencyN:       b.latN,
			MuBps:          mu,
			SigmaBps:       sigma,
			TickK:          tickK,
			FailRate:       failRate,
			StatusSamples:  b.statusN,
			OpenStepsPeak:  b.openStepsPeak,
			OldestStepAgeS: b.oldestStepAgeMax,
			RescueCostUSD:  Fixed8(b.rescueCostUSD),
		})
	}
	out := Metrics{
		UptimeSec:    time.Since(m.start).Seconds(),
		Opens:        m.opens,
		Closes:       m.closes,
		Flips:        m.flips,
		Executions:   m.executions,
		OneLegFails:  m.oneLeg,
		Rescues:      m.rescues,
		Emergencies:  m.emergencies,
		Errors:       m.errors,
		OpenSteps:    m.openSteps,
		VolumeUSD:    Fixed8(m.volumeUSD),
		RealizedUSD:  Fixed8(m.realizedUSD),
		LatencyN:     uint64(m.latN),
		LatencyAvgMs: latAvg,
		LatencyMaxMs: m.latMax,
		MuBps:        m.muBps,
		SigmaBps:     m.sigmaBps,
		TickK:        m.tickK,
		FailRate:     fail,
		Alerts:       append([]string(nil), m.alerts...),
		Pairs:        pairs,
		TimeBuckets:  timeBuckets,
		Anomalies:    append([]Anomaly(nil), m.anomalies...),
	}
	if totalSet {
		out.Account = &total
	}
	return out
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
