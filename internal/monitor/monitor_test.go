package monitor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestMonitorAggregates(t *testing.T) {
	m := New(nil)
	m.Record(Event{Kind: KindStatus, Steps: 0, MuBps: 2.2, SigmaBps: 0.36, TickK: 2.5, FailRate: 0})
	m.Record(Event{Kind: KindStepOpened})
	m.Record(Event{Kind: KindExecuted, USD: 31, LatencyMs: 320})
	m.Record(Event{Kind: KindExecuted, USD: 31, LatencyMs: 340, OneLeg: true})
	m.Record(Event{Kind: KindPnL, USD: 0.1})
	m.Record(Event{Kind: KindStepClosed})
	g := m.Metrics()
	if g.Opens != 1 || g.Closes != 1 || g.Executions != 2 || g.OneLegFails != 1 {
		t.Fatalf("%+v", g)
	}
	if g.OpenSteps != 0 || g.VolumeUSD != 62 || g.RealizedUSD != 0.1 {
		t.Fatalf("%+v", g)
	}
	if g.LatencyAvgMs != 330 || g.LatencyMaxMs != 340 {
		t.Fatalf("latency %+v", g)
	}
	if g.FailRate != 0.5 {
		t.Fatalf("fail rate %v", g.FailRate)
	}
}

func TestMonitorAlerts(t *testing.T) {
	var got []string
	m := New(func(msg string) { got = append(got, msg) })
	m.Record(Event{Kind: KindEmergency, Pair: "SPY"})
	m.Record(Event{Kind: KindError, Pair: "SPY", Msg: "hedge broken"})
	if len(got) != 2 {
		t.Fatalf("alerts %v", got)
	}
	if g := m.Metrics(); g.Emergencies != 1 || g.Errors != 1 {
		t.Fatalf("%+v", g)
	}
}

func TestMonitorPerPair(t *testing.T) {
	m := New(nil)
	m.Record(Event{Kind: KindPairPromoted, Pair: "SPY", Score: 3.2})
	m.Record(Event{Kind: KindExecuted, Pair: "SPY", USD: 31, OneLeg: true})
	m.Record(Event{Kind: KindPairHealth, Pair: "SPY", Score: 2.1, DepthQ: 0.9, TickQ: 1, Stability: 0.8, Reverts: 4})
	m.Record(Event{Kind: KindPairWarmup, Pair: "QQQ", Samples: 3600, Coverage: 0.5, Ready: false})
	g := m.Metrics()
	spy, ok := g.Pairs["SPY"]
	if !ok {
		t.Fatalf("missing SPY pair metrics: %+v", g.Pairs)
	}
	if !spy.Active || spy.Executions != 1 || spy.OneLegFails != 1 {
		t.Fatalf("SPY pair metrics: %+v", spy)
	}
	if spy.FailRate != 1 || spy.Score != 2.1 || spy.DepthQ != 0.9 {
		t.Fatalf("SPY pair gauges: %+v", spy)
	}
	if qqq, ok := g.Pairs["QQQ"]; !ok || qqq.Samples != 3600 || qqq.Ready {
		t.Fatalf("QQQ pair warmup: %+v", g.Pairs["QQQ"])
	}
}

func TestHandlerAuth(t *testing.T) {
	m := New(nil)
	h := m.Handler("secret")

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("no token => %d, want 401", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("bearer token => %d, want 200", rr.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics?token=secret", nil)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("query token => %d, want 200", rr.Code)
	}
}

func TestPnLIncludesFeesAndPartialSettlementOnce(t *testing.T) {
	m := New(nil)
	m.Record(Event{Pair: "BTC_L_R", Kind: KindStepOpened})
	m.Record(Event{Pair: "BTC_L_R", Kind: KindPnL, USD: -1})
	m.Record(Event{Pair: "BTC_L_R", Kind: KindPnL, USD: 3}) // partial close
	if g := m.Metrics(); g.RealizedUSD != 2 || g.Closes != 0 || g.OpenSteps != 1 {
		t.Fatalf("partial settlement: %+v", g)
	}
	m.Record(Event{Pair: "BTC_L_R", Kind: KindPnL, USD: 4})
	m.Record(Event{Pair: "BTC_L_R", Kind: KindStepClosed, USD: 99}) // step event is not a second PnL delta
	m.Record(Event{Pair: "ETH_L_R", Kind: KindPnL, USD: -2})        // failed opening still incurs fees
	g := m.Metrics()
	if g.RealizedUSD != 4 || g.Pairs["BTC_L_R"].RealizedUSD != 6 || g.Pairs["ETH_L_R"].RealizedUSD != -2 || g.OpenSteps != 0 || g.Closes != 1 {
		t.Fatalf("settlement counted incorrectly: %+v", g)
	}
}

func TestStatusSumsAbsoluteStepsAndPreservesModes(t *testing.T) {
	m := New(nil)
	m.Record(Event{Pair: "BTC_L_R", Kind: KindStatus, Steps: -2, Mode: "REDUCE_ONLY", FailRate: 0.25})
	m.Record(Event{Pair: "ETH_L_R", Kind: KindStatus, Steps: 3, Mode: "RUNNING", FailRate: 0.5})
	m.Record(Event{Pair: "BTC_L_R", Kind: KindExecuted, OneLeg: true})
	g := m.Metrics()
	if g.OpenSteps != 5 || g.Pairs["BTC_L_R"].OpenSteps != 2 || g.Pairs["BTC_L_R"].Mode != "REDUCE_ONLY" || g.Pairs["ETH_L_R"].Mode != "RUNNING" {
		t.Fatalf("per-pair status clobbered global state: %+v", g)
	}
	if g.Pairs["BTC_L_R"].FailRate != 1 || g.Pairs["BTC_L_R"].RollingFailRate != 0.25 || m.RollingFailRate("BTC_L_R") != 0.25 {
		t.Fatalf("lifetime and rolling failure rates confused: %+v", g.Pairs["BTC_L_R"])
	}
	m.Record(Event{Pair: "BTC_L_R", Kind: KindStepClosed})
	m.Record(Event{Pair: "ETH_L_R", Kind: KindStatus, Steps: 0, Mode: "PAUSED"})
	if g := m.Metrics(); g.OpenSteps != 1 || g.Pairs["ETH_L_R"].Mode != "PAUSED" {
		t.Fatalf("close/status did not update aggregate: %+v", g)
	}
	m.Record(Event{Pair: "ETH_L_R", Kind: KindStepClosed})
	if g := m.Metrics(); g.OpenSteps != 1 || g.Pairs["ETH_L_R"].OpenSteps < 0 {
		t.Fatalf("close without ledger produced negative step count: %+v", g)
	}
}

func TestAlertCallbackCanReadAndRecord(t *testing.T) {
	var m *Monitor
	var count int
	m = New(func(msg string) {
		if msg == "" || m.Metrics().Errors != 1 {
			t.Errorf("callback sees uncommitted alert: %q", msg)
		}
		m.Record(Event{Pair: "BTC", Kind: KindStatus, Mode: "PAUSED"})
		count++
	})
	m.Record(Event{Pair: "BTC", Kind: KindError, Msg: "unresolved"})
	if count != 1 || m.Metrics().Pairs["BTC"].Mode != "PAUSED" {
		t.Fatal("alert callback did not complete")
	}
}

func TestAlertsRetainedWithoutCallbackAndSnapshotsIsolated(t *testing.T) {
	m := New(nil)
	for i := range 70 {
		m.Record(Event{Kind: KindError, Pair: "BTC", Msg: fmt.Sprintf("error %d", i)})
	}
	g := m.Metrics()
	if len(g.Alerts) != 64 || g.Alerts[0] != "error on BTC: error 6" {
		t.Fatalf("alert history not bounded/retained: %+v", g.Alerts)
	}
	g.Alerts[0] = "mutated"
	delete(g.Pairs, "BTC")
	if again := m.Metrics(); again.Alerts[0] == "mutated" || len(again.Pairs) != 1 {
		t.Fatal("snapshot exposes mutable monitor state")
	}
}

func TestHandlerRejectsMalformedAndAmbiguousCredentials(t *testing.T) {
	h := New(nil).Handler("secret")
	for _, tc := range []struct {
		name, query string
		headers     []string
		want        int
	}{
		{"raw token is not bearer", "", []string{"secret"}, http.StatusUnauthorized},
		{"wrong auth scheme", "", []string{"Basic secret"}, http.StatusUnauthorized},
		{"case insensitive bearer", "", []string{"bearer secret"}, http.StatusOK},
		{"duplicate headers", "", []string{"Bearer secret", "Bearer secret"}, http.StatusUnauthorized},
		{"extra spaces", "", []string{"Bearer  secret"}, http.StatusUnauthorized},
		{"duplicate query tokens", "?token=secret&token=secret", nil, http.StatusUnauthorized},
		{"bad header cannot fall back to query", "?token=secret", []string{"Basic secret"}, http.StatusUnauthorized},
		{"empty token", "?token=", nil, http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/metrics"+tc.query, nil)
			for _, value := range tc.headers {
				r.Header.Add("Authorization", value)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.want {
				t.Fatalf("status=%d, want %d", w.Code, tc.want)
			}
		})
	}
}

func TestConcurrentRecordingAndSnapshots(t *testing.T) {
	m := New(nil)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pair := fmt.Sprintf("pair%d", i)
			for range 20 {
				m.Record(Event{Pair: pair, Kind: KindExecuted, USD: 10})
				m.Record(Event{Pair: pair, Kind: KindStatus, Steps: -2, Mode: "RUNNING"})
				if m.Metrics().OpenSteps < 0 || m.RollingFailRate(pair) < 0 {
					t.Error("invalid concurrent snapshot")
				}
			}
		}()
	}
	wg.Wait()
	if g := m.Metrics(); g.Executions != 160 || g.VolumeUSD != 1600 || g.OpenSteps != 16 {
		t.Fatalf("lost concurrent events: %+v", g)
	}
}

func TestFlipCounter(t *testing.T) {
	m := New(nil)
	m.Record(Event{Pair: "A", Kind: KindFlip, Dir: "SHORT_L/LONG_R"})
	m.Record(Event{Pair: "B", Kind: KindFlip, Dir: "LONG_L/SHORT_R"})
	g := m.Metrics()
	if g.Flips != 2 || g.Pairs["A"].Flips != 1 || g.Pairs["B"].Flips != 1 {
		t.Fatalf("flip counters wrong: %+v", g)
	}
}

func TestFixed8MarshalsFixedDecimals(t *testing.T) {
	for _, tc := range []struct {
		in   Fixed8
		want string
	}{
		{2.2737367544323206e-15, "0.00000000"},
		{123.804, "123.80400000"},
		{-0.0072, "-0.00720000"},
	} {
		b, err := tc.in.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tc.want {
			t.Fatalf("Fixed8(%v) = %s, want %s", tc.in, b, tc.want)
		}
	}
}

func testAnalytics() AnalyticsCfg {
	return AnalyticsCfg{
		Bucket:        15 * time.Minute,
		RetainBuckets: 672,
		Cooldown:      0,
		Anomalies: AnomalyCfg{
			ConsecutiveFails:  3,
			FlipsWindowMin:    10,
			FlipsThreshold:    3,
			FullLoadHeadroom:  2,
			FullLoadMinutes:   30,
			OldestStepMinutes: 60,
			LossBucketCount:   3,
			LossPerBucketUSD:  0.05,
			RescueSlipUSD:     0.01,
			LatencyP95Ms:      600,
			SilentMinutes:     30,
			RiskFlapsPerHour:  8,
			MuJumpSigma:       3,
		},
	}
}

func TestTimeBucketsAggregateAndRoll(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	m.Record(Event{Time: t0, Pair: "A", Kind: KindStepOpened})
	m.Record(Event{Time: t0, Pair: "A", Kind: KindExecuted, USD: 31, LatencyLMs: 300, LatencyRMs: 340, OneLeg: true})
	m.Record(Event{Time: t0, Pair: "A", Kind: KindPnL, USD: -0.005})
	m.Record(Event{Time: t0.Add(16 * time.Minute), Pair: "A", Kind: KindStepClosed})
	g := m.Metrics()
	if len(g.TimeBuckets) != 4 {
		t.Fatalf("buckets=%d, want 4 (2 buckets x global+pair)", len(g.TimeBuckets))
	}
	var globalFirst *BucketMetrics
	for i := range g.TimeBuckets {
		if g.TimeBuckets[i].Pair == "" && g.TimeBuckets[i].StartUTC == "2026-10-16T09:00:00Z" {
			globalFirst = &g.TimeBuckets[i]
		}
	}
	if globalFirst == nil {
		t.Fatal("missing global bucket row")
	}
	if globalFirst.Opens != 1 || globalFirst.Executions != 1 || globalFirst.OneLegFails != 1 ||
		globalFirst.VolumeUSD != 31 || globalFirst.RealizedUSD != -0.005 {
		t.Fatalf("global bucket wrong: %+v", globalFirst)
	}
	if globalFirst.LatencyAvgMs != 320 || globalFirst.LatencyP95Ms != 340 || globalFirst.LatencyMaxMs != 340 {
		t.Fatalf("latency aggregates wrong: %+v", globalFirst)
	}
}

func TestAnomalyConsecutiveFails(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		m.Record(Event{Time: t0.Add(time.Duration(i) * time.Second), Pair: "A", Kind: KindExecuted, USD: 1, OneLeg: true})
	}
	if len(m.Metrics().Anomalies) != 1 || m.Metrics().Anomalies[0].Rule != "consecutive_fails" {
		t.Fatalf("anomalies=%+v", m.Metrics().Anomalies)
	}
}

func TestAnomalyFlipFriction(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		m.Record(Event{Time: t0.Add(time.Duration(i) * time.Minute), Pair: "A", Kind: KindFlip})
	}
	found := false
	for _, a := range m.Metrics().Anomalies {
		if a.Rule == "flip_friction" {
			found = true
		}
	}
	if !found {
		t.Fatalf("flip friction not fired: %+v", m.Metrics().Anomalies)
	}
}

func TestAnomalyFullLoadAndOldestAgeAndSilent(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	m.Record(Event{Time: t0, Pair: "A", Kind: KindExecuted, USD: 1})
	for i := 0; i <= 30; i++ {
		at := t0.Add(time.Duration(i) * time.Minute)
		m.Record(Event{Time: at, Pair: "A", Kind: KindStatus, Steps: 13, MaxSteps: 15, OldestStepAgeS: 7200})
	}
	got := map[string]bool{}
	for _, a := range m.Metrics().Anomalies {
		got[a.Rule] = true
	}
	if !got["full_load"] || !got["oldest_step_age"] || !got["silent"] {
		t.Fatalf("expected full_load/oldest_step_age/silent, got %+v", m.Metrics().Anomalies)
	}
}

func TestAnomalyLossBucketsAndMuJump(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 4; i++ {
		at := t0.Add(time.Duration(i) * 15 * time.Minute)
		m.Record(Event{Time: at, Pair: "A", Kind: KindPnL, USD: -0.1})
		m.Record(Event{Time: at, Pair: "A", Kind: KindStatus, MuBps: float64(i) * 5, SigmaBps: 1})
	}
	got := map[string]bool{}
	for _, a := range m.Metrics().Anomalies {
		got[a.Rule] = true
	}
	if !got["loss_buckets"] || !got["mu_jump"] {
		t.Fatalf("expected loss_buckets/mu_jump, got %+v", m.Metrics().Anomalies)
	}
}

func TestAnomalyRiskFlappingAndRescueSlip(t *testing.T) {
	m := New(nil)
	m.analytics = testAnalytics()
	t0 := time.Date(2026, 10, 16, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 8; i++ {
		m.Record(Event{Time: t0.Add(time.Duration(i) * time.Minute), Pair: "A", Kind: KindRiskState, Msg: "position_risk_unknown"})
	}
	m.Record(Event{Time: t0.Add(9 * time.Minute), Pair: "A", Kind: KindExecuted, USD: 1, RescueCostUSD: 0.02})
	got := map[string]bool{}
	for _, a := range m.Metrics().Anomalies {
		got[a.Rule] = true
	}
	if !got["risk_flapping"] || !got["rescue_slippage"] {
		t.Fatalf("expected risk_flapping/rescue_slippage, got %+v", m.Metrics().Anomalies)
	}
}
