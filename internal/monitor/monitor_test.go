package monitor

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMonitorAggregates(t *testing.T) {
	m := New(nil)
	m.Record(Event{Kind: KindStatus, Steps: 0, MuBps: 2.2, SigmaBps: 0.36, TickK: 2.5, FailRate: 0})
	m.Record(Event{Kind: KindStepOpened})
	m.Record(Event{Kind: KindExecuted, USD: 31, LatencyMs: 320})
	m.Record(Event{Kind: KindExecuted, USD: 31, LatencyMs: 340, OneLeg: true})
	m.Record(Event{Kind: KindStepClosed, USD: 0.1})
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
