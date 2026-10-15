package engine

import (
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

func testAdaptiveCfg() adaptiveLatencyCfg {
	return adaptiveLatencyCfg{enabled: true, minSamples: 8, ringN: 32,
		windowMin: 200 * time.Millisecond, windowMax: 3 * time.Second,
		coverageK: 1.3, updateInterval: time.Millisecond}
}

func TestLatencySeriesColdStartAndStats(t *testing.T) {
	s := newLatencySeries(32)
	if slots, ok := targetWindowSlots(s, testAdaptiveCfg(), 100*time.Millisecond); ok || slots != 0 {
		t.Fatalf("cold start produced a window: %d %v", slots, ok)
	}
	for i := 0; i < 8; i++ {
		s.add(750 * time.Millisecond)
	}
	slots, ok := targetWindowSlots(s, testAdaptiveCfg(), 100*time.Millisecond)
	if !ok || slots != 8 {
		t.Fatalf("constant latency window=%d ok=%v, want 8", slots, ok)
	}
	mean, std, ok := s.stats()
	if !ok || mean != 750*time.Millisecond || std != 0 {
		t.Fatalf("stats mean=%v std=%v ok=%v", mean, std, ok)
	}
}

func TestTargetWindowSlotsClamps(t *testing.T) {
	s := newLatencySeries(32)
	for i := 0; i < 8; i++ {
		s.add(10 * time.Millisecond)
	}
	slots, ok := targetWindowSlots(s, testAdaptiveCfg(), 100*time.Millisecond)
	if !ok || slots != 2 {
		t.Fatalf("window_min clamp failed: %d %v", slots, ok)
	}
	for i := 0; i < 8; i++ {
		s.add(10 * time.Second)
	}
	slots, ok = targetWindowSlots(s, testAdaptiveCfg(), 100*time.Millisecond)
	if !ok || slots != 30 {
		t.Fatalf("window_max clamp failed: %d %v", slots, ok)
	}
}

func TestApplyAdaptiveWindowsWritesEstimators(t *testing.T) {
	a := &PairActor{cfg: PairConfig{
		Vol:             volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute, MaxHorizon: time.Second}),
		VolL:            volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute, MaxHorizon: time.Second}),
		VolR:            volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute, MaxHorizon: time.Second}),
		VolEvery:        100 * time.Millisecond,
		AdaptiveLatency: adaptiveLatencyCfg{enabled: true, minSamples: 4, ringN: 32, windowMin: 200 * time.Millisecond, windowMax: time.Second, coverageK: 1.3, updateInterval: time.Millisecond},
	}}
	a.latL, a.latR = newLatencySeries(32), newLatencySeries(32)
	for i := 0; i < 4; i++ {
		a.latL.add(300 * time.Millisecond)
		a.latR.add(900 * time.Millisecond)
	}
	a.applyAdaptiveWindows(time.Now())
	if a.cfg.VolL.Window() != 3 || a.cfg.VolR.Window() != 9 || a.cfg.Vol.Window() != 9 {
		t.Fatalf("windows L=%d R=%d spread=%d, want 3/9/9",
			a.cfg.VolL.Window(), a.cfg.VolR.Window(), a.cfg.Vol.Window())
	}
}

func TestAdaptiveWindowsDisabledIsNoop(t *testing.T) {
	a := &PairActor{cfg: PairConfig{
		Vol:      volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}),
		VolL:     volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}),
		VolR:     volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute}),
		VolEvery: 100 * time.Millisecond,
	}}
	a.latL, a.latR = newLatencySeries(32), newLatencySeries(32)
	for i := 0; i < 8; i++ {
		a.latL.add(900 * time.Millisecond)
		a.latR.add(900 * time.Millisecond)
	}
	a.applyAdaptiveWindows(time.Now())
	if a.cfg.Vol.Window() != 5 || a.cfg.VolL.Window() != 5 || a.cfg.VolR.Window() != 5 {
		t.Fatalf("disabled adaptation changed windows: %d/%d/%d",
			a.cfg.Vol.Window(), a.cfg.VolL.Window(), a.cfg.VolR.Window())
	}
}
