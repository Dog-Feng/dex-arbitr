package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestRemainingMargin(t *testing.T) {
	cases := []struct {
		name                  string
		qty, entry, liq, mark float64
		want                  float64
		ok                    bool
	}{
		{"long healthy", 1, 100, 90, 99, 0.9, true},
		{"long at liq", 1, 100, 90, 90, 0, true},
		{"long through liq", 1, 100, 90, 89, 0, true},
		{"short healthy", -1, 100, 110, 108, 0.2, true},
		{"short at liq", -1, 100, 110, 110, 0, true},
		{"flat", 0, 100, 90, 99, 0, false},
		{"no liq", 1, 100, 0, 99, 0, false},
		{"invalid long", 1, 100, 110, 99, 0, false},
		{"invalid quantity", math.NaN(), 100, 110, 99, 0, false},
		{"invalid mark", 1, 100, 90, math.Inf(1), 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := remainingMargin(c.qty, c.entry, c.liq, c.mark)
			if ok != c.ok || (ok && math.Abs(got-c.want) > 1e-9) {
				t.Fatalf("remainingMargin=%v,%v want %v,%v", got, ok, c.want, c.ok)
			}
		})
	}
}

func TestMarginMonitorAssess(t *testing.T) {
	m := marginMonitor{cfg: marginConfig{floor: 0.2, trigger: 0.1, hysteresis: 0.05}, have: true}
	m.qtyL, m.entryL, m.liqL = 1, 100, 90
	m.qtyR, m.entryR, m.liqR = -1, 100, 110

	if got := m.assess(99, 104, true); got != marginNone { // L 0.9, R 0.6
		t.Fatalf("none case got %v", got)
	}
	if got := m.assess(91.5, 104, true); got != marginArm { // L 0.15 remaining
		t.Fatalf("arm case got %v", got)
	}
	if got := m.assess(91, 104, true); got != marginTrigger { // L 0.1 remaining
		t.Fatalf("trigger case got %v", got)
	}
	m.armed = true
	if got := m.assess(99, 104, true); got != marginDisarm { // recovered
		t.Fatalf("disarm case got %v", got)
	}
	if got := m.assess(99, 104, false); got != marginNone { // stale books
		t.Fatalf("stale case got %v", got)
	}
}

func TestMarginMonitorNotionalCap(t *testing.T) {
	m := marginMonitor{cfg: marginConfig{floor: 0.2, trigger: 0.1, hysteresis: 0.05, maxNotional: 1000}, have: true}
	m.qtyL, m.entryL, m.liqL = 1, 100, 90
	m.qtyR, m.entryR, m.liqR = -1, 100, 110
	m.aggL, m.aggR = 500, 1200 // margin healthy but R account notional over cap
	if got := m.assess(99, 104, true); got != marginArm {
		t.Fatalf("notional cap should arm, got %v", got)
	}
}

func TestMarginTriggerPrecedesAccountCapAndMissingPeer(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name  string
		cap   bool
		peer  bool
		markR float64
	}{
		{name: "over account cap", cap: true, peer: true, markR: 104},
		{name: "missing peer positions", peer: false, markR: 104},
		{name: "both", cap: true, peer: false, markR: 104},
		{name: "missing peer mark", peer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := marginMonitor{cfg: marginConfig{floor: .2, trigger: .1, refresh: time.Second, maxNotional: 1000}}
			m.qtyL, m.entryL, m.liqL, m.lastL = 1, 100, 90, now
			if tc.peer {
				m.lastR = now
				m.qtyR, m.entryR, m.liqR = -1, 100, 110
			}
			if tc.cap {
				m.aggR = 1200
			}
			if got := m.assessAt(now, 90.5, tc.markR, true); got != marginTrigger {
				t.Fatalf("known liquidation risk must trigger first, got %v", got)
			}
		})
	}
}

func TestMarginRefreshRetainsSuccessfulLegWhenPeerFails(t *testing.T) {
	l, r := newActorTestVenue("L"), newActorTestVenue("R")
	l.positions = func(context.Context) ([]domain.Position, error) {
		return []domain.Position{{MarketID: 1, Qty: 1, AvgEntry: 100, LiquidationPrice: 94.8}}, nil
	}
	r.positions = func(context.Context) ([]domain.Position, error) {
		return nil, errors.New("positions unavailable")
	}
	m := marginMonitor{cfg: marginConfig{floor: .2, trigger: .1, refresh: time.Second}}
	m.qtyL, m.entryL, m.liqL = 1, 100, 90
	m.lastL, m.lastR = time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)
	m.refresh(context.Background(), l, r, 1, 1)
	if m.liqL != 94.8 || time.Since(m.lastL) > time.Second {
		t.Fatalf("successful left update discarded: liq=%v time=%v", m.liqL, m.lastL)
	}
	if got := m.assessAt(time.Now(), 95, 100, true); got != marginTrigger {
		t.Fatalf("new left liquidation risk hidden by peer error, got %v", got)
	}
}

func TestMarginExpiredCacheBlocksAdmission(t *testing.T) {
	now := time.Now()
	for _, age := range []time.Duration{0, 3 * time.Second} {
		t.Run(age.String(), func(t *testing.T) {
			m := marginMonitor{cfg: marginConfig{floor: .2, trigger: .1, refresh: time.Second}}
			m.lastL, m.lastR = now.Add(-age), now.Add(-age)
			m.qtyL, m.entryL, m.liqL = 1, 100, 90
			want := marginNone
			if age > 2*time.Second {
				want = marginArm
			}
			if got := m.assessAt(now, 99, 104, true); got != want {
				t.Fatalf("age %v: action=%v want %v", age, got, want)
			}
		})
	}
}
