package engine

import (
	"math"
	"testing"
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
