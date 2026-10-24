package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestZeroLiquidationRequiresSameSnapshotCrossEvidence(t *testing.T) {
	for _, tc := range []struct {
		name           string
		qty, liq, mark float64
		mode           domain.MarginMode
		value          float64
		cross          *domain.CrossMarginRisk
		floor          float64
		ok             bool
	}{
		{"healthy cross long", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50, Maintenance: 1}, 1, true},
		{"headroom exactly covers value", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 5, Maintenance: 1}, 1, true},
		{"headroom covers floor fraction", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 1.8, Maintenance: 1}, .2, true},
		{"headroom below floor fraction", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 1.7, Maintenance: 1}, .2, false},
		{"equity not above maintenance", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 1, Maintenance: 1}, .2, false},
		{"missing account risk", .04, 0, 99, domain.MarginCross, 4, nil, 1, false},
		{"unknown mode", .04, 0, 99, domain.MarginUnknown, 4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"isolated", .04, 0, 99, domain.MarginIsolated, 4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"short", -.04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"missing snapshot value", .04, 0, 99, domain.MarginCross, 0, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"negative snapshot value", .04, 0, 99, domain.MarginCross, -4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"nan snapshot value", .04, 0, 99, domain.MarginCross, math.NaN(), &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"infinite equity", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: math.Inf(1)}, 1, false},
		{"nan maintenance", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50, Maintenance: math.NaN()}, 1, false},
		{"negative maintenance", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50, Maintenance: -1}, 1, false},
		{"negative equity", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: -1}, 1, false},
		{"insufficient headroom", .04, 0, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 4, Maintenance: 1}, 1, false},
		// A falling book price must not make old, insufficient equity appear
		// sufficient: use the original value=100, not current qty*mark=40.
		{"book fall cannot approve old snapshot", 1, 0, 40, domain.MarginCross, 100, &domain.CrossMarginRisk{Equity: 51, Maintenance: 1}, 1, false},
		{"invalid mark", .04, 0, math.NaN(), domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
		{"negative liquidation", .04, -1, 99, domain.MarginCross, 4, &domain.CrossMarginRisk{Equity: 50}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ratio, ok := remainingPositionMargin(tc.qty, 100, tc.liq, tc.mark, tc.mode, tc.value, tc.cross, tc.floor)
			if ok != tc.ok || ok && math.Abs(ratio-tc.mark/100) > 1e-12 {
				t.Fatalf("ratio=%v usable=%v want usable=%v", ratio, ok, tc.ok)
			}
		})
	}
}

func TestCrossMarginKeepsDistanceThresholdsAndTriggerPriority(t *testing.T) {
	now := time.Now()
	m := marginMonitor{cfg: marginConfig{floor: .2, trigger: .1, hysteresis: .05, refresh: time.Second}, lastL: now, lastR: now,
		qtyL: 1, entryL: 100, modeL: domain.MarginCross, notionalL: 100,
		crossL: &domain.CrossMarginRisk{Equity: 200, Maintenance: 1}}
	for _, tc := range []struct {
		mark float64
		want marginAction
	}{
		{99, marginNone}, {20, marginArm}, {25, marginNone}, {26, marginDisarm}, {10, marginTrigger},
	} {
		if got := m.assessAt(now, tc.mark, 100, true); got != tc.want {
			t.Fatalf("mark=%v action=%v want=%v", tc.mark, got, tc.want)
		}
	}
	// Expired account evidence cannot approve the zero-liq long.
	if got := m.assessAt(now.Add(3*time.Second), 100, 100, true); got != marginArm || m.reason != "position_cache_stale" {
		t.Fatalf("expired evidence: action=%v reason=%s", got, m.reason)
	}
	// A known danger still takes priority when the peer is unknown.
	m.qtyR, m.entryR, m.liqR = -1, 100, 110
	m.crossL = nil
	if got := m.assessAt(now, 100, 109.5, true); got != marginTrigger || m.reason != "liquidation_distance" {
		t.Fatalf("peer risk hidden: action=%v reason=%s", got, m.reason)
	}
	// An exhausted cross account is known danger even without book marks.
	m.crossL = &domain.CrossMarginRisk{Equity: 1, Maintenance: 1}
	if got := m.assessAt(now, 0, 0, false); got != marginTrigger || m.reason != "cross_margin_exhausted" {
		t.Fatalf("exhausted account: action=%v reason=%s", got, m.reason)
	}
}

func TestComputeReconReconcilesRealizedUnrealizedAndFunding(t *testing.T) {
	prev := reconBaseline{total: 100, unreal: 2, realized: -3, book: 90}
	// The right leg carries a spot balance (unified account): its book baseline
	// is spot + book-unrealized, so a lagging exchange mark never leaks into
	// the funding residual.
	snap := reconAccount{QtyL: -0.3, EntryL: 1975.2, QtyR: 0.3, EntryR: 1932.11, EquityL: 455.9, EquityR: 493.65, SpotR: 490}
	unrealL, unrealR, total, dT, dU, dR, funding, book, dB, _ := computeRecon(snap, 1977.2, 1933.11, -3.14, prev, true)
	if math.Abs(unrealL-(-0.6)) > 1e-9 || math.Abs(unrealR-0.3) > 1e-9 {
		t.Fatalf("unrealized wrong: l=%v r=%v", unrealL, unrealR)
	}
	if math.Abs(total-949.55) > 1e-9 || math.Abs(dT-(total-100)) > 1e-9 || math.Abs(dR-(-0.14)) > 1e-9 {
		t.Fatalf("baseline math wrong: total=%v dT=%v dR=%v", total, dT, dR)
	}
	// book = EquityL (no spot) + SpotR + unreal = 455.9 + 490 - 0.3 = 945.6
	if math.Abs(book-945.6) > 1e-9 || math.Abs(dB-(book-90)) > 1e-9 {
		t.Fatalf("book baseline wrong: book=%v dB=%v", book, dB)
	}
	if funding != dB-dR-dU {
		t.Fatalf("funding residual inconsistent: %v != %v", funding, dB-dR-dU)
	}
	// First sample: no baseline, all deltas and the residual are zero.
	_, _, _, dT2, dU2, dR2, fund2, _, dB2, _ := computeRecon(snap, 1977.2, 1933.11, -3.14, reconBaseline{}, false)
	if dT2 != 0 || dU2 != 0 || dR2 != 0 || fund2 != 0 || dB2 != 0 {
		t.Fatalf("uninitialized baseline must zero deltas: %v %v %v %v %v", dT2, dU2, dR2, fund2, dB2)
	}
}

type crossSnapshotVenue struct {
	*actorTestVenue
	snapshot      domain.AccountSnapshot
	err           error
	snapshotCalls int
	positionCalls int
}

func (v *crossSnapshotVenue) AccountSnapshot(context.Context) (domain.AccountSnapshot, error) {
	v.snapshotCalls++
	return v.snapshot, v.err
}

func (v *crossSnapshotVenue) Positions(context.Context) ([]domain.Position, error) {
	v.positionCalls++
	return v.snapshot.Positions, nil
}

func TestMarginRefreshReplacesEvidenceWithPositionsAtomically(t *testing.T) {
	pos := domain.Position{MarketID: 1, Qty: 1, AvgEntry: 100, MarginMode: domain.MarginCross, Notional: 100}
	risk := &domain.CrossMarginRisk{Equity: 200, Maintenance: 1}
	l := &crossSnapshotVenue{actorTestVenue: newActorTestVenue("L"), snapshot: domain.AccountSnapshot{Positions: []domain.Position{pos}, CrossMargin: risk}}
	r := newActorTestVenue("R")
	m := marginMonitor{cfg: marginConfig{floor: .2, trigger: .1, refresh: time.Second}}
	m.refresh(context.Background(), l, r, 1, 1)
	if l.snapshotCalls != 1 || l.positionCalls != 0 || m.crossL == nil || m.crossL == risk {
		t.Fatalf("snapshot not copied atomically: calls=%d/%d risk=%+v", l.snapshotCalls, l.positionCalls, m.crossL)
	}
	if got := m.assessAt(time.Now(), 100, 100, true); got != marginNone {
		t.Fatalf("healthy snapshot action=%v", got)
	}
	// A failed snapshot cannot fall back to another request and combine data.
	last := m.lastL
	l.err = errors.New("snapshot unavailable")
	m.refresh(context.Background(), l, r, 1, 1)
	if m.lastL != last || l.positionCalls != 0 || m.crossL.Equity != 200 {
		t.Fatal("failed snapshot mixed or replaced evidence")
	}
	// A successful refresh with missing risk must discard the old proof.
	l.err, l.snapshot.CrossMargin = nil, nil
	m.refresh(context.Background(), l, r, 1, 1)
	if m.crossL != nil {
		t.Fatal("fresh positions retained stale risk")
	}
	if got := m.assessAt(time.Now(), 100, 100, true); got != marginArm {
		t.Fatalf("missing proof did not block: %v", got)
	}
}
