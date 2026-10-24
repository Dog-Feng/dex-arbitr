package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// marginConfig configures the liquidation-risk monitor.
type marginConfig struct {
	floor       float64
	trigger     float64
	hysteresis  float64
	refresh     time.Duration
	marketBps   float64
	maxNotional float64
	// maxAutoResume caps how many external-intervention recoveries may
	// auto-resume trading per 24h window before the pair stays paused.
	maxAutoResume int
}

// marginAction is the decision the monitor asks the actor to apply.
type marginAction int

const (
	marginNone marginAction = iota
	marginArm
	marginDisarm
	marginTrigger
)

// marginMonitor tracks the remaining-margin ratio of both legs from cached
// liquidation prices (REST refresh) and mark prices (book mid). It is owned by
// one risk watcher goroutine, so no locking is needed.
type marginMonitor struct {
	cfg          marginConfig
	armed        bool
	mu           sync.Mutex // guards fields copied by the reconciliation log
	lastRefresh  time.Time
	lastL, lastR time.Time

	have                 bool
	liqL, liqR           float64
	entryL, entryR       float64
	qtyL, qtyR           float64
	accountValueL        float64
	accountValueR        float64
	aggL, aggR           float64
	modeL, modeR         domain.MarginMode
	notionalL, notionalR float64
	crossL, crossR       *domain.CrossMarginRisk
	reason               string
}

// refresh updates each successful venue independently. A failed peer must not
// discard liquidation data that was successfully read from the other venue.
func (m *marginMonitor) refresh(ctx context.Context, l, r exchange.Venue, leftID, rightID int) {
	m.lastRefresh = time.Now()
	pl, aggL, crossL, avL, errL := venueMarginSnapshot(ctx, l, leftID)
	pr, aggR, crossR, avR, errR := venueMarginSnapshot(ctx, r, rightID)
	m.mu.Lock()
	defer m.mu.Unlock()
	if errL == nil {
		m.liqL, m.entryL, m.qtyL, m.aggL = pl.LiquidationPrice, pl.AvgEntry, pl.Qty, aggL
		m.modeL, m.notionalL, m.crossL = pl.MarginMode, pl.Notional, crossL
		m.accountValueL = avL
		if m.accountValueL <= 0 && crossL != nil {
			m.accountValueL = crossL.Equity
		}
		m.lastL = time.Now()
	} else {
		slog.Warn("margin positions refresh", "venue", l.Name(), "err", errL)
	}
	if errR == nil {
		m.liqR, m.entryR, m.qtyR, m.aggR = pr.LiquidationPrice, pr.AvgEntry, pr.Qty, aggR
		m.modeR, m.notionalR, m.crossR = pr.MarginMode, pr.Notional, crossR
		m.accountValueR = avR
		if m.accountValueR <= 0 && crossR != nil {
			m.accountValueR = crossR.Equity
		}
		m.lastR = time.Now()
	} else {
		slog.Warn("margin positions refresh", "venue", r.Name(), "err", errR)
	}
	m.have = !m.lastL.IsZero() || !m.lastR.IsZero()
}

// reconAccount is the read-only account view the per-minute reconciliation
// log copies. It is safe to call from the actor loop while watchRisk refreshes.
type reconAccount struct {
	QtyL, QtyR       float64
	EntryL, EntryR   float64
	EquityL, EquityR float64
}

func (m *marginMonitor) reconAccount() reconAccount {
	m.mu.Lock()
	defer m.mu.Unlock()
	return reconAccount{QtyL: m.qtyL, QtyR: m.qtyR, EntryL: m.entryL, EntryR: m.entryR,
		EquityL: m.accountValueL, EquityR: m.accountValueR}
}

// reconBaseline is the previous minute's reconciliation snapshot.
type reconBaseline struct {
	total, unreal, realized float64
}

// computeRecon derives the per-minute reconciliation numbers. funding is the
// residual account-value change not explained by ledger-realized or
// unrealized moves (funding payments, fee rebates, and the unexplained rest).
func computeRecon(snap reconAccount, markL, markR, realized float64, prev reconBaseline, initialized bool) (unrealL, unrealR, total, deltaTotal, deltaUnreal, deltaRealized, funding float64, next reconBaseline) {
	if snap.QtyL != 0 && snap.EntryL > 0 && markL > 0 {
		unrealL = snap.QtyL * (markL - snap.EntryL)
	}
	if snap.QtyR != 0 && snap.EntryR > 0 && markR > 0 {
		unrealR = snap.QtyR * (markR - snap.EntryR)
	}
	unreal := unrealL + unrealR
	total = snap.EquityL + snap.EquityR
	if initialized {
		deltaTotal = total - prev.total
		deltaUnreal = unreal - prev.unreal
		deltaRealized = realized - prev.realized
	}
	funding = deltaTotal - deltaRealized - deltaUnreal
	return unrealL, unrealR, total, deltaTotal, deltaUnreal, deltaRealized, funding, reconBaseline{total: total, unreal: unreal, realized: realized}
}

// assess decides the action from the current mark prices; fresh=false means
// no mark is usable and no assessment can be made. A zero mark represents an
// unavailable leg; it must not hide a known liquidation trigger on its peer.
func (m *marginMonitor) assess(markL, markR float64, fresh bool) marginAction {
	return m.assessAt(time.Now(), markL, markR, fresh)
}

func (m *marginMonitor) assessAt(now time.Time, markL, markR float64, fresh bool) marginAction {
	m.reason = "healthy"
	validL, validR := m.cacheFresh(now, m.lastL), m.cacheFresh(now, m.lastR)
	// Hand-built monitors with no refresh interval are used by pure decision
	// tests. Production monitors always track individual successful reads.
	if m.cfg.refresh <= 0 && m.have && m.lastL.IsZero() && m.lastR.IsZero() {
		validL, validR = true, true
	}
	// A known exhausted cross account is actionable without a book mark or a
	// usable peer. Do not let missing liquidation data mask that evidence.
	if (validL && crossMarginExhausted(m.qtyL, m.modeL, m.crossL)) ||
		(validR && crossMarginExhausted(m.qtyR, m.modeR, m.crossR)) {
		m.armed = false
		m.reason = "cross_margin_exhausted"
		return marginTrigger
	}
	if !fresh {
		m.reason = "mark_unavailable"
		return marginNone
	}
	minR := 1.0
	considered := false
	unknown := !validL || !validR
	if r, ok := remainingPositionMargin(m.qtyL, m.entryL, m.liqL, markL, m.modeL, m.notionalL, m.crossL, m.cfg.floor); ok && validL {
		considered = true
		if r < minR {
			minR = r
		}
	} else if validL && m.qtyL != 0 {
		unknown = true
	}
	if r, ok := remainingPositionMargin(m.qtyR, m.entryR, m.liqR, markR, m.modeR, m.notionalR, m.crossR, m.cfg.floor); ok && validR {
		considered = true
		if r < minR {
			minR = r
		}
	} else if validR && m.qtyR != 0 {
		unknown = true
	}
	// Emergency liquidation protection takes precedence over admission caps
	// and missing peer data.
	if considered && minR <= m.cfg.trigger {
		m.armed = false
		m.reason = "liquidation_distance"
		return marginTrigger
	}
	if unknown {
		m.reason = "position_risk_unknown"
		if !validL || !validR {
			m.reason = "position_cache_stale"
		}
		m.armed = true
		return marginArm
	}
	if m.cfg.maxNotional > 0 && (m.aggL > m.cfg.maxNotional || m.aggR > m.cfg.maxNotional) {
		m.reason = "account_notional_cap"
		m.armed = true
		return marginArm
	}
	if !considered {
		if m.armed {
			m.armed = false
			return marginDisarm
		}
		return marginNone
	}
	switch {
	case minR <= m.cfg.floor:
		m.reason = "margin_floor"
		m.armed = true
		return marginArm
	case m.armed && minR > m.cfg.floor+m.cfg.hysteresis:
		m.armed = false
		return marginDisarm
	}
	if m.armed {
		m.reason = "margin_hysteresis"
	}
	return marginNone
}

func (m *marginMonitor) cacheFresh(now, last time.Time) bool {
	if last.IsZero() {
		return false
	}
	maxAge := 2 * m.cfg.refresh
	return maxAge > 0 && !now.Before(last) && now.Sub(last) <= maxAge
}

// remainingPositionMargin keeps the ordinary liquidation-distance calculation
// unchanged. Only an explicitly cross-margined long with sufficient account
// headroom may use zero as a conservative liquidation bound. Both headroom and
// position value must come from the same fresh REST account snapshot: using
// today's book value could incorrectly approve a position after a price fall.
//
// floor scales how much free equity the account must retain relative to the
// leg's notional: headroom >= floor*value. A floor <= 0 falls back to 100%
// coverage so an unconfigured monitor never loosens the bound.
func remainingPositionMargin(qty, entry, liq, mark float64, mode domain.MarginMode, value float64, cross *domain.CrossMarginRisk, floor float64) (float64, bool) {
	if r, ok := remainingMargin(qty, entry, liq, mark); ok {
		return r, true
	}
	if mode != domain.MarginCross || qty <= 0 || !finite(qty) || liq != 0 ||
		entry <= 0 || !finite(entry) || mark <= 0 || !finite(mark) ||
		value <= 0 || !finite(value) || !validCrossMargin(cross) {
		return 0, false
	}
	headroom := cross.Equity - cross.Maintenance
	if floor <= 0 || !finite(floor) {
		floor = 1
	}
	if !finite(headroom) || cross.Equity <= cross.Maintenance || headroom < floor*value {
		return 0, false
	}
	ratio := mark / entry
	return ratio, finite(ratio)
}

func validCrossMargin(cross *domain.CrossMarginRisk) bool {
	return cross != nil && finite(cross.Equity) && finite(cross.Maintenance) && cross.Maintenance >= 0
}

func crossMarginExhausted(qty float64, mode domain.MarginMode, cross *domain.CrossMarginRisk) bool {
	return qty != 0 && finite(qty) && mode == domain.MarginCross && validCrossMargin(cross) && cross.Equity <= cross.Maintenance
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

// remainingMargin returns the fraction of initial margin still available (0 at
// liquidation), or ok=false when the data is unusable.
func remainingMargin(qty, entry, liq, mark float64) (float64, bool) {
	if math.IsNaN(qty) || math.IsInf(qty, 0) || math.IsNaN(entry) || math.IsInf(entry, 0) ||
		math.IsNaN(liq) || math.IsInf(liq, 0) || math.IsNaN(mark) || math.IsInf(mark, 0) ||
		qty == 0 || entry <= 0 || liq <= 0 || mark <= 0 {
		return 0, false
	}
	if qty > 0 { // long: liquidation is below entry
		if entry <= liq {
			return 0, false
		}
		if mark <= liq {
			return 0, true
		}
		return (mark - liq) / (entry - liq), true
	}
	// short: liquidation is above entry
	if liq <= entry {
		return 0, false
	}
	if mark >= liq {
		return 0, true
	}
	return (liq - mark) / (liq - entry), true
}

// venuePositions returns the pair's position on marketID and the venue's total
// notional aggregated across all markets (the account-level exposure).
func venuePositions(ctx context.Context, v exchange.Venue, marketID int) (domain.Position, float64, error) {
	pos, agg, _, _, err := venueMarginSnapshot(ctx, v, marketID)
	return pos, agg, err
}

// venueMarginSnapshot obtains positions and optional account evidence in one
// request. A snapshot failure must not fall back to a separate positions read:
// combining those responses could approve a position with stale account data.
func venueMarginSnapshot(ctx context.Context, v exchange.Venue, marketID int) (domain.Position, float64, *domain.CrossMarginRisk, float64, error) {
	var ps []domain.Position
	var cross *domain.CrossMarginRisk
	var accountValue float64
	var err error
	if s, ok := v.(exchange.AccountSnapshotter); ok {
		var snapshot domain.AccountSnapshot
		snapshot, err = s.AccountSnapshot(ctx)
		ps = snapshot.Positions
		accountValue = snapshot.AccountValue
		if snapshot.CrossMargin != nil {
			copyRisk := *snapshot.CrossMargin
			cross = &copyRisk
		}
	} else {
		ps, err = v.Positions(ctx)
	}
	if err != nil {
		return domain.Position{}, 0, nil, 0, err
	}
	var pos domain.Position
	var agg float64
	for _, p := range ps {
		if math.IsNaN(p.Qty) || math.IsInf(p.Qty, 0) || math.IsNaN(p.AvgEntry) || math.IsInf(p.AvgEntry, 0) || p.AvgEntry < 0 || math.IsNaN(p.LiquidationPrice) || math.IsInf(p.LiquidationPrice, 0) {
			return domain.Position{}, 0, nil, 0, fmt.Errorf("%s market %d has invalid position data", v.Name(), p.MarketID)
		}
		agg += math.Abs(p.Qty) * p.AvgEntry
		if !finite(agg) {
			return domain.Position{}, 0, nil, 0, fmt.Errorf("%s has invalid aggregate position value", v.Name())
		}
		if p.MarketID == marketID {
			pos = p
		}
	}
	return pos, agg, cross, accountValue, nil
}
