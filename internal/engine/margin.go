package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math"
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
	lastRefresh  time.Time
	lastL, lastR time.Time

	have           bool
	liqL, liqR     float64
	entryL, entryR float64
	qtyL, qtyR     float64
	aggL, aggR     float64
}

// refresh updates each successful venue independently. A failed peer must not
// discard liquidation data that was successfully read from the other venue.
func (m *marginMonitor) refresh(ctx context.Context, l, r exchange.Venue, leftID, rightID int) {
	m.lastRefresh = time.Now()
	pl, aggL, errL := venuePositions(ctx, l, leftID)
	if errL == nil {
		m.liqL, m.entryL, m.qtyL, m.aggL = pl.LiquidationPrice, pl.AvgEntry, pl.Qty, aggL
		m.lastL = time.Now()
	} else {
		slog.Warn("margin positions refresh", "venue", l.Name(), "err", errL)
	}
	pr, aggR, errR := venuePositions(ctx, r, rightID)
	if errR == nil {
		m.liqR, m.entryR, m.qtyR, m.aggR = pr.LiquidationPrice, pr.AvgEntry, pr.Qty, aggR
		m.lastR = time.Now()
	} else {
		slog.Warn("margin positions refresh", "venue", r.Name(), "err", errR)
	}
	m.have = !m.lastL.IsZero() || !m.lastR.IsZero()
}

// assess decides the action from the current mark prices; fresh=false means
// no mark is usable and no assessment can be made. A zero mark represents an
// unavailable leg; it must not hide a known liquidation trigger on its peer.
func (m *marginMonitor) assess(markL, markR float64, fresh bool) marginAction {
	return m.assessAt(time.Now(), markL, markR, fresh)
}

func (m *marginMonitor) assessAt(now time.Time, markL, markR float64, fresh bool) marginAction {
	if !fresh {
		return marginNone
	}
	validL, validR := m.cacheFresh(now, m.lastL), m.cacheFresh(now, m.lastR)
	// Hand-built monitors with no refresh interval are used by pure decision
	// tests. Production monitors always track individual successful reads.
	if m.cfg.refresh <= 0 && m.have && m.lastL.IsZero() && m.lastR.IsZero() {
		validL, validR = true, true
	}
	minR := 1.0
	considered := false
	unknown := !validL || !validR
	if r, ok := remainingMargin(m.qtyL, m.entryL, m.liqL, markL); ok && validL {
		considered = true
		if r < minR {
			minR = r
		}
	} else if validL && m.qtyL != 0 {
		unknown = true
	}
	if r, ok := remainingMargin(m.qtyR, m.entryR, m.liqR, markR); ok && validR {
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
		return marginTrigger
	}
	if unknown || (m.cfg.maxNotional > 0 && (m.aggL > m.cfg.maxNotional || m.aggR > m.cfg.maxNotional)) {
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
		m.armed = true
		return marginArm
	case m.armed && minR > m.cfg.floor+m.cfg.hysteresis:
		m.armed = false
		return marginDisarm
	}
	return marginNone
}

func (m *marginMonitor) cacheFresh(now, last time.Time) bool {
	if last.IsZero() {
		return false
	}
	maxAge := 2 * m.cfg.refresh
	return maxAge > 0 && now.Sub(last) <= maxAge
}

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
	ps, err := v.Positions(ctx)
	if err != nil {
		return domain.Position{}, 0, err
	}
	var pos domain.Position
	var agg float64
	for _, p := range ps {
		if math.IsNaN(p.Qty) || math.IsInf(p.Qty, 0) || math.IsNaN(p.AvgEntry) || math.IsInf(p.AvgEntry, 0) || p.AvgEntry < 0 || math.IsNaN(p.LiquidationPrice) || math.IsInf(p.LiquidationPrice, 0) {
			return domain.Position{}, 0, fmt.Errorf("%s market %d has invalid position data", v.Name(), p.MarketID)
		}
		agg += math.Abs(p.Qty) * p.AvgEntry
		if p.MarketID == marketID {
			pos = p
		}
	}
	return pos, agg, nil
}
