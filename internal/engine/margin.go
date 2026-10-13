package engine

import (
	"context"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
)

// marginConfig configures the liquidation-risk monitor.
type marginConfig struct {
	floor      float64
	trigger    float64
	hysteresis float64
	refresh    time.Duration
	marketBps  float64
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
// the actor goroutine, so no locking is needed.
type marginMonitor struct {
	cfg         marginConfig
	armed       bool
	lastRefresh time.Time

	have           bool
	liqL, liqR     float64
	entryL, entryR float64
	qtyL, qtyR     float64
}

// refresh polls both venues for liquidation prices; failures keep the old cache.
func (m *marginMonitor) refresh(ctx context.Context, l, r exchange.Venue, leftID, rightID int) {
	m.lastRefresh = time.Now()
	pl, okL, errL := venuePosition(ctx, l, leftID)
	pr, okR, errR := venuePosition(ctx, r, rightID)
	if errL != nil || errR != nil {
		return
	}
	if !okL {
		pl = domain.Position{}
	}
	if !okR {
		pr = domain.Position{}
	}
	m.liqL, m.entryL, m.qtyL = pl.LiquidationPrice, pl.AvgEntry, pl.Qty
	m.liqR, m.entryR, m.qtyR = pr.LiquidationPrice, pr.AvgEntry, pr.Qty
	m.have = true
}

// assess decides the action from the current mark prices; fresh=false means the
// books are stale and no assessment can be made.
func (m *marginMonitor) assess(markL, markR float64, fresh bool) marginAction {
	if !fresh || !m.have {
		return marginNone
	}
	minR := 1.0
	considered := false
	if r, ok := remainingMargin(m.qtyL, m.entryL, m.liqL, markL); ok {
		considered = true
		if r < minR {
			minR = r
		}
	}
	if r, ok := remainingMargin(m.qtyR, m.entryR, m.liqR, markR); ok {
		considered = true
		if r < minR {
			minR = r
		}
	}
	if !considered {
		if m.armed {
			m.armed = false
			return marginDisarm
		}
		return marginNone
	}
	switch {
	case minR <= m.cfg.trigger:
		m.armed = false
		return marginTrigger
	case minR <= m.cfg.floor:
		m.armed = true
		return marginArm
	case m.armed && minR > m.cfg.floor+m.cfg.hysteresis:
		m.armed = false
		return marginDisarm
	}
	return marginNone
}

// remainingMargin returns the fraction of initial margin still available (0 at
// liquidation), or ok=false when the data is unusable.
func remainingMargin(qty, entry, liq, mark float64) (float64, bool) {
	if qty == 0 || entry == 0 || liq <= 0 || mark <= 0 {
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

func venuePosition(ctx context.Context, v exchange.Venue, marketID int) (domain.Position, bool, error) {
	ps, err := v.Positions(ctx)
	if err != nil {
		return domain.Position{}, false, err
	}
	for _, p := range ps {
		if p.MarketID == marketID {
			return p, true, nil
		}
	}
	return domain.Position{}, false, nil
}
