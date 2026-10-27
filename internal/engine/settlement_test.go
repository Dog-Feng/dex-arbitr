package engine

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func TestDirectionFlipRecordedInMonitor(t *testing.T) {
	mon := monitor.New(nil)
	a := settlementActor(t)
	a.cfg.Monitor = mon
	openAccounting(t, a, strategy.DirLS, 100, 110)
	if a.lastDir != strategy.DirLS {
		t.Fatalf("open did not set lastDir: %v", a.lastDir)
	}
	if got := mon.Metrics().Flips; got != 0 {
		t.Fatalf("open from flat recorded a flip: %d", got)
	}
	// Close the DirLS step; the direction memory must survive the flat state.
	closeAct := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: 1,
		L: strategy.Leg{Side: domain.Sell, Price: 101}, R: strategy.Leg{Side: domain.Buy, Price: 110}}
	if err := a.accountExecution(closeAct, spread.Quote{}, 1, -1, fillResult(1, 101, 1, 110, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if a.lastDir != strategy.DirLS {
		t.Fatalf("close lost the direction memory: %v", a.lastDir)
	}
	// Opening the opposite direction is a flip and must be counted.
	openAct := strategy.Action{Kind: strategy.Open, Dir: strategy.DirSL, Qty: 1,
		L: strategy.Leg{Side: domain.Sell, Price: 110}, R: strategy.Leg{Side: domain.Buy, Price: 101}}
	if err := a.accountExecution(openAct, spread.Quote{}, 0, 0, fillResult(1, 110, 1, 101, -1, 1)); err != nil {
		t.Fatal(err)
	}
	if a.lastDir != strategy.DirSL || mon.Metrics().Flips != 1 {
		t.Fatalf("flip not recorded: lastDir=%v flips=%d", a.lastDir, mon.Metrics().Flips)
	}
}

func TestCloseSpillRecordsClosedEventForEveryConsumedStep(t *testing.T) {
	mon := monitor.New(nil)
	a := settlementActor(t)
	a.cfg.Monitor = mon
	a.state.Steps = []strategy.Step{
		{ID: 1, Dir: strategy.DirLS, Qty: 1, OriginalQty: 1, OpenL: 100, OpenR: 110},
		{ID: 2, Dir: strategy.DirLS, Qty: 1, OriginalQty: 1, OpenL: 100, OpenR: 110},
	}
	a.next = 3
	a.posL, a.posR = 2, -2
	a.syncCounter()
	// Closing 2 units through step 1 (which holds 1) consumes step 2 too.
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: 2,
		L: strategy.Leg{Side: domain.Sell, Price: 110}, R: strategy.Leg{Side: domain.Buy, Price: 100}}
	if err := a.accountExecution(act, spread.Quote{}, 2, -2, fillResult(2, 110, 2, 100, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if len(a.state.Steps) != 0 {
		t.Fatalf("steps not cleared after spill: %+v", a.state.Steps)
	}
	if got := mon.Metrics().Closes; got != 2 {
		t.Fatalf("close events = %d, want one per consumed step", got)
	}
}

func settlementActor(t *testing.T) *PairActor {
	t.Helper()
	s, err := store.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, TakerFee: .001}
	a, err := NewPairActor(PairConfig{Pair: symbols.NewPair("TEST", inst, inst),
		L: newActorTestVenue("L"), R: newActorTestVenue("R"), DryRun: true,
		Store: s, Est: actorTestEstimator{}, MaxBookAge: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a.resetVolumeIfNewDay(time.Now())
	return a
}

func nearSettlement(t *testing.T, label string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("%s: got %.12f want %.12f", label, got, want)
	}
}

func fillResult(lqty, lpx, rqty, rpx, pl, pr float64) execution.Result {
	return execution.Result{L: execution.LegResult{Filled: lqty, AvgPrice: lpx, Resolved: true},
		R:    execution.LegResult{Filled: rqty, AvgPrice: rpx, Resolved: true},
		PosL: pl, PosR: pr, PositionsKnown: true, SettlementKnown: true}
}

func openAccounting(t *testing.T, a *PairActor, dir strategy.Dir, lp, rp float64) {
	t.Helper()
	l, r := dir.LegSides()
	act := strategy.Action{Kind: strategy.Open, Dir: dir, Qty: 1, L: strategy.Leg{Side: l, Price: lp}, R: strategy.Leg{Side: r, Price: rp}}
	if err := a.accountExecution(act, spread.Quote{}, 0, 0, fillResult(1, lp, 1, rp, float64(l), float64(r))); err != nil {
		t.Fatal(err)
	}
}

func TestSettlementUsesCashFlowsWhenPriceLevelChanges(t *testing.T) {
	for _, tc := range []struct {
		name       string
		dir        strategy.Dir
		l, r, want float64
	}{
		{"long left actual loss", strategy.DirLS, 200, 211, -1.621},
		{"long left actual gain", strategy.DirLS, 200, 209, .381},
		{"short left actual gain", strategy.DirSL, 200, 211, .379},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := settlementActor(t)
			openAccounting(t, a, tc.dir, 100, 110)
			st := a.state.Steps[0]
			l, r := tc.dir.LegSides()
			act := strategy.Action{Kind: strategy.Close, Dir: tc.dir, StepID: st.ID, Qty: 1,
				L: strategy.Leg{Side: -l}, R: strategy.Leg{Side: -r}}
			if err := a.accountExecution(act, spread.Quote{}, float64(l), float64(r), fillResult(1, tc.l, 1, tc.r, 0, 0)); err != nil {
				t.Fatal(err)
			}
			nearSettlement(t, "total net PnL", a.volAcc.pnlUSD, tc.want)
			nearSettlement(t, "all physical volume", a.volAcc.volumeUSD, 210+tc.l+tc.r)
			if len(a.state.Steps) != 0 {
				t.Fatal("confirmed flat step still held")
			}
		})
	}
}

func TestSettlementRollbackKeepsSignedProfitAndEveryFee(t *testing.T) {
	for _, tc := range []struct {
		name        string
		side        domain.Side
		close, want float64
	}{
		{"buy favorable", domain.Buy, 101, .799},
		{"buy adverse", domain.Buy, 99, -1.199},
		{"sell favorable", domain.Sell, 99, .801},
		{"sell adverse", domain.Sell, 101, -1.201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := settlementActor(t)
			res := fillResult(1, 100, 0, 0, 0, 0)
			res.L.RescueFilled, res.L.RescuePrice = 1, tc.close
			res.RescueOrders, res.RescueQty = 1, 1
			act := strategy.Action{Kind: strategy.Open, Qty: 1, L: strategy.Leg{Side: tc.side}, R: strategy.Leg{Side: -tc.side}}
			if err := a.accountExecution(act, spread.Quote{}, 0, 0, res); err != nil {
				t.Fatal(err)
			}
			nearSettlement(t, "rollback net", a.volAcc.pnlUSD, tc.want)
			nearSettlement(t, "rollback volume", a.volAcc.volumeUSD, 100+tc.close)
			if len(a.state.Steps) != 0 || a.FailRate() != 1 {
				t.Fatal("rollback created a step or missed failure gate")
			}
		})
	}
}

func TestSettlementPartialCloseAndRescueWeightedPrice(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110)
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: 1,
		L: strategy.Leg{Side: domain.Sell}, R: strategy.Leg{Side: domain.Buy}}
	// First attempt closes only .4 on each leg; entry fees are allocated once.
	if err := a.accountExecution(act, spread.Quote{}, 1, -1, fillResult(.4, 101, .4, 110, .6, -.6)); err != nil {
		t.Fatal(err)
	}
	st := a.state.Steps[0]
	nearSettlement(t, "remaining qty", st.Qty, .6)
	nearSettlement(t, "remaining entry fees", st.EntryFeesUSD, .126)
	nearSettlement(t, "partial step net", st.RealizedUSD, .2316)
	// Original IOC and rescue complete different quantities/prices on each leg.
	res := fillResult(.2, 102, .4, 109, 0, 0)
	res.L.RescueFilled, res.L.RescuePrice = .4, 103
	res.R.RescueFilled, res.R.RescuePrice = .2, 108
	res.RescueOrders, res.RescueQty = 2, .6
	if err := a.accountExecution(act, spread.Quote{}, .6, -.6, res); err != nil {
		t.Fatal(err)
	}
	// Gross .4 + 2.4, entry fees .21, close fees .0844 + .1268.
	nearSettlement(t, "net after both partial closes", a.volAcc.pnlUSD, 2.3788)
	nearSettlement(t, "physical USD volume", a.volAcc.volumeUSD, 421.2)
	if len(a.state.Steps) != 0 {
		t.Fatal("fully closed step remains")
	}
}

func TestSettlementUnknownOrCanceledCannotMutateLedger(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*execution.Result)
	}{
		{"position unknown", func(r *execution.Result) { r.PositionsKnown = false }},
		{"order unknown", func(r *execution.Result) { r.SettlementKnown = false }},
		{"canceled despite flags", func(r *execution.Result) { r.Err = context.Canceled }},
		{"missing fill price", func(r *execution.Result) { r.L.AvgPrice = 0 }},
		{"positions disagree", func(r *execution.Result) { r.PosR = -.5 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := settlementActor(t)
			openAccounting(t, a, strategy.DirLS, 100, 110)
			original := a.state.Steps[0]
			res := fillResult(1, 101, 1, 110, 0, 0)
			tc.mutate(&res)
			err := a.accountExecution(strategy.Action{Kind: strategy.Close, StepID: 1, Qty: 1}, spread.Quote{}, 1, -1, res)
			if err == nil {
				t.Fatal("untrusted result was accepted")
			}
			if len(a.state.Steps) != 1 || a.state.Steps[0] != original {
				t.Fatal("ledger mutated on rejected result")
			}
			nearSettlement(t, "unchanged budget", a.volAcc.pnlUSD, -.21)
		})
	}
}

func TestInterventionBooksFlattenedGross(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110) // entry fees -0.21
	// Flatten closes both legs at adverse prices: long L 100->90, short R 110->120.
	res := fillResult(1, 90, 1, 120, 0, 0)
	if err := a.finishIntervention(res); err != nil {
		t.Fatal(err)
	}
	if len(a.state.Steps) != 0 {
		t.Fatalf("ledger not cleared: %+v", a.state.Steps)
	}
	// gross = -20 (DirLS: long L -10, short R -10), flatten fees -0.21,
	// entry fees already -0.21 -> total -20.42.
	nearSettlement(t, "intervention books gross", a.procPnlUSD, -20.42)
	if a.volAcc.pnlUncertain {
		t.Fatal("clean-priced matching flatten marked uncertain")
	}
}

func TestInterventionSkipsGrossWhenLedgerMismatches(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110) // entry fees -0.21
	// Flatten closes only half of each leg yet reports flat: the account was
	// modified externally, so the ledger quantity cannot price a gross.
	res := fillResult(0.5, 90, 0.5, 120, 0, 0)
	if err := a.finishIntervention(res); err != nil {
		t.Fatal(err)
	}
	// Only the flatten fees are booked: (0.5*90 + 0.5*120)*0.001 = 0.105.
	nearSettlement(t, "mismatched intervention books fees only", a.procPnlUSD, -0.21-0.105)
	if !a.volAcc.pnlUncertain {
		t.Fatal("mismatched flatten did not mark uncertainty")
	}
}

func TestSettlementDrainRequiresEachLegFlat(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110)
	res := execution.Result{PositionsKnown: true, SettlementKnown: true, PosL: 1, PosR: -1}
	if err := a.finishUnwind(res); err == nil {
		t.Fatal("net zero was mistaken for both legs flat")
	}
	if len(a.state.Steps) != 1 {
		t.Fatal("held ledger deleted")
	}
	res = fillResult(1, 101, 1, 110, 0, 0)
	if err := a.finishUnwind(res); err != nil {
		t.Fatal(err)
	}
	nearSettlement(t, "unwind net with original and exit fees", a.volAcc.pnlUSD, .579)
	if !a.Flat() {
		t.Fatal("confirmed unwind is not flat")
	}
}

func TestSettlementUnknownBasisAndEstimatePersistUncertainty(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{true: "legacy entry", false: "estimated fill"}[legacy], func(t *testing.T) {
			a := settlementActor(t)
			openAccounting(t, a, strategy.DirLS, 100, 110)
			if legacy {
				a.state.Steps[0].OpenL = 0
			}
			res := fillResult(1, 101, 1, 110, 0, 0)
			res.L.PriceEstimated = !legacy
			if err := a.finishUnwind(res); err != nil {
				t.Fatal(err)
			}
			if !a.volAcc.pnlUncertain {
				t.Fatal("unknown PnL did not mark the audit trail uncertain")
			}
			if err := a.persist(); err != nil {
				t.Fatal(err)
			}
			b, err := NewPairActor(a.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if !b.volAcc.pnlUncertain {
				t.Fatal("actor restart forgot uncertainty")
			}
			b.resetVolumeIfNewDay(time.Now().Add(24 * time.Hour))
			if !b.volAcc.pnlUncertain {
				t.Fatal("day rollover cleared the uncertainty marker")
			}
		})
	}
}

func TestSettlementCarriedEstimatedEntryKeepsNewDayUncertain(t *testing.T) {
	for _, unwind := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal close", true: "unwind"}[unwind], func(t *testing.T) {
			a := settlementActor(t)
			openAccounting(t, a, strategy.DirLS, 100, 110)
			a.state.Steps[0].PriceEstimated = true
			a.volAcc.day = time.Now().Add(-24 * time.Hour).UTC().Format("2006-01-02")
			a.resetVolumeIfNewDay(time.Now())
			res := fillResult(1, 101, 1, 110, 0, 0)
			var err error
			if unwind {
				err = a.finishUnwind(res)
			} else {
				err = a.accountExecution(strategy.Action{Kind: strategy.Close, StepID: 1, Qty: 1}, spread.Quote{}, 1, -1, res)
			}
			if err != nil {
				t.Fatal(err)
			}
			if !a.volAcc.pnlUncertain {
				t.Fatal("estimated entry lost its uncertainty marker after a day reset")
			}
		})
	}
}

func TestSettlementCompletesAgainstCurrentUTCDay(t *testing.T) {
	a := settlementActor(t)
	a.volAcc = volumeAcc{day: time.Now().Add(-24 * time.Hour).UTC().Format("2006-01-02"), volumeUSD: 999, pnlUSD: -999}
	openAccounting(t, a, strategy.DirLS, 100, 110)
	nearSettlement(t, "current day volume", a.volAcc.volumeUSD, 210)
	nearSettlement(t, "current day fees", a.volAcc.pnlUSD, -.21)
	if a.volAcc.day != time.Now().UTC().Format("2006-01-02") {
		t.Fatal("settlement kept yesterday's day")
	}
	a.volAcc.day = time.Now().Add(-24 * time.Hour).UTC().Format("2006-01-02")
	if err := a.finishUnwind(fillResult(1, 101, 1, 110, 0, 0)); err != nil {
		t.Fatal(err)
	}
	nearSettlement(t, "unwind current day volume", a.volAcc.volumeUSD, 211)
	nearSettlement(t, "unwind current day delta", a.volAcc.pnlUSD, .789)
}

func TestProcessLifetimeVolumeAccumulatesAcrossDays(t *testing.T) {
	a := settlementActor(t)
	a.volAcc = volumeAcc{day: time.Now().Add(-24 * time.Hour).UTC().Format("2006-01-02"), volumeUSD: 999, pnlUSD: -999}
	openAccounting(t, a, strategy.DirLS, 100, 110)
	nearSettlement(t, "process volume after open", a.procVolumeUSD, 210)
	nearSettlement(t, "process pnl after open", a.procPnlUSD, -.21)
	// A settlement confirmed on the next UTC day resets the daily budget but
	// the process-lifetime counters keep accumulating.
	a.volAcc.day = time.Now().Add(-24 * time.Hour).UTC().Format("2006-01-02")
	if err := a.finishUnwind(fillResult(1, 101, 1, 110, 0, 0)); err != nil {
		t.Fatal(err)
	}
	nearSettlement(t, "day budget reset to new day", a.volAcc.volumeUSD, 211)
	nearSettlement(t, "process volume cumulative", a.procVolumeUSD, 421)
	nearSettlement(t, "process pnl cumulative", a.procPnlUSD, .579)
}

func TestCapEngagedBandTransitions(t *testing.T) {
	a := settlementActor(t)
	a.cfg.Params = strategy.Params{MaxSteps: 5, CapBand: 2, StepQty: 1, DepthMult: 1}
	for i := 0; i < 5; i++ {
		l, r := strategy.DirLS.LegSides()
		act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: 1,
			L: strategy.Leg{Side: l, Price: 100}, R: strategy.Leg{Side: r, Price: 110}}
		n := float64(i + 1)
		if err := a.accountExecution(act, spread.Quote{}, n-1, -(n - 1), fillResult(1, 100, 1, 110, n, -n)); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.state.Steps) != 5 {
		t.Fatalf("steps=%d counter=%d", len(a.state.Steps), a.state.Counter())
	}
	if !a.capEngaged {
		t.Fatal("cap not engaged at max steps")
	}
	closeAct := func(id int64) strategy.Action {
		return strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: id, Qty: 1,
			L: strategy.Leg{Side: domain.Sell, Price: 101}, R: strategy.Leg{Side: domain.Buy, Price: 110}}
	}
	if err := a.accountExecution(closeAct(1), spread.Quote{}, 5, -5, fillResult(1, 101, 1, 110, 4, -4)); err != nil {
		t.Fatal(err)
	}
	if len(a.state.Steps) != 4 {
		t.Fatalf("steps=%d counter=%d", len(a.state.Steps), a.state.Counter())
	}
	if !a.capEngaged {
		t.Fatal("band disengaged at max-1")
	}
	if err := a.accountExecution(closeAct(2), spread.Quote{}, 4, -4, fillResult(1, 101, 1, 110, 3, -3)); err != nil {
		t.Fatal(err)
	}
	if a.capEngaged {
		t.Fatal("band stayed engaged below max-cap_band")
	}
}

// TestCloseSpillsIntoOtherStepsFIFO verifies an oversized step close consumes
// the selected step first and the overflow from the remaining steps in
// opening order, instead of failing with "close exceeds selected step".
func TestCloseSpillsIntoOtherStepsFIFO(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110)
	a.next++
	a.state.Steps = append(a.state.Steps, strategy.Step{
		ID: 2, Dir: strategy.DirLS, Qty: 0.01, OriginalQty: 0.01,
		OpenL: 100, OpenR: 110, EntryBps: strategy.EntryBps(strategy.DirLS, 100, 110),
		EntryFeesUSD: 0.01 * (100*0.001 + 110*0.001), OpenedAt: time.Now().Add(time.Minute),
	})
	a.state.Steps[0].Qty, a.state.Steps[0].OriginalQty = 0.01, 0.01
	a.state.Steps[0].EntryFeesUSD = 0.01 * (100*0.001 + 110*0.001)
	a.posL, a.posR = 0.02, -0.02
	act := strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: 1, Qty: 0.01,
		L: strategy.Leg{Side: domain.Sell, Price: 101}, R: strategy.Leg{Side: domain.Buy, Price: 110.5}}
	if err := a.accountExecution(act, spread.Quote{}, 0.02, -0.02, fillResult(0.014, 101, 0.014, 110.5, 0.006, -0.006)); err != nil {
		t.Fatal(err)
	}
	if len(a.state.Steps) != 1 || a.state.Steps[0].ID != 2 {
		t.Fatalf("spill did not consume FIFO: %+v", a.state.Steps)
	}
	nearSettlement(t, "remaining step quantity", a.state.Steps[0].Qty, 0.006)
	nearSettlement(t, "remaining entry fees", a.state.Steps[0].EntryFeesUSD, 0.0021*0.006/0.01)
}

func TestCapEngagedOversizedBandReleasesAtFlat(t *testing.T) {
	a := settlementActor(t)
	a.cfg.Params = strategy.Params{MaxSteps: 2, CapBand: 9, StepQty: 1, DepthMult: 1}
	for i := 0; i < 2; i++ {
		l, r := strategy.DirLS.LegSides()
		act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: 1,
			L: strategy.Leg{Side: l, Price: 100}, R: strategy.Leg{Side: r, Price: 110}}
		n := float64(i + 1)
		if err := a.accountExecution(act, spread.Quote{}, n-1, -(n - 1), fillResult(1, 100, 1, 110, n, -n)); err != nil {
			t.Fatal(err)
		}
	}
	if !a.capEngaged {
		t.Fatal("cap not engaged at max steps")
	}
	closeAct := func(id int64) strategy.Action {
		return strategy.Action{Kind: strategy.Close, Dir: strategy.DirLS, StepID: id, Qty: 1,
			L: strategy.Leg{Side: domain.Sell, Price: 101}, R: strategy.Leg{Side: domain.Buy, Price: 110}}
	}
	if err := a.accountExecution(closeAct(1), spread.Quote{}, 2, -2, fillResult(1, 101, 1, 110, 1, -1)); err != nil {
		t.Fatal(err)
	}
	if !a.capEngaged {
		t.Fatal("oversized band should stay engaged while holding")
	}
	if err := a.accountExecution(closeAct(2), spread.Quote{}, 1, -1, fillResult(1, 101, 1, 110, 0, 0)); err != nil {
		t.Fatal(err)
	}
	if a.capEngaged {
		t.Fatal("oversized band never released at flat")
	}
}

func TestBookWakeCoalesces(t *testing.T) {
	a := settlementActor(t)
	a.wakeDecide()
	a.wakeDecide()
	if len(a.bookWake) != 1 {
		t.Fatalf("wake channel length=%d, want 1 (coalesced)", len(a.bookWake))
	}
	<-a.bookWake
	a.wakeDecide()
	if len(a.bookWake) != 1 {
		t.Fatalf("wake channel not reusable: len=%d", len(a.bookWake))
	}
}

func TestOldestStepAge(t *testing.T) {
	a := settlementActor(t)
	now := time.Now()
	a.state.Steps = []strategy.Step{
		{ID: 1, Dir: strategy.DirLS, OpenedAt: now.Add(-time.Minute)},
		{ID: 2, Dir: strategy.DirLS, OpenedAt: now.Add(-10 * time.Second)},
	}
	if got := a.oldestStepAge(now); got < 59.9 || got > 60.1 {
		t.Fatalf("oldest step age=%v, want ~60s", got)
	}
	a.state.Steps = nil
	if got := a.oldestStepAge(now); got != 0 {
		t.Fatalf("flat book age=%v, want 0", got)
	}
}

func TestSettlementCheckpointSurvivesEmptyLedgerRestart(t *testing.T) {
	a := settlementActor(t)
	a.checkpoint = true
	if err := a.persist(); err != nil {
		t.Fatal(err)
	}
	b, err := NewPairActor(a.cfg)
	if err != nil {
		t.Fatal(err)
	}
	if b.Flat() || !b.checkpoint || b.state.Mode != strategy.ModePaused {
		t.Fatal("unresolved empty ledger appeared idle after restart")
	}
	res := execution.Result{Err: errors.New("unknown"), PositionsKnown: false}
	b.failSettlement(&pendingExecution{result: res})
	if b.Flat() || b.pending == nil {
		t.Fatal("unknown result was lost")
	}
}

func TestSettlementRecoveredUnwindFinishesDrainOnce(t *testing.T) {
	a := settlementActor(t)
	openAccounting(t, a, strategy.DirLS, 100, 110)
	a.exec = execution.NewExecutor(execution.DefaultConfig(), nil, execution.NewIDGen(), a.book, nil, nil, nil)
	a.draining.Store(true)
	a.emergencyRequested.Store(true)
	res := fillResult(1, 101, 1, 110, 0, 0)
	res.Err = context.Canceled
	a.failSettlement(&pendingExecution{beforeL: 1, beforeR: -1, result: res, unwind: true})
	a.resolvePending(context.Background())
	if a.pending != nil || a.checkpoint || !a.Drained() || a.emergencyRequested.Load() {
		t.Fatal("recovered unwind did not release drain/emergency lifecycle")
	}
	nearSettlement(t, "single unwind accounting", a.volAcc.pnlUSD, .579)
	st, err := a.cfg.Store.Load(a.key)
	if err != nil {
		t.Fatal(err)
	}
	if st.Unresolved || len(st.Steps) != 0 {
		t.Fatal("recovered flat state was not persisted")
	}
}

func TestSettlementWriteFailureCannotAppearIdle(t *testing.T) {
	a := settlementActor(t)
	if err := a.cfg.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.persist(); err == nil {
		t.Fatal("closed store write succeeded")
	}
	if a.Flat() || a.fatalErr == nil || a.state.Mode != strategy.ModePaused {
		t.Fatal("failed persistence appeared safe to retire")
	}
}

func TestSettlementDrainInterruptsOnlyCurrentExecutionOnce(t *testing.T) {
	a := settlementActor(t)
	ctx, finish := a.executionContext(context.Background())
	a.Drain()
	if ctx.Err() != context.Canceled {
		t.Fatal("drain did not interrupt normal execution")
	}
	finish()
	recovery, complete := a.executionContext(context.Background())
	defer complete()
	a.Drain()
	if recovery.Err() != nil {
		t.Fatal("repeated drain canceled recovery")
	}
}

func TestSettlementDrainAndEmergencyBlockAlreadySelectedOpen(t *testing.T) {
	for _, emergency := range []bool{false, true} {
		t.Run(map[bool]string{false: "drain", true: "emergency"}[emergency], func(t *testing.T) {
			a := settlementActor(t)
			if emergency {
				a.emergencyRequested.Store(true)
			} else {
				a.Drain()
			}
			a.apply(context.Background(), strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: 1,
				L: strategy.Leg{Side: domain.Buy, Price: 100}, R: strategy.Leg{Side: domain.Sell, Price: 110}}, spread.Quote{})
			if len(a.state.Steps) != 0 || a.volAcc.volumeUSD != 0 {
				t.Fatal("selected open ignored stop request")
			}
		})
	}
}

func TestSettlementRejectsWrongBookIdentityAndFutureTime(t *testing.T) {
	a := settlementActor(t)
	a.bookL, a.bookR = actorTestBook("L", 100), actorTestBook("R", 110)
	if _, ok := a.book("OTHER", 1); ok {
		t.Fatal("unknown venue selected right book")
	}
	if _, ok := a.book("L", 2); ok {
		t.Fatal("wrong market accepted")
	}
	a.bookL.Ts = time.Now().Add(time.Hour)
	if _, ok := a.book("L", 1); ok {
		t.Fatal("future timestamp accepted")
	}
	if _, ok := a.quote(time.Now()); ok {
		t.Fatal("future quote accepted")
	}
	a.bookL = actorTestBook("R", 100)
	if _, ok := a.quote(time.Now()); ok {
		t.Fatal("wrong venue quote accepted")
	}
}

// TestSettlementUsesReportedFees locks the L4 fix: exchange-reported fees win
// over the static rate estimate, and a missing report falls back to it.
func TestSettlementUsesReportedFees(t *testing.T) {
	t.Run("reported fees win", func(t *testing.T) {
		a := settlementActor(t)
		l, r := strategy.DirLS.LegSides()
		act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: 1,
			L: strategy.Leg{Side: l, Price: 100}, R: strategy.Leg{Side: r, Price: 110}}
		res := fillResult(1, 100, 1, 110, float64(l), float64(r))
		res.L.Fee, res.R.Fee = 0.05, 0.06 // reported fees, below the static 0.1% each
		if err := a.accountExecution(act, spread.Quote{}, 0, 0, res); err != nil {
			t.Fatal(err)
		}
		nearSettlement(t, "entry fees", a.state.Steps[0].EntryFeesUSD, 0.11)
	})
	t.Run("static fallback when unreported", func(t *testing.T) {
		a := settlementActor(t)
		l, r := strategy.DirLS.LegSides()
		act := strategy.Action{Kind: strategy.Open, Dir: strategy.DirLS, Qty: 1,
			L: strategy.Leg{Side: l, Price: 100}, R: strategy.Leg{Side: r, Price: 110}}
		if err := a.accountExecution(act, spread.Quote{}, 0, 0, fillResult(1, 100, 1, 110, float64(l), float64(r))); err != nil {
			t.Fatal(err)
		}
		nearSettlement(t, "entry fees", a.state.Steps[0].EntryFeesUSD, 100*0.001+110*0.001)
	})
}
