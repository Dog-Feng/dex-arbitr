package strategy

import (
	"math"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func TestProfitUsesFixedEntryCashflows(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		dir                                Dir
		openL, openR, closeL, closeR, want float64
	}{
		{"LS price doubles", DirLS, 100, 102, 200, 203, -1},
		{"SL price doubles", DirSL, 102, 100, 203, 200, -1},
		{"LS profitable", DirLS, 100, 102, 200, 201, 1},
		{"SL profitable", DirSL, 102, 100, 201, 200, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := Step{ID: 1, Dir: tc.dir, Qty: 2, OpenL: tc.openL, OpenR: tc.openR, EntryBps: EntryBps(tc.dir, tc.openL, tc.openR)}
			if got := GrossProfitUSD(st, tc.closeL, tc.closeR, 2); math.Abs(got-2*tc.want) > 1e-10 {
				t.Fatalf("gross=%v", got)
			}
			if got := ProfitBps(st, tc.closeL, tc.closeR); math.Abs(got-tc.want/101*1e4) > 1e-10 {
				t.Fatalf("profit bps=%v", got)
			}
			feeL, feeR := 0.0005, 0.001
			wantFee := 2 * ((tc.openL+tc.closeL)*feeL + (tc.openR+tc.closeR)*feeR)
			if got := RoundTripFeeUSD(st, tc.closeL, tc.closeR, 2, feeL, feeR); math.Abs(got-wantFee) > 1e-10 {
				t.Fatalf("fee=%v want %v", got, wantFee)
			}
		})
	}
	q, ok := spread.Compute(book(200, 200.02, 10), book(202.98, 203, 10))
	if !ok {
		t.Fatal("invalid books")
	}
	st := Step{ID: 1, Dir: DirLS, Qty: 1, OpenL: 100, OpenR: 102, EntryBps: EntryBps(DirLS, 100, 102)}
	p := P
	p.FeeBps = 10
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeReduceOnly, Steps: []Step{st}}, q, 0, false, 0); a.Kind != None {
		t.Fatalf("a dollar loss must not be closed as profitable: %+v", a)
	}
}

func TestCloseUsesActualNotionalFees(t *testing.T) {
	st := Step{ID: 1, Dir: DirLS, Qty: 1, OpenL: 100, OpenR: 102, EntryBps: EntryBps(DirLS, 100, 102)}
	q, ok := spread.Compute(book(1000, 1000.02, 10), book(1000.98, 1001, 10))
	if !ok {
		t.Fatal("invalid books")
	}
	p := P
	p.TakerFeeL, p.TakerFeeR = 0.0005, 0.0005
	// Gross price gain is $1, but four actual fills cost $1.1015.
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeReduceOnly, Steps: []Step{st}}, q, 0, false, 0); a.Kind != None {
		t.Fatalf("actual notional fees must block close: %+v", a)
	}
}

func TestCloseUsesStoredEntryFeesAcrossFeeTierChanges(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		qty, entryFees, closeL float64
		want                   Kind
	}{
		{"old expensive entry", 1, 1.01, 100.1, None},
		{"partially closed expensive entry", 0.5, 0.505, 100.1, None},
		{"known free entry", 1, 0, 100.03, Close},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := Step{ID: 1, Dir: DirLS, Qty: tc.qty, OriginalQty: 1, OpenL: 100, OpenR: 102, EntryFeesUSD: tc.entryFees}
			q, ok := spread.Compute(book(tc.closeL, tc.closeL+0.02, 10), book(101.98, 102, 10))
			if !ok {
				t.Fatal("invalid books")
			}
			p := P
			p.CloseBps = 0
			p.TakerFeeL, p.TakerFeeR = 0.0001, 0.0001
			a := (Arbitrage{}).Decide(p, State{Mode: ModeReduceOnly, Steps: []Step{st}}, q, 0, false, 0)
			if a.Kind != tc.want {
				t.Fatalf("stored fees repriced after fee-tier change: action=%+v want=%s", a, tc.want)
			}
		})
	}
}

func TestInvalidInputsDoNotProduceActions(t *testing.T) {
	q, ok := spread.Compute(book(99.9, 100, 10), book(102, 102.1, 10))
	if !ok {
		t.Fatal("invalid test quote")
	}
	for _, tc := range []struct {
		name   string
		change func(*Params, *spread.Quote, *float64, *float64)
	}{
		{"invalid reference", func(p *Params, q *spread.Quote, mu, vol *float64) { q.RefMid = 0 }},
		{"invalid spread", func(p *Params, q *spread.Quote, mu, vol *float64) { q.LS = math.NaN() }},
		{"invalid mean", func(p *Params, q *spread.Quote, mu, vol *float64) { *mu = math.Inf(1) }},
		{"invalid volatility", func(p *Params, q *spread.Quote, mu, vol *float64) { *vol = -1 }},
		{"invalid quantity", func(p *Params, q *spread.Quote, mu, vol *float64) { p.StepQty = 0 }},
		{"invalid fee", func(p *Params, q *spread.Quote, mu, vol *float64) { p.TakerFeeL = math.NaN() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := P
			quote := q
			mu, vol := 0.0, 0.0
			tc.change(&p, &quote, &mu, &vol)
			if a := (Arbitrage{}).Decide(p, State{Mode: ModeRunning}, quote, mu, true, vol); a.Kind != None {
				t.Fatalf("invalid inputs traded: %+v", a)
			}
		})
	}
	st := Step{Dir: DirLS, Qty: 1, EntryBps: 100}
	if got := GrossProfitUSD(st, 100, 101, 1); !math.IsNaN(got) {
		t.Fatalf("missing entry prices silently estimated gross=%v", got)
	}
}
