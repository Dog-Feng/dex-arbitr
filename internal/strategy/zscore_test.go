package strategy

import (
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

// zQuote builds a quote whose LS/SL deviates dev bps around zero, with
// unit-sized top-of-book levels so the depth gate passes.
func zQuote(dev float64) spread.Quote {
	return spread.Quote{
		LS: dev, SL: -dev, Mid: 0, RefMid: 100,
		L: domain.Book{Bids: []domain.Level{{Price: 99.9, Size: 1}}, Asks: []domain.Level{{Price: 100.1, Size: 1}}},
		R: domain.Book{Bids: []domain.Level{{Price: 99.9, Size: 1}}, Asks: []domain.Level{{Price: 100.1, Size: 1}}},
	}
}

func zParams() Params {
	return Params{StepQty: 0.01, MaxSteps: 10, DepthMult: 1, ZOpen: 2}
}

func TestZScoreOpensAboveThreshold(t *testing.T) {
	p := zParams()
	s := State{Mode: ModeRunning}
	// dev=6, sigma=2 => z=3 >= 2; fees are zero.
	act := (ZScore{}).Decide(p, s, zQuote(6), 0, true, 2)
	if act.Kind != Open || act.Dir != DirLS {
		t.Fatalf("expected DirLS open, got %+v", act)
	}
	// dev=3, sigma=2 => z=1.5 < 2: no signal.
	act = (ZScore{}).Decide(p, s, zQuote(3), 0, true, 2)
	if act.Kind != None {
		t.Fatalf("below-threshold z must not open: %+v", act)
	}
	// Large z but fees not covered: still no open.
	p.FeeBps = 10
	act = (ZScore{}).Decide(p, s, zQuote(6), 0, true, 1)
	if act.Kind != None {
		t.Fatalf("fee gate must block: %+v", act)
	}
}

func TestZScoreHysteresisAndCapacity(t *testing.T) {
	p := zParams()
	p.FlipHysteresisSigma = 1
	p.CapBand = 1
	p.CapHysteresisSigma = 1
	// Reversal from flat needs one extra sigma: z=2.5 < 3.
	s := State{Mode: ModeRunning, LastDir: DirSL}
	if act := (ZScore{}).Decide(p, s, zQuote(5), 0, true, 2); act.Kind != None {
		t.Fatalf("flip hysteresis bypassed: %+v", act)
	}
	s.LastDir = DirSL
	if act := (ZScore{}).Decide(p, s, zQuote(7), 0, true, 2); act.Kind != Open {
		t.Fatalf("z=3.5 must clear the flip hysteresis: %+v", act)
	}
	// Inside the top band one extra sigma applies at max-1.
	steps := make([]Step, 9)
	for i := range steps {
		steps[i] = Step{ID: int64(i + 1), Dir: DirLS, Qty: 0.01}
	}
	s = State{Mode: ModeRunning, Steps: steps}
	if act := (ZScore{}).Decide(p, s, zQuote(5), 0, true, 2); act.Kind != None {
		t.Fatalf("cap hysteresis bypassed at max-1: %+v", act)
	}
	steps = append(steps, Step{ID: 10, Dir: DirLS, Qty: 0.01})
	s = State{Mode: ModeRunning, Steps: steps}
	if act := (ZScore{}).Decide(p, s, zQuote(9), 0, true, 2); act.Kind != None {
		t.Fatalf("max steps must hard-block: %+v", act)
	}
}

func TestZScoreClosesLikeArbitrage(t *testing.T) {
	p := zParams()
	// A zero-entry step has zero gross at the current books, which meets the
	// zero-fee close threshold.
	s := State{Mode: ModeRunning, Steps: []Step{{ID: 1, Dir: DirLS, Qty: 0.01, OpenL: 99.9, OpenR: 100.1, OpenedAt: time.Now()}}}
	act := (ZScore{}).Decide(p, s, zQuote(6), 0, true, 2)
	if act.Kind != Close || act.StepID != 1 {
		t.Fatalf("close-first must win: %+v", act)
	}
}

func TestZScoreStepLadder(t *testing.T) {
	p := zParams()
	p.StepZ = 0.5
	steps := make([]Step, 4)
	for i := range steps {
		steps[i] = Step{ID: int64(i + 1), Dir: DirLS, Qty: 0.01}
	}
	s := State{Mode: ModeRunning, Steps: steps} // cnt = -4, ladder +2
	// z=2.5 clears z_open=2 but not 2+4*0.5=4.
	if act := (ZScore{}).Decide(p, s, zQuote(5), 0, true, 2); act.Kind != None {
		t.Fatalf("ladder not applied: %+v", act)
	}
	// z=4.1 clears the fourth rung.
	if act := (ZScore{}).Decide(p, s, zQuote(8.2), 0, true, 2); act.Kind != Open {
		t.Fatalf("fourth rung must open above the ladder: %+v", act)
	}
	// With the ladder off, z=2.5 opens at the same step count.
	p.StepZ = 0
	if act := (ZScore{}).Decide(p, s, zQuote(5), 0, true, 2); act.Kind != Open {
		t.Fatalf("step_z=0 must preserve the flat threshold: %+v", act)
	}
}
