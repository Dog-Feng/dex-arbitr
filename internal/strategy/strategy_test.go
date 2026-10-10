package strategy

import (
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func book(bid, ask, sz float64) domain.Book {
	return domain.Book{Bids: []domain.Level{{Price: bid, Size: sz}}, Asks: []domain.Level{{Price: ask, Size: sz}}}
}

var P = Params{StepQty: 0.01, MaxSteps: 3, OpenBps: 1, CloseBps: 1, DepthMult: 2, CloseOrder: "best"}

func TestOpenLS(t *testing.T) {
	// R rich: R.bid 1001 vs L.ask 1000 => LS ~ +10bps, mu 0
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	a := Decide(P, State{Mode: ModeRunning}, q, 0, true)
	if a.Kind != Open || a.Dir != DirLS || a.L.Side != domain.Buy || a.R.Side != domain.Sell || a.L.Price != 1000 || a.R.Price != 1001 {
		t.Fatalf("%+v", a)
	}
}

func TestNoOpenWhenMeanAbsorbsSpread(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1)) // LS ~ 10bps
	a := Decide(P, State{Mode: ModeRunning}, q, 10, true)               // mean already 10bps
	if a.Kind != None {
		t.Fatalf("%+v", a)
	}
}

func TestDepthGuard(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 0.015), book(1001, 1001.1, 1))
	if a := Decide(P, State{Mode: ModeRunning}, q, 0, true); a.Kind != None {
		t.Fatalf("depth 0.015 < 0.02 must block: %+v", a)
	}
}

func TestMaxStepsAndOpposite(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	s := State{Mode: ModeRunning, Steps: []Step{{ID: 1, Dir: DirLS, EntryBps: -50}, {ID: 2, Dir: DirLS, EntryBps: -50}, {ID: 3, Dir: DirLS, EntryBps: -50}}}
	if a := Decide(P, s, q, 0, true); a.Kind != None {
		t.Fatalf("max steps: %+v", a)
	}
	s2 := State{Mode: ModeRunning, Steps: []Step{{ID: 1, Dir: DirSL, EntryBps: -50}}}
	if a := Decide(P, s2, q, 0, true); a.Kind != None {
		t.Fatalf("opposite: %+v", a)
	}
}

func TestCloseBestFirst(t *testing.T) {
	// Holding LS steps; now L rich: L.bid 1001, R.ask 1000.5 => SL ~ +5bps
	q, _ := spread.Compute(book(1001, 1001.1, 1), book(1000.4, 1000.5, 1))
	s := State{Mode: ModeReduceOnly, Steps: []Step{{ID: 1, Dir: DirLS, Qty: 0.01, EntryBps: -3}, {ID: 2, Dir: DirLS, Qty: 0.01, EntryBps: 2}}}
	a := Decide(P, s, q, 0, false)
	if a.Kind != Close || a.StepID != 2 || a.L.Side != domain.Sell || a.R.Side != domain.Buy {
		t.Fatalf("%+v", a)
	}
	P2 := P
	P2.CloseOrder = "fifo"
	if a := Decide(P2, s, q, 0, false); a.StepID != 1 {
		t.Fatalf("fifo: %+v", a)
	}
}

func TestWarmupNoOpen(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	if a := Decide(P, State{Mode: ModeRunning}, q, 0, false); a.Kind != None {
		t.Fatalf("%+v", a)
	}
}
