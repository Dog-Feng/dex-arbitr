package strategy

import (
	"math"
	"testing"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func book(bid, ask, sz float64) domain.Book {
	return domain.Book{Bids: []domain.Level{{Price: bid, Size: sz}}, Asks: []domain.Level{{Price: ask, Size: sz}}}
}

var P = Params{StepQty: 0.01, MaxSteps: 3, OpenBps: 1, CloseBps: 1, DepthMult: 2, CloseOrder: "best"}

func testStep(id int64, d Dir, entry float64) Step {
	gap := entry / 1e4 * 1000
	if d == DirSL {
		gap = -gap
	}
	return Step{ID: id, Dir: d, Qty: 0.01, EntryBps: entry, OpenL: 1000 - gap/2, OpenR: 1000 + gap/2}
}

func TestEntryBpsFillTimeMid(t *testing.T) {
	// DirLS buys L / sells R: (100.1-100)/100.05 ≈ 9.995 bps
	if got := EntryBps(DirLS, 100, 100.1); math.Abs(got-9.995) > 0.01 {
		t.Fatalf("DirLS entry=%v, want ~9.995", got)
	}
	// DirSL sells L / buys R: (100.1-100)/100.05 ≈ 9.995 bps
	if got := EntryBps(DirSL, 100.1, 100); math.Abs(got-9.995) > 0.01 {
		t.Fatalf("DirSL entry=%v, want ~9.995", got)
	}
}

func TestOpenLS(t *testing.T) {
	// R rich: R.bid 1001 vs L.ask 1000 => LS ~ +10bps, mu 0
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	a := (Arbitrage{}).Decide(P, State{Mode: ModeRunning}, q, 0, true, 0)
	if a.Kind != Open || a.Dir != DirLS || a.L.Side != domain.Buy || a.R.Side != domain.Sell || a.L.Price != 1000 || a.R.Price != 1001 {
		t.Fatalf("%+v", a)
	}
}

func TestNoOpenWhenMeanAbsorbsSpread(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))    // LS ~ 10bps
	a := (Arbitrage{}).Decide(P, State{Mode: ModeRunning}, q, 10, true, 0) // mean already 10bps
	if a.Kind != None {
		t.Fatalf("%+v", a)
	}
}

func TestDepthGuard(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 0.015), book(1001, 1001.1, 1))
	if a := (Arbitrage{}).Decide(P, State{Mode: ModeRunning}, q, 0, true, 0); a.Kind != None {
		t.Fatalf("depth 0.015 < 0.02 must block: %+v", a)
	}
}

func TestMaxStepsAndOpposite(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	s := State{Mode: ModeRunning, Steps: []Step{testStep(1, DirLS, -50), testStep(2, DirLS, -50), testStep(3, DirLS, -50)}}
	if a := (Arbitrage{}).Decide(P, s, q, 0, true, 0); a.Kind != None {
		t.Fatalf("max steps: %+v", a)
	}
	s2 := State{Mode: ModeRunning, Steps: []Step{testStep(1, DirSL, -50)}}
	if a := (Arbitrage{}).Decide(P, s2, q, 0, true, 0); a.Kind != None {
		t.Fatalf("opposite: %+v", a)
	}
}

func TestCloseBestFirst(t *testing.T) {
	// Holding LS steps; now L rich: L.bid 1001, R.ask 1000.5 => SL ~ +5bps
	q, _ := spread.Compute(book(1001, 1001.1, 1), book(1000.4, 1000.5, 1))
	s := State{Mode: ModeReduceOnly, Steps: []Step{testStep(1, DirLS, -3), testStep(2, DirLS, 2)}}
	a := (Arbitrage{}).Decide(P, s, q, 0, false, 0)
	if a.Kind != Close || a.StepID != 2 || a.L.Side != domain.Sell || a.R.Side != domain.Buy {
		t.Fatalf("%+v", a)
	}
	P2 := P
	P2.CloseOrder = "fifo"
	if a := (Arbitrage{}).Decide(P2, s, q, 0, false, 0); a.StepID != 1 {
		t.Fatalf("fifo: %+v", a)
	}
}

func TestWarmupNoOpen(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	if a := (Arbitrage{}).Decide(P, State{Mode: ModeRunning}, q, 0, false, 0); a.Kind != None {
		t.Fatalf("%+v", a)
	}
}

func TestVolBufferBlocksOpen(t *testing.T) {
	// LS ~ +10bps; with VolK=1 and vol=9bps, needDev = 10bps, so it must not open
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1))
	p := Params{StepQty: 0.01, MaxSteps: 3, OpenBps: 1, CloseBps: 1, DepthMult: 2, CloseOrder: "best", VolK: 1}
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeRunning}, q, 0, true, 9); a.Kind != None {
		t.Fatalf("vol buffer should block open: %+v", a)
	}
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeRunning}, q, 0, true, 5); a.Kind != Open {
		t.Fatalf("dev 10 >= need 6 should open: %+v", a)
	}
}

func TestFeeBpsRaisesThreshold(t *testing.T) {
	q, _ := spread.Compute(book(999.9, 1000, 1), book(1001, 1001.1, 1)) // LS ~ 10bps
	p := Params{StepQty: 0.01, MaxSteps: 3, OpenBps: 1, CloseBps: 1, DepthMult: 2, CloseOrder: "best", FeeBps: 9.5}
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeRunning}, q, 0, true, 0); a.Kind != None {
		t.Fatalf("dev 10 < 1+9.5 should block: %+v", a)
	}
}

func TestMinInventoryFloor(t *testing.T) {
	q, _ := spread.Compute(book(1001, 1001.1, 1), book(1000.4, 1000.5, 1)) // SL ~ +5bps, profitable close
	steps := []Step{testStep(1, DirLS, -3), testStep(2, DirLS, 2)}
	p := Params{StepQty: 0.01, MaxSteps: 5, OpenBps: 1, CloseBps: 1, DepthMult: 2, CloseOrder: "best", MinInventory: 2}
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeReduceOnly, Steps: steps}, q, 0, false, 0); a.Kind != None {
		t.Fatalf("floor should block close: %+v", a)
	}
	steps = append(steps, testStep(3, DirLS, 5))
	if a := (Arbitrage{}).Decide(p, State{Mode: ModeReduceOnly, Steps: steps}, q, 0, false, 0); a.Kind != Close {
		t.Fatalf("above floor should close: %+v", a)
	}
}

func TestVolumeStrategyRegistered(t *testing.T) {
	if v, err := New("volume"); err != nil || v == nil {
		t.Fatalf("volume strategy: %v", err)
	}
}
