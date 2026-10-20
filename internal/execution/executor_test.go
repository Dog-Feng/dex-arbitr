package execution

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// fakeVenue fills orders according to fillFn and maintains a position.
type fakeVenue struct {
	name   string
	mu     sync.Mutex
	pos    float64
	ch     chan domain.OrderUpdate
	fillFn func(n int, r domain.OrderReq) float64 // returns filled qty
	n      int
	silent bool // never emit WS events (forces fallback)
	orders []domain.OrderReq
}

func newFake(name string, f func(int, domain.OrderReq) float64) *fakeVenue {
	return &fakeVenue{name: name, ch: make(chan domain.OrderUpdate, 100), fillFn: f}
}

func TestLimitPriceRoundsTightly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PriceTicks = 0
	e := NewExecutor(cfg, nil, nil, nil, nil, nil, nil)
	inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
	if got := e.limitPrice(LegSpec{Inst: inst, Side: domain.Buy, Price: 100.005}, 1); got != 100.00 {
		t.Fatalf("buy limit rounded outward: %v", got)
	}
	if got := e.limitPrice(LegSpec{Inst: inst, Side: domain.Sell, Price: 99.995}, 1); got != 100.00 {
		t.Fatalf("sell limit rounded outward: %v", got)
	}
}

func (f *fakeVenue) Name() string                                             { return f.name }
func (f *fakeVenue) Instruments(context.Context) ([]domain.Instrument, error) { return nil, nil }
func (f *fakeVenue) SubscribeBook(context.Context, domain.Instrument) (<-chan domain.Book, error) {
	return nil, nil
}
func (f *fakeVenue) Submit(_ context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	f.mu.Lock()
	q := f.fillFn(f.n, r)
	f.n++
	f.orders = append(f.orders, r)
	f.pos += float64(r.Side) * q
	f.mu.Unlock()
	st := domain.StatusFilled
	if q < r.Qty {
		st = domain.StatusCanceled
	}
	if !f.silent {
		f.ch <- domain.OrderUpdate{Venue: f.name, ClientID: r.ClientID, Status: st, FilledQty: q, FilledQuote: q * r.Price}
	}
	return domain.SubmitAck{ClientID: r.ClientID}, nil
}
func (f *fakeVenue) SubscribeOrders(context.Context) (<-chan domain.OrderUpdate, error) {
	return f.ch, nil
}
func (f *fakeVenue) QueryOrder(_ context.Context, i domain.Instrument, cid int64) (domain.OrderUpdate, error) {
	return domain.OrderUpdate{ClientID: cid, Status: domain.StatusUnknown}, nil
}
func (f *fakeVenue) CancelOrder(context.Context, domain.Instrument, int64) error { return nil }
func (f *fakeVenue) Positions(context.Context) ([]domain.Position, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return []domain.Position{{MarketID: 1, Qty: f.pos}}, nil
}
func (f *fakeVenue) CanTrade() bool { return true }
func (f *fakeVenue) Close() error   { return nil }

var inst = domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}

func testExec(t *testing.T, l, r *fakeVenue) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tr, err := NewTracker(ctx, l, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	cfg := DefaultConfig()
	cfg.TopUpBps = 0 // legacy fixtures assume the pre-completion behavior
	cfg.AckTimeout, cfg.QueryInterval, cfg.SettleDelay, cfg.RescueInterval = 50*time.Millisecond, 10*time.Millisecond, time.Millisecond, time.Millisecond
	bk := domain.Book{Bids: []domain.Level{{Price: 100, Size: 1}}, Asks: []domain.Level{{Price: 100.1, Size: 1}}}
	return NewExecutor(cfg, tr, NewIDGen(), func(string, int) (domain.Book, bool) { return bk, true }, nil, nil, func(string) {})
}

// topUpExec enables step completion for tests of the completion paths.
func topUpExec(t *testing.T, l, r *fakeVenue) *Executor {
	e := testExec(t, l, r)
	e.cfg.TopUpBps = 25
	return e
}

func full(_ int, r domain.OrderReq) float64 { return r.Qty }

func TestDynamicAllowance(t *testing.T) {
	leg := LegSpec{Venue: newFake("L", full), Inst: domain.Instrument{PriceDecimals: 2}, Side: domain.Buy, Price: 100}
	e := NewExecutor(Config{DynamicTicks: true, TickK: 1, TickMin: 1, TickMax: 100, PriceTicks: 30},
		nil, NewIDGen(), nil, func(string) float64 { return 0.1 }, nil, func(string) {})
	// adverse move = 1 * 0.1 price units => 10 ticks of 0.01
	if n := e.allowance(leg, 0.01); n != 10 {
		t.Fatalf("dynamic allowance=%d, want 10", n)
	}
	// volatility not ready falls back to the fixed allowance
	e.vol = func(string) float64 { return 0 }
	if n := e.allowance(leg, 0.01); n != 30 {
		t.Fatalf("fallback allowance=%d, want 30", n)
	}
	// high volatility is capped at TickMax
	e.vol = func(string) float64 { return 50 }
	if n := e.allowance(leg, 0.01); n != 100 {
		t.Fatalf("capped allowance=%d, want 100", n)
	}
}

func TestDepthScale(t *testing.T) {
	inst := domain.Instrument{PriceDecimals: 2}
	bk := domain.Book{
		Bids: []domain.Level{{Price: 100, Size: 0.1}},
		Asks: []domain.Level{{Price: 100.1, Size: 0.01}},
	}
	e := NewExecutor(Config{DepthMinMult: 5, DepthMaxScale: 4}, nil, NewIDGen(),
		func(string, int) (domain.Book, bool) { return bk, true }, nil, nil, func(string) {})
	// buy leg: ask size 0.01, qty 0.005 => multiple 2 (<5) => scale 2.5
	if s := e.depthScale(LegSpec{Venue: newFake("L", full), Inst: inst, Side: domain.Buy}, 0.005); s != 2.5 {
		t.Fatalf("buy depthScale=%v, want 2.5", s)
	}
	// sell leg: bid size 0.1, qty 0.005 => multiple 20 (>=5) => scale 1
	if s := e.depthScale(LegSpec{Venue: newFake("L", full), Inst: inst, Side: domain.Sell}, 0.005); s != 1 {
		t.Fatalf("sell depthScale=%v, want 1", s)
	}
}

func TestDepthTicks(t *testing.T) {
	inst := domain.Instrument{PriceDecimals: 2} // tick 0.01
	bk := domain.Book{
		Asks: []domain.Level{{Price: 100, Size: 0.005}, {Price: 100.2, Size: 1}},
		Bids: []domain.Level{{Price: 100, Size: 0.005}, {Price: 99.9, Size: 1}},
	}
	if n := depthTicks(LegSpec{Inst: inst, Side: domain.Buy, Price: 100}, bk, 0.01); n != 20 {
		t.Fatalf("buy depth ticks=%d, want 20", n)
	}
	if n := depthTicks(LegSpec{Inst: inst, Side: domain.Sell, Price: 100}, bk, 0.01); n != 10 {
		t.Fatalf("sell depth ticks=%d, want 10", n)
	}
}

func TestBothFilled(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	e := testExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Buy, 100.1}, LegSpec{r, inst, domain.Sell, 100}, 0.01, false, 0, 0)
	if res.L.Filled != 0.01 || res.R.Filled != 0.01 || res.RescueOrders != 0 || res.PosL != 0.01 || res.PosR != -0.01 {
		t.Fatalf("%+v", res)
	}
}

func TestOneLegFailsThenFlatten(t *testing.T) {
	l := newFake("L", full)
	r := newFake("R", func(int, domain.OrderReq) float64 { return 0 }) // R never fills
	e := testExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Buy, 100.1}, LegSpec{r, inst, domain.Sell, 100}, 0.01, false, 0, 0)
	if res.RescueOrders != 1 || res.PosL != 0 || res.PosR != 0 {
		t.Fatalf("%+v", res)
	}
	last := l.orders[len(l.orders)-1]
	if !last.ReduceOnly || last.Side != domain.Sell || last.Qty != 0.01 {
		t.Fatalf("rescue order %+v", last)
	}
}

func TestRescueEscalatesPrice(t *testing.T) {
	// L: first order fills, rescue orders fill nothing for 9 attempts, then fill
	l := newFake("L", func(n int, r domain.OrderReq) float64 {
		if n == 0 || n >= 10 {
			return r.Qty
		}
		return 0
	})
	r := newFake("R", func(int, domain.OrderReq) float64 { return 0 })
	e := testExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Buy, 100.1}, LegSpec{r, inst, domain.Sell, 100}, 0.01, false, 0, 0)
	if res.PosL != 0 || res.RescueOrders != 10 {
		t.Fatalf("%+v", res)
	}
	got := []float64{}
	for _, o := range l.orders[1:] {
		got = append(got, o.Price)
	}
	want := []float64{100, 100, 100, 99.99, 99.98, 99.97, 99.96, 99.95, 99.74, 99.74} // mid 100.05 * (1-30bps) floored to tick
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("prices %v want %v", got, want)
		}
	}
	if l.orders[9].Type != domain.Market {
		t.Fatal("attempt 9 should be market")
	}
}

func TestCloseCompletesLaggingLeg(t *testing.T) {
	// holding step: L long 0.01, R short 0.01; closing: L sells ok, R buy fails
	l, r := newFake("L", full), newFake("R", func(n int, q domain.OrderReq) float64 {
		if n == 0 {
			return 0
		}
		return q.Qty
	})
	l.pos, r.pos = 0.01, -0.01
	e := testExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Sell, 100}, LegSpec{r, inst, domain.Buy, 100.1}, 0.01, true, 0.01, -0.01)
	if res.PosL != 0 || res.PosR != 0 || res.RescueOrders != 1 {
		t.Fatalf("%+v", res)
	}
	if res.R.RescueFilled != 0.01 || math.Abs(res.R.RescuePrice-100.1) > 1e-9 {
		t.Fatalf("rescue fill not recorded: %+v", res.R)
	}
	if o := r.orders[1]; o.Side != domain.Buy || !o.ReduceOnly {
		t.Fatalf("%+v", o)
	}
}

func TestMergeRescue(t *testing.T) {
	var lr LegResult
	mergeRescue(&lr, 0.01, 100)
	mergeRescue(&lr, 0.01, 102)
	if lr.RescueFilled != 0.02 || math.Abs(lr.RescuePrice-101) > 1e-9 {
		t.Fatalf("%+v", lr)
	}
	mergeRescue(&lr, 0, 50) // zero fill must be ignored
	if lr.RescueFilled != 0.02 || math.Abs(lr.RescuePrice-101) > 1e-9 {
		t.Fatalf("%+v", lr)
	}
}

func TestSilentVenueInferredFromPositions(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	l.silent = true
	e := testExec(t, l, r)
	res := e.Execute(context.Background(), LegSpec{l, inst, domain.Buy, 100.1}, LegSpec{r, inst, domain.Sell, 100}, 0.01, false, 0, 0)
	if res.L.Resolved || res.L.Filled != 0.01 || res.RescueOrders != 0 {
		t.Fatalf("%+v", res)
	}
}

// topUpInst carries a venue minimum that allows completing a 0.007 shortfall
// at price ~100.
var topUpInst = domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.5}

func TestTopUpCompletesPartialOpen(t *testing.T) {
	lv := newFake("L", full)
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.013 // first leg partially fills
		}
		return r.Qty // the completion order fills fully
	})
	e := topUpExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: topUpInst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: topUpInst, Side: domain.Sell, Price: 101}, 0.02, false, 0, 0)
	if res.Err != nil || !res.PositionsKnown || !res.SettlementKnown {
		t.Fatalf("unsettled: %+v", res)
	}
	requireQty(t, res.PosL, 0.02)
	requireQty(t, res.PosR, -0.02)
	if res.RescueOrders != 0 {
		t.Fatalf("step not completed: %+v", res)
	}
	if res.R.Filled != 0.02 {
		t.Fatalf("R fills not merged: %+v", res.R)
	}
	if len(rv.orders) != 2 || rv.orders[1].Side != domain.Sell || rv.orders[1].ReduceOnly {
		t.Fatalf("top-up orders %+v", rv.orders)
	}
	requireQty(t, rv.orders[1].Qty, 0.007)
}

func TestTopUpRejectedFallsBackToShrink(t *testing.T) {
	lv := newFake("L", full)
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.013
		}
		return 0 // completion rejected
	})
	e := topUpExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: topUpInst, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: topUpInst, Side: domain.Sell, Price: 101}, 0.02, false, 0, 0)
	if res.Err != nil {
		t.Fatalf("shrink fallback failed: %+v", res)
	}
	requireQty(t, res.PosL, 0.013)
	requireQty(t, res.PosR, -0.013)
	if res.RescueOrders != 1 {
		t.Fatalf("expected one shrink order, got %+v", res)
	}
}

func TestTopUpSkippedBelowVenueMinimumShrinks(t *testing.T) {
	// R's minimum (0.009) blocks a 0.007 top-up, while L's minimum (0.005)
	// still allows shrinking L by 0.007.
	instL := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.5}
	instR := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.9}
	lv := newFake("L", full)
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.013
		}
		return r.Qty
	})
	e := topUpExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: instL, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: instR, Side: domain.Sell, Price: 101}, 0.02, false, 0, 0)
	if res.Err != nil || res.RescueOrders != 1 {
		t.Fatalf("expected shrink: %+v", res)
	}
	requireQty(t, res.PosL, 0.013)
	requireQty(t, res.PosR, -0.013)
}

func TestStickyResidualUnwindsWholeStep(t *testing.T) {
	// Shortfall 0.007 and shrink 0.007 both below min 0.009: neither order is
	// possible, so the whole step unwinds to zero.
	instQ := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.9}
	lv := newFake("L", full)
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.013
		}
		return r.Qty
	})
	e := topUpExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: instQ, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: instQ, Side: domain.Sell, Price: 101}, 0.02, false, 0, 0)
	if res.Err != nil || res.PosL != 0 || res.PosR != 0 {
		t.Fatalf("expected full unwind: %+v", res)
	}
	if res.RescueOrders != 2 {
		t.Fatalf("expected two unwind orders, got %+v", res)
	}
}

func TestSubMinPositionRequiresIntervention(t *testing.T) {
	// The short leg itself is below the venue minimum and cannot be traded.
	instQ := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2, MinQuote: 0.9}
	lv := newFake("L", full)
	rv := newFake("R", func(n int, r domain.OrderReq) float64 {
		if n == 0 {
			return 0.008
		}
		return r.Qty
	})
	e := topUpExec(t, lv, rv)
	res := e.Execute(context.Background(),
		LegSpec{Venue: lv, Inst: instQ, Side: domain.Buy, Price: 100},
		LegSpec{Venue: rv, Inst: instQ, Side: domain.Sell, Price: 101}, 0.012, false, 0, 0)
	if res.Err == nil {
		t.Fatalf("expected manual-intervention error, got %+v", res)
	}
}

func TestTopUpPriceBounds(t *testing.T) {
	i := domain.Instrument{PriceDecimals: 2}
	if got := topUpPrice(LegSpec{Inst: i, Side: domain.Buy, Price: 100}, 25); got != 100.25 {
		t.Fatalf("buy top-up price %v", got)
	}
	if got := topUpPrice(LegSpec{Inst: i, Side: domain.Sell, Price: 100}, 25); got != 99.75 {
		t.Fatalf("sell top-up price %v", got)
	}
}
