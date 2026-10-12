package execution

import (
	"context"
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
	cfg := DefaultConfig()
	cfg.AckTimeout, cfg.QueryInterval, cfg.SettleDelay, cfg.RescueInterval = 50*time.Millisecond, 10*time.Millisecond, time.Millisecond, time.Millisecond
	bk := domain.Book{Bids: []domain.Level{{Price: 100, Size: 1}}, Asks: []domain.Level{{Price: 100.1, Size: 1}}}
	return NewExecutor(cfg, tr, NewIDGen(), func(string, int) (domain.Book, bool) { return bk, true }, nil, func(string) {})
}

func full(_ int, r domain.OrderReq) float64 { return r.Qty }

func TestDynamicAllowance(t *testing.T) {
	leg := LegSpec{Inst: domain.Instrument{PriceDecimals: 2}, Side: domain.Buy, Price: 100}
	e := NewExecutor(Config{DynamicTicks: true, TickK: 1, TickMin: 1, TickMax: 100, PriceTicks: 30},
		nil, NewIDGen(), nil, func() float64 { return 10 }, func(string) {})
	// adverse move = 100 * 1 * 10bps / 1e4 = 0.1 => 10 ticks of 0.01
	if n := e.allowance(leg, 0.01); n != 10 {
		t.Fatalf("dynamic allowance=%d, want 10", n)
	}
	// volatility not ready falls back to the fixed allowance
	e.vol = func() float64 { return 0 }
	if n := e.allowance(leg, 0.01); n != 30 {
		t.Fatalf("fallback allowance=%d, want 30", n)
	}
	// high volatility is capped at TickMax
	e.vol = func() float64 { return 5000 }
	if n := e.allowance(leg, 0.01); n != 100 {
		t.Fatalf("capped allowance=%d, want 100", n)
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
	if o := r.orders[1]; o.Side != domain.Buy || !o.ReduceOnly {
		t.Fatalf("%+v", o)
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
