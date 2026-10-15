package engine

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

type actorTestVenue struct {
	name      string
	books     chan domain.Book
	orders    chan domain.OrderUpdate
	flat      chan struct{}
	mu        sync.Mutex
	pos       float64
	traded    bool
	submit    func(context.Context, domain.OrderReq) (float64, error)
	positions func(context.Context) ([]domain.Position, error)
}

func newActorTestVenue(name string) *actorTestVenue {
	return &actorTestVenue{name: name, books: make(chan domain.Book, 1), orders: make(chan domain.OrderUpdate, 16), flat: make(chan struct{}, 1)}
}

func (v *actorTestVenue) Name() string { return v.name }
func (v *actorTestVenue) Instruments(context.Context) ([]domain.Instrument, error) {
	return nil, nil
}
func (v *actorTestVenue) SubscribeBook(context.Context, domain.Instrument) (<-chan domain.Book, error) {
	return v.books, nil
}
func (v *actorTestVenue) SubscribeOrders(context.Context) (<-chan domain.OrderUpdate, error) {
	return v.orders, nil
}
func (v *actorTestVenue) Submit(ctx context.Context, req domain.OrderReq) (domain.SubmitAck, error) {
	qty, err := v.submit(ctx, req)
	if err != nil {
		return domain.SubmitAck{}, err
	}
	v.mu.Lock()
	v.pos += float64(req.Side) * qty
	v.traded = v.traded || qty > 0
	v.mu.Unlock()
	status := domain.StatusFilled
	if qty < req.Qty {
		status = domain.StatusCanceled
	}
	v.orders <- domain.OrderUpdate{Venue: v.name, ClientID: req.ClientID, Status: status,
		FilledQty: qty, FilledQuote: qty * req.Price}
	return domain.SubmitAck{ClientID: req.ClientID}, nil
}
func (v *actorTestVenue) Positions(ctx context.Context) ([]domain.Position, error) {
	if v.positions != nil {
		return v.positions(ctx)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.traded && v.pos == 0 {
		select {
		case v.flat <- struct{}{}:
		default:
		}
	}
	return []domain.Position{{MarketID: 1, Qty: v.pos}}, nil
}
func (v *actorTestVenue) QueryOrder(context.Context, domain.Instrument, int64) (domain.OrderUpdate, error) {
	return domain.OrderUpdate{Status: domain.StatusUnknown}, nil
}
func (v *actorTestVenue) CancelOrder(context.Context, domain.Instrument, int64) error {
	return nil
}
func (v *actorTestVenue) CanTrade() bool { return true }
func (v *actorTestVenue) Close() error   { return nil }

// A fixed ready estimator makes the actor open without a real warmup period.
type actorTestEstimator struct{ mean float64 }

func (e actorTestEstimator) Update(time.Time, float64) {}
func (e actorTestEstimator) Mean() float64             { return e.mean }
func (e actorTestEstimator) Ready() bool               { return true }
func (e actorTestEstimator) Stats() estimator.Stats {
	return estimator.Stats{Mean: e.mean, Ready: true}
}

func actorTestBook(name string, bid float64) domain.Book {
	return domain.Book{Venue: name, MarketID: 1, Ts: time.Now(),
		Bids: []domain.Level{{Price: bid, Size: 1}}, Asks: []domain.Level{{Price: bid + 0.1, Size: 1}}}
}

// readyVolEstimator returns a warmed short-horizon volatility estimator whose
// most recent sample is current, so the volatility admission gate passes
// without waiting for a live book cadence.
func readyVolEstimator() *volatility.Estimator {
	now := time.Now()
	v := volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	for i := 0; i <= 5; i++ {
		v.AddAt(now.Add(time.Duration(i-5)*100*time.Millisecond), float64(i))
	}
	return v
}

func awaitActorCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func awaitActorOrder(t *testing.T, ch <-chan domain.OrderReq) domain.OrderReq {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for order")
		return domain.OrderReq{}
	}
}

func TestPairActorUpdatesBooksDuringExecutionAndRescue(t *testing.T) {
	for _, dir := range []strategy.Dir{strategy.DirLS, strategy.DirSL} {
		for _, successfulLeg := range []string{"L", "R"} {
			t.Run(dir.String()+"/"+successfulLeg, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(cancel)
				l, r := newActorTestVenue("L"), newActorTestVenue("R")
				successful, failed := l, r
				if successfulLeg == "R" {
					successful, failed = r, l
				}
				failed.submit = func(context.Context, domain.OrderReq) (float64, error) {
					return 0, domain.ErrRejected
				}
				opening, rescues := make(chan domain.OrderReq, 1), make(chan domain.OrderReq, 2)
				releaseOpening, releaseRescue := make(chan struct{}), make(chan struct{})
				rescueCount := 0
				successful.submit = func(ctx context.Context, req domain.OrderReq) (float64, error) {
					if !req.ReduceOnly {
						opening <- req
						select {
						case <-releaseOpening:
							return req.Qty, nil
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					rescueCount++
					rescues <- req
					if rescueCount == 1 {
						select {
						case <-releaseRescue:
							return 0, nil // first rescue cancels without a fill
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					return req.Qty, nil
				}
				tracker, err := execution.NewTracker(ctx, l, r)
				if err != nil {
					t.Fatal(err)
				}
				st, err := store.New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				inst := domain.Instrument{MarketID: 1, SizeDecimals: 4, PriceDecimals: 2}
				cfg := execution.DefaultConfig()
				cfg.AckTimeout = 50 * time.Millisecond
				cfg.SettleDelay, cfg.QueryInterval, cfg.RescueInterval = time.Millisecond, time.Millisecond, time.Millisecond
				mean := 0.0
				if dir == strategy.DirSL {
					mean = 100
				}
				a, err := NewPairActor(PairConfig{
					Pair: symbols.NewPair("TEST", inst, inst), L: l, R: r,
					Params: strategy.Params{StepQty: 0.01, MaxSteps: 1, OpenBps: 1, CloseBps: 1, DepthMult: 2},
					Est:    actorTestEstimator{mean: mean}, Store: st, Tracker: tracker, IDs: execution.NewIDGen(), ExecCfg: cfg,
					SampleEvery: time.Millisecond, MaxBookAge: time.Second,
					Vol: readyVolEstimator(), VolEvery: time.Hour, VolMaxAge: time.Second,
					IntervalMin: 20 * time.Millisecond, IntervalMax: 20 * time.Millisecond,
				})
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- a.Run(ctx) }()
				t.Cleanup(func() {
					cancel()
					select {
					case err := <-done:
						if err != nil {
							t.Errorf("actor stopped: %v", err)
						}
					case <-time.After(time.Second):
						t.Error("actor did not stop after cancellation")
					}
				})
				l.books <- actorTestBook("L", 100)
				r.books <- actorTestBook("R", 100.2)
				open := awaitActorOrder(t, opening) // Run is now blocked in execution.
				pushBooks := func(bid float64) {
					l.books <- actorTestBook("L", bid)
					r.books <- actorTestBook("R", bid+0.2)
					awaitActorCondition(t, "both books to update while execution is blocked", func() bool {
						lb, lok := a.book("L", 1)
						rb, rok := a.book("R", 1)
						return lok && rok && lb.Bids[0].Price == bid && rb.Bids[0].Price == bid+0.2
					})
				}
				assertRescue := func(req domain.OrderReq, bid float64) {
					want := bid
					if successfulLeg == "R" {
						want += 0.2
					}
					if open.Side == domain.Sell { // closing a short buys at the new ask
						want += 0.1
					}
					if req.Side != open.Side.Opposite() || !req.ReduceOnly || req.Type != domain.Limit || req.Qty != open.Qty || math.Abs(req.Price-want) > 1e-9 {
						t.Fatalf("rescue %+v, want opposite reduce-only order at latest price %.2f", req, want)
					}
				}
				pushBooks(101)
				close(releaseOpening)
				assertRescue(awaitActorOrder(t, rescues), 101)
				pushBooks(102) // Run is still blocked, now waiting for a rescue order.
				close(releaseRescue)
				assertRescue(awaitActorOrder(t, rescues), 102)
				select {
				case <-successful.flat: // Executor has verified the rescue filled.
				case <-time.After(time.Second):
					t.Fatal("executor did not verify the successful leg was flattened")
				}
			})
		}
	}
}

func TestPairActorRescueBookFreshness(t *testing.T) {
	l, r := newActorTestVenue("L"), newActorTestVenue("R")
	a := &PairActor{cfg: PairConfig{L: l, R: r, MaxBookAge: time.Second, Pair: symbols.NewPair("TEST", domain.Instrument{MarketID: 1}, domain.Instrument{MarketID: 1})}}
	for _, name := range []string{"L", "R"} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name string
				age  time.Duration
				want bool
			}{{"fresh", 0, true}, {"expired", 2 * time.Second, false}} {
				t.Run(tc.name, func(t *testing.T) {
					b := actorTestBook(name, 100)
					b.Ts = time.Now().Add(-tc.age)
					a.bookL, a.bookR = b, b
					if _, ok := a.book(name, 1); ok != tc.want {
						t.Fatalf("book usable=%v, want %v", ok, tc.want)
					}
				})
			}
		})
	}
}

func TestPairActorBookCollectorStops(t *testing.T) {
	for _, stop := range []string{"closed streams", "context canceled"} {
		t.Run(stop, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			l, r := newActorTestVenue("L"), newActorTestVenue("R")
			a := &PairActor{cfg: PairConfig{L: l, R: r, MaxBookAge: time.Second, Pair: symbols.NewPair("TEST", domain.Instrument{MarketID: 1}, domain.Instrument{MarketID: 1})}}
			done := make(chan struct{})
			go func() {
				defer close(done)
				a.collectBooks(ctx, l.books, r.books)
			}()
			l.books <- actorTestBook("L", 100)
			awaitActorCondition(t, "left book", func() bool {
				_, ok := a.book("L", 1)
				return ok
			})
			close(l.books)
			awaitActorCondition(t, "closed left stream to invalidate its book", func() bool {
				_, ok := a.book("L", 1)
				return !ok
			})
			r.books <- actorTestBook("R", 101)
			awaitActorCondition(t, "right book after left stream closes", func() bool {
				_, ok := a.book("R", 1)
				return ok
			})
			if stop == "closed streams" {
				close(r.books)
			} else {
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("book collector did not stop")
			}
		})
	}
}
