package main

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

type checkVenue struct {
	name                                                       string
	inst                                                       domain.Instrument
	mu                                                         sync.Mutex
	pos                                                        float64
	requests                                                   []domain.OrderReq
	orders                                                     map[int64]domain.OrderUpdate
	updates                                                    chan domain.OrderUpdate
	bookClosed, bookMissing, noFill                            bool
	signErr, positionErr                                       error
	signCalls, ephemeralCalls, keyCalls, closeCalls, bookCalls int
	afterSubmit                                                func(domain.OrderReq)
	queryDelay                                                 int
	queries                                                    int
	unknownSubmit                                              bool
}

func newCheckVenue(name string) *checkVenue {
	return &checkVenue{name: name, inst: domain.Instrument{Venue: name, Symbol: "TEST", MarketID: 1, SizeDecimals: 3, PriceDecimals: 2, MinBase: 0.01, MinQuote: 0.5}, orders: make(map[int64]domain.OrderUpdate), updates: make(chan domain.OrderUpdate, 100)}
}

func (v *checkVenue) Name() string { return v.name }
func (v *checkVenue) Instruments(context.Context) ([]domain.Instrument, error) {
	return []domain.Instrument{v.inst}, nil
}
func (v *checkVenue) SubscribeBook(context.Context, domain.Instrument) (<-chan domain.Book, error) {
	v.bookCalls++
	ch := make(chan domain.Book, 4)
	if v.bookClosed {
		close(ch)
	} else if !v.bookMissing {
		ch <- checkBook(v.name)
	}
	return ch, nil
}
func (v *checkVenue) SubscribeOrders(context.Context) (<-chan domain.OrderUpdate, error) {
	return v.updates, nil
}
func (v *checkVenue) Submit(ctx context.Context, r domain.OrderReq) (domain.SubmitAck, error) {
	if ctx.Err() != nil {
		return domain.SubmitAck{}, fmtRejected(ctx.Err())
	}
	v.mu.Lock()
	v.requests = append(v.requests, r)
	qty := r.Qty
	if v.noFill && !r.ReduceOnly {
		qty = 0
	}
	if r.ReduceOnly {
		if float64(r.Side)*v.pos >= 0 {
			qty = 0
		} else {
			qty = math.Min(qty, math.Abs(v.pos))
		}
	}
	status := domain.StatusFilled
	if qty < r.Qty {
		status = domain.StatusCanceled
	}
	u := domain.OrderUpdate{Venue: v.name, MarketID: r.Inst.MarketID, ClientID: r.ClientID, Status: status, FilledQty: qty, FilledQuote: qty * r.Price, Ts: time.Now()}
	v.orders[r.ClientID] = u
	unknown := v.unknownSubmit && !r.ReduceOnly
	if !unknown {
		v.pos = domain.Round(v.pos+float64(r.Side)*qty, v.inst.SizeDecimals)
	}
	v.mu.Unlock()
	if v.afterSubmit != nil {
		v.afterSubmit(r)
	}
	if unknown {
		return domain.SubmitAck{ClientID: r.ClientID}, errors.New("transport outcome unknown")
	}
	v.updates <- u
	return domain.SubmitAck{ClientID: r.ClientID}, nil
}
func fmtRejected(err error) error { return errors.Join(domain.ErrRejected, err) }
func (v *checkVenue) QueryOrder(_ context.Context, _ domain.Instrument, cid int64) (domain.OrderUpdate, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.queries++
	u, ok := v.orders[cid]
	if !ok || v.queries <= v.queryDelay {
		return domain.OrderUpdate{Status: domain.StatusUnknown}, nil
	}
	if v.unknownSubmit && u.FilledQty > 0 {
		for _, req := range v.requests {
			if req.ClientID == cid && !req.ReduceOnly {
				v.pos = domain.Round(v.pos+float64(req.Side)*u.FilledQty, v.inst.SizeDecimals)
				// Account for this delayed fill only once.
				v.unknownSubmit = false
				break
			}
		}
	}
	return u, nil
}
func (v *checkVenue) CancelOrder(context.Context, domain.Instrument, int64) error { return nil }
func (v *checkVenue) Positions(context.Context) ([]domain.Position, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.positionErr != nil {
		return nil, v.positionErr
	}
	return []domain.Position{{MarketID: 1, Qty: v.pos}}, nil
}
func (v *checkVenue) CanTrade() bool                 { return true }
func (v *checkVenue) Close() error                   { v.closeCalls++; return nil }
func (v *checkVenue) CheckKey(context.Context) error { v.keyCalls++; return v.signErr }
func (v *checkVenue) SignOnly(domain.OrderReq, int64) (string, error) {
	v.signCalls++
	return "offline", v.signErr
}
func (v *checkVenue) SignEphemeral(domain.OrderReq, int64) (string, error) {
	v.ephemeralCalls++
	return "offline", v.signErr
}

func checkBook(name string) domain.Book {
	return domain.Book{Venue: name, MarketID: 1, Ts: time.Now(), Bids: []domain.Level{{Price: 100, Size: 10}}, Asks: []domain.Level{{Price: 100.1, Size: 10}}}
}
func checkPair(l, r *checkVenue) symbols.Pair { return symbols.NewPair("TEST", l.inst, r.inst) }

func checkConfig() *config.Config {
	c := &config.Config{Venues: []config.Venue{{Name: "L"}, {Name: "R"}}, Pairs: []config.Pair{{Left: "L", Right: "R", MaxSteps: 1}}}
	c.Strategy.Kind = "arbitrage"
	c.Strategy.DepthMult = 1
	c.Strategy.MaxBookAge = time.Second
	return c
}

func fastExecution() execution.Config {
	c := execution.DefaultConfig()
	c.AckTimeout, c.SettleDelay, c.QueryInterval, c.RescueInterval = time.Millisecond, time.Millisecond, time.Millisecond, time.Millisecond
	c.QueryRetries = 0
	return c
}

func TestSelectPairSupportsAutoWithoutManualPairs(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	c := checkConfig()
	c.Pairs = nil
	pc, lv, rv, err := selectPair(c, map[string]exchange.Venue{"L": l, "R": r})
	if err != nil || pc.Left != "L" || pc.Right != "R" || lv != l || rv != r {
		t.Fatalf("selection=%+v %v", pc, err)
	}
	c.Venues = append(c.Venues, config.Venue{Name: "third"})
	if _, _, _, err := selectPair(c, map[string]exchange.Venue{"L": l, "R": r}); err == nil {
		t.Fatal("ambiguous venues accepted")
	}
}

func TestRunChecksFailureStopsBeforeTrading(t *testing.T) {
	for _, tc := range []struct {
		name   string
		qty    float64
		change func(*checkVenue)
	}{
		{name: "extra precision", qty: 0.0101, change: func(*checkVenue) {}},
		{name: "zero quantity", qty: 0, change: func(*checkVenue) {}},
		{name: "closed book", qty: 0.01, change: func(v *checkVenue) { v.bookClosed = true }},
		{name: "missing book", qty: 0.01, change: func(v *checkVenue) { v.bookMissing = true }},
		{name: "key check fails", qty: 0.01, change: func(v *checkVenue) { v.signErr = errors.New("wrong key") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, r := newCheckVenue("L"), newCheckVenue("R")
			tc.change(l)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var out bytes.Buffer
			code := runChecks(ctx, checkConfig(), options{base: "TEST", qty: tc.qty, watch: 20 * time.Millisecond, live: true}, map[string]exchange.Venue{"L": l, "R": r}, &out)
			if code != 1 || len(l.requests) != 0 || len(r.requests) != 0 {
				t.Fatalf("exit=%d trades=%d/%d\n%s", code, len(l.requests), len(r.requests), out.String())
			}
			if l.closeCalls != 1 || r.closeCalls != 1 {
				t.Fatal("venues not closed on failure")
			}
			if tc.name == "key check fails" && l.keyCalls == 0 {
				t.Fatalf("key prerequisite was not exercised\n%s", out.String())
			}
		})
	}
}

func TestRunChecksReadOnlyCompletesWithoutSubmitting(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	var out bytes.Buffer
	code := runChecks(context.Background(), checkConfig(), options{base: "TEST", qty: 0.01, watch: 20 * time.Millisecond}, map[string]exchange.Venue{"L": readOnlyCheckVenue{l}, "R": readOnlyCheckVenue{r}}, &out)
	if code != 0 || l.ephemeralCalls != 1 || r.ephemeralCalls != 1 || len(l.requests) != 0 || len(r.requests) != 0 {
		t.Fatalf("exit=%d signcalls=%d/%d\n%s", code, l.ephemeralCalls, r.ephemeralCalls, out.String())
	}
}

func TestRunChecksZeroWatchFailsNormally(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	var out bytes.Buffer
	if code := runChecks(context.Background(), checkConfig(), options{qty: 0.01, live: true}, map[string]exchange.Venue{"L": l, "R": r}, &out); code != 1 {
		t.Fatalf("exit=%d", code)
	}
	if l.bookCalls != 0 || r.bookCalls != 0 || len(l.requests) != 0 {
		t.Fatal("invalid arguments continued")
	}
}

func TestLiveFlowRequiresZeroBaseline(t *testing.T) {
	for _, tc := range []struct {
		name        string
		left, right float64
		positionErr error
	}{
		{name: "opposite baseline", left: -0.01, right: 0.01},
		{name: "same baseline", left: 0.01, right: 0.01},
		{name: "tiny nonzero baseline", left: 1e-12},
		{name: "unknown baseline", positionErr: errors.New("account unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l, r := newCheckVenue("L"), newCheckVenue("R")
			l.pos, r.pos = tc.left, tc.right
			l.positionErr = tc.positionErr
			var out bytes.Buffer
			err := liveFlow(context.Background(), l, r, checkPair(l, r), 0.01, 1, time.Second, true, fastExecution(), &out)
			if err == nil || len(l.requests) != 0 || len(r.requests) != 0 {
				t.Fatalf("error=%v trades=%d/%d", err, len(l.requests), len(r.requests))
			}
		})
	}
}

func TestLiveRoundTripAndRescueAreFlat(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	var out bytes.Buffer
	err := liveFlow(context.Background(), l, r, checkPair(l, r), 0.01, 1, time.Second, true, fastExecution(), &out)
	if err != nil {
		t.Fatalf("flow error=%v\n%s", err, out.String())
	}
	if l.pos != 0 || r.pos != 0 {
		t.Fatalf("final positions=%v/%v", l.pos, r.pos)
	}
	if len(l.requests) != 4 || len(r.requests) != 2 {
		t.Fatalf("unexpected trades L=%+v R=%+v", l.requests, r.requests)
	}
	if !l.requests[1].ReduceOnly || !l.requests[3].ReduceOnly || !r.requests[1].ReduceOnly {
		t.Fatal("close or rescue was not reduce-only")
	}
}

func TestLiveCanceledAfterSubmissionStillCleansUp(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	l.afterSubmit = func(req domain.OrderReq) {
		if !req.ReduceOnly {
			cancel()
		}
	}
	var out bytes.Buffer
	err := liveFlow(ctx, l, r, checkPair(l, r), 0.01, 1, time.Second, false, fastExecution(), &out)
	if err == nil {
		t.Fatal("canceled round trip reported success")
	}
	if l.pos != 0 || r.pos != 0 {
		t.Fatalf("cancellation left positions=%v/%v, err=%v\n%s", l.pos, r.pos, err, out.String())
	}
	if len(l.requests) < 2 || !l.requests[len(l.requests)-1].ReduceOnly {
		t.Fatalf("left cleanup missing: %+v", l.requests)
	}
}

func TestLiveUnknownOpeningOutcomeRecoveredWithoutResubmit(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	l.unknownSubmit, r.unknownSubmit = true, true
	l.queryDelay, r.queryDelay = 2, 2
	var out bytes.Buffer
	err := liveFlow(context.Background(), l, r, checkPair(l, r), 0.01, 1, time.Second, false, fastExecution(), &out)
	if err == nil {
		t.Fatal("unknown initial outcome reported success")
	}
	if l.pos != 0 || r.pos != 0 {
		t.Fatalf("unknown outcome left positions=%v/%v: %v", l.pos, r.pos, err)
	}
	for _, v := range []*checkVenue{l, r} {
		opens := 0
		for _, req := range v.requests {
			if !req.ReduceOnly {
				opens++
			}
		}
		if opens != 1 || v.queries < 3 {
			t.Fatalf("%s opens=%d queries=%d requests=%+v", v.name, opens, v.queries, v.requests)
		}
	}
}

func TestRescueCheckFailsWhenLeftNeverFills(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	r.afterSubmit = func(req domain.OrderReq) {
		if req.ReduceOnly {
			l.mu.Lock()
			l.noFill = true
			l.mu.Unlock()
		}
	}
	var out bytes.Buffer
	err := liveFlow(context.Background(), l, r, checkPair(l, r), 0.01, 1, time.Second, true, fastExecution(), &out)
	if err == nil {
		t.Fatalf("rescue without left fill passed\n%s", out.String())
	}
	if l.pos != 0 || r.pos != 0 {
		t.Fatalf("failed rescue check left positions %v/%v", l.pos, r.pos)
	}
	if len(r.requests) != 2 {
		t.Fatalf("injected right order reached venue: %+v", r.requests)
	}
}

func TestReadOnlySigningUsesActualVenueEphemeralSigner(t *testing.T) {
	v := newCheckVenue("secondary")
	readonly := readOnlyCheckVenue{v}
	if err := verifySigning(context.Background(), readonly, domain.OrderReq{Inst: v.inst, ClientID: 1, Side: domain.Buy, Qty: 0.01, Price: 100}); err != nil {
		t.Fatal(err)
	}
	if v.ephemeralCalls != 1 || v.keyCalls != 0 || v.signCalls != 0 {
		t.Fatalf("sign calls ephemeral=%d configured=%d key=%d", v.ephemeralCalls, v.signCalls, v.keyCalls)
	}
}

type readOnlyCheckVenue struct{ *checkVenue }

func (readOnlyCheckVenue) CanTrade() bool { return false }

func TestBookInvalidationClearsFreshness(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	s := &bookStreams{L: l, R: r, pair: checkPair(l, r), maxAge: time.Second}
	if err := s.receive(true, checkBook("L"), true); err != nil {
		t.Fatal(err)
	}
	if err := s.receive(false, checkBook("R"), true); err != nil {
		t.Fatal(err)
	}
	if !s.ready() {
		t.Fatal("valid books not ready")
	}
	if err := s.receive(true, domain.Book{Venue: "L", MarketID: 1, Ts: time.Now()}, true); err != nil {
		t.Fatal(err)
	}
	if s.ready() || s.bad != 0 {
		t.Fatal("invalidation did not clear freshness")
	}
	if err := s.receive(true, checkBook("L"), true); err != nil {
		t.Fatal(err)
	}
	if !s.ready() {
		t.Fatal("snapshot did not recover freshness")
	}
}

func TestBookRefreshRejectsClosedAndCanceledStreams(t *testing.T) {
	l, r := newCheckVenue("L"), newCheckVenue("R")
	ch := make(chan domain.Book)
	close(ch)
	s := &bookStreams{L: l, R: r, pair: checkPair(l, r), chL: ch, chR: make(chan domain.Book), maxAge: time.Second}
	if err := s.refresh(context.Background()); err == nil {
		t.Fatal("closed stream accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.refresh(ctx); err == nil {
		t.Fatal("canceled refresh accepted")
	}
}
