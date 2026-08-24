package entry

import (
	"testing"
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testMarket() market.Market {
	return market.Market{
		Symbol:        "BTC",
		TickSize:      d("0.1"),
		LotSize:       d("0.001"),
		MinQty:        d("0.001"),
		MinNotional:   d("10"),
		MaxLeverage:   50,
		PriceDecimals: 1,
		SizeDecimals:  3,
	}
}

func book(bid, ask string) market.BookTicker {
	return market.BookTicker{Bid: d(bid), Ask: d(ask)}
}

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func firstPlace(t *testing.T, acts []strategy.Action) strategy.PlaceOrder {
	t.Helper()
	for _, a := range acts {
		if p, ok := a.(strategy.PlaceOrder); ok {
			return p
		}
	}
	t.Fatalf("no PlaceOrder in %d actions", len(acts))
	return strategy.PlaceOrder{}
}

func countPlace(acts []strategy.Action) int {
	n := 0
	for _, a := range acts {
		if _, ok := a.(strategy.PlaceOrder); ok {
			n++
		}
	}
	return n
}

func TestMarketEntryFillsInOneShot(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMarket
	p.SliceCount = 1
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("2"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if po.Type != order.Limit || po.TIF != order.PostOnly || po.Side != order.Buy {
		t.Fatalf("got %+v, want post-only buy", po)
	}
	if !po.Price.Equal(d("149.9")) {
		t.Fatalf("maker entry price = %s, want bid 149.9", po.Price)
	}
	if !po.Quantity.Equal(d("2")) {
		t.Fatalf("qty = %s", po.Quantity)
	}

	_, done, failed := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po.ClientOrderID,
			Side:          order.Buy,
			FilledQty:     d("2"),
			Quantity:      d("2"),
			State:         order.StateFilled,
		},
		Now: t0,
	})
	if failed || !done {
		t.Fatalf("done=%v failed=%v reason=%s", done, failed, tr.reason)
	}
	filled, _ := tr.Result()
	if !filled.Equal(d("2")) {
		t.Fatalf("filled = %s", filled)
	}
}

func TestMarketEntrySlices(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMarket
	p.SliceCount = 2
	p.SliceInterval = strategy.MustParseDuration("1s")
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("2"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if !po.Quantity.Equal(d("1")) {
		t.Fatalf("first slice qty = %s, want 1", po.Quantity)
	}

	acts, done, _ := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po.ClientOrderID, Side: order.Buy,
			FilledQty: d("1"), Quantity: d("1"), State: order.StateFilled,
		},
		Now: t0,
	})
	if done {
		t.Fatal("should wait for second slice")
	}
	if len(acts) != 0 {
		t.Fatal("second slice must wait for slice interval")
	}

	acts, done, _ = tr.OnEvent(strategy.TickEvent{Now: t0.Add(time.Second)})
	if done {
		t.Fatal("second slice just placed")
	}
	po2 := firstPlace(t, acts)
	if !po2.Quantity.Equal(d("1")) {
		t.Fatalf("second slice qty = %s", po2.Quantity)
	}
}

func TestMakerFollowReprices(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	p.RepriceTicks = 1
	p.RepriceInterval = 0
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if po.TIF != order.PostOnly || !po.Price.Equal(d("149.9")) {
		t.Fatalf("follow price = %s tif = %s", po.Price, po.TIF)
	}

	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	acts, _, _ = tr.OnEvent(strategy.BookEvent{
		Book: book("150.2", "150.4"),
		Mark: d("150.3"),
		Now:  t0.Add(time.Second),
	})
	if len(acts) != 1 {
		t.Fatalf("expected cancel, got %d actions", len(acts))
	}
	if _, ok := acts[0].(strategy.CancelOrder); !ok {
		t.Fatalf("expected CancelOrder, got %T", acts[0])
	}

	acts, _, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateCanceled, Quantity: d("1")},
		Now:   t0.Add(time.Second),
	})
	po2 := firstPlace(t, acts)
	if !po2.Price.Equal(d("150.2")) {
		t.Fatalf("repriced to %s, want 150.2", po2.Price)
	}
}

func TestMakerFollowDoesNotRepriceWhenBidDrops(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	p.RepriceTicks = 1
	p.RepriceInterval = 0
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})
	acts, _, _ = tr.OnEvent(strategy.BookEvent{
		Book: book("149.7", "149.9"),
		Mark: d("149.8"),
		Now:  t0.Add(time.Second),
	})
	if countPlace(acts) != 0 {
		t.Fatalf("bid drop must not reprice, got %+v", acts)
	}
}

func TestRejectedDoesNotPlaceImmediately(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	tr := New(p, testMarket(), 0, 1)
	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	acts, done, failed := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateRejected, Quantity: d("1")},
		Now:   t0,
	})
	if done || failed {
		t.Fatalf("done=%v failed=%v", done, failed)
	}
	if countPlace(acts) != 0 {
		t.Fatalf("reject must not immediately replace, got %+v", acts)
	}
	acts, _, _ = tr.OnEvent(strategy.TickEvent{Now: t0.Add(time.Second)})
	po2 := firstPlace(t, acts)
	if po2.ClientOrderID == po.ClientOrderID {
		t.Fatal("retry must use a new client id")
	}
}

func TestNoSecondPlaceWhilePending(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	tr := New(p, testMarket(), 0, 1)
	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	if countPlace(acts) != 1 {
		t.Fatal("expected first place")
	}
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: firstPlace(t, acts).ClientOrderID, State: order.StatePending, Quantity: d("1")},
		Now:   t0,
	})
	acts, _, _ = tr.OnEvent(strategy.TickEvent{Now: t0.Add(time.Second)})
	if countPlace(acts) != 0 {
		t.Fatalf("pending order must not be doubled, got %+v", acts)
	}
	acts, _, _ = tr.OnEvent(strategy.BookEvent{Book: book("150.0", "150.2"), Mark: d("150.1"), Now: t0.Add(time.Second)})
	if countPlace(acts) != 0 {
		t.Fatalf("book update while pending must not place another, got %+v", acts)
	}
}

func TestLimitPriceWaitsAndAbortsOnTimeout(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryLimitPrice
	p.Price = d("140")
	p.Timeout = strategy.MustParseDuration("5s")
	p.OnTimeout = strategy.TimeoutAbort
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if !po.Price.Equal(d("140")) || po.TIF != order.PostOnly {
		t.Fatalf("limit order = %+v", po)
	}
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	acts, done, failed := tr.OnEvent(strategy.TickEvent{Now: t0.Add(5 * time.Second)})
	if done || !failed {
		t.Fatalf("done=%v failed=%v", done, failed)
	}
	if len(acts) != 1 {
		t.Fatalf("expected cancel on abort, got %d", len(acts))
	}
}

func TestAlreadyAtTargetIsDone(t *testing.T) {
	tr := New(strategy.DefaultEntryParams(), testMarket(), 0, 1)
	acts := tr.Start(d("2"), d("2"), book("149.9", "150.1"), d("150"), t0)
	if len(acts) != 0 {
		t.Fatalf("expected no actions, got %d", len(acts))
	}
	if tr.phase != PhaseDone {
		t.Fatalf("phase = %s", tr.phase)
	}
}

func TestShortEntrySells(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMarket
	tr := New(p, testMarket(), 0, 1)
	acts := tr.Start(d("-2"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if po.Side != order.Sell || po.TIF != order.PostOnly {
		t.Fatalf("side = %s tif = %s, want post-only sell", po.Side, po.TIF)
	}
	if !po.Price.Equal(d("150.1")) {
		t.Fatalf("short maker price = %s, want ask 150.1", po.Price)
	}
}

func TestPartialFillAccumulates(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryLimitPrice
	p.Price = d("140")
	p.FillTolerance = d("0.01")
	tr := New(p, testMarket(), 0, 1)
	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)

	_, done, _ := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po.ClientOrderID, Side: order.Buy,
			FilledQty: d("0.4"), Quantity: d("1"), State: order.StatePartiallyFilled,
		},
		Now: t0,
	})
	if done {
		t.Fatal("0.4/1 should not finish")
	}
	_, done, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po.ClientOrderID, Side: order.Buy,
			FilledQty: d("1"), Quantity: d("1"), State: order.StateFilled,
		},
		Now: t0,
	})
	if !done {
		t.Fatal("full fill should finish")
	}
	filled, _ := tr.Result()
	if !filled.Equal(d("1")) {
		t.Fatalf("filled = %s (double-counted?)", filled)
	}
}

func TestEntryTimeoutSwitchesToMarket(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	p.Timeout = strategy.MustParseDuration("5s")
	p.OnTimeout = strategy.TimeoutMarket
	p.MaxSlippage = d("0.005")
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	if po.TIF != order.PostOnly {
		t.Fatalf("before timeout want post-only, got %+v", po)
	}
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	acts, done, failed := tr.OnEvent(strategy.TickEvent{Now: t0.Add(5 * time.Second)})
	if done || failed {
		t.Fatalf("timeout should cancel then market, done=%v failed=%v", done, failed)
	}
	if len(acts) != 1 {
		t.Fatalf("expected cancel on timeout, got %d", len(acts))
	}

	acts, _, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, State: order.StateCanceled, Quantity: d("1")},
		Now:   t0.Add(5 * time.Second),
	})
	mkt := firstPlace(t, acts)
	if mkt.Type != order.Market || mkt.TIF != order.IOC || mkt.Side != order.Buy {
		t.Fatalf("timeout order = %+v, want market IOC buy", mkt)
	}
	if !mkt.Price.Equal(d("150.9")) { // ask 150.1 * 1.005 → 150.8505, RoundUp tick 0.1 → 150.9
		t.Fatalf("protection price = %s, want 150.9", mkt.Price)
	}
}

func TestMakerFollowLateFillAfterRepriceDoesNotPlaceAgain(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	p.RepriceTicks = 1
	p.RepriceInterval = 0
	p.FillTolerance = d("0.01")
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po1 := firstPlace(t, acts)
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po1.ClientOrderID, Side: order.Buy, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	_, done, _ := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po1.ClientOrderID, Side: order.Buy,
			FilledQty: d("0.4"), Quantity: d("1"), State: order.StatePartiallyFilled,
		},
		Now: t0.Add(time.Second),
	})
	if done {
		t.Fatal("0.4/1 should not finish")
	}

	acts, _, _ = tr.OnEvent(strategy.BookEvent{
		Book: book("150.2", "150.4"),
		Mark: d("150.3"),
		Now:  t0.Add(2 * time.Second),
	})
	if len(acts) != 1 {
		t.Fatalf("expected cancel, got %d actions", len(acts))
	}

	acts, done, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po1.ClientOrderID, Side: order.Buy,
			FilledQty: d("0.4"), Quantity: d("1"), State: order.StateCanceled,
		},
		Now: t0.Add(2 * time.Second),
	})
	if done {
		t.Fatal("should still place remainder")
	}
	po2 := firstPlace(t, acts)
	if !po2.Quantity.Equal(d("0.6")) {
		t.Fatalf("remainder qty = %s, want 0.6", po2.Quantity)
	}

	acts, done, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po1.ClientOrderID, Side: order.Buy,
			FilledQty: d("1"), Quantity: d("1"), State: order.StateFilled,
		},
		Now: t0.Add(3 * time.Second),
	})
	if !done {
		t.Fatal("late fill of old order should complete entry")
	}
	if countPlace(acts) != 0 {
		t.Fatalf("must not place another entry, got %+v", acts)
	}
	if len(acts) != 1 {
		t.Fatalf("expected cancel of replacement, got %d actions", len(acts))
	}
	c, ok := acts[0].(strategy.CancelOrder)
	if !ok || c.ClientOrderID != po2.ClientOrderID {
		t.Fatalf("expected cancel %v, got %#v", po2.ClientOrderID, acts[0])
	}
	filled, _ := tr.Result()
	if !filled.Equal(d("1")) {
		t.Fatalf("filled = %s", filled)
	}
}

func TestMakerFollowRepriceThenOldOrderFillsFully(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	p.RepriceTicks = 1
	p.RepriceInterval = 0
	tr := New(p, testMarket(), 0, 1)

	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po1 := firstPlace(t, acts)
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po1.ClientOrderID, Side: order.Buy, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	acts, _, _ = tr.OnEvent(strategy.BookEvent{
		Book: book("150.2", "150.4"),
		Mark: d("150.3"),
		Now:  t0.Add(time.Second),
	})
	if len(acts) != 1 {
		t.Fatalf("expected cancel, got %d", len(acts))
	}

	acts, done, _ := tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po1.ClientOrderID, Side: order.Buy,
			Quantity: d("1"), State: order.StateCanceled,
		},
		Now: t0.Add(time.Second),
	})
	if done {
		t.Fatal("empty cancel should re-place full remainder")
	}
	po2 := firstPlace(t, acts)
	if !po2.Quantity.Equal(d("1")) {
		t.Fatalf("replacement qty = %s, want 1", po2.Quantity)
	}

	acts, done, _ = tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: po1.ClientOrderID, Side: order.Buy,
			FilledQty: d("1"), Quantity: d("1"), State: order.StateFilled,
		},
		Now: t0.Add(2 * time.Second),
	})
	if !done {
		t.Fatal("old order full fill should complete entry")
	}
	if countPlace(acts) != 0 {
		t.Fatalf("must not place another initial entry, got %+v", acts)
	}
	c, ok := acts[0].(strategy.CancelOrder)
	if !ok || c.ClientOrderID != po2.ClientOrderID {
		t.Fatalf("expected cancel replacement, got %#v", acts)
	}
}

func TestPositionAtTargetCancelsRestingEntry(t *testing.T) {
	p := strategy.DefaultEntryParams()
	p.Mode = strategy.EntryMakerFollow
	tr := New(p, testMarket(), 0, 1)
	acts := tr.Start(d("1"), d("0"), book("149.9", "150.1"), d("150"), t0)
	po := firstPlace(t, acts)
	tr.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: po.ClientOrderID, Side: order.Buy, State: order.StateOpen, Quantity: d("1")},
		Now:   t0,
	})

	acts, done, failed := tr.OnEvent(strategy.PositionEvent{
		Position: position.Position{Size: d("1")},
		Now:      t0.Add(time.Second),
	})
	if failed || !done {
		t.Fatalf("done=%v failed=%v", done, failed)
	}
	if countPlace(acts) != 0 {
		t.Fatalf("must not place again, got %+v", acts)
	}
	c, ok := acts[0].(strategy.CancelOrder)
	if !ok || c.ClientOrderID != po.ClientOrderID {
		t.Fatalf("expected cancel resting entry, got %#v", acts)
	}
}
