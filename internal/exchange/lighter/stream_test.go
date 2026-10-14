package lighter

import (
	"context"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func idleWebsocket(v *Venue) {
	v.ws.dial = func(ctx context.Context, _ string) (wsConn, error) { <-ctx.Done(); return nil, ctx.Err() }
}

func receiveBook(t *testing.T, ch <-chan domain.Book) domain.Book {
	t.Helper()
	select {
	case b, ok := <-ch:
		if !ok {
			t.Fatal("book stream closed")
		}
		return b
	case <-time.After(time.Second):
		t.Fatal("book update missing")
		return domain.Book{}
	}
}

func TestBookGapAndResetInvalidateQueuedSnapshot(t *testing.T) {
	v := testVenue(t, false)
	idleWebsocket(v)
	ch, err := v.SubscribeBook(context.Background(), validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := []byte(`{"order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"100","size":"3"}]}}`)
	v.onBook("subscribed/order_book", "order_book:1", snapshot)
	// Leave the valid snapshot queued: the invalidation must replace it.
	v.onBook("update/order_book", "order_book:1", []byte(`{"order_book":{"nonce":13,"begin_nonce":12,"bids":[],"asks":[]}}`))
	if b := receiveBook(t, ch); b.Valid() || b.Venue != "test" || b.MarketID != 1 {
		t.Fatalf("gap delivered %+v", b)
	}
	v.onBook("update/order_book", "order_book:1", []byte(`{"order_book":{"nonce":14,"begin_nonce":13,"bids":[{"price":"98","size":"1"}],"asks":[]}}`))
	select {
	case b := <-ch:
		t.Fatalf("unsynced delta published %+v", b)
	default:
	}
	v.onBook("subscribed/order_book", "order_book:1", snapshot)
	if b := receiveBook(t, ch); !b.Valid() || b.Seq != 10 {
		t.Fatalf("resync=%+v", b)
	}
	v.resetBooks()
	if b := receiveBook(t, ch); b.Valid() {
		t.Fatalf("reset delivered valid book %+v", b)
	}
}

func TestBookSubscriptionPreservesSnapshotAge(t *testing.T) {
	v := testVenue(t, false)
	idleWebsocket(v)
	ch, err := v.SubscribeBook(context.Background(), validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	v.onBook("subscribed/order_book", "order_book:1", []byte(`{"order_book":{"nonce":10,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"100","size":"3"}]}}`))
	b := receiveBook(t, ch)
	ch2, err := v.SubscribeBook(context.Background(), validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	if next := receiveBook(t, ch2); !next.Ts.Equal(b.Ts) {
		t.Fatalf("subscription changed timestamp: %v -> %v", b.Ts, next.Ts)
	}
}

func TestInvalidBookNumbersFailClosed(t *testing.T) {
	for _, number := range []string{"NaN", "+Inf", "bad", "-1"} {
		t.Run(number, func(t *testing.T) {
			v := testVenue(t, false)
			idleWebsocket(v)
			ch, err := v.SubscribeBook(context.Background(), validRequest().Inst)
			if err != nil {
				t.Fatal(err)
			}
			v.onBook("subscribed/order_book", "order_book:1", []byte(`{"order_book":{"nonce":1,"bids":[{"price":"99","size":"2"}],"asks":[{"price":"100","size":"3"}]}}`))
			v.onBook("update/order_book", "order_book:1", []byte(`{"order_book":{"nonce":2,"begin_nonce":1,"bids":[{"price":"`+number+`","size":"1"}]}}`))
			if b := receiveBook(t, ch); b.Valid() {
				t.Fatalf("invalid number kept valid book %+v", b)
			}
		})
	}
}

func TestBookMissingNonceDoesNotRefreshOldSnapshot(t *testing.T) {
	v := testVenue(t, false)
	idleWebsocket(v)
	ch, err := v.SubscribeBook(context.Background(), validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	v.onBook("subscribed/order_book", "order_book:1", []byte(`{"order_book":{"nonce":1,"bids":[{"price":"99","size":"1"}],"asks":[{"price":"100","size":"1"}]}}`))
	v.onBook("update/order_book", "order_book:1", []byte(`{"order_book":{"bids":[],"asks":[]}}`))
	if b := receiveBook(t, ch); b.Valid() {
		t.Fatal("missing nonce refreshed old snapshot")
	}
}

func TestSubscriptionsCancelAndCloseChannels(t *testing.T) {
	v := testVenue(t, true)
	idleWebsocket(v)
	ctx, cancel := context.WithCancel(context.Background())
	books, err := v.SubscribeBook(ctx, validRequest().Inst)
	if err != nil {
		t.Fatal(err)
	}
	orders, err := v.SubscribeOrders(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case _, ok := <-books:
		if ok {
			t.Fatal("cancelled book stream open")
		}
	case <-time.After(time.Second):
		t.Fatal("book cancellation hung")
	}
	select {
	case _, ok := <-orders:
		if ok {
			t.Fatal("cancelled order stream open")
		}
	case <-time.After(time.Second):
		t.Fatal("order cancellation hung")
	}
	v.booksMu.Lock()
	bookSubs := len(v.books[1].subs)
	v.booksMu.Unlock()
	v.orderMu.Lock()
	orderSubs := len(v.orderSubs)
	v.orderMu.Unlock()
	if bookSubs != 0 || orderSubs != 0 {
		t.Fatalf("subscriptions retained: book=%d order=%d", bookSubs, orderSubs)
	}
	if _, err := v.SubscribeOrders(ctx); err == nil {
		t.Fatal("cancelled order context accepted")
	}
	if err := v.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := v.SubscribeBook(context.Background(), validRequest().Inst); err == nil {
		t.Fatal("closed venue accepted subscription")
	}
}
