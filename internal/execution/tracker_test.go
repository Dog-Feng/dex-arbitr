package execution

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

func TestTrackerStreamClosureStopsReader(t *testing.T) {
	v := newFake("L", full)
	tr, err := NewTracker(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	watch := tr.Watch(v.Name(), 1)

	close(v.ch)
	assertTrackerWatchClosed(t, watch)

	// Waiting before Close proves the closed stream exits independently of
	// context cancellation, rather than continuously receiving zero values.
	exited := make(chan struct{})
	go func() {
		tr.wg.Wait()
		close(exited)
	}()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("order-stream reader did not exit after stream closure")
	}
	assertTrackerWatchClosed(t, tr.Watch(v.Name(), 2))

	tr.mu.Lock()
	remaining := len(tr.waiters)
	tr.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("closed stream retained %d waiters", remaining)
	}
}

func TestTrackerCloseStopsReaders(t *testing.T) {
	l, r := newFake("L", full), newFake("R", full)
	tr, err := NewTracker(context.Background(), l, r)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	left, right := tr.Watch(l.Name(), 1), tr.Watch(r.Name(), 1)

	closed := make(chan struct{})
	go func() {
		tr.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not stop readers of open, idle order streams")
	}
	assertTrackerWatchClosed(t, left)
	assertTrackerWatchClosed(t, right)
	assertTrackerWatchClosed(t, tr.Watch(l.Name(), 2))
	assertTrackerWatchClosed(t, tr.Watch("new-venue", 2))

	tr.mu.Lock()
	remaining := len(tr.waiters)
	tr.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("Close retained %d waiters", remaining)
	}
	tr.Close() // Closing an already stopped tracker must remain safe.
}

func TestTrackerBackpressurePreservesTerminalUpdate(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status domain.OrderStatus
		qty    float64
	}{
		{name: "filled", status: domain.StatusFilled, qty: 2},
		{name: "canceled", status: domain.StatusCanceled, qty: 1},
		{name: "rejected", status: domain.StatusRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := newFake("L", full)
			tr, err := NewTracker(context.Background(), v)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(tr.Close)
			watch := tr.Watch(v.Name(), 1)

			// Keep the consumer idle through more updates than the old queue
			// could hold. Dispatch directly to make the order deterministic.
			for i := 0; i < 128; i++ {
				tr.dispatch(domain.OrderUpdate{
					Venue: v.Name(), ClientID: 1, Status: domain.StatusPending,
					FilledQty: tc.qty * float64(i) / 128,
				})
			}
			terminal := domain.OrderUpdate{
				Venue: v.Name(), ClientID: 1, Status: tc.status,
				FilledQty: tc.qty, FilledQuote: tc.qty * 100,
			}
			tr.dispatch(terminal)
			for i := 0; i < 128; i++ {
				tr.dispatch(domain.OrderUpdate{
					Venue: v.Name(), ClientID: 1, Status: domain.StatusOpen,
					FilledQty: float64(i),
				})
			}

			select {
			case got := <-watch:
				if got != terminal {
					t.Fatalf("received %+v, want terminal update %+v", got, terminal)
				}
			default:
				t.Fatal("terminal update was lost behind nonterminal updates")
			}
		})
	}
}

func TestTrackerDoneDropsLateUpdates(t *testing.T) {
	v := newFake("L", full)
	tr, err := NewTracker(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tr.Close)
	initialEntries := trackerMapEntries(tr)

	for cid := int64(1); cid <= 1024; cid++ {
		tr.Watch(v.Name(), cid)
		tr.Done(v.Name(), cid)
		tr.dispatch(domain.OrderUpdate{
			Venue: v.Name(), ClientID: cid, Status: domain.StatusFilled,
			FilledQty: 1, FilledQuote: 100,
		})
	}
	if got := trackerMapEntries(tr); got != initialEntries {
		t.Fatalf("late updates retained %d map entries, want %d", got, initialEntries)
	}

	watch := tr.Watch(v.Name(), 1)
	select {
	case got := <-watch:
		t.Fatalf("new watch replayed completed order update: %+v", got)
	default:
	}
	tr.Done(v.Name(), 1)
}

func assertTrackerWatchClosed(t *testing.T, ch <-chan domain.OrderUpdate) {
	t.Helper()
	select {
	case u, ok := <-ch:
		if ok {
			t.Fatalf("watch received %+v, want closed channel", u)
		}
	case <-time.After(time.Second):
		t.Fatal("watch channel did not close")
	}
}

// Count all map entries so adding another last-update cache cannot silently
// reintroduce per-order retention after Done. The subscribed venue stays open.
func trackerMapEntries(tr *Tracker) int {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	fields := reflect.ValueOf(tr).Elem()
	entries := 0
	for i := 0; i < fields.NumField(); i++ {
		if field := fields.Field(i); field.Kind() == reflect.Map {
			entries += field.Len()
		}
	}
	return entries
}
