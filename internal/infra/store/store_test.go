package store

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

func TestConfigAndRuntimeRoundTrip(t *testing.T) {
	s := openTemp(t)
	raw := json.RawMessage(`{"symbol":"BTC","leverage":5}`)
	if err := s.SaveConfig(Config{
		Exchange: "lighter", Symbol: "BTC", Strategy: "grid",
		Direction: "long", Params: raw,
	}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.LoadConfig("lighter")
	if err != nil || !ok {
		t.Fatalf("load config: ok=%v err=%v", ok, err)
	}
	if got.Symbol != "BTC" || got.Direction != "long" || string(got.Params) != string(raw) {
		t.Fatalf("got %+v", got)
	}
	if _, ok, _ = s.LoadConfig("missing"); ok {
		t.Fatal("missing config should not exist")
	}

	if err := s.SaveRuntime(Runtime{
		Exchange: "lighter", Status: "running", Epoch: 3, Snapshot: []byte(`{"epoch":3}`),
	}); err != nil {
		t.Fatal(err)
	}
	rt, ok, err := s.LoadRuntime("lighter")
	if err != nil || !ok {
		t.Fatalf("load runtime: ok=%v err=%v", ok, err)
	}
	if rt.Status != "running" || rt.Epoch != 3 || string(rt.Snapshot) != `{"epoch":3}` {
		t.Fatalf("got %+v", rt)
	}
}

func TestFillsRespectResetAt(t *testing.T) {
	s := openTemp(t)
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = s.InsertFill(Fill{Exchange: "lighter", Symbol: "SOL", COID: 1, Side: "buy", Price: "100", Qty: "1", Fee: "0", Time: t0})
	_ = s.InsertFill(Fill{Exchange: "lighter", Symbol: "SOL", COID: 2, Side: "sell", Price: "110", Qty: "1", Fee: "0", Time: t0.Add(time.Hour)})
	_ = s.InsertFill(Fill{Exchange: "lighter", Symbol: "BTC", COID: 3, Side: "buy", Price: "90", Qty: "1", Fee: "0", Time: t0.Add(2 * time.Hour)})
	if err := s.ResetStats("lighter", t0.Add(30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	fills, err := s.ListFills("lighter", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fills) != 2 {
		t.Fatalf("after reset without symbol filter got %+v", fills)
	}
	sol, err := s.ListFills("lighter", "SOL", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(sol) != 1 || sol[0].COID != order.ClientOrderID(2) || sol[0].Symbol != "SOL" {
		t.Fatalf("SOL fills after reset got %+v", sol)
	}
}

func TestRecordOrderFillIsIncremental(t *testing.T) {
	s := openTemp(t)
	o := order.Order{
		ClientOrderID: order.MustEncode(order.Ref{Slot: 1, Epoch: 1, Cell: 0, Purpose: order.PurposeOpen, Seq: 0}),
		Symbol:        "BTC",
		Side:          order.Buy,
		Price:         d("100"),
		Quantity:      d("1"),
		FilledQty:     d("0.4"),
		AvgFillPrice:  d("100"),
		State:         order.StatePartiallyFilled,
		IsMaker:       true,
		UpdatedAt:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := s.RecordOrderFill("rh_lighter", o); err != nil {
		t.Fatal(err)
	}
	o.FilledQty = d("1")
	o.AvgFillPrice = d("101")
	o.State = order.StateFilled
	if err := s.RecordOrderFill("rh_lighter", o); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordOrderFill("rh_lighter", o); err != nil {
		t.Fatal(err)
	}
	fills, err := s.ListFills("rh_lighter", "BTC", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(fills) != 2 {
		t.Fatalf("got %d fills, want 2 incremental rows", len(fills))
	}
	var total float64
	for _, f := range fills {
		q, _ := decimal.NewFromString(f.Qty)
		total += q.InexactFloat64()
	}
	if total < 0.99 || total > 1.01 {
		t.Fatalf("sum qty = %v, want 1", total)
	}
}

func openTemp(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }
