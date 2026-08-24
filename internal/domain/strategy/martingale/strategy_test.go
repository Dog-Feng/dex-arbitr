package martingale

import (
	"encoding/json"
	"testing"
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"
)

var epoch0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testState(mark, pos string) strategy.State {
	m := d(mark)
	return strategy.State{
		Market:   testMarket(),
		Position: position.Position{Symbol: "SOL", Size: d(pos), MarkPrice: m, EntryPrice: m},
		Account:  account.Snapshot{Available: d("100000")},
		Book:     market.BookTicker{Bid: m.Sub(d("0.1")), Ask: m.Add(d("0.1"))},
		Mark:     m,
		Now:      epoch0,
	}
}

func newStrategy(t *testing.T, p Params) *Strategy {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(raw)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func placements(acts []strategy.Action) []strategy.PlaceOrder {
	var out []strategy.PlaceOrder
	for _, a := range acts {
		if p, ok := a.(strategy.PlaceOrder); ok {
			out = append(out, p)
		}
	}
	return out
}

func confirm(t *testing.T, s *Strategy, p strategy.PlaceOrder) {
	t.Helper()
	o := order.Order{
		ClientOrderID: p.ClientOrderID,
		Side:          p.Side,
		Price:         p.Price,
		Quantity:      p.Quantity,
		State:         order.StateOpen,
	}
	if _, err := s.OnEvent(strategy.OrderEvent{Order: o, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
}

func fill(t *testing.T, s *Strategy, p strategy.PlaceOrder, now time.Time) []strategy.Action {
	t.Helper()
	o := order.Order{
		ClientOrderID: p.ClientOrderID,
		Side:          p.Side,
		Price:         p.Price,
		Quantity:      p.Quantity,
		FilledQty:     p.Quantity,
		AvgFillPrice:  p.Price,
		State:         order.StateFilled,
	}
	acts, err := s.OnEvent(strategy.OrderEvent{Order: o, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	return acts
}

func TestInitEntersThenPlacesAddsAndTP(t *testing.T) {
	s := newStrategy(t, testParams())
	acts, err := s.Init(testState("100", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if s.phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s", s.phase)
	}
	if n := 0; true {
		for _, a := range acts {
			if _, ok := a.(strategy.EnsurePosition); ok {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("EnsurePosition count = %d", n)
		}
	}

	qty := s.target
	next, err := s.OnEvent(strategy.EntryDoneEvent{Filled: qty, Now: epoch0.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	placed := placements(next)
	// 3 笔加仓 + 1 笔止盈
	if len(placed) != 4 {
		t.Fatalf("placed %d, want 4: %+v", len(placed), placed)
	}
	var adds, tps int
	for _, p := range placed {
		ref := p.ClientOrderID.Decode()
		switch ref.Purpose {
		case order.PurposeOpen:
			adds++
			if p.Side != order.Buy {
				t.Fatalf("add side = %s", p.Side)
			}
		case order.PurposeTakeProfit:
			tps++
			if !p.ReduceOnly {
				t.Fatal("tp must be reduce-only")
			}
			if p.Side != order.Sell {
				t.Fatalf("tp side = %s", p.Side)
			}
		}
	}
	if adds != 3 || tps != 1 {
		t.Fatalf("adds=%d tp=%d", adds, tps)
	}
	v := s.View()
	if v.OrderTarget != 4 {
		t.Fatalf("order target = %d", v.OrderTarget)
	}
}

func TestInitKeepsExistingInventoryAndPlaces(t *testing.T) {
	s := newStrategy(t, testParams())
	acts, err := s.Init(testState("100", "20"))
	if err != nil {
		t.Fatal(err)
	}
	if s.phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s, want running", s.phase)
	}
	for _, a := range acts {
		if _, ok := a.(strategy.EnsurePosition); ok {
			t.Fatal("must not dump existing inventory via EnsurePosition")
		}
	}
	placed := placements(acts)
	if len(placed) == 0 {
		t.Fatal("expected add/tp orders on existing inventory")
	}
}

func TestInitRejectsOppositePosition(t *testing.T) {
	s := newStrategy(t, testParams())
	_, err := s.Init(testState("100", "-5"))
	if err == nil {
		t.Fatal("expected opposite-position error")
	}
}

func TestAddFillRehangsTakeProfitQtyAndPrice(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	var add1, tp0 strategy.PlaceOrder
	for _, p := range placements(acts) {
		ref := p.ClientOrderID.Decode()
		if ref.Purpose == order.PurposeTakeProfit {
			tp0 = p
		}
		if ref.Purpose == order.PurposeOpen && ref.Cell == 1 {
			add1 = p
		}
	}
	if tp0.Quantity.IsZero() || add1.Quantity.IsZero() {
		t.Fatal("missing initial tp or add")
	}
	confirm(t, s, add1)
	confirm(t, s, tp0)

	next := fill(t, s, add1, epoch0.Add(time.Second))
	var modTP strategy.ModifyOrder
	var cancelTP, placeTP bool
	for _, a := range next {
		if c, ok := a.(strategy.CancelOrder); ok && c.ClientOrderID == tp0.ClientOrderID {
			cancelTP = true
		}
		if p, ok := a.(strategy.PlaceOrder); ok && p.ClientOrderID.Decode().Purpose == order.PurposeTakeProfit {
			placeTP = true
		}
		if m, ok := a.(strategy.ModifyOrder); ok && m.ClientOrderID == tp0.ClientOrderID {
			modTP = m
		}
	}
	if cancelTP {
		t.Fatal("add fill must not cancel old take-profit")
	}
	if placeTP {
		t.Fatal("add fill must not place a second take-profit")
	}
	if modTP.Quantity.IsZero() {
		t.Fatal("add fill must modify the resting take-profit")
	}
	wantQty := s.mkt.RoundQty(s.target.Add(add1.Quantity).Abs())
	if !modTP.Quantity.Equal(wantQty) {
		t.Fatalf("tp qty = %s, want %s (base+add)", modTP.Quantity, wantQty)
	}
	if !modTP.Price.LessThan(tp0.Price) {
		t.Fatalf("tp price %s should drop after buying lower (old %s)", modTP.Price, tp0.Price)
	}
	if !modTP.ReduceOnly {
		t.Fatal("modified tp must stay reduce-only")
	}
}

func TestAddFillDoesNotDoubleCountIfPositionAlreadyUpdated(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	var add1, tp0 strategy.PlaceOrder
	for _, p := range placements(acts) {
		ref := p.ClientOrderID.Decode()
		if ref.Purpose == order.PurposeTakeProfit {
			tp0 = p
		}
		if ref.Purpose == order.PurposeOpen && ref.Cell == 1 {
			add1 = p
		}
	}
	confirm(t, s, add1)
	confirm(t, s, tp0)
	newPos := s.target.Add(add1.Quantity)
	posActs, err := s.OnEvent(strategy.PositionEvent{
		Position: position.Position{Size: newPos, EntryPrice: d("99")},
		Now:      epoch0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantQty := s.mkt.RoundQty(newPos.Abs())
	var posMod strategy.ModifyOrder
	for _, a := range posActs {
		if p, ok := a.(strategy.PlaceOrder); ok && p.ClientOrderID.Decode().Purpose == order.PurposeTakeProfit {
			t.Fatalf("position update must not place a second tp, got qty %s", p.Quantity)
		}
		if m, ok := a.(strategy.ModifyOrder); ok && m.ClientOrderID == tp0.ClientOrderID {
			posMod = m
		}
	}
	if posMod.Quantity.IsZero() {
		t.Fatal("position increase must modify take-profit qty")
	}
	if !posMod.Quantity.Equal(wantQty) {
		t.Fatalf("tp qty after position = %s, want %s", posMod.Quantity, wantQty)
	}
	next := fill(t, s, add1, epoch0.Add(2*time.Second))
	for _, a := range next {
		if p, ok := a.(strategy.PlaceOrder); ok && p.ClientOrderID.Decode().Purpose == order.PurposeTakeProfit {
			t.Fatalf("fill must not place another tp, got qty %s", p.Quantity)
		}
		if m, ok := a.(strategy.ModifyOrder); ok && !m.Quantity.Equal(wantQty) {
			t.Fatalf("tp qty after fill = %s, want %s (must not double-count add)", m.Quantity, wantQty)
		}
	}
}

func TestTakeProfitCancelsRestingAdds(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	var tp strategy.PlaceOrder
	addCount := 0
	for _, p := range placements(acts) {
		ref := p.ClientOrderID.Decode()
		if ref.Purpose == order.PurposeTakeProfit {
			tp = p
		} else if ref.Purpose == order.PurposeOpen {
			addCount++
			confirm(t, s, p)
		}
	}
	if addCount == 0 || tp.Quantity.IsZero() {
		t.Fatal("missing resting adds or tp")
	}
	confirm(t, s, tp)

	next := fill(t, s, tp, epoch0.Add(time.Second))
	cancelAll := false
	for _, a := range next {
		if _, ok := a.(strategy.CancelAll); ok {
			cancelAll = true
		}
	}
	if !cancelAll {
		t.Fatal("take-profit must CancelAll resting add orders")
	}
	for k, lv := range s.adds {
		if lv != nil && lv.COID != 0 {
			t.Fatalf("add level %d still tracked locally after TP", k)
		}
	}
	if s.tp != nil && s.tp.COID != 0 {
		t.Fatal("take-profit slot must be cleared after TP fill")
	}
}

func TestResyncDuringEnteringAfterTPKeepsEntry(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	var tp strategy.PlaceOrder
	for _, p := range placements(acts) {
		if p.ClientOrderID.Decode().Purpose == order.PurposeTakeProfit {
			tp = p
		}
	}
	confirm(t, s, tp)
	fill(t, s, tp, epoch0.Add(time.Second))
	if s.phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s, want entering", s.phase)
	}

	// 模拟 Resync 仍带着止盈前的旧持仓（交易所推送滞后）。
	resync, err := s.OnEvent(strategy.ResyncEvent{
		Position: position.Position{Size: s.target, EntryPrice: d("100")},
		Orders:   nil,
		Now:      epoch0.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.position.IsPositive() {
		t.Fatalf("position = %s, must stay zero during entering after TP", s.position)
	}
	if s.phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s, must not jump to running on stale resync", s.phase)
	}
	ensure := 0
	for _, a := range resync {
		if _, ok := a.(strategy.EnsurePosition); ok {
			ensure++
		}
		if _, ok := a.(strategy.PlaceOrder); ok {
			t.Fatal("stale resync must not place add/tp orders before entry completes")
		}
	}
	if ensure != 1 {
		t.Fatalf("EnsurePosition count = %d, want 1 to restart first lot", ensure)
	}
}

func TestTakeProfitRestartsCycle(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	var tp strategy.PlaceOrder
	for _, p := range placements(acts) {
		if p.ClientOrderID.Decode().Purpose == order.PurposeTakeProfit {
			tp = p
		}
	}
	confirm(t, s, tp)
	next := fill(t, s, tp, epoch0.Add(time.Second))
	var ensure, stop, cancelAll, closePos int
	for _, a := range next {
		switch a.(type) {
		case strategy.EnsurePosition:
			ensure++
		case strategy.Stop:
			stop++
		case strategy.CancelAll:
			cancelAll++
		case strategy.ClosePosition:
			closePos++
		}
	}
	if ensure != 1 || stop != 0 || cancelAll != 1 || closePos != 1 {
		t.Fatalf("restart acts ensure=%d stop=%d cancelAll=%d close=%d", ensure, stop, cancelAll, closePos)
	}
	if s.phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s", s.phase)
	}
	if s.View().Epoch <= 1 {
		t.Fatalf("epoch = %d, want advanced for next cycle", s.View().Epoch)
	}
	if s.View().Stats.CompletedGrids != 1 {
		t.Fatalf("cycles = %d", s.View().Stats.CompletedGrids)
	}
}

func TestResyncCancelsStaleTakeProfit(t *testing.T) {
	s := newStrategy(t, testParams())
	_, _ = s.Init(testState("100", "0"))
	_, _ = s.OnEvent(strategy.EntryDoneEvent{Filled: s.target, Now: epoch0})
	tp1, err := order.Encode(order.Ref{
		Slot: 0, Epoch: s.epoch, Cell: 0, Purpose: order.PurposeTakeProfit, Seq: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tp2, err := order.Encode(order.Ref{
		Slot: 0, Epoch: s.epoch, Cell: 0, Purpose: order.PurposeTakeProfit, Seq: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	live, err := order.Encode(order.Ref{
		Slot: 0, Epoch: s.epoch, Cell: 1, Purpose: order.PurposeOpen, Seq: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	acts, err := s.OnEvent(strategy.ResyncEvent{
		Position: position.Position{Size: s.target, EntryPrice: d("100")},
		Orders: []order.Order{
			{ClientOrderID: tp1, State: order.StateOpen, Price: d("101.5"), Quantity: s.target},
			{ClientOrderID: tp2, State: order.StateOpen, Price: d("102"), Quantity: s.target},
			{ClientOrderID: live, State: order.StateOpen, Price: d("98"), Quantity: d("1")},
		},
		Now: epoch0,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancels := 0
	for _, a := range acts {
		if _, ok := a.(strategy.CancelOrder); ok {
			cancels++
		}
	}
	if cancels < 1 {
		t.Fatalf("expected extra TP cancelled, acts=%d", len(acts))
	}
}

func TestAdjustRangeUnsupported(t *testing.T) {
	s := newStrategy(t, testParams())
	_, err := s.OnCommand(strategy.Command{Kind: strategy.CmdAdjustRange, Now: epoch0})
	if err != strategy.ErrUnsupportedCommand {
		t.Fatalf("got %v", err)
	}
}
