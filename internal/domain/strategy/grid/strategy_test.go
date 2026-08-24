package grid

import (
	"encoding/json"
	"testing"
	"time"

	"dex-grid/internal/domain/account"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/position"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

var epoch0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func testState(mark string, pos string) strategy.State {
	m := d(mark)
	return strategy.State{
		Market:   testMarket(),
		Position: position.Position{Symbol: "BTC", Size: d(pos), MarkPrice: m},
		Account:  account.Snapshot{Available: d("100000")},
		Book:     market.BookTicker{Bid: m.Sub(d("0.1")), Ask: m.Add(d("0.1"))},
		Mark:     m,
		Slot:     0,
		Epoch:    0,
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

// placements 抽出所有下单意图，便于断言。
func placements(acts []strategy.Action) []strategy.PlaceOrder {
	var out []strategy.PlaceOrder
	for _, a := range acts {
		if p, ok := a.(strategy.PlaceOrder); ok {
			out = append(out, p)
		}
	}
	return out
}

func countAction[T strategy.Action](acts []strategy.Action) int {
	n := 0
	for _, a := range acts {
		if _, ok := a.(T); ok {
			n++
		}
	}
	return n
}

// 参数经过 JSON 往返后语义不变——页面下发的就是这个格式。
func TestParamsJSONRoundTrip(t *testing.T) {
	p := smallParams(Short)
	p.Risk.TakeProfitPrice = d("50")
	p.Risk.StopLossPrice = d("250")
	p.Risk.OutOfRange = strategy.OutOfRangeStopAndCancel
	p.Entry.Mode = strategy.EntryLimitPrice

	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var back Params
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Direction != Short {
		t.Errorf("direction = %s, want short", back.Direction)
	}
	if back.Risk.OutOfRange != strategy.OutOfRangeStopAndCancel {
		t.Errorf("out_of_range = %s, want stop_and_cancel", back.Risk.OutOfRange)
	}
	if back.Entry.Mode != strategy.EntryLimitPrice {
		t.Errorf("entry mode = %s, want limit_price", back.Entry.Mode)
	}
	if back.Order.MakerTIF != order.PostOnly {
		t.Errorf("maker tif = %s, want post_only", back.Order.MakerTIF)
	}
}

// 做多网格需要初始底仓，Init 先要求建仓而不是直接铺网格。
func TestInitLongRequestsEntryFirst(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	acts, err := s.Init(testState("150", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if countAction[strategy.SetLeverage](acts) != 1 {
		t.Fatal("expected a SetLeverage action")
	}
	if len(placements(acts)) != 0 {
		t.Fatal("must not place grid orders before the entry position is in place")
	}

	var ensure *strategy.EnsurePosition
	for _, a := range acts {
		if e, ok := a.(strategy.EnsurePosition); ok {
			ensure = &e
		}
	}
	if ensure == nil {
		t.Fatal("expected an EnsurePosition action")
	}
	if !ensure.Target.Equal(d("2")) {
		t.Fatalf("entry target = %s, want 2", ensure.Target)
	}
	if s.View().Phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s, want entering", s.View().Phase)
	}
}

// 中性网格初始目标仓位为 0，不需要建仓，直接铺网格。
func TestInitNeutralSkipsEntry(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, err := s.Init(testState("150", "0"))
	if err != nil {
		t.Fatal(err)
	}
	if countAction[strategy.EnsurePosition](acts) != 0 {
		t.Fatal("neutral grid should not request an entry")
	}
	if got := len(placements(acts)); got != 4 {
		t.Fatalf("placed %d orders, want 4", got)
	}
	if s.View().Phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s, want running", s.View().Phase)
	}
}

// 铺出的单必须每格一笔、方向正确、价格互不重复，且全部是 post-only。
func TestInitialPlacementLayout(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, err := s.Init(testState("150", "0"))
	if err != nil {
		t.Fatal(err)
	}

	want := map[string]order.Side{
		"100": order.Buy,
		"125": order.Buy,
		"175": order.Sell,
		"200": order.Sell,
	}
	got := placements(acts)
	if len(got) != len(want) {
		t.Fatalf("placed %d orders, want %d", len(got), len(want))
	}
	for _, p := range got {
		side, ok := want[p.Price.String()]
		if !ok {
			t.Errorf("unexpected order at price %s", p.Price)
			continue
		}
		if p.Side != side {
			t.Errorf("order at %s is %s, want %s", p.Price, p.Side, side)
		}
		if p.TIF != order.PostOnly {
			t.Errorf("order at %s uses %s, want post_only", p.Price, p.TIF)
		}
		delete(want, p.Price.String())
	}
	if len(want) != 0 {
		t.Errorf("missing orders at %v", want)
	}
}

// 中性网格两侧都可能开仓，挂单不能带 reduce-only，否则会被直接拒绝。
func TestNeutralOrdersAreNotReduceOnly(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	for _, p := range placements(acts) {
		if p.ReduceOnly {
			t.Fatalf("neutral grid order at %s must not be reduce-only", p.Price)
		}
	}
}

// 做多网格的卖出腿是平仓单，应当带 reduce-only。
func TestLongCloseLegIsReduceOnly(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	acts, err := s.OnEvent(strategy.EntryDoneEvent{Filled: d("2"), Now: epoch0})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range placements(acts) {
		wantReduce := p.Side == order.Sell
		if p.ReduceOnly != wantReduce {
			t.Errorf("%s order at %s: reduceOnly = %v, want %v", p.Side, p.Price, p.ReduceOnly, wantReduce)
		}
	}
}

// 一笔成交后只在对手价补一笔单，不会重复铺满整个网格。
func TestFillPlacesSingleOppositeOrder(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))

	buy100 := findPlacement(t, acts, "100")
	confirm(t, s, buy100, order.StateOpen, decimal.Zero)

	// 买单要在 100 成交，市价必须先跌到那里。
	moveMarket(t, s, "100.05", time.Second)

	next, err := s.OnEvent(orderEvent(buy100, order.StateFilled, buy100.Quantity))
	if err != nil {
		t.Fatal(err)
	}

	placed := placements(next)
	if len(placed) != 1 {
		t.Fatalf("placed %d orders after a fill, want 1", len(placed))
	}
	if !placed[0].Price.Equal(d("125")) || placed[0].Side != order.Sell {
		t.Fatalf("follow-up order = %s @ %s, want sell @ 125", placed[0].Side, placed[0].Price)
	}

	st := s.View().Stats
	if st.Fills != 1 || st.BuyFills != 1 {
		t.Errorf("stats = %+v, want one buy fill", st)
	}
	if st.CompletedGrids != 0 {
		t.Errorf("opening fill should not count as a completed grid")
	}
}

// 一买一卖闭合一个循环，毛利等于格子价差乘数量。
func TestCompletedGridCountsProfit(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))

	buy100 := findPlacement(t, acts, "100")
	confirm(t, s, buy100, order.StateOpen, decimal.Zero)

	moveMarket(t, s, "100.05", time.Second)
	next, _ := s.OnEvent(orderEvent(buy100, order.StateFilled, buy100.Quantity))

	sell125 := findPlacement(t, next, "125")
	confirm(t, s, sell125, order.StateOpen, decimal.Zero)

	moveMarket(t, s, "125", 2*time.Second)
	if _, err := s.OnEvent(orderEvent(sell125, order.StateFilled, sell125.Quantity)); err != nil {
		t.Fatal(err)
	}

	st := s.View().Stats
	if st.CompletedGrids != 1 {
		t.Fatalf("completed grids = %d, want 1", st.CompletedGrids)
	}
	if !st.GridProfit.Equal(d("25")) {
		t.Fatalf("grid profit = %s, want 25", st.GridProfit)
	}
	if !st.FeePaid.IsPositive() {
		t.Fatal("self-calculated realized path should accrue maker fees")
	}
	if !st.CycleFee.Equal(st.FeePaid) {
		t.Fatalf("cycle fee = %s, want all fees on a fully closed round trip (%s)", st.CycleFee, st.FeePaid)
	}
	if !st.RealizedPnL.Equal(st.GridProfit) {
		t.Fatalf("realized pnl = %s, want grid_profit %s (no estimated fee subtract)", st.RealizedPnL, st.GridProfit)
	}
}

// 已实现按真实成交均价，不按格子 High−Low。买在 99、卖在 125，毛利 26 而不是 25。
func TestCompletedGridUsesFillVWAP(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))

	buy100 := findPlacement(t, acts, "100")
	confirm(t, s, buy100, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "100.05", time.Second)

	buyFill := orderEvent(buy100, order.StateFilled, buy100.Quantity)
	buyFill.Order.AvgFillPrice = d("99")
	next, err := s.OnEvent(buyFill)
	if err != nil {
		t.Fatal(err)
	}

	sell125 := findPlacement(t, next, "125")
	confirm(t, s, sell125, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "125", 2*time.Second)
	if _, err := s.OnEvent(orderEvent(sell125, order.StateFilled, sell125.Quantity)); err != nil {
		t.Fatal(err)
	}

	st := s.View().Stats
	if st.CompletedGrids != 1 {
		t.Fatalf("completed grids = %d, want 1", st.CompletedGrids)
	}
	if !st.GridProfit.Equal(d("26")) {
		t.Fatalf("grid profit = %s, want 26 (125-99)", st.GridProfit)
	}
	if !st.RealizedPnL.Equal(st.GridProfit) {
		t.Fatalf("self-calculated realized = %s, want grid_profit %s", st.RealizedPnL, st.GridProfit)
	}
}

// 已实现只扣闭合循环两腿的手续费，不能把下一格未配对开腿的手续费也减掉。
func TestRealizedPnLExcludesOpenInventoryFee(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))

	buy100 := findPlacement(t, acts, "100")
	confirm(t, s, buy100, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "100.05", time.Second)
	buyFill := orderEvent(buy100, order.StateFilled, buy100.Quantity)
	next, err := s.OnEvent(buyFill)
	if err != nil {
		t.Fatal(err)
	}

	sell125 := findPlacement(t, next, "125")
	confirm(t, s, sell125, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "125", 2*time.Second)
	afterClose, err := s.OnEvent(orderEvent(sell125, order.StateFilled, sell125.Quantity))
	if err != nil {
		t.Fatal(err)
	}
	closed := s.View().Stats
	if closed.CompletedGrids != 1 {
		t.Fatalf("completed = %d", closed.CompletedGrids)
	}
	if !closed.CycleFee.Equal(closed.FeePaid) {
		t.Fatalf("after close cycle_fee=%s fee_paid=%s", closed.CycleFee, closed.FeePaid)
	}

	buyAgain := findPlacement(t, afterClose, "100")
	confirm(t, s, buyAgain, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "100.05", 3*time.Second)
	if _, err := s.OnEvent(orderEvent(buyAgain, order.StateFilled, buyAgain.Quantity)); err != nil {
		t.Fatal(err)
	}
	st := s.View().Stats
	if !st.FeePaid.GreaterThan(st.CycleFee) {
		t.Fatalf("open-leg fee should remain in fee_paid (%s) not cycle_fee (%s)", st.FeePaid, st.CycleFee)
	}
	if !st.RealizedPnL.Equal(closed.RealizedPnL) {
		t.Fatalf("realized changed after unpaired open fill: %s vs %s", st.RealizedPnL, closed.RealizedPnL)
	}
	if !st.RealizedPnL.Equal(st.GridProfit) {
		t.Fatalf("realized = %s, want grid_profit %s", st.RealizedPnL, st.GridProfit)
	}
}

// 交易所成交里的已实现已经扣过手续费，页面用它，不再 GridProfit − 估算手续费。
func TestVenueTradePnLOverridesGridProfit(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	coid := order.MustEncode(order.Ref{Slot: 0, Epoch: s.epoch, Cell: 0, Purpose: order.PurposeClose, Seq: 1})
	tr := order.Trade{
		ID:            77,
		ClientOrderID: coid,
		Side:          order.Sell,
		Price:         d("125"),
		Quantity:      d("1"),
		Fee:           d("0.01"),
		RealizedPnL:   d("24.5"),
	}
	if _, err := s.OnEvent(strategy.TradeEvent{Trade: tr, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	st := s.View().Stats
	if !st.RealizedPnL.Equal(d("24.5")) {
		t.Fatalf("realized = %s, want venue net 24.5", st.RealizedPnL)
	}
	if _, err := s.OnEvent(strategy.TradeEvent{Trade: tr, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	if !s.View().Stats.RealizedPnL.Equal(d("24.5")) {
		t.Fatal("duplicate trade_id must not double-count realized pnl")
	}
	foreign := tr
	foreign.ID = 78
	foreign.ClientOrderID = order.MustEncode(order.Ref{Slot: 9, Epoch: 99, Cell: 0, Purpose: order.PurposeClose})
	if _, err := s.OnEvent(strategy.TradeEvent{Trade: foreign, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	if !s.View().Stats.RealizedPnL.Equal(d("24.5")) {
		t.Fatal("foreign slot/epoch trade must not affect realized pnl")
	}
}

// RH 成交里 ask/bid_account_pnl 常为 0：不能因此改走交易历史口径，否则页面已实现一直是 0。
func TestZeroVenuePnLKeepsSelfCalculatedRealized(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))

	buy100 := findPlacement(t, acts, "100")
	confirm(t, s, buy100, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "100.05", time.Second)
	next, _ := s.OnEvent(orderEvent(buy100, order.StateFilled, buy100.Quantity))

	sell125 := findPlacement(t, next, "125")
	confirm(t, s, sell125, order.StateOpen, decimal.Zero)
	moveMarket(t, s, "125", 2*time.Second)
	if _, err := s.OnEvent(orderEvent(sell125, order.StateFilled, sell125.Quantity)); err != nil {
		t.Fatal(err)
	}

	before := s.View().Stats
	if before.VenueRealized {
		t.Fatal("self-calculated path must not set venue_realized")
	}
	if !before.RealizedPnL.Equal(before.GridProfit) {
		t.Fatalf("realized = %s, want grid_profit %s", before.RealizedPnL, before.GridProfit)
	}

	coid := order.MustEncode(order.Ref{Slot: 0, Epoch: s.epoch, Cell: 0, Purpose: order.PurposeClose, Seq: 1})
	tr := order.Trade{
		ID:            88,
		ClientOrderID: coid,
		Side:          order.Sell,
		Price:         d("125"),
		Quantity:      d("1"),
		Fee:           d("0.01"),
		RealizedPnL:   decimal.Zero,
	}
	if _, err := s.OnEvent(strategy.TradeEvent{Trade: tr, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	after := s.View().Stats
	if after.VenueRealized {
		t.Fatal("zero venue pnl must not switch to venue path")
	}
	if !after.RealizedPnL.Equal(before.RealizedPnL) {
		t.Fatalf("realized = %s after zero venue trade, want %s", after.RealizedPnL, before.RealizedPnL)
	}
}

// 会立即成交的价格不下单：post-only 必被拒，等价格移开再挂。
func TestSkipsOrdersThatWouldCrossTheBook(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	// 撤单后价格跌到 110：cell1 仍想在 125 买入，这会立即吃单，必须跳过。
	if _, err := s.OnCommand(strategy.Command{Kind: strategy.CmdCancelOrders, Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	moveMarket(t, s, "110", time.Second)

	acts, err := s.OnCommand(strategy.Command{Kind: strategy.CmdRefill, Now: epoch0.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	got := placements(acts)
	if len(got) != 4 {
		t.Fatalf("placed %d orders, want 4 after realign at lower mark", len(got))
	}
	for _, p := range got {
		if p.Side == order.Buy && p.Price.GreaterThanOrEqual(d("110")) {
			t.Errorf("placed a buy at %s at or above the market", p.Price)
		}
		if p.Side == order.Sell && p.Price.LessThanOrEqual(d("110")) {
			t.Errorf("placed a sell at %s at or below the market", p.Price)
		}
	}
}

// 区间外策略 pause：挂起但不撤单、不平仓，价格回归并确认后自动恢复。
func TestOutOfRangePauseAndResume(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}

	drop := strategy.BookEvent{
		Book: market.BookTicker{Bid: d("98"), Ask: d("98.2")},
		Mark: d("99"),
		Now:  epoch0.Add(time.Second),
	}
	acts, err := s.OnEvent(drop)
	if err != nil {
		t.Fatal(err)
	}
	if len(acts) != 0 {
		t.Fatalf("pause policy must not emit any action, got %d", len(acts))
	}
	if s.View().Phase != strategy.PhaseOutOfRange {
		t.Fatalf("phase = %s, want out_of_range", s.View().Phase)
	}

	// 刚回到区间内还不能恢复，要等确认时长
	back := strategy.BookEvent{
		Book: market.BookTicker{Bid: d("110"), Ask: d("110.2")},
		Mark: d("110"),
		Now:  epoch0.Add(2 * time.Second),
	}
	if acts, _ = s.OnEvent(back); len(acts) != 0 {
		t.Fatal("should wait for the resume confirmation window")
	}
	if s.View().Phase != strategy.PhaseOutOfRange {
		t.Fatal("phase should stay out_of_range until confirmed")
	}

	acts, err = s.OnEvent(strategy.TickEvent{Now: epoch0.Add(10 * time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	if s.View().Phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s, want running after confirmation", s.View().Phase)
	}
	_ = acts
}

// 区间外策略 stop_and_cancel：撤单并停止，但不平仓。
func TestOutOfRangeStopAndCancelKeepsPosition(t *testing.T) {
	p := smallParams(Neutral)
	p.Risk.OutOfRange = strategy.OutOfRangeStopAndCancel
	s := newStrategy(t, p)
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}

	acts, err := s.OnEvent(strategy.BookEvent{
		Book: market.BookTicker{Bid: d("98"), Ask: d("98.2")},
		Mark: d("99"),
		Now:  epoch0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if countAction[strategy.CancelAll](acts) != 1 {
		t.Fatal("expected a CancelAll action")
	}
	if countAction[strategy.ClosePosition](acts) != 0 {
		t.Fatal("stop_and_cancel must not close the position")
	}
	var stop *strategy.Stop
	for _, a := range acts {
		if v, ok := a.(strategy.Stop); ok {
			stop = &v
		}
	}
	if stop == nil || stop.Reason != strategy.StopOutOfRange {
		t.Fatalf("expected Stop{out_of_range}, got %+v", stop)
	}
	if stop.Reason.ClosesPosition() {
		t.Fatal("out_of_range must be flagged as position-preserving")
	}
}

// 撤单保留持仓 → 补格恢复。
func TestCancelOrdersThenRefill(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}

	acts, err := s.OnCommand(strategy.Command{Kind: strategy.CmdCancelOrders, Now: epoch0})
	if err != nil {
		t.Fatal(err)
	}
	if countAction[strategy.CancelAll](acts) != 1 {
		t.Fatal("expected CancelAll")
	}
	if s.View().Phase != strategy.PhasePaused {
		t.Fatalf("phase = %s, want paused", s.View().Phase)
	}
	if s.View().OrderResting != 0 {
		t.Fatal("no cell should be marked as resting after cancelling")
	}

	acts, err = s.OnCommand(strategy.Command{Kind: strategy.CmdRefill, Now: epoch0})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(placements(acts)); got != 4 {
		t.Fatalf("refill placed %d orders, want 4", got)
	}
	if s.View().Phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s, want running", s.View().Phase)
	}
}

// 调整区间会撤掉旧单、递增轮次并按新网格重铺。
func TestAdjustRange(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	before := s.View().Epoch

	acts, err := s.OnCommand(strategy.Command{
		Kind:    strategy.CmdAdjustRange,
		Payload: AdjustRange{LowerPrice: d("120"), UpperPrice: d("220")},
		Mark:    d("150"),
		Now:     epoch0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if countAction[strategy.CancelAll](acts) != 1 {
		t.Fatal("expected CancelAll before re-placing")
	}
	v := s.View()
	if v.Epoch != before+1 {
		t.Fatalf("epoch = %d, want %d", v.Epoch, before+1)
	}
	if !v.LowerPrice.Equal(d("120")) || !v.UpperPrice.Equal(d("220")) {
		t.Fatalf("range = [%s, %s], want [120, 220]", v.LowerPrice, v.UpperPrice)
	}
	if len(placements(acts)) == 0 {
		t.Fatal("expected the new grid to be placed")
	}
}

// 调整到一个非法区间时必须整体失败，保持原区间不变。
func TestAdjustRangeRejectsInvalidTarget(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}

	_, err := s.OnCommand(strategy.Command{
		Kind:    strategy.CmdAdjustRange,
		Payload: AdjustRange{LowerPrice: d("300"), UpperPrice: d("200")},
		Mark:    d("150"),
		Now:     epoch0,
	})
	requireIssue(t, err, CodeInvalidRange)
	if v := s.View(); !v.LowerPrice.Equal(d("100")) {
		t.Fatalf("range should be unchanged, got lower = %s", v.LowerPrice)
	}
}

func TestResetStats(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	buy := findPlacement(t, acts, "100")
	confirm(t, s, buy, order.StateOpen, decimal.Zero)
	if _, err := s.OnEvent(orderEvent(buy, order.StateFilled, buy.Quantity)); err != nil {
		t.Fatal(err)
	}
	if s.View().Stats.Fills == 0 {
		t.Fatal("precondition: expected a recorded fill")
	}

	later := epoch0.Add(time.Hour)
	if _, err := s.OnCommand(strategy.Command{Kind: strategy.CmdResetStats, Now: later}); err != nil {
		t.Fatal(err)
	}
	st := s.View().Stats
	if st.Fills != 0 || !st.GridProfit.IsZero() {
		t.Fatalf("stats not cleared: %+v", st)
	}
	if !st.ResetAt.Equal(later) {
		t.Fatalf("reset time = %s, want %s", st.ResetAt, later)
	}
}

// 停止时只撤单，任何原因都不平仓。
func TestOnStop(t *testing.T) {
	reasons := []strategy.StopReason{
		strategy.StopManual,
		strategy.StopShutdown,
		strategy.StopCircuit,
		strategy.StopError,
		strategy.StopEntryFailed,
		strategy.StopTakeProfit,
		strategy.StopStopLoss,
	}
	for _, reason := range reasons {
		s := newStrategy(t, smallParams(Neutral))
		if _, err := s.Init(testState("150", "0")); err != nil {
			t.Fatal(err)
		}
		acts, err := s.OnStop(reason)
		if err != nil {
			t.Fatal(err)
		}
		if countAction[strategy.CancelAll](acts) != 1 {
			t.Fatalf("%s: expected CancelAll", reason)
		}
		if countAction[strategy.ClosePosition](acts) != 0 {
			t.Fatalf("%s: stop must keep the position", reason)
		}
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	acts, _ := s.OnEvent(strategy.EntryDoneEvent{Filled: d("2"), Now: epoch0})
	buy := findPlacement(t, acts, "100")
	confirm(t, s, buy, order.StateOpen, decimal.Zero)
	if _, err := s.OnEvent(orderEvent(buy, order.StateFilled, buy.Quantity)); err != nil {
		t.Fatal(err)
	}
	want := s.View()

	blob, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(blob); err != nil {
		t.Fatal(err)
	}
	got := restored.View()

	if got.Epoch != want.Epoch || got.Phase != want.Phase || got.GridCount != want.GridCount {
		t.Fatalf("view mismatch after restore:\n got %+v\nwant %+v", got, want)
	}
	if got.Stats.Fills != want.Stats.Fills || !got.Stats.GridProfit.Equal(want.Stats.GridProfit) {
		t.Fatalf("stats mismatch: got %+v want %+v", got.Stats, want.Stats)
	}
	if !got.LowerPrice.Equal(want.LowerPrice) || !got.UpperPrice.Equal(want.UpperPrice) {
		t.Fatal("range mismatch after restore")
	}
}

// 恢复运行时不重建网格，而是按对账结果同步格子状态，不重复下单。
func TestRestoredInitSyncsFromOrders(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	placed := placements(acts)
	blob, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	restored, _ := New(nil)
	if err := restored.Restore(blob); err != nil {
		t.Fatal(err)
	}

	// 交易所侧四笔单都还在
	live := make([]order.Order, 0, len(placed))
	for _, p := range placed {
		live = append(live, order.Order{
			ClientOrderID: p.ClientOrderID,
			Side:          p.Side,
			Price:         p.Price,
			Quantity:      p.Quantity,
			State:         order.StateOpen,
		})
	}
	st := testState("150", "0")
	st.Orders = live

	next, err := restored.Init(st)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(placements(next)); got != 0 {
		t.Fatalf("re-placed %d orders that were already resting", got)
	}
	if restored.View().OrderResting != 4 {
		t.Fatalf("resting = %d, want 4", restored.View().OrderResting)
	}
}

// 不属于当前轮次的订单回报要被忽略，交给对账处理。
func TestIgnoresForeignOrderEvents(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	before := s.View().Stats

	foreign := order.MustEncode(order.Ref{Slot: 9, Epoch: 99, Cell: 0, Purpose: order.PurposeOpen})
	acts, err := s.OnEvent(strategy.OrderEvent{
		Order: order.Order{ClientOrderID: foreign, Side: order.Buy, State: order.StateFilled, FilledQty: d("1")},
		Now:   epoch0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acts) != 0 {
		t.Fatal("foreign order events must not produce actions")
	}
	after := s.View().Stats
	if after.Fills != before.Fills || after.CompletedGrids != before.CompletedGrids || !after.GridProfit.Equal(before.GridProfit) {
		t.Fatal("foreign order events must not affect stats")
	}
}

func TestPartialFillIsTracked(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	buy := findPlacement(t, acts, "100")
	confirm(t, s, buy, order.StateOpen, decimal.Zero)

	// 只成交一半后被撤销
	if _, err := s.OnEvent(orderEvent(buy, order.StateCanceled, d("0.5"))); err != nil {
		t.Fatal(err)
	}
	st := s.View().Stats
	if st.PartialFills != 1 {
		t.Fatalf("partial fills = %d, want 1", st.PartialFills)
	}
	if st.Fills != 1 {
		t.Fatalf("fills = %d, want 1", st.Fills)
	}
}

// 交易所拒单（Lighter 的 canceled-post-only）要让格子回到可重挂状态，
// 并递增重挂计数，达到上限后停手等价格移开。
func TestPostOnlyRejectionRetriesThenBacksOff(t *testing.T) {
	p := smallParams(Neutral)
	p.Order.PostOnlyRetry = 1
	s := newStrategy(t, p)
	acts, _ := s.Init(testState("150", "0"))
	buy := findPlacement(t, acts, "100")

	// 第一次被拒 → 格子放回 Empty，下个 tick 会重挂
	confirm(t, s, buy, order.StateRejected, decimal.Zero)
	next := s.tick(epoch0.Add(time.Second))
	if len(placements(next)) != 1 {
		t.Fatalf("第一次被拒后应当重挂一次，实际挂了 %d 笔", len(placements(next)))
	}

	// 连续被拒到上限后不再重挂，避免对着穿价的价位反复送死单
	confirm(t, s, next[0].(strategy.PlaceOrder), order.StateRejected, decimal.Zero)
	again := s.tick(epoch0.Add(2 * time.Second))
	for _, pl := range placements(again) {
		if pl.Price.Equal(d("100")) {
			t.Fatal("超过重挂上限后不应继续对同一格下单")
		}
	}
	if s.View().OrderRetrying == 0 {
		t.Error("待重试计数应当大于 0，页面要能看到")
	}
}

// 下单后迟迟收不到回报，格子必须被超时回收，否则这一格会永远空着且不报错。
func TestPendingTimeoutReleasesCell(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	if len(placements(acts)) != 4 {
		t.Fatalf("初始应铺 4 笔，实际 %d", len(placements(acts)))
	}

	// 一笔回报都不给：超时前不重复下单
	early := s.tick(epoch0.Add(10 * time.Second))
	if len(placements(early)) != 0 {
		t.Fatalf("超时前不应重复下单，实际下了 %d 笔", len(placements(early)))
	}

	// 超时后全部回收并重挂
	late := s.tick(epoch0.Add(pendingTimeout + time.Second))
	if len(placements(late)) != 4 {
		t.Fatalf("超时后应重挂 4 笔，实际 %d", len(placements(late)))
	}
	if got := s.View().Stats.PendingTimeouts; got != 4 {
		t.Fatalf("超时计数 = %d，期望 4", got)
	}
}

// 已确认挂单的格子不受超时影响。
func TestPendingTimeoutIgnoresRestingOrders(t *testing.T) {
	s := newStrategy(t, smallParams(Neutral))
	acts, _ := s.Init(testState("150", "0"))
	for _, pl := range placements(acts) {
		confirm(t, s, pl, order.StateOpen, decimal.Zero)
	}

	late := s.tick(epoch0.Add(pendingTimeout + time.Minute))
	if len(placements(late)) != 0 {
		t.Fatalf("已确认的挂单不该被超时回收，却重挂了 %d 笔", len(placements(late)))
	}
	if s.View().Stats.PendingTimeouts != 0 {
		t.Error("已确认的挂单不应计入超时")
	}
}

func TestEntryDoneDoesNotDoubleCountPosition(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OnEvent(strategy.PositionEvent{
		Position: position.Position{Size: d("2")},
		Now:      epoch0,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OnEvent(strategy.EntryDoneEvent{Filled: d("2"), Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	if !s.position.Equal(d("2")) {
		t.Fatalf("position = %s, want 2 (EntryDone must not add on top of PositionEvent)", s.position)
	}
	if s.needsEntry() {
		t.Fatal("at target, needsEntry should be false")
	}
}

func TestSyncFromOrdersIgnoresPurposeEntry(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	entryID := order.MustEncode(order.Ref{Slot: 0, Epoch: 1, Cell: 0, Purpose: order.PurposeEntry, Seq: 1})
	acts, err := s.OnEvent(strategy.ResyncEvent{
		Position: position.Position{Size: d("2")},
		Orders: []order.Order{{
			ClientOrderID: entryID,
			Side:          order.Buy,
			Quantity:      d("2"),
			State:         order.StateOpen,
		}},
		Now: epoch0,
	})
	if err != nil {
		t.Fatal(err)
	}
	placed := placements(acts)
	if len(placed) == 0 {
		t.Fatal("entry 单不应占用格子，网格仍应铺开")
	}
	for _, p := range placed {
		if p.ClientOrderID == entryID {
			t.Fatal("must not treat PurposeEntry as a grid cell order")
		}
		if p.ClientOrderID.Decode().Purpose == order.PurposeEntry {
			t.Fatal("grid placements must not reuse PurposeEntry")
		}
	}
}

func TestRunningGridDoesNotReenterOnPositionDrift(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	if _, err := s.Init(testState("150", "0")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.OnEvent(strategy.EntryDoneEvent{Filled: d("2"), Now: epoch0}); err != nil {
		t.Fatal(err)
	}
	if s.phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s", s.phase)
	}
	acts, err := s.OnEvent(strategy.ResyncEvent{
		Position: position.Position{Size: d("3")},
		Now:      epoch0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := countAction[strategy.EnsurePosition](acts); n != 0 {
		t.Fatalf("看门狗对账时仓位大于初始目标，不应再触发建仓/减仓，got %d", n)
	}
	acts, err = s.OnEvent(strategy.ResyncEvent{
		Position: position.Position{Size: d("1")},
		Now:      epoch0.Add(2 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := countAction[strategy.EnsurePosition](acts); n != 0 {
		t.Fatalf("看门狗对账时仓位小于初始目标，不应再补建仓，got %d", n)
	}
}

func solNeutralParams() Params {
	p := DefaultParams()
	p.Direction = Neutral
	p.Leverage = 10
	p.Grid.LowerPrice = d("72")
	p.Grid.UpperPrice = d("77")
	p.Grid.GridCount = 50
	p.Grid.SizingMode = MarginBased
	p.Grid.Margin = d("1500")
	return p
}

// 启动 mark 高于 76 时，75.9–76.0 格会被 Arm 成买单；价格回落后应 realign 成卖 @76。
func TestRealignAfterMarkDropEnablesSellAt76(t *testing.T) {
	s := newStrategy(t, solNeutralParams())
	st := testState("76.2", "0")
	st.Book = market.BookTicker{Bid: d("76.1"), Ask: d("76.2")}
	st.Mark = d("76.2")
	if _, err := s.Init(st); err != nil {
		t.Fatal(err)
	}
	c := s.grid.Cells[39]
	if !c.Low.Equal(d("75.9")) || !c.High.Equal(d("76")) {
		t.Fatalf("cell39 bounds = %s-%s, want 75.9-76", c.Low, c.High)
	}
	if c.Side != order.Buy || !c.OrderPrice().Equal(d("75.9")) {
		t.Fatalf("before drop cell39 = %s @ %s, want buy @ 75.9", c.Side, c.OrderPrice())
	}

	acts, err := s.OnEvent(strategy.BookEvent{
		Book: market.BookTicker{Bid: d("75.5"), Ask: d("75.7")},
		Mark: d("75.6"),
		Now:  epoch0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range placements(acts) {
		if p.Side == order.Sell && p.Price.Equal(d("76")) {
			return
		}
	}
	t.Fatalf("expected sell @ 76 after mark drop, got %d placements", len(placements(acts)))
}

// 已确认的买单被现价穿过时，realign 不得先撤单；等成交回报再翻转到对手价。
func TestRealignSkipsRestingOrderCrossedByMark(t *testing.T) {
	s := newStrategy(t, smallParams(Long))
	acts, err := s.Init(testState("150", "2"))
	if err != nil {
		t.Fatal(err)
	}
	buy125 := findPlacement(t, acts, "125")
	confirm(t, s, buy125, order.StateOpen, decimal.Zero)

	moved, err := s.OnEvent(strategy.BookEvent{
		Book: market.BookTicker{Bid: d("124.9"), Ask: d("125.1")},
		Mark: d("125"),
		Now:  epoch0.Add(time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range moved {
		if c, ok := a.(strategy.CancelOrder); ok && c.ClientOrderID == buy125.ClientOrderID {
			t.Fatal("must not cancel a resting buy that mark has already crossed")
		}
		if p, ok := a.(strategy.PlaceOrder); ok && p.Side == order.Sell && p.Price.Equal(d("150")) {
			t.Fatal("must not place the opposite sell before the crossed buy fills")
		}
	}

	next, err := s.OnEvent(orderEvent(buy125, order.StateFilled, buy125.Quantity))
	if err != nil {
		t.Fatal(err)
	}
	sell := findPlacement(t, next, "150")
	if sell.Side != order.Sell {
		t.Fatalf("after fill want sell @ 150, got %s @ %s", sell.Side, sell.Price)
	}
	if s.View().Stats.Fills != 1 {
		t.Fatalf("fills = %d, want 1", s.View().Stats.Fills)
	}
}

// 建仓完成时应按运行时 mark 重新 Arm，而不是沿用 Init 时偏高的 mark。
func TestRealignAfterEntryDoneUsesRuntimeMark(t *testing.T) {
	p := solNeutralParams()
	p.Grid.NeutralBaseRatio = d("0.05")
	s := newStrategy(t, p)
	st := testState("76.2", "0")
	st.Book = market.BookTicker{Bid: d("76.1"), Ask: d("76.2")}
	st.Mark = d("76.2")
	if _, err := s.Init(st); err != nil {
		t.Fatal(err)
	}
	if s.phase != strategy.PhaseEntering {
		t.Fatalf("phase = %s, want entering", s.phase)
	}
	moveMarket(t, s, "75.6", time.Second)
	acts, err := s.OnEvent(strategy.EntryDoneEvent{Filled: d("1"), Now: epoch0.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range placements(acts) {
		if p.Side == order.Sell && p.Price.Equal(d("76")) {
			return
		}
	}
	t.Fatalf("expected sell @ 76 after entry done, got %d placements", len(placements(acts)))
}

// post-only 拒单达上限后，价格移开且可 maker 时应恢复重试。
func TestPostOnlyRetryResetsWhenMakerPriceReturns(t *testing.T) {
	p := smallParams(Neutral)
	p.Order.PostOnlyRetry = 1
	s := newStrategy(t, p)
	acts, _ := s.Init(testState("150", "0"))
	buy := findPlacement(t, acts, "100")

	confirm(t, s, buy, order.StateRejected, decimal.Zero)
	if len(placements(s.tick(epoch0.Add(time.Second)))) != 1 {
		t.Fatal("expected one retry placement")
	}
	confirm(t, s, buy, order.StateRejected, decimal.Zero)
	if len(placements(s.tick(epoch0.Add(2*time.Second)))) != 0 {
		t.Fatal("expected backoff after retry limit")
	}

	m := d("110")
	acts, err := s.OnEvent(strategy.BookEvent{
		Book: market.BookTicker{Bid: m.Sub(d("0.1")), Ask: m.Add(d("0.1"))},
		Mark: m,
		Now:  epoch0.Add(3 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if s.retrying[0] > 0 {
		t.Fatalf("retry count = %d, want cleared after mark move", s.retrying[0])
	}
	if len(placements(acts)) == 0 {
		t.Fatal("expected placement after price moved away and retry reset")
	}
}

// --- 测试辅助 ---

func findPlacement(t *testing.T, acts []strategy.Action, price string) strategy.PlaceOrder {
	t.Helper()
	for _, p := range placements(acts) {
		if p.Price.Equal(d(price)) {
			return p
		}
	}
	t.Fatalf("no order placed at %s", price)
	return strategy.PlaceOrder{}
}

func orderEvent(p strategy.PlaceOrder, state order.State, filled decimal.Decimal) strategy.OrderEvent {
	return strategy.OrderEvent{
		Order: order.Order{
			ClientOrderID: p.ClientOrderID,
			Side:          p.Side,
			Type:          p.Type,
			Price:         p.Price,
			Quantity:      p.Quantity,
			FilledQty:     filled,
			AvgFillPrice:  p.Price,
			State:         state,
			IsMaker:       true,
		},
		Now: epoch0,
	}
}

func confirm(t *testing.T, s *Strategy, p strategy.PlaceOrder, state order.State, filled decimal.Decimal) {
	t.Helper()
	if _, err := s.OnEvent(orderEvent(p, state, filled)); err != nil {
		t.Fatal(err)
	}
}

// moveMarket 推动行情到指定价格，模拟盘口同步移动。
func moveMarket(t *testing.T, s *Strategy, mark string, after time.Duration) {
	t.Helper()
	m := d(mark)
	_, err := s.OnEvent(strategy.BookEvent{
		Book: market.BookTicker{Bid: m.Sub(d("0.1")), Ask: m.Add(d("0.1"))},
		Mark: m,
		Now:  epoch0.Add(after),
	})
	if err != nil {
		t.Fatal(err)
	}
}
