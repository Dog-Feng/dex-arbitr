package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"
	"dex-grid/internal/domain/strategy/grid"
	"dex-grid/internal/domain/strategy/martingale"
	"dex-grid/internal/exchange/fake"

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
		MakerFeeRate:  d("0.0002"),
		TakerFeeRate:  d("0.0005"),
	}
}

func smallParams(dir grid.Direction) grid.Params {
	p := grid.DefaultParams()
	p.Symbol = "BTC"
	p.Direction = dir
	p.Leverage = 5
	p.Grid.LowerPrice = d("100")
	p.Grid.UpperPrice = d("200")
	p.Grid.GridCount = 4
	p.Grid.SizingMode = grid.PerGridQty
	p.Grid.PerGridQty = d("1")
	p.Entry.Mode = strategy.EntryMarket
	p.Entry.SliceCount = 1
	p.Entry.Timeout = strategy.MustParseDuration("1m")
	p.ApplyDefaults()
	return p
}

func newRunner(t *testing.T, dir grid.Direction) (*Runner, *fake.Exchange) {
	t.Helper()
	ex := fake.New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))

	raw, err := json.Marshal(smallParams(dir))
	if err != nil {
		t.Fatal(err)
	}
	s, err := grid.New(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := New(ex, s, Config{Name: "fake", Slot: 0, TickInterval: time.Second, MaxRetries: 2})
	return r, ex
}

func startOK(t *testing.T, r *Runner, risk strategy.RiskParams) {
	t.Helper()
	ctx := context.Background()
	res := r.Do(ctx, CmdStart, StartPayload{
		Symbol: "BTC",
		Entry:  strategy.DefaultEntryParams(),
		Risk:   risk,
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	// 覆盖成跟价建仓（旧 market 名），测试里再配合 Trade 成交。
	r.entryP.Mode = strategy.EntryMarket
	r.entryP.SliceCount = 1
	r.Drain(ctx)
}

func drainEntry(t *testing.T, r *Runner, ex *fake.Exchange) {
	t.Helper()
	r.Drain(context.Background())
	ex.Trade(d("149.9"))
	ex.Trade(d("150.1"))
	r.Drain(context.Background())
}

func TestRuntimeLogsStartPlaceAndFill(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	ex := fake.New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))
	raw, err := json.Marshal(smallParams(grid.Neutral))
	if err != nil {
		t.Fatal(err)
	}
	s, err := grid.New(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := New(ex, s, Config{Name: "fake", Slot: 0, TickInterval: time.Second, MaxRetries: 2, Log: log})
	startOK(t, r, strategy.DefaultRiskParams())
	out := buf.String()
	if !strings.Contains(out, "已启动 BTC 中性网格") {
		t.Fatalf("missing start log: %s", out)
	}
	if !strings.Contains(out, "挂了 4 笔") {
		t.Fatalf("missing place log: %s", out)
	}

	ex.SetBook(d("124.9"), d("125.1"))
	ex.SetMark(d("125"))
	ex.Trade(d("125"))
	r.Drain(context.Background())
	out = buf.String()
	if !strings.Contains(out, "买成交") || !strings.Contains(out, "125") {
		t.Fatalf("missing fill log: %s", out)
	}
}

func TestNeutralStartPlacesGrid(t *testing.T) {
	r, ex := newRunner(t, grid.Neutral)
	startOK(t, r, strategy.DefaultRiskParams())
	r.Drain(context.Background())

	if r.Status() != StatusRunning {
		t.Fatalf("status = %s", r.Status())
	}
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("resting = %d, want 4", n)
	}
	if r.View().Strategy.Phase != strategy.PhaseRunning {
		t.Fatalf("phase = %s", r.View().Strategy.Phase)
	}
}

func TestLongStartEntersThenPlaces(t *testing.T) {
	r, ex := newRunner(t, grid.Long)
	p := strategy.DefaultRiskParams()
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.01"),
		},
		Risk: p,
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)

	pos, _ := ex.Position(context.Background(), "BTC")
	if !pos.Size.Equal(d("2")) {
		t.Fatalf("position after entry = %s, want 2", pos.Size)
	}
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("grid resting = %d, want 4", n)
	}
	if r.Status() != StatusRunning {
		t.Fatalf("status = %s", r.Status())
	}
}

func TestFillPairsOppositeOrder(t *testing.T) {
	r, ex := newRunner(t, grid.Long)
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.01"),
		},
		Risk: strategy.DefaultRiskParams(),
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)
	before := len(ex.Resting())

	// 击中 125 的买单，该格应翻转为在 150 挂卖。
	ex.SetBook(d("124.9"), d("125.1"))
	ex.SetMark(d("125"))
	ex.Trade(d("125"))
	r.Drain(context.Background())

	after := len(ex.Resting())
	if after < before-1 {
		t.Fatalf("resting before=%d after=%d, pairing should replace the filled order", before, after)
	}
	if r.View().Strategy.Stats.Fills < 1 {
		t.Fatalf("expected at least one fill, stats=%+v", r.View().Strategy.Stats)
	}
}

func TestStopLossClosesAndStops(t *testing.T) {
	r, ex := newRunner(t, grid.Long)
	riskP := strategy.DefaultRiskParams()
	riskP.StopLossPrice = d("140")
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.01"),
		},
		Risk: riskP,
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)

	ex.SetBook(d("138.9"), d("139.1"))
	ex.SetMark(d("139"))
	r.Drain(context.Background())

	if r.Status() != StatusStopped && r.Status() != StatusError {
		t.Fatalf("status = %s, want stopped", r.Status())
	}
	if r.View().StopReason != strategy.StopStopLoss.String() {
		t.Fatalf("reason = %s", r.View().StopReason)
	}
	pos, _ := ex.Position(context.Background(), "BTC")
	if !pos.IsFlat() {
		t.Fatalf("position should be flat after stop-loss, got %s", pos.Size)
	}
	if n := len(ex.Resting()); n != 0 {
		t.Fatalf("resting = %d after stop", n)
	}
}

func TestCancelOrdersPauses(t *testing.T) {
	r, ex := newRunner(t, grid.Neutral)
	startOK(t, r, strategy.DefaultRiskParams())
	r.Drain(context.Background())

	res := r.Do(context.Background(), CmdCancelOrders, nil)
	if !res.OK {
		t.Fatalf("cancel: %s", res.Message)
	}
	if r.Status() != StatusPaused {
		t.Fatalf("status = %s, want paused", r.Status())
	}
	if n := len(ex.Resting()); n != 0 {
		t.Fatalf("resting = %d after cancel", n)
	}

	res = r.Do(context.Background(), CmdRefill, nil)
	if !res.OK {
		t.Fatalf("refill: %s", res.Message)
	}
	r.Drain(context.Background())
	if r.Status() != StatusRunning {
		t.Fatalf("status = %s after refill", r.Status())
	}
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("resting = %d after refill", n)
	}
}

func TestManualStopKeepsPositionAndCancelsOrders(t *testing.T) {
	r, ex := newRunner(t, grid.Long)
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.01"),
		},
		Risk: strategy.DefaultRiskParams(),
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)
	before, _ := ex.Position(context.Background(), "BTC")
	if before.IsFlat() {
		t.Fatal("expected a long position before stop")
	}

	res = r.Do(context.Background(), CmdStop, nil)
	if !res.OK {
		t.Fatalf("stop: %s", res.Message)
	}
	if r.Status() != StatusStopped {
		t.Fatalf("status = %s", r.Status())
	}
	if n := len(ex.Resting()); n != 0 {
		t.Fatalf("resting = %d after stop, want 0", n)
	}
	pos, _ := ex.Position(context.Background(), "BTC")
	if !pos.Size.Equal(before.Size) {
		t.Fatalf("stop must keep position %s, got %s", before.Size, pos.Size)
	}
}

func TestWatchdogCancelsExtraAndRefillsMissing(t *testing.T) {
	r, ex := newRunner(t, grid.Neutral)
	startOK(t, r, strategy.DefaultRiskParams())
	r.Drain(context.Background())
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("resting = %d, want 4", n)
	}

	extra := order.MustEncode(order.Ref{Slot: 0, Epoch: 1, Cell: 0, Purpose: order.PurposeEntry, Seq: 1})
	ex.InjectOpenOrder(order.Order{
		ClientOrderID: extra,
		Side:          order.Buy,
		Price:         d("90"),
		Quantity:      d("1"),
		State:         order.StateOpen,
	})
	if n := len(ex.Resting()); n != 5 {
		t.Fatalf("after inject resting = %d, want 5", n)
	}
	r.watchdog(context.Background())
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("watchdog should cancel extra, resting = %d", n)
	}
	for _, o := range ex.Resting() {
		if o.ClientOrderID == extra {
			t.Fatal("entry leftover should be cancelled")
		}
	}

	drop := ex.Resting()[0].ClientOrderID
	ex.DropOrder(drop)
	if n := len(ex.Resting()); n != 3 {
		t.Fatalf("after drop resting = %d, want 3", n)
	}
	r.watchdog(context.Background())
	if n := len(ex.Resting()); n != 4 {
		t.Fatalf("watchdog should refill missing, resting = %d", n)
	}
}

func TestMartingaleCycleRestartKeepsNewEpochOrders(t *testing.T) {
	ex := fake.New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))

	p := martingale.DefaultParams()
	p.Symbol = "BTC"
	p.Leverage = 5
	p.Martingale.InitialMargin = d("50")
	p.Martingale.AddMargin = d("50")
	p.Martingale.MaxAddTimes = 2
	p.Martingale.TakeProfitPct = d("1.5")
	p.Martingale.AddDropPct = d("2")
	p.Entry.Mode = strategy.EntryMarket
	p.Entry.SliceCount = 1
	p.Entry.FillTolerance = d("0.01")
	p.Entry.MaxSlippage = d("0.05")
	p.ApplyDefaults()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s, err := martingale.New(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := New(ex, s, Config{Name: "fake", Slot: 0, TickInterval: time.Second, MaxRetries: 2})
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.05"),
		},
		Risk: strategy.DefaultRiskParams(),
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)

	pos, _ := ex.Position(context.Background(), "BTC")
	if !pos.Size.IsPositive() {
		t.Fatalf("expected long inventory after first entry, got %s", pos.Size)
	}
	epoch1 := r.View().Strategy.Epoch
	if epoch1 == 0 {
		t.Fatal("strategy epoch should be assigned on start")
	}

	var tpPx decimal.Decimal
	for _, o := range ex.Resting() {
		if o.ReduceOnly && o.Side == order.Sell {
			tpPx = o.Price
			break
		}
	}
	if !tpPx.IsPositive() {
		t.Fatal("missing reduce-only take-profit")
	}

	ex.SetBook(tpPx, tpPx.Add(d("0.1")))
	ex.SetMark(tpPx)
	ex.Trade(tpPx)
	r.Drain(context.Background())
	ex.Trade(d("149.9"))
	ex.Trade(d("150.1"))
	r.Drain(context.Background())

	pos, _ = ex.Position(context.Background(), "BTC")
	if pos.Size.IsNegative() {
		t.Fatalf("restart opened a short, size=%s", pos.Size)
	}

	epoch2 := r.View().Strategy.Epoch
	if epoch2 <= epoch1 {
		t.Fatalf("epoch did not advance after cycle: %d -> %d", epoch1, epoch2)
	}
	if r.epoch != epoch2 {
		t.Fatalf("runner epoch %d != strategy epoch %d (watchdog would cancel new adds)", r.epoch, epoch2)
	}

	adds := 0
	for _, o := range ex.Resting() {
		if o.ClientOrderID.Decode().Purpose == order.PurposeOpen {
			adds++
		}
	}
	if adds == 0 {
		t.Fatal("expected add orders after cycle restart")
	}
	before := len(ex.Resting())
	r.watchdog(context.Background())
	if n := len(ex.Resting()); n < before {
		t.Fatalf("watchdog cancelled new-epoch adds: %d -> %d", before, n)
	}
}

func TestMartingaleTakeProfitClearsOldEpochAdds(t *testing.T) {
	ex := fake.New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))

	p := martingale.DefaultParams()
	p.Symbol = "BTC"
	p.Leverage = 5
	p.Martingale.InitialMargin = d("50")
	p.Martingale.AddMargin = d("50")
	p.Martingale.MaxAddTimes = 2
	p.ApplyDefaults()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	s, err := martingale.New(raw)
	if err != nil {
		t.Fatal(err)
	}
	r := New(ex, s, Config{Name: "fake", Slot: 0, TickInterval: time.Second, MaxRetries: 2})
	res := r.Do(context.Background(), CmdStart, StartPayload{
		Symbol: "BTC",
		Entry: strategy.EntryParams{
			Mode:          strategy.EntryMarket,
			SliceCount:    1,
			FillTolerance: d("0.01"),
			MaxSlippage:   d("0.05"),
		},
		Risk: strategy.DefaultRiskParams(),
	})
	if !res.OK {
		t.Fatalf("start: %s", res.Message)
	}
	drainEntry(t, r, ex)

	epoch1 := r.View().Strategy.Epoch
	oldAdds := 0
	var tpPx decimal.Decimal
	for _, o := range ex.Resting() {
		ref := o.ClientOrderID.Decode()
		if ref.Epoch == epoch1 && ref.Purpose == order.PurposeOpen {
			oldAdds++
		}
		if o.ReduceOnly && o.Side == order.Sell {
			tpPx = o.Price
		}
	}
	if oldAdds == 0 {
		t.Fatal("expected resting add orders before take-profit")
	}
	if !tpPx.IsPositive() {
		t.Fatal("missing take-profit")
	}

	ex.SetBook(tpPx, tpPx.Add(d("0.1")))
	ex.SetMark(tpPx)
	ex.Trade(tpPx)
	r.Drain(context.Background())
	ex.Trade(d("149.9"))
	ex.Trade(d("150.1"))
	r.Drain(context.Background())

	for _, o := range ex.Resting() {
		ref := o.ClientOrderID.Decode()
		if ref.Epoch == epoch1 && ref.Purpose == order.PurposeOpen {
			t.Fatalf("old-epoch add order still resting after take-profit: %v", o.ClientOrderID)
		}
	}
}
