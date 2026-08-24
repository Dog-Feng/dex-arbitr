package grid

import (
	"testing"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testMarket() market.Market {
	return market.Market{
		Symbol:          "BTC",
		TickSize:        d("0.1"),
		LotSize:         d("0.001"),
		MinQty:          d("0.001"),
		MinNotional:     d("10"),
		MaxLeverage:     50,
		PriceDecimals:   1,
		SizeDecimals:    3,
		MakerFeeRate:    d("0.0002"),
		TakerFeeRate:    d("0.0005"),
		MaintMarginRate: d("0.005"),
	}
}

// smallParams 是一个便于手算的网格：100-200 分 4 格，每格 1 个币。
func smallParams(dir Direction) Params {
	p := DefaultParams()
	p.Symbol = "BTC"
	p.Direction = dir
	p.Leverage = 5
	p.Grid.LowerPrice = d("100")
	p.Grid.UpperPrice = d("200")
	p.Grid.GridCount = 4
	p.Grid.SizingMode = PerGridQty
	p.Grid.PerGridQty = d("1")
	p.ApplyDefaults()
	return p
}

func TestBuildPricesArithmetic(t *testing.T) {
	g, err := Build(smallParams(Long), testMarket())
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"100", "125", "150", "175", "200"}
	if len(g.Prices) != len(want) {
		t.Fatalf("got %d price lines, want %d", len(g.Prices), len(want))
	}
	for i, w := range want {
		if !g.Prices[i].Equal(d(w)) {
			t.Errorf("price[%d] = %s, want %s", i, g.Prices[i], w)
		}
	}
	// n 格对应 n 个格子，不是 n+1。
	if g.Count() != 4 {
		t.Fatalf("got %d cells, want 4", g.Count())
	}
	for i := range g.Cells {
		if !g.Cells[i].Low.Equal(g.Prices[i]) || !g.Cells[i].High.Equal(g.Prices[i+1]) {
			t.Errorf("cell %d bounds mismatch: [%s, %s]", i, g.Cells[i].Low, g.Cells[i].High)
		}
	}
}

func TestBuildRejectsStepBelowTick(t *testing.T) {
	p := smallParams(Long)
	p.Grid.LowerPrice = d("100")
	p.Grid.UpperPrice = d("100.5")
	p.Grid.GridCount = 100 // step 0.005 < tick 0.1
	_, err := Build(p, testMarket())
	if err == nil {
		t.Fatal("expected RANGE_TOO_NARROW")
	}
	var issue *Issue
	if !asIssue(err, &issue) || issue.Code != CodeRangeTooNarrow {
		t.Fatalf("got %v, want %s", err, CodeRangeTooNarrow)
	}
}

func TestSizingPerGridQty(t *testing.T) {
	g, err := Build(smallParams(Long), testMarket())
	if err != nil {
		t.Fatal(err)
	}
	for i := range g.Cells {
		if !g.Cells[i].Qty.Equal(d("1")) {
			t.Errorf("cell %d qty = %s, want 1", i, g.Cells[i].Qty)
		}
	}
	if !g.TotalQty().Equal(d("4")) {
		t.Fatalf("TotalQty() = %s, want 4", g.TotalQty())
	}
}

func TestSizingMarginEqualNotional(t *testing.T) {
	p := smallParams(Long)
	p.Grid.SizingMode = MarginBased
	p.Grid.QtyMode = EqualNotional
	p.Grid.Margin = d("100") // 100 × 5x = 500 名义，4 格每格 125
	g, err := Build(p, testMarket())
	if err != nil {
		t.Fatal(err)
	}
	// 每格 125 名义除以格子下沿：125/100=1.25, 125/125=1, 125/150=0.833, 125/175=0.714
	want := []string{"1.25", "1", "0.833", "0.714"}
	for i, w := range want {
		if !g.Cells[i].Qty.Equal(d(w)) {
			t.Errorf("cell %d qty = %s, want %s", i, g.Cells[i].Qty, w)
		}
	}
}

func TestSizingMarginEqualQty(t *testing.T) {
	p := smallParams(Long)
	p.Grid.SizingMode = MarginBased
	p.Grid.QtyMode = EqualQty
	p.Grid.Margin = d("110") // 110 × 5x = 550，Σ下沿 = 100+125+150+175 = 550
	g, err := Build(p, testMarket())
	if err != nil {
		t.Fatal(err)
	}
	for i := range g.Cells {
		if !g.Cells[i].Qty.Equal(d("1")) {
			t.Errorf("cell %d qty = %s, want 1", i, g.Cells[i].Qty)
		}
	}
}

// 三种方向共用同一份价格线与同一套挂单方向规则，
// 差异只在初始目标仓位与哪些格子被初始仓位背书（Armed）。
func TestArmAndTargetPosition(t *testing.T) {
	m := testMarket()
	mark := d("150")

	cases := []struct {
		dir        Direction
		wantTarget string
		wantArmed  []bool
	}{
		{Long, "2", []bool{false, false, true, true}},
		{Short, "-2", []bool{true, true, false, false}},
		{Neutral, "0", []bool{false, false, false, false}},
	}

	for _, c := range cases {
		t.Run(c.dir.String(), func(t *testing.T) {
			g, err := Build(smallParams(c.dir), m)
			if err != nil {
				t.Fatal(err)
			}
			g.Arm(c.dir, mark)

			// 挂单方向与方向无关：现价之上挂卖，其余挂买。
			wantSide := []order.Side{order.Buy, order.Buy, order.Sell, order.Sell}
			for i := range g.Cells {
				if g.Cells[i].Side != wantSide[i] {
					t.Errorf("cell %d side = %s, want %s", i, g.Cells[i].Side, wantSide[i])
				}
				if g.Cells[i].Armed != c.wantArmed[i] {
					t.Errorf("cell %d armed = %v, want %v", i, g.Cells[i].Armed, c.wantArmed[i])
				}
			}

			got := g.TargetPosition(c.dir, decimal.Zero)
			if !got.Equal(d(c.wantTarget)) {
				t.Fatalf("TargetPosition() = %s, want %s", got, c.wantTarget)
			}
		})
	}
}

func TestNeutralBaseRatio(t *testing.T) {
	g, _ := Build(smallParams(Neutral), testMarket())
	g.Arm(Neutral, d("150"))
	got := g.TargetPosition(Neutral, d("0.5"))
	if !got.Equal(d("1")) { // 上方 2 个币的一半
		t.Fatalf("TargetPosition(0.5) = %s, want 1", got)
	}
}

// 每个格子任意时刻只有一笔挂单，且相邻格子不会撞在同一条价格线上。
func TestNoTwoCellsShareAnOrderPrice(t *testing.T) {
	g, _ := Build(smallParams(Long), testMarket())
	g.Arm(Long, d("150"))
	seen := map[string]int{}
	for i := range g.Cells {
		price := g.Cells[i].OrderPrice().String()
		if prev, dup := seen[price]; dup {
			t.Fatalf("cell %d and %d both want to place at %s", prev, i, price)
		}
		seen[price] = i
	}
}

// 配对规则对三种方向完全统一：买单成交后挂卖，卖单成交后挂买。
func TestOnFillPairing(t *testing.T) {
	g, _ := Build(smallParams(Long), testMarket())
	g.Arm(Long, d("150"))

	// cell0 初始挂买（未 armed）：买入成交 → 转为挂卖，未闭合循环
	res := g.OnFill(0, order.Buy)
	if res.Completed {
		t.Fatal("first buy on an un-armed cell should not complete a cycle")
	}
	if res.NextSide != order.Sell || g.Cells[0].Side != order.Sell {
		t.Fatalf("cell0 should flip to sell, got %s", g.Cells[0].Side)
	}
	if !g.Cells[0].Armed {
		t.Fatal("cell0 should be armed after the opening fill")
	}
	if g.Cells[0].State != CellEmpty || g.Cells[0].COID != 0 {
		t.Fatal("cell0 order slot should be cleared after fill")
	}

	// 卖出成交 → 闭合循环，毛利 = 价差 × 数量
	res = g.OnFill(0, order.Sell)
	if !res.Completed {
		t.Fatal("closing fill should complete a cycle")
	}
	if !res.GrossProfit.Equal(d("25")) {
		t.Fatalf("GrossProfit = %s, want 25", res.GrossProfit)
	}
	if g.Cells[0].Side != order.Buy {
		t.Fatal("cell0 should flip back to buy")
	}
}

// 做多网格里被初始仓位背书的格子，第一笔卖出就闭合一个循环。
func TestArmedCellCompletesOnFirstFill(t *testing.T) {
	g, _ := Build(smallParams(Long), testMarket())
	g.Arm(Long, d("150"))

	res := g.OnFill(2, order.Sell) // cell2 初始 armed
	if !res.Completed {
		t.Fatal("armed cell should complete on its first fill")
	}
	if !res.GrossProfit.Equal(d("25")) {
		t.Fatalf("GrossProfit = %s, want 25", res.GrossProfit)
	}
}

func TestSeqAdvancesOnEachFill(t *testing.T) {
	g, _ := Build(smallParams(Long), testMarket())
	g.Arm(Long, d("150"))
	for i := 0; i < 3; i++ {
		g.OnFill(0, order.Buy)
	}
	if g.Cells[0].Seq != 3 {
		t.Fatalf("Seq = %d, want 3", g.Cells[0].Seq)
	}
}

func TestActiveWindow(t *testing.T) {
	p := smallParams(Long)
	p.Grid.GridCount = 10
	p.Grid.LowerPrice = d("100")
	p.Grid.UpperPrice = d("200")
	g, err := Build(p, testMarket())
	if err != nil {
		t.Fatal(err)
	}

	if got := len(g.ActiveWindow(d("150"), 0)); got != 10 {
		t.Fatalf("unlimited window should include all 10 cells, got %d", got)
	}
	w := g.ActiveWindow(d("150"), 4)
	if len(w) != 4 {
		t.Fatalf("window size = %d, want 4", len(w))
	}
	// 现价 150 落在 cell4 [140,150) / cell5 [150,160)，窗口应围绕它展开
	for idx := range w {
		if idx < 2 || idx > 7 {
			t.Errorf("cell %d should not be in a window centred on the mark", idx)
		}
	}
}

func TestContains(t *testing.T) {
	g, _ := Build(smallParams(Long), testMarket())
	for _, c := range []struct {
		price string
		want  bool
	}{
		{"100", true}, {"150", true}, {"200", true}, {"99.9", false}, {"200.1", false},
	} {
		if got := g.Contains(d(c.price)); got != c.want {
			t.Errorf("Contains(%s) = %v, want %v", c.price, got, c.want)
		}
	}
}
