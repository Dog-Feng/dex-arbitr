package grid

import (
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

// CellState 是格子上挂单的状态。
type CellState uint8

const (
	CellEmpty   CellState = iota // 无挂单，等待铺单
	CellPending                  // 已发出下单请求，等待交易所确认
	CellResting                  // 挂单已确认
)

func (s CellState) String() string {
	switch s {
	case CellPending:
		return "pending"
	case CellResting:
		return "resting"
	default:
		return "empty"
	}
}

// Cell 是一个网格格子，覆盖价格区间 [Low, High]。
//
// 为什么用「格子」而不是「价格线 + 角色」建模：价格线模型在配对时会撞车。
// 做多网格里买单在 P_i 成交后要在 P_{i+1} 挂卖单，但 P_{i+1} 上可能已经有
// 一笔初始卖单，两者数量还未必相同。格子模型天然没有这个问题——每个格子
// 独立完成「Low 买入、High 卖出」的循环，任意时刻只持有一笔挂单，
// 而相邻两格不可能同时想在同一条价格线上挂单。
type Cell struct {
	Index int             `json:"index"`
	Low   decimal.Decimal `json:"low"`
	High  decimal.Decimal `json:"high"`
	Qty   decimal.Decimal `json:"qty"`

	// Side 是该格当前应该挂的方向：Buy 挂在 Low，Sell 挂在 High。
	Side  order.Side          `json:"side"`
	State CellState           `json:"state"`
	COID  order.ClientOrderID `json:"coid"`
	Seq   uint8               `json:"seq"`
	// PendingSince 记录进入 Pending 的时刻，用于超时兜底。
	// 下单到收到回报有几秒延迟，回报若一直不来必须把格子放出来重试，
	// 否则这一格会永远空着且不报错。
	PendingSince time.Time `json:"pending_since,omitempty"`

	// Armed 表示该格已完成循环的前半程（持有一条已成交的开腿），
	// 下一笔成交就构成一个完整的网格循环。
	Armed bool `json:"armed"`

	// OpenQty / OpenPrice / OpenFee 是开腿的成交量、均价与已付手续费，闭合时用真实均价算已实现。
	OpenQty   decimal.Decimal `json:"open_qty,omitempty"`
	OpenPrice decimal.Decimal `json:"open_price,omitempty"`
	OpenFee   decimal.Decimal `json:"open_fee,omitempty"`
}

// OrderPrice 返回该格当前应挂的价格。
func (c Cell) OrderPrice() decimal.Decimal {
	if c.Side == order.Buy {
		return c.Low
	}
	return c.High
}

// Spread 返回格子的价差，也就是完成一个循环的毛利单价。
func (c Cell) Spread() decimal.Decimal { return c.High.Sub(c.Low) }

// GrossProfit 返回完成一个循环的毛利。
func (c Cell) GrossProfit() decimal.Decimal { return c.Spread().Mul(c.Qty) }

// Grid 是完整的网格：n+1 条价格线切出 n 个格子。
type Grid struct {
	Prices []decimal.Decimal `json:"prices"`
	Cells  []Cell            `json:"cells"`
}

// Lower / Upper 返回规整后的实际区间边界。
func (g *Grid) Lower() decimal.Decimal { return g.Prices[0] }
func (g *Grid) Upper() decimal.Decimal { return g.Prices[len(g.Prices)-1] }

// Count 返回格子数。
func (g *Grid) Count() int { return len(g.Cells) }

// TotalQty 返回所有格子的数量合计，也就是网格跑满时的最大仓位绝对值。
func (g *Grid) TotalQty() decimal.Decimal {
	sum := decimal.Zero
	for i := range g.Cells {
		sum = sum.Add(g.Cells[i].Qty)
	}
	return sum
}

// Contains 判断价格是否落在区间内（含边界）。
func (g *Grid) Contains(price decimal.Decimal) bool {
	return price.GreaterThanOrEqual(g.Lower()) && price.LessThanOrEqual(g.Upper())
}

// buildPrices 生成规整后的价格线。
func buildPrices(p GridParams, m market.Market) ([]decimal.Decimal, error) {
	if p.SpacingMode != Arithmetic {
		return nil, errf(CodeUnsupportedSpacing, "grid.spacing_mode",
			"等比网格尚未实现，当前只支持等差（arithmetic）")
	}

	n := decimal.NewFromInt(int64(p.GridCount))
	step := p.UpperPrice.Sub(p.LowerPrice).Div(n)

	// 步长小于一个 tick 时，规整会把相邻价格线压成同一个价，网格失去意义。
	if step.LessThan(m.TickSize) {
		maxCount := p.UpperPrice.Sub(p.LowerPrice).Div(m.TickSize).IntPart()
		return nil, errf(CodeRangeTooNarrow, "grid.grid_count",
			"区间宽度 %s 除以 %d 格后步长 %s 小于最小报价单位 %s，网格数最多 %d",
			p.UpperPrice.Sub(p.LowerPrice), p.GridCount, step, m.TickSize, maxCount)
	}

	prices := make([]decimal.Decimal, 0, p.GridCount+1)
	for i := 0; i <= p.GridCount; i++ {
		raw := p.LowerPrice.Add(step.Mul(decimal.NewFromInt(int64(i))))
		prices = append(prices, m.RoundPrice(raw, market.RoundNearest))
	}

	// step >= tick 时规整后必然严格递增，这里只是防御性检查。
	for i := 1; i < len(prices); i++ {
		if prices[i].LessThanOrEqual(prices[i-1]) {
			return nil, errf(CodeRangeTooNarrow, "grid.grid_count",
				"价格线规整后出现重复（%s），请减少网格数或放宽区间", prices[i])
		}
	}
	return prices, nil
}

// buildQtys 按 sizing 模式计算每个格子的数量。
func buildQtys(p Params, m market.Market, prices []decimal.Decimal) ([]decimal.Decimal, error) {
	nCells := len(prices) - 1
	qtys := make([]decimal.Decimal, nCells)

	switch p.Grid.SizingMode {
	case PerGridQty:
		q := m.RoundQty(p.Grid.PerGridQty)
		for i := range qtys {
			qtys[i] = q
		}

	case MarginBased:
		notional := p.Grid.Margin.Mul(decimal.NewFromInt(int64(p.Leverage)))
		switch p.Grid.QtyMode {
		case EqualQty:
			// 每格数量相同：总名义 = q × Σ Low_i
			sumLow := decimal.Zero
			for i := 0; i < nCells; i++ {
				sumLow = sumLow.Add(prices[i])
			}
			q := m.RoundQty(notional.Div(sumLow))
			for i := range qtys {
				qtys[i] = q
			}
		default: // EqualNotional
			// 每格投入金额相同，低价位买得多。以格子下沿作为买入价基准。
			perCell := notional.Div(decimal.NewFromInt(int64(nCells)))
			for i := 0; i < nCells; i++ {
				qtys[i] = m.RoundQty(perCell.Div(prices[i]))
			}
		}

	default:
		return nil, errf(CodeInvalidSizing, "grid.sizing_mode", "未知的数量输入方式")
	}
	return qtys, nil
}

// Build 构造网格。只做结构构造，不做业务校验，校验在 Preview 里统一做。
func Build(p Params, m market.Market) (*Grid, error) {
	prices, err := buildPrices(p.Grid, m)
	if err != nil {
		return nil, err
	}
	qtys, err := buildQtys(p, m, prices)
	if err != nil {
		return nil, err
	}

	cells := make([]Cell, len(prices)-1)
	for i := range cells {
		cells[i] = Cell{
			Index: i,
			Low:   prices[i],
			High:  prices[i+1],
			Qty:   qtys[i],
			State: CellEmpty,
		}
	}
	return &Grid{Prices: prices, Cells: cells}, nil
}

// Arm 按当前价与方向初始化每个格子的挂单方向与 Armed 标记。
//
// 规则对三种方向是统一的：
//   - 格子整体在现价之上（Low >= mark）→ 挂卖单在 High
//   - 否则（含现价所在的那一格）→ 挂买单在 Low
//
// 方向只影响哪些格子是「被初始仓位背书」的：做多是上方的卖单格，
// 做空是下方的买单格，中性两者都不是。被背书的格子 Armed 为真，
// 它们的第一笔成交就构成一个完整循环。
func (g *Grid) Arm(dir Direction, mark decimal.Decimal) {
	for i := range g.Cells {
		c := &g.Cells[i]
		c.Side = SideForMark(mark, *c)
		c.State = CellEmpty
		c.COID = 0
		c.Seq = 0
		applyArmed(dir, c)
	}
}

// SideForMark 返回该格在当前价下应挂的方向（与 Arm 规则一致）。
func SideForMark(mark decimal.Decimal, c Cell) order.Side {
	if c.Low.GreaterThanOrEqual(mark) {
		return order.Sell
	}
	return order.Buy
}

func (c Cell) priceForSide(side order.Side) decimal.Decimal {
	if side == order.Buy {
		return c.Low
	}
	return c.High
}

func applyArmed(dir Direction, c *Cell) {
	switch dir {
	case Long:
		c.Armed = c.Side == order.Sell
	case Short:
		c.Armed = c.Side == order.Buy
	default:
		c.Armed = false
	}
}

// TargetPosition 返回铺网格前需要持有的初始仓位（带符号）。
//
//   - 做多：上方的卖单是平仓单，必须先持有对应数量的多头
//   - 做空：下方的买单是平空单，必须先持有对应数量的空头
//   - 中性：默认为 0，不需要建仓；配置了底仓比例时按上方总量的该比例建多头
func (g *Grid) TargetPosition(dir Direction, neutralBaseRatio decimal.Decimal) decimal.Decimal {
	sellQty, buyQty := decimal.Zero, decimal.Zero
	for i := range g.Cells {
		if g.Cells[i].Side == order.Sell {
			sellQty = sellQty.Add(g.Cells[i].Qty)
		} else {
			buyQty = buyQty.Add(g.Cells[i].Qty)
		}
	}
	switch dir {
	case Long:
		return sellQty
	case Short:
		return buyQty.Neg()
	default:
		if neutralBaseRatio.IsPositive() {
			return sellQty.Mul(neutralBaseRatio)
		}
		return decimal.Zero
	}
}

// FillResult 是一次成交对格子造成的影响。
type FillResult struct {
	// Completed 表示这笔成交闭合了一个网格循环。
	Completed bool
	// GrossProfit 是该循环的毛利（未扣手续费），Completed 为假时是零。
	GrossProfit decimal.Decimal
	// NextSide 是该格接下来应该挂的方向。
	NextSide order.Side
}

// OnFill 处理某个格子上的成交，翻转挂单方向并判定循环是否闭合。
//
// 配对规则对三种方向完全统一：买单成交后在 High 挂卖，卖单成交后在 Low 挂买。
func (g *Grid) OnFill(index int, side order.Side) FillResult {
	c := &g.Cells[index]

	res := FillResult{NextSide: side.Opposite()}
	if c.Armed {
		res.Completed = true
		res.GrossProfit = c.GrossProfit()
		c.Armed = false
	} else {
		c.Armed = true
	}

	c.Side = res.NextSide
	c.State = CellEmpty
	c.COID = 0
	c.Seq = order.NextSeq(c.Seq)
	return res
}

// ActiveWindow 返回在挂单窗口限制下应该挂单的格子索引集合。
//
// maxActive 为 0 时返回全部格子。否则只保留距现价最近的若干格，
// 上下各一半，用于挂单额度紧张的交易所。
func (g *Grid) ActiveWindow(mark decimal.Decimal, maxActive int) map[int]bool {
	out := make(map[int]bool, len(g.Cells))
	if maxActive <= 0 || maxActive >= len(g.Cells) {
		for i := range g.Cells {
			out[i] = true
		}
		return out
	}

	// 找到距现价最近的格子，向两侧扩散。
	pivot := 0
	for i := range g.Cells {
		if g.Cells[i].High.LessThanOrEqual(mark) {
			pivot = i
		}
	}
	lo, hi := pivot, pivot
	out[pivot] = true
	for len(out) < maxActive {
		grew := false
		if lo > 0 {
			lo--
			out[lo] = true
			grew = true
		}
		if len(out) >= maxActive {
			break
		}
		if hi < len(g.Cells)-1 {
			hi++
			out[hi] = true
			grew = true
		}
		if !grew {
			break
		}
	}
	return out
}
