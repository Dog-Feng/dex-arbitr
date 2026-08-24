package grid

import (
	"encoding/json"
	"fmt"
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

// Name 是策略在注册表中的名称。
const Name = "grid"

func init() {
	strategy.Register(Name, func(params []byte) (strategy.Strategy, error) {
		return New(params)
	})
	strategy.RegisterPreview(Name, func(params []byte, in strategy.PreviewContext) (any, error) {
		p := DefaultParams()
		if len(params) > 0 {
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, fmt.Errorf("grid: invalid params: %w", err)
			}
		}
		p.ApplyDefaults()
		return Preview(PreviewInput{
			Params:        p,
			Market:        in.Market,
			Mark:          in.Mark,
			Available:     in.Available,
			MaxOpenOrders: in.MaxOpenOrders,
		})
	})
}

// positionTolerance 是判定「仓位已到位」的相对容忍度。
var positionTolerance = decimal.RequireFromString("0.01")

// pendingTimeout 是「已发出下单请求但迟迟收不到回报」的容忍时长。
//
// Lighter 主网实测，从 sendTx 返回到订单回报推回来要 3-7 秒（排序器入块时间）。
// 超过这个时长还没有任何回报，就把格子放回 Empty 让它重试——否则一次丢失的
// 回报会让这一格永远空着，而且没有任何报错。重试是安全的：客户端订单号不变，
// 如果原单其实还活着，交易所会以「重复订单号」拒绝新单。
const pendingTimeout = 30 * time.Second

// Strategy 是普通合约网格策略。
//
// 它是纯逻辑：所有输入来自事件与命令，所有输出是意图（Action）。
// 不持有交易所客户端，不做 IO，不调用 time.Now()。
type Strategy struct {
	params Params
	mkt    market.Market
	slot   uint8
	epoch  uint16

	grid  *Grid
	phase strategy.Phase

	// target 是铺网格前需要持有的初始仓位（带符号）。
	target decimal.Decimal
	// position 是策略认知的当前仓位，由 PositionEvent 更新。
	position decimal.Decimal

	mark decimal.Decimal
	book market.BookTicker

	// backInRangeSince 记录价格回到区间内的时刻，用于回归确认。
	backInRangeSince time.Time

	// retrying 记录各格子的连续下单失败次数，供页面展示「待重试」。
	retrying map[int]int
	// lastRetryMark 是上次刷新 retrying 时的 mark，用于在价格变动后恢复被拒格子。
	lastRetryMark decimal.Decimal

	stats      strategy.Stats
	seenTrades map[int64]struct{}
	restored   bool
}

// New 从 JSON 参数构造策略。
func New(params []byte) (*Strategy, error) {
	p := DefaultParams()
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("grid: invalid params: %w", err)
		}
	}
	p.ApplyDefaults()
	return &Strategy{
		params:   p,
		phase:    strategy.PhaseIdle,
		retrying: map[int]int{},
	}, nil
}

// Params 返回当前参数的副本。
func (s *Strategy) Params() Params { return s.params }

// Init 在启动时调用一次。
func (s *Strategy) Init(st strategy.State) ([]strategy.Action, error) {
	s.mkt = st.Market
	s.slot = st.Slot
	s.mark = effectiveMark(st.Mark, st.Book)
	s.book = st.Book
	s.position = st.Position.Size

	if !s.mark.IsPositive() {
		return nil, errf(CodeNoMarkPrice, "", "启动时没有有效的行情价格")
	}

	if s.restored && s.grid != nil {
		// 从快照恢复：沿用原有网格与轮次，按对账结果同步格子状态。
		cancels := s.syncFromOrders(st.Orders)
		return append(cancels, s.resumeActions(st.Now)...), nil
	}

	if st.Epoch >= order.MaxEpoch {
		return nil, errf(CodeInvalidGridCount, "", "轮次已达上限 %d，请重置实例状态", order.MaxEpoch)
	}
	s.epoch = st.Epoch + 1

	g, err := Build(s.params, s.mkt)
	if err != nil {
		return nil, err
	}
	g.Arm(s.params.Direction, s.mark)
	if err := validateCells(g, s.mkt); err != nil {
		return nil, err
	}
	s.grid = g
	s.target = g.TargetPosition(s.params.Direction, s.params.Grid.NeutralBaseRatio)
	s.stats = strategy.Stats{ResetAt: st.Now}
	s.retrying = map[int]int{}
	s.seenTrades = map[int64]struct{}{}
	s.lastRetryMark = s.mark

	acts := []strategy.Action{
		strategy.SetLeverage{Leverage: s.params.Leverage, Mode: s.params.MarginMode},
	}

	if s.needsEntry() {
		s.phase = strategy.PhaseEntering
		return append(acts, strategy.EnsurePosition{Target: s.target}), nil
	}

	s.phase = strategy.PhaseRunning
	return append(acts, s.placeActions(st.Now)...), nil
}

// OnEvent 是运行时事件的唯一入口。
func (s *Strategy) OnEvent(ev strategy.Event) ([]strategy.Action, error) {
	switch e := ev.(type) {
	case strategy.BookEvent:
		s.book = e.Book
		s.mark = effectiveMark(e.Mark, e.Book)
		return s.tick(e.Now), nil

	case strategy.TickEvent:
		return s.tick(e.Now), nil

	case strategy.PositionEvent:
		s.position = e.Position.Size
		return nil, nil

	case strategy.OrderEvent:
		return s.onOrder(e)

	case strategy.TradeEvent:
		s.onTrade(e.Trade)
		return nil, nil

	case strategy.EntryDoneEvent:
		if s.phase != strategy.PhaseEntering {
			return nil, nil
		}
		// 建仓期间仓位事件可能已经把 size 写到目标附近；Filled 是同一增量，取更接近目标的值，禁止双加。
		next := s.position.Add(e.Filled)
		if s.target.Sub(next).Abs().LessThan(s.target.Sub(s.position).Abs()) {
			s.position = next
		}
		s.phase = strategy.PhaseRunning
		s.realignGrid()
		s.refreshRetrying()
		return s.placeActions(e.Now), nil

	case strategy.EntryFailedEvent:
		s.phase = strategy.PhaseStopped
		return []strategy.Action{
			strategy.CancelAll{},
			strategy.Stop{Reason: strategy.StopEntryFailed},
		}, nil

	case strategy.ResyncEvent:
		s.position = e.Position.Size
		cancels := s.syncFromOrders(e.Orders)
		return append(cancels, s.resumeActions(e.Now)...), nil

	default:
		return nil, nil
	}
}

// OnCommand 处理页面命令。
func (s *Strategy) OnCommand(cmd strategy.Command) ([]strategy.Action, error) {
	switch cmd.Kind {
	case strategy.CmdCancelOrders:
		s.clearCells()
		s.phase = strategy.PhasePaused
		return []strategy.Action{strategy.CancelAll{}}, nil

	case strategy.CmdRefill:
		if s.phase != strategy.PhasePaused && s.phase != strategy.PhaseRunning {
			return nil, fmt.Errorf("grid: 当前状态 %s 不能补格", s.phase)
		}
		s.phase = strategy.PhaseRunning
		s.realignGrid()
		s.refreshRetrying()
		return s.placeActions(cmd.Now), nil

	case strategy.CmdResetStats:
		s.stats = strategy.Stats{ResetAt: cmd.Now}
		s.seenTrades = map[int64]struct{}{}
		return nil, nil

	case strategy.CmdAdjustRange:
		req, ok := cmd.Payload.(AdjustRange)
		if !ok {
			return nil, fmt.Errorf("grid: 调整区间的参数类型不正确")
		}
		return s.adjustRange(req, cmd)

	default:
		return nil, strategy.ErrUnsupportedCommand
	}
}

// OnStop 返回收尾意图：只撤销本交易对挂单，任何停止原因都保留仓位。
func (s *Strategy) OnStop(reason strategy.StopReason) ([]strategy.Action, error) {
	s.phase = strategy.PhaseStopped
	s.clearCells()
	return []strategy.Action{strategy.CancelAll{}}, nil
}

// AdjustRange 是「调整区间（不停止网格）」命令的载荷。
//
// 只有非零字段会覆盖原参数，这样页面可以只提交用户改动的部分。
type AdjustRange struct {
	LowerPrice decimal.Decimal `json:"lower_price"`
	UpperPrice decimal.Decimal `json:"upper_price"`
	GridCount  int             `json:"grid_count"`
	PerGridQty decimal.Decimal `json:"per_grid_qty"`
	Margin     decimal.Decimal `json:"margin"`
}

// adjustRange 迁移到新区间。
//
// 实现上是「撤全部旧单 + 轮次递增 + 按新网格重铺」，而不是只动边缘几格的
// 增量迁移。原因是 ClientOrderID 里编码了轮次与格子索引：区间变化后格子
// 索引会整体平移，被保留的旧单再解码就会指向错误的格子，除非额外持久化
// 一张 COID → 格子的映射表。为这点请求量引入一张映射表并不划算——80 格的
// 网格用批量接口重铺也就几个请求。
func (s *Strategy) adjustRange(req AdjustRange, cmd strategy.Command) ([]strategy.Action, error) {
	if s.epoch >= order.MaxEpoch {
		return nil, errf(CodeInvalidGridCount, "", "轮次已达上限 %d，请停止后重新启动", order.MaxEpoch)
	}

	next := s.params
	if req.LowerPrice.IsPositive() {
		next.Grid.LowerPrice = req.LowerPrice
	}
	if req.UpperPrice.IsPositive() {
		next.Grid.UpperPrice = req.UpperPrice
	}
	if req.GridCount > 0 {
		next.Grid.GridCount = req.GridCount
	}
	if req.PerGridQty.IsPositive() {
		next.Grid.PerGridQty = req.PerGridQty
	}
	if req.Margin.IsPositive() {
		next.Grid.Margin = req.Margin
	}

	mark := s.mark
	if cmd.Mark.IsPositive() {
		mark = cmd.Mark
	}
	if _, err := Preview(PreviewInput{Params: next, Market: s.mkt, Mark: mark}); err != nil {
		return nil, err
	}

	g, err := Build(next, s.mkt)
	if err != nil {
		return nil, err
	}
	g.Arm(next.Direction, mark)

	s.params = next
	s.grid = g
	s.mark = mark
	s.epoch++
	s.retrying = map[int]int{}
	s.target = g.TargetPosition(next.Direction, next.Grid.NeutralBaseRatio)
	s.backInRangeSince = time.Time{}

	acts := []strategy.Action{strategy.CancelAll{}}
	if s.needsEntry() {
		s.phase = strategy.PhaseEntering
		return append(acts, strategy.EnsurePosition{Target: s.target}), nil
	}
	s.phase = strategy.PhaseRunning
	return append(acts, s.placeActions(cmd.Now)...), nil
}

// tick 处理时间推进：先判区间，再回收超时的挂单请求，最后补挂缺失的单。
func (s *Strategy) tick(now time.Time) []strategy.Action {
	if acts, handled := s.checkRange(now); handled {
		return acts
	}
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	s.expirePending(now)
	acts := s.realignGrid()
	s.refreshRetrying()
	acts = append(acts, s.placeActions(now)...)
	return acts
}

// realignGrid 把尚未成交（Seq=0）的格子对齐到当前 mark 下的 Arm 方向。
// 已跑完至少一腿的格子不在此列，避免覆盖 OnFill 翻转后的对手单。
func (s *Strategy) realignGrid() []strategy.Action {
	if s.grid == nil || !s.mark.IsPositive() {
		return nil
	}
	var acts []strategy.Action
	for i := range s.grid.Cells {
		c := &s.grid.Cells[i]
		if c.Seq != 0 {
			continue
		}
		want := SideForMark(s.mark, *c)
		wantPrice := c.priceForSide(want)
		if c.Side == want && c.OrderPrice().Equal(wantPrice) {
			continue
		}
		// 存活挂单已被现价穿过时等成交回报，不要先撤再反向挂。
		// 否则 ticker 早于 fill 到达会清掉 COID，成交被当成陈旧回报丢掉，
		// 开腿均价也记不上。
		if c.State == CellResting && crossedByMark(c.Side, c.OrderPrice(), s.mark) {
			continue
		}
		if c.State != CellEmpty && c.COID != 0 {
			acts = append(acts, strategy.CancelOrder{ClientOrderID: c.COID})
		}
		c.Side = want
		applyArmed(s.params.Direction, c)
		c.State = CellEmpty
		c.COID = 0
		c.PendingSince = time.Time{}
		delete(s.retrying, i)
	}
	return acts
}

func (s *Strategy) refreshRetrying() {
	if s.grid == nil || !s.mark.IsPositive() || s.mark.Equal(s.lastRetryMark) {
		return
	}
	s.lastRetryMark = s.mark
	for i := range s.grid.Cells {
		c := &s.grid.Cells[i]
		if c.State != CellEmpty {
			continue
		}
		if s.isMakerPrice(c.Side, c.OrderPrice()) {
			delete(s.retrying, i)
		}
	}
}

// expirePending 把长时间收不到回报的格子放回 Empty。
func (s *Strategy) expirePending(now time.Time) {
	if s.grid == nil {
		return
	}
	for i := range s.grid.Cells {
		c := &s.grid.Cells[i]
		if c.State != CellPending || c.PendingSince.IsZero() {
			continue
		}
		if now.Sub(c.PendingSince) < pendingTimeout {
			continue
		}
		c.State = CellEmpty
		c.PendingSince = time.Time{}
		s.stats.PendingTimeouts++
	}
}

// checkRange 实现区间外策略。handled 为真时调用方不应再做别的事。
func (s *Strategy) checkRange(now time.Time) (acts []strategy.Action, handled bool) {
	if s.grid == nil {
		return nil, false
	}
	if s.phase != strategy.PhaseRunning && s.phase != strategy.PhaseOutOfRange {
		return nil, false
	}

	buffer := s.mkt.TickSize.Mul(decimal.NewFromInt(int64(s.params.Risk.ExitBufferTicks)))
	outside := s.mark.LessThan(s.grid.Lower().Sub(buffer)) ||
		s.mark.GreaterThan(s.grid.Upper().Add(buffer))

	if s.phase == strategy.PhaseRunning {
		if !outside {
			return nil, false
		}
		if s.params.Risk.OutOfRange == strategy.OutOfRangeStopAndCancel {
			s.clearCells()
			s.phase = strategy.PhaseStopped
			return []strategy.Action{
				strategy.CancelAll{},
				strategy.Stop{Reason: strategy.StopOutOfRange},
			}, true
		}
		// pause：保留全部挂单与仓位，仅挂起。做多网格跌破下沿时买单本就
		// 已成交完毕，剩下的都是上方的平仓卖单，留着它们才能在反弹时止盈。
		s.phase = strategy.PhaseOutOfRange
		s.backInRangeSince = time.Time{}
		return nil, true
	}

	// PhaseOutOfRange：等待价格回归并确认足够时长。
	if outside {
		s.backInRangeSince = time.Time{}
		return nil, true
	}
	if s.backInRangeSince.IsZero() {
		s.backInRangeSince = now
		return nil, true
	}
	if now.Sub(s.backInRangeSince) < s.params.Risk.ResumeConfirm.Std() {
		return nil, true
	}
	s.phase = strategy.PhaseRunning
	s.backInRangeSince = time.Time{}
	return s.placeActions(now), true
}

func (s *Strategy) onTrade(t order.Trade) {
	if s.seenTrades == nil {
		s.seenTrades = map[int64]struct{}{}
	}
	strategy.NoteVenueTrade(&s.stats, s.seenTrades, s.slot, s.epoch, t)
}

// onOrder 处理订单状态变化。
func (s *Strategy) onOrder(e strategy.OrderEvent) ([]strategy.Action, error) {
	o := e.Order
	ref := o.ClientOrderID.Decode()
	if !o.ClientOrderID.Valid() || ref.Slot != s.slot || ref.Epoch != s.epoch {
		return nil, nil // 不属于本轮次，交给对账处理
	}
	if s.grid == nil || int(ref.Cell) >= len(s.grid.Cells) {
		return nil, nil
	}
	idx := int(ref.Cell)
	c := &s.grid.Cells[idx]
	if c.COID != o.ClientOrderID {
		return nil, nil // 陈旧回报，当前格子上挂的是另一笔
	}

	switch o.State {
	case order.StateOpen:
		c.State = CellResting
		delete(s.retrying, idx)
		return nil, nil

	case order.StatePartiallyFilled:
		c.State = CellResting
		return nil, nil

	case order.StateFilled:
		return s.handleFill(idx, o, e.Now), nil

	case order.StateCanceled, order.StateExpired:
		if o.FilledQty.IsPositive() {
			return s.handleFill(idx, o, e.Now), nil
		}
		c.State = CellEmpty
		c.PendingSince = time.Time{}
		return nil, nil

	case order.StateRejected:
		// 交易所主动拒绝，最常见的是 post-only 单会立即成交
		// （Lighter 回的是 canceled-post-only）。递增重挂计数，等价格移开再试。
		c.State = CellEmpty
		c.PendingSince = time.Time{}
		s.retrying[idx]++
		return nil, nil

	default:
		return nil, nil
	}
}

// handleFill 处理成交：更新统计、翻转格子方向、挂出对手单。
func (s *Strategy) handleFill(idx int, o order.Order, now time.Time) []strategy.Action {
	c := &s.grid.Cells[idx]

	// 部分成交后被撤/过期时，仍按整格翻转处理，由对账修正仓位偏差。
	// 这是有意的简化：让一个格子同时挂两笔单会破坏「每格一单」的模型，
	// 而仓位漂移本来就有对账兜底。
	partial := o.FilledQty.LessThan(c.Qty)

	res := s.grid.OnFill(idx, o.Side)
	delete(s.retrying, idx)

	s.stats.Fills++
	if o.Side == order.Buy {
		s.stats.BuyFills++
	} else {
		s.stats.SellFills++
	}
	if partial {
		s.stats.PartialFills++
	}
	fee := s.fillFee(o)
	s.stats.FeePaid = s.stats.FeePaid.Add(fee)
	if res.Completed {
		s.stats.CompletedGrids++
		profit := realizedRoundTrip(c, o.Side, o.FillPrice(), o.FilledQty)
		if profit.IsZero() {
			profit = res.GrossProfit
			if partial && c.Qty.IsPositive() {
				profit = profit.Mul(o.FilledQty).Div(c.Qty)
			}
		}
		s.stats.GridProfit = s.stats.GridProfit.Add(profit)
		matched := matchedQty(c.OpenQty, o.FilledQty)
		s.stats.CycleFee = s.stats.CycleFee.Add(scaleDec(c.OpenFee, matched, c.OpenQty)).Add(scaleDec(fee, matched, o.FilledQty))
		c.OpenQty = decimal.Zero
		c.OpenPrice = decimal.Zero
		c.OpenFee = decimal.Zero
	} else {
		c.OpenPrice, c.OpenQty = mergeVWAP(c.OpenPrice, c.OpenQty, o.FillPrice(), o.FilledQty)
		c.OpenFee = c.OpenFee.Add(fee)
	}

	if s.phase != strategy.PhaseRunning {
		return nil
	}
	return s.placeActions(now)
}

// placeActions 为所有空闲格子生成挂单意图，并撤掉落在挂单窗口之外的单。
//
// now 用来给新进入 Pending 的格子打时间戳，供超时回收使用。
func (s *Strategy) placeActions(now time.Time) []strategy.Action {
	if s.grid == nil {
		return nil
	}
	window := s.grid.ActiveWindow(s.mark, s.params.Grid.MaxActiveOrders)
	var acts []strategy.Action

	for i := range s.grid.Cells {
		c := &s.grid.Cells[i]

		if !window[i] {
			if c.State != CellEmpty && c.COID != 0 {
				acts = append(acts, strategy.CancelOrder{ClientOrderID: c.COID})
				c.State = CellEmpty
				c.COID = 0
				c.PendingSince = time.Time{}
			}
			continue
		}
		if c.State != CellEmpty {
			continue
		}
		price := c.OrderPrice()
		if s.retrying[i] > s.params.Order.PostOnlyRetry {
			continue // 反复被拒，等价格移开后再试
		}
		if !s.isMakerPrice(c.Side, price) {
			continue // 会立即成交，post-only 必被拒，等下一个 tick
		}

		coid, err := order.Encode(order.Ref{
			Slot:    s.slot,
			Epoch:   s.epoch,
			Cell:    uint16(i),
			Purpose: s.purposeFor(c.Side),
			Seq:     c.Seq,
		})
		if err != nil {
			continue
		}

		c.State = CellPending
		c.COID = coid
		c.PendingSince = now
		acts = append(acts, strategy.PlaceOrder{
			ClientOrderID: coid,
			Side:          c.Side,
			Type:          order.Limit,
			Price:         price,
			Quantity:      c.Qty,
			TIF:           order.PostOnly,
			ReduceOnly:    s.params.reduceOnlyFor(c.Side),
		})
	}
	return acts
}

// resumeActions 在恢复运行或重连对账后决定下一步。
//
// 建仓只允许在尚未进入 Running 时发生。网格运行中仓位会随买卖腿增减，
// 若这里再 EnsurePosition 拉回初始目标，就会把刚成交的格子立刻反向平掉。
func (s *Strategy) resumeActions(now time.Time) []strategy.Action {
	if s.phase == strategy.PhaseIdle || s.phase == strategy.PhaseEntering {
		if s.needsEntry() {
			s.phase = strategy.PhaseEntering
			return []strategy.Action{strategy.EnsurePosition{Target: s.target}}
		}
		s.phase = strategy.PhaseRunning
	}
	if acts, handled := s.checkRange(now); handled {
		return acts
	}
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	acts := s.realignGrid()
	s.refreshRetrying()
	acts = append(acts, s.placeActions(now)...)
	return acts
}

// syncFromOrders 用对账结果同步格子状态，并撤掉本策略不认的多余单。
func (s *Strategy) syncFromOrders(orders []order.Order) []strategy.Action {
	if s.grid == nil {
		return nil
	}
	for i := range s.grid.Cells {
		s.grid.Cells[i].State = CellEmpty
		s.grid.Cells[i].COID = 0
		s.grid.Cells[i].PendingSince = time.Time{}
	}
	var extra []strategy.Action
	seen := map[int]bool{}
	for _, o := range orders {
		if !o.ClientOrderID.Valid() || !o.State.IsActive() {
			continue
		}
		ref := o.ClientOrderID.Decode()
		if ref.Purpose == order.PurposeEntry {
			continue
		}
		reject := ref.Slot != s.slot || ref.Epoch != s.epoch || int(ref.Cell) >= len(s.grid.Cells)
		if !reject {
			switch ref.Purpose {
			case order.PurposeOpen, order.PurposeClose:
			default:
				reject = true
			}
		}
		if reject || seen[int(ref.Cell)] {
			extra = append(extra, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
			continue
		}
		seen[int(ref.Cell)] = true
		c := &s.grid.Cells[ref.Cell]
		c.State = CellResting
		c.COID = o.ClientOrderID
		c.Seq = ref.Seq
		c.Side = o.Side
	}
	return extra
}

func (s *Strategy) clearCells() {
	if s.grid == nil {
		return
	}
	for i := range s.grid.Cells {
		s.grid.Cells[i].State = CellEmpty
		s.grid.Cells[i].COID = 0
		s.grid.Cells[i].PendingSince = time.Time{}
	}
	s.retrying = map[int]int{}
}

// needsEntry 判断当前仓位是否已经满足目标。
func (s *Strategy) needsEntry() bool {
	diff := s.target.Sub(s.position).Abs()
	if diff.IsZero() {
		return false
	}
	if s.mkt.LotSize.IsPositive() && diff.LessThanOrEqual(s.mkt.LotSize) {
		return false
	}
	base := s.target.Abs()
	if base.IsZero() {
		return s.position.Abs().GreaterThan(s.mkt.LotSize)
	}
	threshold := base.Mul(positionTolerance)
	if s.mkt.LotSize.GreaterThan(threshold) {
		threshold = s.mkt.LotSize
	}
	return diff.GreaterThan(threshold)
}

// isMakerPrice 判断以该价格挂单是否能成为 maker。
//
// 盘口可用时用买一/卖一判断，否则退化为与标记价比较。
func (s *Strategy) isMakerPrice(side order.Side, price decimal.Decimal) bool {
	if s.book.Valid() {
		if side == order.Buy {
			return price.LessThan(s.book.Ask)
		}
		return price.GreaterThan(s.book.Bid)
	}
	if side == order.Buy {
		return price.LessThan(s.mark)
	}
	return price.GreaterThan(s.mark)
}

func (s *Strategy) purposeFor(side order.Side) order.Purpose {
	if s.params.isCloseLeg(side) {
		return order.PurposeClose
	}
	return order.PurposeOpen
}

// crossedByMark 判断现价是否已经穿过挂单价（买单 mark≤价，卖单 mark≥价）。
func crossedByMark(side order.Side, price, mark decimal.Decimal) bool {
	if !mark.IsPositive() || !price.IsPositive() {
		return false
	}
	if side == order.Buy {
		return mark.LessThanOrEqual(price)
	}
	return mark.GreaterThanOrEqual(price)
}

func (s *Strategy) fillFee(o order.Order) decimal.Decimal {
	if o.Fee.IsPositive() {
		return o.Fee
	}
	if !o.FilledQty.IsPositive() {
		return decimal.Zero
	}
	return s.mkt.FeeFor(o.FillPrice(), o.FilledQty, o.IsMaker || o.TIF == order.PostOnly)
}

func matchedQty(openQty, closeQty decimal.Decimal) decimal.Decimal {
	if !openQty.IsPositive() {
		return closeQty
	}
	if openQty.LessThan(closeQty) {
		return openQty
	}
	return closeQty
}

func scaleDec(total, part, whole decimal.Decimal) decimal.Decimal {
	if !total.IsPositive() {
		return decimal.Zero
	}
	if !whole.IsPositive() || part.GreaterThanOrEqual(whole) {
		return total
	}
	if !part.IsPositive() {
		return decimal.Zero
	}
	return total.Mul(part).Div(whole)
}

// mergeVWAP 把新成交并进已有均价。
func mergeVWAP(oldPx, oldQty, px, qty decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	n := oldQty.Add(qty)
	if !n.IsPositive() {
		return decimal.Zero, decimal.Zero
	}
	if !oldQty.IsPositive() {
		return px, qty
	}
	return oldPx.Mul(oldQty).Add(px.Mul(qty)).Div(n), n
}

// realizedRoundTrip 用开腿均价与平腿成交价算已实现毛利。
// 买开卖平：(卖价 − 买价) × 配对量；卖开买平反过来。
func realizedRoundTrip(c *Cell, closeSide order.Side, closePx, closeQty decimal.Decimal) decimal.Decimal {
	if !c.OpenQty.IsPositive() || !c.OpenPrice.IsPositive() || !closePx.IsPositive() || !closeQty.IsPositive() {
		return decimal.Zero
	}
	matched := closeQty
	if c.OpenQty.LessThan(matched) {
		matched = c.OpenQty
	}
	if closeSide == order.Sell {
		return closePx.Sub(c.OpenPrice).Mul(matched)
	}
	return c.OpenPrice.Sub(closePx).Mul(matched)
}

func effectiveMark(mark decimal.Decimal, book market.BookTicker) decimal.Decimal {
	if mark.IsPositive() {
		return mark
	}
	return book.Mid()
}

// View 返回供页面展示的只读视图。
func (s *Strategy) View() strategy.View {
	v := strategy.View{
		Phase:     s.phase,
		Epoch:     s.epoch,
		Direction: s.params.Direction.String(),
		GridCount: s.params.Grid.GridCount,
		Stats:     s.stats.ForView(),
	}
	if s.grid == nil {
		v.LowerPrice = s.params.Grid.LowerPrice
		v.UpperPrice = s.params.Grid.UpperPrice
		return v
	}

	v.LowerPrice = s.grid.Lower()
	v.UpperPrice = s.grid.Upper()
	v.GridCount = s.grid.Count()
	v.TargetPosition = s.target

	window := s.grid.ActiveWindow(s.mark, s.params.Grid.MaxActiveOrders)
	v.OrderTarget = len(window)
	v.Cells = make([]strategy.CellView, 0, len(s.grid.Cells))
	for i := range s.grid.Cells {
		c := &s.grid.Cells[i]
		if c.State == CellResting {
			v.OrderResting++
		}
		v.Cells = append(v.Cells, strategy.CellView{
			Index: c.Index,
			Low:   c.Low,
			High:  c.High,
			Qty:   c.Qty,
			Side:  c.Side.String(),
			Price: c.OrderPrice(),
			State: c.State.String(),
		})
	}
	v.OrderRetrying = len(s.retrying)
	return v
}

// snapshotData 是持久化格式。
type snapshotData struct {
	Params           Params          `json:"params"`
	Epoch            uint16          `json:"epoch"`
	Slot             uint8           `json:"slot"`
	Phase            uint8           `json:"phase"`
	Grid             *Grid           `json:"grid"`
	Target           decimal.Decimal `json:"target"`
	Position         decimal.Decimal `json:"position"`
	Mark             decimal.Decimal `json:"mark"`
	BackInRangeSince time.Time       `json:"back_in_range_since"`
	Retrying         map[int]int     `json:"retrying"`
	Stats            strategy.Stats  `json:"stats"`
	SeenTrades       []int64         `json:"seen_trades,omitempty"`
}

func (s *Strategy) Snapshot() ([]byte, error) {
	ids := make([]int64, 0, len(s.seenTrades))
	for id := range s.seenTrades {
		ids = append(ids, id)
	}
	return json.Marshal(snapshotData{
		Params:           s.params,
		Epoch:            s.epoch,
		Slot:             s.slot,
		Phase:            uint8(s.phase),
		Grid:             s.grid,
		Target:           s.target,
		Position:         s.position,
		Mark:             s.mark,
		BackInRangeSince: s.backInRangeSince,
		Retrying:         s.retrying,
		Stats:            s.stats,
		SeenTrades:       ids,
	})
}

func (s *Strategy) Restore(data []byte) error {
	var d snapshotData
	if err := json.Unmarshal(data, &d); err != nil {
		return fmt.Errorf("grid: invalid snapshot: %w", err)
	}
	s.params = d.Params
	s.params.ApplyDefaults()
	s.epoch = d.Epoch
	s.slot = d.Slot
	s.phase = strategy.Phase(d.Phase)
	s.grid = d.Grid
	s.target = d.Target
	s.position = d.Position
	s.mark = d.Mark
	s.backInRangeSince = d.BackInRangeSince
	s.stats = d.Stats
	s.retrying = d.Retrying
	if s.retrying == nil {
		s.retrying = map[int]int{}
	}
	s.seenTrades = map[int64]struct{}{}
	for _, id := range d.SeenTrades {
		s.seenTrades[id] = struct{}{}
	}
	s.restored = s.grid != nil
	return nil
}
