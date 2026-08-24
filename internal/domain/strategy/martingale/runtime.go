package martingale

import (
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

func (s *Strategy) onOrder(e strategy.OrderEvent) ([]strategy.Action, error) {
	o := e.Order
	ref := o.ClientOrderID.Decode()
	if !o.ClientOrderID.Valid() || ref.Slot != s.slot || ref.Epoch != s.epoch {
		return nil, nil
	}
	switch ref.Purpose {
	case order.PurposeOpen:
		return s.onAddOrder(int(ref.Cell), o, e.Now), nil
	case order.PurposeTakeProfit:
		return s.onTPOrder(o, e.Now)
	default:
		return nil, nil
	}
}

func (s *Strategy) onAddOrder(level int, o order.Order, now time.Time) []strategy.Action {
	lv := s.adds[level]
	if lv == nil || lv.COID != o.ClientOrderID {
		return nil
	}
	switch o.State {
	case order.StateOpen, order.StatePartiallyFilled:
		lv.State = liveResting
		delete(s.retry, level)
		return nil
	case order.StateFilled:
		return s.handleAddFill(level, o, now)
	case order.StateCanceled, order.StateExpired:
		if o.FilledQty.IsPositive() {
			return s.handleAddFill(level, o, now)
		}
		lv.State = liveEmpty
		lv.COID = 0
		lv.PendingSince = time.Time{}
		return nil
	case order.StateRejected:
		lv.State = liveEmpty
		lv.COID = 0
		lv.PendingSince = time.Time{}
		s.retry[level]++
		return nil
	default:
		return nil
	}
}

func (s *Strategy) handleAddFill(level int, o order.Order, now time.Time) []strategy.Action {
	if level > s.addedTimes {
		s.addedTimes = level
	}
	if px := fillPrice(o); px.IsPositive() {
		s.entryPx = firstNonZero(s.entryPx, px)
	}
	s.absorbAddFill(level, o)
	if lv := s.adds[level]; lv != nil {
		lv.State = liveEmpty
		lv.COID = 0
		lv.PendingSince = time.Time{}
	}
	delete(s.retry, level)
	s.noteFill(o)
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	return s.rehangTakeProfit(now)
}

// absorbAddFill 把加仓成交计入持仓与均价。仓位更新可能晚于成交回报，
// 若交易所仓位已经包含这笔加仓则不再累加，避免止盈数量翻倍。
func (s *Strategy) absorbAddFill(level int, o order.Order) {
	qty := o.FilledQty.Abs()
	if !qty.IsPositive() {
		return
	}
	px := fillPrice(o)
	lot := s.mkt.LotSize
	if lv, ok := s.level(level); ok && lot.IsPositive() {
		if s.position.Abs().GreaterThanOrEqual(lv.Position.Abs().Sub(lot)) {
			if !s.avgPrice.IsPositive() && lv.AvgPrice.IsPositive() {
				s.avgPrice = lv.AvgPrice
			}
			return
		}
	}
	signed := qty
	if o.Side == order.Sell {
		signed = qty.Neg()
	}
	next := s.position.Add(signed)
	if next.Abs().LessThanOrEqual(s.position.Abs()) {
		return
	}
	if px.IsPositive() {
		if !s.avgPrice.IsPositive() || s.position.Abs().IsZero() {
			s.avgPrice = px
		} else {
			s.avgPrice = s.avgPrice.Mul(s.position.Abs()).Add(px.Mul(qty)).Div(next.Abs())
		}
	}
	s.position = next
}

func (s *Strategy) rehangTakeProfit(now time.Time) []strategy.Action {
	px, qty := s.tpOrder()
	if s.tp != nil && s.tp.COID != 0 {
		if s.tp.State == liveModifying || s.tp.State == livePending {
			return nil
		}
		if !px.IsPositive() || !qty.IsPositive() {
			id := s.tp.COID
			s.tp.State = liveEmpty
			s.tp.COID = 0
			s.tp.PendingSince = time.Time{}
			return []strategy.Action{strategy.CancelOrder{ClientOrderID: id}}
		}
		if s.tp.Price.Equal(px) && s.tp.Qty.Equal(qty) {
			return nil
		}
		s.tp.State = liveModifying
		s.tp.PendingSince = now
		return []strategy.Action{strategy.ModifyOrder{
			ClientOrderID: s.tp.COID,
			Side:          s.tpSide(),
			Type:          order.Limit,
			Price:         px,
			Quantity:      qty,
			TIF:           order.PostOnly,
			ReduceOnly:    s.params.Order.ShouldReduceOnlyClose(),
		}}
	}
	return s.placeActions(now)
}

func (s *Strategy) tpMismatched() bool {
	if s.tp == nil || s.tp.COID == 0 || s.tp.State == liveEmpty {
		return false
	}
	px, qty := s.tpOrder()
	if !px.IsPositive() || !qty.IsPositive() {
		return true
	}
	return !s.tp.Price.Equal(px) || !s.tp.Qty.Equal(qty)
}

func (s *Strategy) onTPOrder(o order.Order, now time.Time) ([]strategy.Action, error) {
	if s.tp == nil || s.tp.COID != o.ClientOrderID {
		return nil, nil
	}
	switch o.State {
	case order.StateOpen, order.StatePartiallyFilled:
		s.tp.State = liveResting
		s.retryTP = 0
		s.tp.PendingSince = time.Time{}
		if o.Price.IsPositive() {
			s.tp.Price = o.Price
		}
		if o.Quantity.IsPositive() {
			s.tp.Qty = o.Quantity
		}
		return nil, nil
	case order.StateFilled:
		return s.handleTPFill(o, now), nil
	case order.StateCanceled, order.StateExpired:
		if o.FilledQty.IsPositive() {
			return s.handleTPFill(o, now), nil
		}
		s.tp.State = liveEmpty
		s.tp.COID = 0
		s.tp.PendingSince = time.Time{}
		return nil, nil
	case order.StateRejected:
		if s.tp.State == liveModifying {
			s.tp.State = liveResting
			s.tp.PendingSince = time.Time{}
			return nil, nil
		}
		s.tp.State = liveEmpty
		s.tp.COID = 0
		s.tp.PendingSince = time.Time{}
		s.retryTP++
		return nil, nil
	default:
		return nil, nil
	}
}

func (s *Strategy) handleTPFill(o order.Order, now time.Time) []strategy.Action {
	s.noteFill(o)
	s.stats.CompletedGrids++
	if px := fillPrice(o); px.IsPositive() && s.avgPrice.IsPositive() && o.FilledQty.IsPositive() {
		diff := px.Sub(s.avgPrice)
		if s.params.Direction == Short {
			diff = s.avgPrice.Sub(px)
		}
		s.stats.GridProfit = s.stats.GridProfit.Add(diff.Mul(o.FilledQty.Abs()))
	}
	s.stats.CycleFee = s.stats.FeePaid
	s.cycles++
	// 止盈成交：先撤掉本周期全部加仓挂单，再进入下一轮首单建仓。
	s.clearLive()
	s.position = decimal.Zero
	s.addedTimes = 0
	s.avgPrice = decimal.Zero

	max := s.params.Martingale.MaxCycles
	restart := s.params.Martingale.ShouldRestart() && (max <= 0 || s.cycles < max)
	acts := []strategy.Action{strategy.CancelAll{}}
	if !restart || s.epoch >= order.MaxEpoch {
		s.phase = strategy.PhaseStopped
		return append(acts, strategy.Stop{Reason: strategy.StopTakeProfit})
	}
	s.epoch++
	s.entryPx = s.mark
	if err := s.rebuildPlan(s.mark); err != nil {
		s.phase = strategy.PhaseStopped
		return append(acts, strategy.Stop{Reason: strategy.StopError})
	}
	s.target = s.entryTarget()
	s.phase = strategy.PhaseEntering
	// 先 maker 限价减仓清掉止盈没平干净的残留，再建首单。
	// 否则 Runner 若还拿着止盈前的仓位快照，会把「建仓」做成反向空单。
	return append(acts, strategy.ClosePosition{Urgency: strategy.UrgencyMaker}, strategy.EnsurePosition{Target: s.target})
}

func (s *Strategy) noteFill(o order.Order) {
	s.stats.Fills++
	if o.Side == order.Buy {
		s.stats.BuyFills++
	} else {
		s.stats.SellFills++
	}
	s.stats.FeePaid = s.stats.FeePaid.Add(fillFee(o, s.mkt))
}

func (s *Strategy) onTrade(t order.Trade) {
	if s.seenTrades == nil {
		s.seenTrades = map[int64]struct{}{}
	}
	strategy.NoteVenueTrade(&s.stats, s.seenTrades, s.slot, s.epoch, t)
}

func (s *Strategy) placeActions(now time.Time) []strategy.Action {
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	if err := s.ensurePlan(); err != nil {
		return nil
	}
	var acts []strategy.Action
	maxAdd := s.params.Martingale.MaxAddTimes
	for k := 1; k <= maxAdd; k++ {
		want := s.wantAdd(k)
		lv := s.adds[k]
		if !want {
			if lv != nil && lv.COID != 0 {
				acts = append(acts, strategy.CancelOrder{ClientOrderID: lv.COID})
				lv.State = liveEmpty
				lv.COID = 0
				lv.PendingSince = time.Time{}
			}
			continue
		}
		if lv != nil && lv.State != liveEmpty {
			continue
		}
		if s.retry[k] > s.params.Order.PostOnlyRetry {
			continue
		}
		level, ok := s.level(k)
		if !ok {
			continue
		}
		side := s.addSide()
		if !s.isMakerPrice(side, level.TriggerPrice) {
			continue
		}
		if lv == nil {
			lv = &liveOrder{}
			s.adds[k] = lv
		}
		coid, err := order.Encode(order.Ref{
			Slot: s.slot, Epoch: s.epoch, Cell: uint16(k),
			Purpose: order.PurposeOpen, Seq: lv.Seq,
		})
		if err != nil {
			continue
		}
		lv.COID = coid
		lv.State = livePending
		lv.PendingSince = now
		lv.Price = level.TriggerPrice
		lv.Qty = level.Qty
		acts = append(acts, strategy.PlaceOrder{
			ClientOrderID: coid,
			Side:          side,
			Type:          order.Limit,
			Price:         level.TriggerPrice,
			Quantity:      level.Qty,
			TIF:           order.PostOnly,
		})
	}

	if s.position.IsZero() {
		if s.tp != nil && s.tp.COID != 0 {
			acts = append(acts, strategy.CancelOrder{ClientOrderID: s.tp.COID})
			s.tp.State = liveEmpty
			s.tp.COID = 0
		}
		return acts
	}
	if s.tp != nil && s.tp.State != liveEmpty {
		return acts
	}
	if s.retryTP > s.params.Order.PostOnlyRetry {
		return acts
	}
	tpPx, tpQty := s.tpOrder()
	if !tpPx.IsPositive() || !tpQty.IsPositive() {
		return acts
	}
	side := s.tpSide()
	if !s.isMakerPrice(side, tpPx) {
		return acts
	}
	if s.tp == nil {
		s.tp = &liveOrder{}
	}
	coid, err := order.Encode(order.Ref{
		Slot: s.slot, Epoch: s.epoch, Cell: 0,
		Purpose: order.PurposeTakeProfit, Seq: s.tp.Seq,
	})
	if err != nil {
		return acts
	}
	s.tp.COID = coid
	s.tp.State = livePending
	s.tp.PendingSince = now
	s.tp.Price = tpPx
	s.tp.Qty = tpQty
	acts = append(acts, strategy.PlaceOrder{
		ClientOrderID: coid,
		Side:          side,
		Type:          order.Limit,
		Price:         tpPx,
		Quantity:      tpQty,
		TIF:           order.PostOnly,
		ReduceOnly:    s.params.Order.ShouldReduceOnlyClose(),
	})
	return acts
}

func (s *Strategy) wantAdd(level int) bool {
	if level <= s.addedTimes || level > s.params.Martingale.MaxAddTimes {
		return false
	}
	if s.params.Martingale.ShouldPreplace() {
		return true
	}
	return level == s.addedTimes+1
}

func (s *Strategy) resumeActions(now time.Time) []strategy.Action {
	switch s.phase {
	case strategy.PhaseEntering:
		if s.needsEntry() && !s.hasWorkingInventory() {
			return []strategy.Action{strategy.EnsurePosition{Target: s.target}}
		}
		if !s.needsEntry() {
			// 恢复运行：建仓已在崩溃前完成，直接铺单。
			s.phase = strategy.PhaseRunning
			return s.placeActions(now)
		}
		// needsEntry 且已有同向仓：等 ClosePosition / EnsurePosition 完成，勿提前铺单。
		return nil
	case strategy.PhaseIdle:
		if s.needsEntry() && !s.hasWorkingInventory() {
			s.phase = strategy.PhaseEntering
			return []strategy.Action{strategy.EnsurePosition{Target: s.target}}
		}
		s.phase = strategy.PhaseRunning
	}
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	return s.placeActions(now)
}

func (s *Strategy) syncFromOrders(orders []order.Order) []strategy.Action {
	for k, lv := range s.adds {
		if lv == nil {
			continue
		}
		lv.State = liveEmpty
		lv.COID = 0
		lv.PendingSince = time.Time{}
		s.adds[k] = lv
	}
	if s.tp != nil {
		s.tp.State = liveEmpty
		s.tp.COID = 0
		s.tp.PendingSince = time.Time{}
	}
	var extra []strategy.Action
	seenAdd := map[int]bool{}
	seenTP := false
	for _, o := range orders {
		if !o.ClientOrderID.Valid() || !o.State.IsActive() {
			continue
		}
		ref := o.ClientOrderID.Decode()
		if ref.Slot != s.slot || ref.Epoch != s.epoch {
			extra = append(extra, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
			continue
		}
		switch ref.Purpose {
		case order.PurposeOpen:
			k := int(ref.Cell)
			if !s.wantAdd(k) || seenAdd[k] {
				extra = append(extra, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
				continue
			}
			seenAdd[k] = true
			lv := s.adds[k]
			if lv == nil {
				lv = &liveOrder{}
				s.adds[k] = lv
			}
			lv.State = liveResting
			lv.COID = o.ClientOrderID
			lv.Seq = ref.Seq
			lv.Price = o.Price
			lv.Qty = o.Quantity
		case order.PurposeTakeProfit:
			if s.position.IsZero() || seenTP {
				extra = append(extra, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
				continue
			}
			seenTP = true
			if s.tp == nil {
				s.tp = &liveOrder{}
			}
			s.tp.State = liveResting
			s.tp.COID = o.ClientOrderID
			s.tp.Seq = ref.Seq
			s.tp.Price = o.Price
			s.tp.Qty = o.Quantity
		default:
			extra = append(extra, strategy.CancelOrder{ClientOrderID: o.ClientOrderID})
		}
	}
	return extra
}

func (s *Strategy) expirePending(now time.Time) {
	for _, lv := range s.adds {
		if lv == nil || lv.State != livePending || lv.PendingSince.IsZero() {
			continue
		}
		if now.Sub(lv.PendingSince) < pendingTimeout {
			continue
		}
		lv.State = liveEmpty
		lv.PendingSince = time.Time{}
		s.stats.PendingTimeouts++
	}
	if s.tp != nil && s.tp.State == livePending && !s.tp.PendingSince.IsZero() && now.Sub(s.tp.PendingSince) >= pendingTimeout {
		s.tp.State = liveEmpty
		s.tp.PendingSince = time.Time{}
		s.stats.PendingTimeouts++
	}
	if s.tp != nil && s.tp.State == liveModifying && !s.tp.PendingSince.IsZero() && now.Sub(s.tp.PendingSince) >= pendingTimeout {
		s.tp.State = liveResting
		s.tp.PendingSince = time.Time{}
		s.stats.PendingTimeouts++
	}
}

func (s *Strategy) clearLive() {
	s.adds = map[int]*liveOrder{}
	s.tp = nil
	s.retry = map[int]int{}
	s.retryTP = 0
}

func (s *Strategy) rebuildPlan(p0 decimal.Decimal) error {
	if !p0.IsPositive() {
		p0 = s.mark
	}
	plan, err := BuildPlan(s.params, s.mkt, s.mkt.RoundPrice(p0, market.RoundNearest))
	if err != nil {
		return err
	}
	s.plan = plan
	return nil
}

func (s *Strategy) ensurePlan() error {
	if len(s.plan.Levels) > 0 {
		return nil
	}
	p0 := s.entryPx
	if !p0.IsPositive() {
		p0 = s.mark
	}
	return s.rebuildPlan(p0)
}

func (s *Strategy) entryTarget() decimal.Decimal {
	if len(s.plan.Levels) == 0 {
		return decimal.Zero
	}
	qty := s.plan.Levels[0].Qty
	if s.params.Direction == Short {
		return qty.Neg()
	}
	return qty
}

// hasWorkingInventory 表示已经持有不少于首单的同向仓位，可直接进入运行挂单。
func (s *Strategy) hasWorkingInventory() bool {
	if s.target.IsZero() {
		return false
	}
	if s.params.Direction == Short {
		return s.position.LessThanOrEqual(s.target)
	}
	return s.position.GreaterThanOrEqual(s.target)
}

func (s *Strategy) wrongDirectionPosition() bool {
	lot := s.mkt.LotSize
	if lot.IsPositive() && s.position.Abs().LessThanOrEqual(lot) {
		return false
	}
	if s.params.Direction == Short {
		return s.position.IsPositive()
	}
	return s.position.IsNegative()
}

func (s *Strategy) needsEntry() bool {
	if s.target.IsZero() {
		s.target = s.entryTarget()
	}
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

func (s *Strategy) level(k int) (Level, bool) {
	if k < 0 || k >= len(s.plan.Levels) {
		return Level{}, false
	}
	return s.plan.Levels[k], true
}

func (s *Strategy) tpOrder() (decimal.Decimal, decimal.Decimal) {
	avg := s.avgPrice
	if !avg.IsPositive() {
		if cur, ok := s.level(s.addedTimes); ok {
			avg = cur.AvgPrice
		}
	}
	if !avg.IsPositive() {
		return decimal.Zero, decimal.Zero
	}
	pct := s.params.Martingale.TakeProfitPct.Div(hundred)
	var px decimal.Decimal
	if s.params.Direction == Long {
		px = avg.Mul(one.Add(pct))
	} else {
		px = avg.Mul(one.Sub(pct))
	}
	return s.mkt.RoundPrice(px, market.RoundNearest), s.mkt.RoundQty(s.position.Abs())
}

func (s *Strategy) addSide() order.Side {
	if s.params.Direction == Short {
		return order.Sell
	}
	return order.Buy
}

func (s *Strategy) tpSide() order.Side {
	if s.params.Direction == Short {
		return order.Buy
	}
	return order.Sell
}

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

func effectiveMark(mark decimal.Decimal, book market.BookTicker) decimal.Decimal {
	if mark.IsPositive() {
		return mark
	}
	return book.Mid()
}

func fillPrice(o order.Order) decimal.Decimal {
	return o.FillPrice()
}

func fillFee(o order.Order, mkt market.Market) decimal.Decimal {
	if o.Fee.IsPositive() {
		return o.Fee
	}
	if !o.FilledQty.IsPositive() {
		return decimal.Zero
	}
	return mkt.FeeFor(o.FillPrice(), o.FilledQty, o.IsMaker || o.TIF == order.PostOnly)
}

func firstNonZero(a, b decimal.Decimal) decimal.Decimal {
	if a.IsPositive() {
		return a
	}
	return b
}
