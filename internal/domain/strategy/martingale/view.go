package martingale

import (
	"encoding/json"

	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

func (s *Strategy) View() strategy.View {
	v := strategy.View{
		Phase:          s.phase,
		Epoch:          s.epoch,
		Direction:      s.params.Direction.String(),
		GridCount:      s.params.Martingale.MaxAddTimes,
		TargetPosition: s.target,
		Stats:          s.stats.ForView(),
	}
	if n := len(s.plan.Levels); n > 0 {
		last := s.plan.Levels[n-1].TriggerPrice
		firstTP := s.plan.Levels[0].TakeProfit
		v.LowerPrice, v.UpperPrice = last, firstTP
		if s.params.Direction == Short {
			v.LowerPrice, v.UpperPrice = firstTP, last
		}
	}
	v.OrderTarget = s.desiredCount()
	v.Cells = s.cellViews()
	for _, lv := range s.adds {
		if lv != nil && lv.State == liveResting {
			v.OrderResting++
		}
	}
	if s.tp != nil && (s.tp.State == liveResting || s.tp.State == liveModifying) {
		v.OrderResting++
	}
	v.OrderRetrying = len(s.retry)
	if s.retryTP > 0 {
		v.OrderRetrying++
	}
	return v
}

func (s *Strategy) desiredCount() int {
	n := 0
	for k := 1; k <= s.params.Martingale.MaxAddTimes; k++ {
		if s.wantAdd(k) {
			n++
		}
	}
	if !s.position.IsZero() {
		n++
	}
	return n
}

func (s *Strategy) cellViews() []strategy.CellView {
	out := make([]strategy.CellView, 0, s.params.Martingale.MaxAddTimes+1)
	for k := 1; k <= s.params.Martingale.MaxAddTimes; k++ {
		level, ok := s.level(k)
		if !ok {
			continue
		}
		st := liveEmpty.String()
		if lv := s.adds[k]; lv != nil && lv.State != liveEmpty {
			st = lv.State.String()
		}
		out = append(out, strategy.CellView{
			Index: k,
			Low:   level.TriggerPrice,
			High:  level.TriggerPrice,
			Qty:   level.Qty,
			Side:  s.addSide().String(),
			Price: level.TriggerPrice,
			State: st,
		})
	}
	if px, qty := s.tpOrder(); px.IsPositive() {
		st := liveEmpty.String()
		if s.tp != nil && s.tp.State != liveEmpty {
			st = s.tp.State.String()
		}
		out = append(out, strategy.CellView{
			Index: 0,
			Low:   px,
			High:  px,
			Qty:   qty,
			Side:  s.tpSide().String(),
			Price: px,
			State: st,
		})
	}
	return out
}

type snapshotData struct {
	Params     Params          `json:"params"`
	Epoch      uint16          `json:"epoch"`
	Slot       uint8           `json:"slot"`
	Phase      uint8           `json:"phase"`
	Target     decimal.Decimal `json:"target"`
	Position   decimal.Decimal `json:"position"`
	AvgPrice   decimal.Decimal `json:"avg_price"`
	EntryPx    decimal.Decimal `json:"entry_px"`
	Mark       decimal.Decimal `json:"mark"`
	AddedTimes int             `json:"added_times"`
	Cycles     int             `json:"cycles"`
	Stats      strategy.Stats  `json:"stats"`
	SeenTrades []int64         `json:"seen_trades,omitempty"`
}

func (s *Strategy) Snapshot() ([]byte, error) {
	ids := make([]int64, 0, len(s.seenTrades))
	for id := range s.seenTrades {
		ids = append(ids, id)
	}
	return json.Marshal(snapshotData{
		Params:     s.params,
		Epoch:      s.epoch,
		Slot:       s.slot,
		Phase:      uint8(s.phase),
		Target:     s.target,
		Position:   s.position,
		AvgPrice:   s.avgPrice,
		EntryPx:    s.entryPx,
		Mark:       s.mark,
		AddedTimes: s.addedTimes,
		Cycles:     s.cycles,
		Stats:      s.stats,
		SeenTrades: ids,
	})
}

func (s *Strategy) Restore(data []byte) error {
	var d snapshotData
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	s.params = d.Params
	s.params.ApplyDefaults()
	s.epoch = d.Epoch
	s.slot = d.Slot
	s.phase = strategy.Phase(d.Phase)
	s.target = d.Target
	s.position = d.Position
	s.avgPrice = d.AvgPrice
	s.entryPx = d.EntryPx
	s.mark = d.Mark
	s.addedTimes = d.AddedTimes
	s.cycles = d.Cycles
	s.stats = d.Stats
	s.adds = map[int]*liveOrder{}
	s.retry = map[int]int{}
	s.seenTrades = map[int64]struct{}{}
	for _, id := range d.SeenTrades {
		s.seenTrades[id] = struct{}{}
	}
	s.restored = true
	return nil
}
