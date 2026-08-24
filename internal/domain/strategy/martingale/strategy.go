package martingale

import (
	"encoding/json"
	"fmt"
	"time"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/domain/strategy"

	"github.com/shopspring/decimal"
)

func init() {
	strategy.Register(Name, func(params []byte) (strategy.Strategy, error) {
		return New(params)
	})
	strategy.RegisterPreview(Name, func(params []byte, in strategy.PreviewContext) (any, error) {
		p := DefaultParams()
		if len(params) > 0 {
			if err := json.Unmarshal(params, &p); err != nil {
				return nil, fmt.Errorf("martingale: invalid params: %w", err)
			}
		}
		p.ApplyDefaults()
		return Preview(PreviewInput{
			Params:        p,
			Market:        in.Market,
			Mark:          in.Mark,
			Available:     in.Available,
			Position:      in.Position,
			MaxOpenOrders: in.MaxOpenOrders,
		})
	})
}

var positionTolerance = decimal.RequireFromString("0.01")

const pendingTimeout = 30 * time.Second

type liveState uint8

const (
	liveEmpty liveState = iota
	livePending
	liveResting
	liveModifying
)

func (s liveState) String() string {
	switch s {
	case livePending:
		return "pending"
	case liveResting:
		return "resting"
	case liveModifying:
		return "modifying"
	default:
		return "empty"
	}
}

type liveOrder struct {
	COID         order.ClientOrderID
	State        liveState
	PendingSince time.Time
	Seq          uint8
	Price        decimal.Decimal
	Qty          decimal.Decimal
}

// Strategy 是马丁网格策略。
type Strategy struct {
	params Params
	mkt    market.Market
	slot   uint8
	epoch  uint16
	phase  strategy.Phase

	target   decimal.Decimal
	position decimal.Decimal
	avgPrice decimal.Decimal
	entryPx  decimal.Decimal

	mark decimal.Decimal
	book market.BookTicker

	addedTimes int
	cycles     int
	plan       Plan

	adds    map[int]*liveOrder
	tp      *liveOrder
	retry   map[int]int
	retryTP int

	stats      strategy.Stats
	seenTrades map[int64]struct{}
	restored   bool
}

// New 从 JSON 参数构造策略。
func New(params []byte) (*Strategy, error) {
	p := DefaultParams()
	if len(params) > 0 {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, fmt.Errorf("martingale: invalid params: %w", err)
		}
	}
	p.ApplyDefaults()
	return &Strategy{
		params: p,
		phase:  strategy.PhaseIdle,
		adds:   map[int]*liveOrder{},
		retry:  map[int]int{},
	}, nil
}

func (s *Strategy) Init(st strategy.State) ([]strategy.Action, error) {
	s.mkt = st.Market
	s.slot = st.Slot
	s.mark = effectiveMark(st.Mark, st.Book)
	s.book = st.Book
	s.position = st.Position.Size
	if st.Position.EntryPrice.IsPositive() {
		s.avgPrice = st.Position.EntryPrice
	}
	if !s.mark.IsPositive() {
		return nil, strategyErr(CodeNoMarkPrice, "", "启动时没有有效的行情价格")
	}
	if s.restored {
		cancels := s.syncFromOrders(st.Orders)
		return append(cancels, s.resumeActions(st.Now)...), nil
	}
	if st.Epoch >= order.MaxEpoch {
		return nil, strategyErr(CodeInvalidLeverage, "", "轮次已达上限 %d，请重置实例状态", order.MaxEpoch)
	}
	s.epoch = st.Epoch + 1
	if err := s.rebuildPlan(s.mark); err != nil {
		return nil, err
	}
	s.target = s.entryTarget()
	s.stats = strategy.Stats{ResetAt: st.Now}
	s.seenTrades = map[int64]struct{}{}
	s.clearLive()
	s.addedTimes = 0

	if s.wrongDirectionPosition() {
		return nil, strategyErr(CodeInvalidDirection, "direction",
			"当前仓位方向与马丁配置相反，请先平仓后再启动")
	}

	acts := []strategy.Action{
		strategy.CancelAll{},
		strategy.SetLeverage{Leverage: s.params.Leverage, Mode: s.params.MarginMode},
	}
	// 已有同向仓位达到或超过首单量时，不再反向减仓；直接挂加仓与止盈。
	// 否则会卡在建仓阶段去平掉旧网格残留，DEX 上看起来既没建仓也没挂单。
	if s.needsEntry() && !s.hasWorkingInventory() {
		s.phase = strategy.PhaseEntering
		return append(acts, strategy.EnsurePosition{Target: s.target}), nil
	}
	s.phase = strategy.PhaseRunning
	if s.entryPx.IsZero() {
		s.entryPx = firstNonZero(s.avgPrice, s.mark)
	}
	return append(acts, s.placeActions(st.Now)...), nil
}

func (s *Strategy) OnEvent(ev strategy.Event) ([]strategy.Action, error) {
	switch e := ev.(type) {
	case strategy.BookEvent:
		s.book = e.Book
		s.mark = effectiveMark(e.Mark, e.Book)
		return s.tick(e.Now), nil
	case strategy.TickEvent:
		return s.tick(e.Now), nil
	case strategy.PositionEvent:
		sizeChanged := !s.position.Equal(e.Position.Size)
		avgChanged := e.Position.EntryPrice.IsPositive() && !s.avgPrice.Equal(e.Position.EntryPrice)
		s.position = e.Position.Size
		if e.Position.EntryPrice.IsPositive() {
			s.avgPrice = e.Position.EntryPrice
		}
		if s.phase == strategy.PhaseRunning && (sizeChanged || avgChanged) {
			return s.rehangTakeProfit(e.Now), nil
		}
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
		// EnsurePosition 完成即视为仓位已到首单目标；不要用本地被清零的仓位去加成交量。
		s.position = s.target
		if s.mark.IsPositive() {
			s.entryPx = s.mark
		}
		if !s.avgPrice.IsPositive() && s.mark.IsPositive() {
			s.avgPrice = s.mark
		}
		_ = s.rebuildPlan(s.entryPx)
		s.phase = strategy.PhaseRunning
		return s.placeActions(e.Now), nil
	case strategy.EntryFailedEvent:
		s.phase = strategy.PhaseStopped
		return []strategy.Action{
			strategy.CancelAll{},
			strategy.Stop{Reason: strategy.StopEntryFailed},
		}, nil
	case strategy.ResyncEvent:
		// 建仓阶段不采纳交易所仓位快照：止盈重开时本地已清零，
		// 若 Resync 写入旧持仓会误判为首单已到位并跳过 EnsurePosition。
		if s.phase != strategy.PhaseEntering {
			s.position = e.Position.Size
			if e.Position.EntryPrice.IsPositive() {
				s.avgPrice = e.Position.EntryPrice
			}
		}
		cancels := s.syncFromOrders(e.Orders)
		if s.phase == strategy.PhaseRunning && s.tpMismatched() {
			return append(cancels, s.rehangTakeProfit(e.Now)...), nil
		}
		return append(cancels, s.resumeActions(e.Now)...), nil
	default:
		return nil, nil
	}
}

func (s *Strategy) OnCommand(cmd strategy.Command) ([]strategy.Action, error) {
	switch cmd.Kind {
	case strategy.CmdCancelOrders:
		s.clearLive()
		s.phase = strategy.PhasePaused
		return []strategy.Action{strategy.CancelAll{}}, nil
	case strategy.CmdRefill:
		if s.phase != strategy.PhasePaused && s.phase != strategy.PhaseRunning {
			return nil, fmt.Errorf("martingale: 当前状态 %s 不能补单", s.phase)
		}
		s.phase = strategy.PhaseRunning
		return s.placeActions(cmd.Now), nil
	case strategy.CmdResetStats:
		s.stats = strategy.Stats{ResetAt: cmd.Now}
		s.seenTrades = map[int64]struct{}{}
		return nil, nil
	default:
		return nil, strategy.ErrUnsupportedCommand
	}
}

func (s *Strategy) OnStop(strategy.StopReason) ([]strategy.Action, error) {
	s.phase = strategy.PhaseStopped
	s.clearLive()
	return []strategy.Action{strategy.CancelAll{}}, nil
}

func (s *Strategy) tick(now time.Time) []strategy.Action {
	s.expirePending(now)
	if s.phase != strategy.PhaseRunning {
		return nil
	}
	var acts []strategy.Action
	if s.tpMismatched() {
		acts = append(acts, s.rehangTakeProfit(now)...)
	}
	return append(acts, s.placeActions(now)...)
}
