package strategy

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"

	"dex-grid-arbitr/internal/domain"
)

// NormalGrid implements a bounded arithmetic grid. There is at most one
// order per grid level; when a level fills, the opposite order is placed one
// level away so the grid keeps trading inside the configured range.
type NormalGrid struct {
	Config domain.NormalGridConfig
	Market domain.MarketSpec
	Status domain.StrategyStatus

	cycle          *domain.CycleSnapshot
	levels         []decimal.Decimal
	gridStep       decimal.Decimal
	orderQuantity  decimal.Decimal
	activeByLevel  map[int]domain.OrderIntent
	pendingInitial *domain.OrderIntent
}

func NewNormalGrid(config domain.NormalGridConfig, market domain.MarketSpec) *NormalGrid {
	return &NormalGrid{
		Config:        config,
		Market:        market,
		Status:        domain.StrategyIdle,
		activeByLevel: make(map[int]domain.OrderIntent),
	}
}

func (s *NormalGrid) StrategyStatus() domain.StrategyStatus  { return s.Status }
func (s *NormalGrid) SetStatus(status domain.StrategyStatus) { s.Status = status }
func (s *NormalGrid) StrategyType() string                   { return "normal" }
func (s *NormalGrid) Direction() domain.Direction            { return s.Config.Direction }
func (s *NormalGrid) MarketIndex() int                       { return s.Config.MarketIndex }
func (s *NormalGrid) Symbol() string                         { return s.Config.Symbol }
func (s *NormalGrid) CycleSnapshot() *domain.CycleSnapshot   { return s.cycle }
func (s *NormalGrid) Continuous() bool                       { return false }
func (s *NormalGrid) ProjectedFullGrid() (decimal.Decimal, decimal.Decimal) {
	return decimal.Zero, decimal.Zero
}

func (s *NormalGrid) GridSnapshot() *domain.GridSnapshot {
	if len(s.levels) == 0 {
		return nil
	}
	profitPerGrid := s.expectedProfitPerGrid()
	return &domain.GridSnapshot{
		Direction:             s.Config.Direction,
		LowerPrice:            s.levels[0],
		UpperPrice:            s.levels[len(s.levels)-1],
		GridCount:             s.Config.GridCount,
		GridStep:              s.gridStep,
		Margin:                s.Config.GridMargin,
		FollowPriceUp:         s.Config.GridFollowUp,
		FollowPriceDown:       s.Config.GridFollowDown,
		Mode:                  "arithmetic",
		ExpectedProfitPerGrid: profitPerGrid,
	}
}

// expectedProfitPerGrid estimates one completed grid interval. Normal-grid
// orders are submitted as post-only orders, so both sides use the maker fee.
func (s *NormalGrid) expectedProfitPerGrid() *decimal.Decimal {
	if !s.Market.FeeRateKnown || !s.gridStep.GreaterThan(decimal.Zero) || !s.orderQuantity.GreaterThan(decimal.Zero) {
		return nil
	}
	contract := s.Market.ContractMult
	if !contract.GreaterThan(decimal.Zero) {
		contract = decimal.NewFromInt(1)
	}
	gross := s.gridStep.Mul(s.orderQuantity).Mul(contract)
	averageGridPrice := s.levels[0].Add(s.levels[len(s.levels)-1]).Div(decimal.NewFromInt(2))
	feePerSide := averageGridPrice.Mul(s.orderQuantity).Mul(contract).Mul(s.Market.MakerFeeRate)
	net := gross.Sub(feePerSide.Mul(decimal.NewFromInt(2)))
	return &net
}

func (s *NormalGrid) BindOrderID(domain.OrderIntent, domain.ExchangeOrder) {}

func (s *NormalGrid) PendingInitialOrder() (domain.OrderIntent, bool) {
	if s.pendingInitial == nil || !s.pendingInitial.Quantity.GreaterThan(decimal.Zero) {
		return domain.OrderIntent{}, false
	}
	return *s.pendingInitial, true
}

func (s *NormalGrid) UpdatePendingInitialOrder(intent domain.OrderIntent) {
	if s.pendingInitial == nil || intent.Role != domain.OrderRoleInitial {
		return
	}
	*s.pendingInitial = intent
}

func (s *NormalGrid) Start(initialPrice decimal.Decimal, _ bool) (Action, error) {
	if err := s.prepare(initialPrice); err != nil {
		return Action{}, err
	}
	s.Status = domain.StrategyRunning
	s.cycle = &domain.CycleSnapshot{
		CycleID:       1,
		Status:        "running",
		InitialPrice:  s.quantizePrice(initialPrice),
		PlannedMargin: s.Config.GridMargin,
	}
	s.activeByLevel = make(map[int]domain.OrderIntent)
	s.pendingInitial = nil

	if s.Config.Direction == domain.DirectionNeutral {
		place := s.gridOrders(initialPrice)
		return Action{Place: place}, nil
	}
	initial := s.initialOrder(initialPrice)
	s.pendingInitial = &initial
	return Action{Place: []domain.OrderIntent{initial}}, nil
}

func (s *NormalGrid) OnFill(fill domain.Fill) (Action, error) {
	if s.cycle == nil {
		return Action{}, errors.New("normal grid has not been started")
	}
	quantity := fill.Quantity.Abs()
	if !quantity.GreaterThan(decimal.Zero) {
		return Action{}, errors.New("grid fill quantity must be positive")
	}

	if fill.Role == domain.OrderRoleInitial {
		initialCompleted := true
		if s.pendingInitial != nil {
			remaining := s.pendingInitial.Quantity.Sub(quantity)
			if remaining.GreaterThan(decimal.Zero) {
				s.pendingInitial.Quantity = remaining
				initialCompleted = false
			} else {
				s.pendingInitial = nil
			}
		}
		s.applyFill(fill.Side, fill.Price, quantity)
		if !initialCompleted {
			return Action{}, nil
		}
		place := s.gridOrders(fill.Price)
		return Action{Place: place}, nil
	}
	if fill.Role != domain.OrderRoleGrid {
		return Action{}, fmt.Errorf("unsupported normal grid fill role: %s", fill.Role)
	}

	delete(s.activeByLevel, fill.Level)
	s.applyFill(fill.Side, fill.Price, quantity)
	target := fill.Level - 1
	if fill.Side == domain.OrderSideBuy {
		target = fill.Level + 1
	}
	if target < 0 || target >= len(s.levels) {
		return Action{}, nil
	}
	if _, exists := s.activeByLevel[target]; exists {
		return Action{}, nil
	}
	intent := s.gridOrder(target, fill.Price)
	s.activeByLevel[target] = intent
	return Action{Place: []domain.OrderIntent{intent}}, nil
}

func (s *NormalGrid) OnPrice(price decimal.Decimal) (Action, error) {
	if !price.GreaterThan(decimal.Zero) || len(s.levels) == 0 || s.pendingInitial != nil {
		return Action{}, nil
	}
	shifted := false
	for price.GreaterThan(s.levels[len(s.levels)-1]) && s.Config.GridFollowUp {
		s.shift(1)
		shifted = true
	}
	for price.LessThan(s.levels[0]) && s.Config.GridFollowDown {
		s.shift(-1)
		shifted = true
	}
	if !shifted {
		return Action{}, nil
	}
	ids := s.activeOrderIDs()
	place := s.gridOrders(price)
	return Action{CancelClientOrderIDs: ids, CancelBeforePlace: true, Place: place}, nil
}

func (s *NormalGrid) Stop()          { s.Status = domain.StrategyStopped }
func (s *NormalGrid) EmergencyStop() { s.Status = domain.StrategyEmergencyStop }

func (s *NormalGrid) ExpectedOrderIntents() []domain.OrderIntent {
	if s.pendingInitial != nil {
		return []domain.OrderIntent{*s.pendingInitial}
	}
	expected := make([]domain.OrderIntent, 0, len(s.activeByLevel))
	for _, intent := range s.activeByLevel {
		expected = append(expected, intent)
	}
	return expected
}

func (s *NormalGrid) prepare(initialPrice decimal.Decimal) error {
	if !initialPrice.GreaterThan(decimal.Zero) {
		return errors.New("grid reference price must be positive")
	}
	if !s.Config.Leverage.GreaterThan(decimal.Zero) {
		return errors.New("leverage must be positive")
	}
	if !s.Config.GridLowerPrice.GreaterThan(decimal.Zero) || !s.Config.GridUpperPrice.GreaterThan(s.Config.GridLowerPrice) {
		return errors.New("grid lower and upper prices are invalid")
	}
	if s.Config.GridCount < 1 {
		return errors.New("grid_count must be at least 1")
	}
	if !s.Config.GridMargin.GreaterThan(decimal.Zero) {
		return errors.New("grid_margin must be positive")
	}
	if s.Config.Direction != domain.DirectionLong && s.Config.Direction != domain.DirectionShort && s.Config.Direction != domain.DirectionNeutral {
		return errors.New("normal grid direction must be long, short, or neutral")
	}
	if s.Config.Direction == domain.DirectionNeutral && (s.Config.GridFollowUp || s.Config.GridFollowDown) {
		return errors.New("neutral grid cannot follow price up or down")
	}
	if mode := strings.ToLower(strings.TrimSpace(s.Config.GridMode)); mode != "" && mode != "arithmetic" {
		return errors.New("only arithmetic grid mode is supported")
	}
	s.Config.GridMode = "arithmetic"
	if !s.Config.MarketMaxSlippage.GreaterThan(decimal.Zero) {
		s.Config.MarketMaxSlippage = decimal.RequireFromString("0.005")
	}

	s.Config.GridLowerPrice = s.quantizePrice(s.Config.GridLowerPrice)
	s.Config.GridUpperPrice = s.quantizePrice(s.Config.GridUpperPrice)
	s.rebuildLevels()
	if len(s.levels) != s.Config.GridCount+1 || !s.gridStep.GreaterThan(decimal.Zero) {
		return errors.New("grid range is too small for the selected grid count and market price step")
	}
	if initialPrice.LessThan(s.levels[0]) {
		if !s.Config.GridFollowDown || s.Config.Direction == domain.DirectionNeutral {
			return errors.New("grid reference price is below the configured range")
		}
		s.shiftUntilContains(initialPrice, -1)
	}
	if initialPrice.GreaterThan(s.levels[len(s.levels)-1]) {
		if !s.Config.GridFollowUp || s.Config.Direction == domain.DirectionNeutral {
			return errors.New("grid reference price is above the configured range")
		}
		s.shiftUntilContains(initialPrice, 1)
	}
	marginPerOrder := s.Config.GridMargin.Div(decimal.NewFromInt(int64(s.Config.GridCount + 1)))
	s.orderQuantity = s.quantityForMargin(marginPerOrder, initialPrice)
	if !s.orderQuantity.GreaterThan(decimal.Zero) {
		return errors.New("grid margin is below the market minimum order amount")
	}
	for index, price := range s.levels {
		if s.Market.MinQuoteAmount.GreaterThan(decimal.Zero) && s.orderQuantity.Mul(price).LessThan(s.Market.MinQuoteAmount) {
			return fmt.Errorf("grid order at level %d is below the market minimum order amount", index)
		}
	}
	return nil
}

func (s *NormalGrid) rebuildLevels() {
	s.levels = nil
	s.gridStep = s.Config.GridUpperPrice.Sub(s.Config.GridLowerPrice).Div(decimal.NewFromInt(int64(s.Config.GridCount)))
	if !s.gridStep.GreaterThan(decimal.Zero) {
		return
	}
	for index := 0; index <= s.Config.GridCount; index++ {
		price := s.quantizePrice(s.Config.GridLowerPrice.Add(s.gridStep.Mul(decimal.NewFromInt(int64(index)))))
		if len(s.levels) > 0 && price.LessThanOrEqual(s.levels[len(s.levels)-1]) {
			s.levels = nil
			return
		}
		s.levels = append(s.levels, price)
	}
	s.gridStep = s.levels[len(s.levels)-1].Sub(s.levels[0]).Div(decimal.NewFromInt(int64(s.Config.GridCount)))
}

func (s *NormalGrid) shift(direction int) {
	delta := s.gridStep.Mul(decimal.NewFromInt(int64(direction)))
	s.Config.GridLowerPrice = s.Config.GridLowerPrice.Add(delta)
	s.Config.GridUpperPrice = s.Config.GridUpperPrice.Add(delta)
	s.rebuildLevels()
}

func (s *NormalGrid) shiftUntilContains(price decimal.Decimal, direction int) {
	for (direction > 0 && price.GreaterThan(s.levels[len(s.levels)-1])) || (direction < 0 && price.LessThan(s.levels[0])) {
		s.shift(direction)
	}
}

func (s *NormalGrid) gridOrders(anchor decimal.Decimal) []domain.OrderIntent {
	s.activeByLevel = make(map[int]domain.OrderIntent)
	place := make([]domain.OrderIntent, 0, len(s.levels))
	for level, price := range s.levels {
		if price.Equal(anchor) {
			continue
		}
		intent := s.gridOrder(level, anchor)
		s.activeByLevel[level] = intent
		place = append(place, intent)
	}
	return place
}

func (s *NormalGrid) initialOrder(price decimal.Decimal) domain.OrderIntent {
	side := domain.OrderSideBuy
	if s.Config.Direction == domain.DirectionShort {
		side = domain.OrderSideSell
	}
	entryMode := domain.NormalizeEntryMode(string(s.Config.EntryMode), s.Config.InitialPrice != nil)
	intent := domain.OrderIntent{
		ClientOrderID:    s.orderID("initial", 0),
		MarketIndex:      s.Config.MarketIndex,
		Symbol:           s.Config.Symbol,
		Side:             side,
		Role:             domain.OrderRoleInitial,
		Level:            -1,
		Price:            price,
		Quantity:         s.orderQuantity,
		OrderType:        domain.OrderTypeLimit,
		PostOnly:         entryMode == domain.EntryModeMaker || entryMode == domain.EntryModeLimitMaker,
		MaxSlippageRatio: s.Config.MarketMaxSlippage,
	}
	if entryMode == domain.EntryModeMarket {
		intent.OrderType = domain.OrderTypeMarket
		intent.PostOnly = false
	}
	return intent
}

func (s *NormalGrid) gridOrder(level int, anchor decimal.Decimal) domain.OrderIntent {
	price := s.levels[level]
	side := domain.OrderSideBuy
	if price.GreaterThan(anchor) {
		side = domain.OrderSideSell
	}
	reduceOnly := (s.Config.Direction == domain.DirectionLong && side == domain.OrderSideSell) ||
		(s.Config.Direction == domain.DirectionShort && side == domain.OrderSideBuy)
	return domain.OrderIntent{
		ClientOrderID: s.orderID("grid", level), MarketIndex: s.Config.MarketIndex, Symbol: s.Config.Symbol,
		Side: side, Role: domain.OrderRoleGrid, Level: level, Price: price,
		Quantity:   s.orderQuantity,
		ReduceOnly: reduceOnly, PostOnly: true, OrderType: domain.OrderTypeLimit,
	}
}

func (s *NormalGrid) applyFill(side domain.OrderSide, price, quantity decimal.Decimal) {
	old := s.cycle.PositionQuantity
	delta := quantity
	if side == domain.OrderSideSell {
		delta = delta.Neg()
	}
	if old.IsZero() || (old.GreaterThan(decimal.Zero) && delta.GreaterThan(decimal.Zero)) || (old.LessThan(decimal.Zero) && delta.LessThan(decimal.Zero)) {
		oldAbs := old.Abs()
		newAbs := oldAbs.Add(delta.Abs())
		if oldAbs.IsZero() {
			s.cycle.AverageEntryPrice = price
		} else {
			s.cycle.AverageEntryPrice = s.cycle.AverageEntryPrice.Mul(oldAbs).Add(price.Mul(delta.Abs())).Div(newAbs)
		}
		s.cycle.PositionQuantity = old.Add(delta)
		return
	}

	closeQuantity := decimal.Min(old.Abs(), delta.Abs())
	if old.GreaterThan(decimal.Zero) {
		s.cycle.RealizedPnl = s.cycle.RealizedPnl.Add(price.Sub(s.cycle.AverageEntryPrice).Mul(closeQuantity))
	} else {
		s.cycle.RealizedPnl = s.cycle.RealizedPnl.Add(s.cycle.AverageEntryPrice.Sub(price).Mul(closeQuantity))
	}
	remaining := delta.Abs().Sub(closeQuantity)
	if remaining.IsZero() {
		s.cycle.PositionQuantity = old.Add(delta)
		if s.cycle.PositionQuantity.IsZero() {
			s.cycle.AverageEntryPrice = decimal.Zero
		}
		return
	}
	if delta.LessThan(decimal.Zero) {
		remaining = remaining.Neg()
	}
	s.cycle.PositionQuantity = remaining
	s.cycle.AverageEntryPrice = price
}

func (s *NormalGrid) quantityForMargin(margin, price decimal.Decimal) decimal.Decimal {
	contract := s.Market.ContractMult
	if !contract.GreaterThan(decimal.Zero) {
		contract = decimal.NewFromInt(1)
	}
	raw := margin.Mul(s.Config.Leverage).Div(price.Mul(contract))
	quantity := roundDown(raw, s.Market.SizeStep)
	if quantity.LessThan(s.Market.MinBaseAmount) {
		return decimal.Zero
	}
	if s.Market.MinQuoteAmount.GreaterThan(decimal.Zero) && quantity.Mul(price).LessThan(s.Market.MinQuoteAmount) {
		return decimal.Zero
	}
	return quantity
}

func (s *NormalGrid) quantizePrice(value decimal.Decimal) decimal.Decimal {
	return roundDown(value, s.Market.PriceStep)
}

func (s *NormalGrid) activeOrderIDs() []string {
	ids := make([]string, 0, len(s.activeByLevel)+1)
	if s.pendingInitial != nil {
		ids = append(ids, s.pendingInitial.ClientOrderID)
	}
	for _, intent := range s.activeByLevel {
		if intent.ClientOrderID != "" {
			ids = append(ids, intent.ClientOrderID)
		}
	}
	return ids
}

func (s *NormalGrid) orderID(role string, level int) string {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("ng-%s-%d", role, level)
	}
	return fmt.Sprintf("ng-%s-%d-%s", role, level, hex.EncodeToString(random))
}

func roundDown(value, step decimal.Decimal) decimal.Decimal {
	if step.IsZero() {
		return value
	}
	return value.Div(step).Truncate(0).Mul(step)
}
