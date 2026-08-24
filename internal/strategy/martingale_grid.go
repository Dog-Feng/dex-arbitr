package strategy

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

type Action struct {
	Place                     []domain.OrderIntent
	Modify                    *ModifyAction
	CancelClientOrderIDs      []string
	CancelBeforePlace         bool
	CycleCompleted            bool
	CompletedCycleRealizedPnl decimal.Decimal
}

type ModifyAction struct {
	ClientOrderID string
	Intent        domain.OrderIntent
}

type SafetyLevel struct {
	Level    int
	Price    decimal.Decimal
	Margin   decimal.Decimal
	Quantity decimal.Decimal
}

type MartingaleGrid struct {
	Config              domain.MartingaleConfig
	Market              domain.MarketSpec
	Status              domain.StrategyStatus
	CycleID             int
	Cycle               *domain.CycleSnapshot
	initialIntent       *domain.OrderIntent
	safetyIntents       []domain.OrderIntent
	takeProfitIntent    *domain.OrderIntent
	filledSafetyOrderID map[string]struct{}
}

func NewMartingaleGrid(config domain.MartingaleConfig, market domain.MarketSpec) *MartingaleGrid {
	return &MartingaleGrid{
		Config:              config,
		Market:              market,
		Status:              domain.StrategyIdle,
		filledSafetyOrderID: make(map[string]struct{}),
	}
}

func (s *MartingaleGrid) StrategyStatus() domain.StrategyStatus   { return s.Status }
func (s *MartingaleGrid) SetStatus(status domain.StrategyStatus)  { s.Status = status }
func (s *MartingaleGrid) StrategyType() string                    { return "martingale" }
func (s *MartingaleGrid) Direction() domain.Direction             { return s.Config.Direction }
func (s *MartingaleGrid) MarketIndex() int                        { return s.Config.MarketIndex }
func (s *MartingaleGrid) Symbol() string                          { return s.Config.Symbol }
func (s *MartingaleGrid) CycleSnapshot() *domain.CycleSnapshot    { return s.Cycle }
func (s *MartingaleGrid) Continuous() bool                        { return s.Config.Continuous }
func (s *MartingaleGrid) OnPrice(decimal.Decimal) (Action, error) { return Action{}, nil }

func (s *MartingaleGrid) GridSnapshot() *domain.GridSnapshot { return nil }

func (s *MartingaleGrid) ExpectedOrderIntents() []domain.OrderIntent {
	if s.Cycle == nil {
		return nil
	}
	expected := make([]domain.OrderIntent, 0, len(s.safetyIntents)+2)
	if s.initialIntent != nil && s.initialIntent.Quantity.GreaterThan(decimal.Zero) {
		expected = append(expected, *s.initialIntent)
	}
	if s.Cycle.PositionQuantity.IsZero() {
		return expected
	}
	if s.takeProfitIntent != nil && s.takeProfitIntent.Quantity.GreaterThan(decimal.Zero) {
		expected = append(expected, *s.takeProfitIntent)
	}
	for _, intent := range s.safetyIntents {
		if _, filled := s.filledSafetyOrderID[intent.ClientOrderID]; filled {
			continue
		}
		expected = append(expected, intent)
	}
	return expected
}

func (s *MartingaleGrid) PendingInitialOrder() (domain.OrderIntent, bool) {
	if s.initialIntent == nil || !s.initialIntent.Quantity.GreaterThan(decimal.Zero) {
		return domain.OrderIntent{}, false
	}
	return *s.initialIntent, true
}

func (s *MartingaleGrid) UpdatePendingInitialOrder(intent domain.OrderIntent) {
	if s.initialIntent == nil || intent.Role != domain.OrderRoleInitial {
		return
	}
	*s.initialIntent = intent
}

func (s *MartingaleGrid) BindOrderID(intent domain.OrderIntent, order domain.ExchangeOrder) {
	if intent.Role != domain.OrderRoleTakeProfit || s.takeProfitIntent == nil {
		return
	}
	if s.takeProfitIntent.ClientOrderID == intent.ClientOrderID && order.ClientOrderID != "" {
		s.takeProfitIntent.ClientOrderID = order.ClientOrderID
	}
}

func (s *MartingaleGrid) Start(initialPrice decimal.Decimal, marketEntry bool) (Action, error) {
	if err := s.validateConfig(initialPrice); err != nil {
		return Action{}, err
	}
	s.CycleID++
	s.Status = domain.StrategyRunning
	s.resetCycle(initialPrice, marketEntry)
	return Action{Place: []domain.OrderIntent{*s.initialIntent}}, nil
}

func (s *MartingaleGrid) OnFill(fill domain.Fill) (Action, error) {
	if s.Cycle == nil {
		return Action{}, errors.New("strategy has no active cycle")
	}
	switch fill.Role {
	case domain.OrderRoleInitial, domain.OrderRoleSafety:
		firstInitialFill := fill.Role == domain.OrderRoleInitial && s.Cycle.PositionQuantity.IsZero()
		initialCompleted := false
		if fill.Role == domain.OrderRoleInitial {
			initialCompleted = s.consumeInitialFill(fill.Quantity)
		}
		s.applyEntryFill(fill)
		if fill.Role == domain.OrderRoleInitial && !initialCompleted {
			// Keep the initial Maker order as the sole working order until its
			// requested quantity is fully filled. The remaining quantity is
			// maintained by the entry watchdog and the grid is created once.
			return Action{}, nil
		}
		if firstInitialFill {
			tp := s.makeTakeProfitIntent()
			s.takeProfitIntent = &tp
			place := []domain.OrderIntent{tp}
			if initialCompleted {
				s.safetyIntents = s.makeSafetyIntents()
				place = append(place, s.safetyIntents...)
			}
			return Action{Place: place}, nil
		}
		if fill.Role == domain.OrderRoleInitial && initialCompleted && len(s.safetyIntents) == 0 {
			s.safetyIntents = s.makeSafetyIntents()
			action := s.replaceTakeProfit()
			action.Place = append(action.Place, s.safetyIntents...)
			return action, nil
		}
		return s.replaceTakeProfit(), nil

	case domain.OrderRoleTakeProfit:
		s.applyExitFill(fill)
		if !s.Cycle.PositionQuantity.IsZero() {
			return s.replaceTakeProfit(), nil
		}
		s.Cycle.Status = "completed"
		completed := s.Cycle.RealizedPnl
		cancel := make([]string, 0, len(s.safetyIntents))
		for _, intent := range s.safetyIntents {
			cancel = append(cancel, intent.ClientOrderID)
		}
		if !s.Config.Continuous {
			s.Status = domain.StrategyStopped
			return Action{CancelClientOrderIDs: cancel, CycleCompleted: true, CompletedCycleRealizedPnl: completed}, nil
		}
		s.CycleID++
		s.resetCycle(fill.Price, true)
		return Action{Place: []domain.OrderIntent{*s.initialIntent}, CancelClientOrderIDs: cancel, CycleCompleted: true, CompletedCycleRealizedPnl: completed}, nil
	default:
		return Action{}, fmt.Errorf("unsupported fill role: %s", fill.Role)
	}
}

func (s *MartingaleGrid) Stop()                                 { s.Status = domain.StrategyStopped }
func (s *MartingaleGrid) EmergencyStop()                        { s.Status = domain.StrategyEmergencyStop }
func (s *MartingaleGrid) TakeProfitIntent() *domain.OrderIntent { return s.takeProfitIntent }

// ProjectedFullGrid returns the weighted average entry price and an isolated
// margin liquidation estimate after the initial order and every safety order
// have filled. Exchanges calculate the actual liquidation price using their
// own maintenance-margin and cross-margin rules; this estimate is for planning
// only, not an exchange liquidation guarantee.
func (s *MartingaleGrid) ProjectedFullGrid() (decimal.Decimal, decimal.Decimal) {
	return s.projectedFullGrid(decimal.Zero, false)
}

// ProjectedFullGridWithAvailableBalance calculates the full-grid estimate
// from the account's currently available cross-margin balance. The planned
// position is still built from the initial order and every safety order, but
// configured strategy margins are not added to the account balance again.
func (s *MartingaleGrid) ProjectedFullGridWithAvailableBalance(availableBalance decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	return s.projectedFullGrid(availableBalance, true)
}

// ProjectedFullGridWithAccountState starts the full-grid projection from the
// live account position and adds the strategy's currently pending entry and
// safety orders. This represents the position after all remaining add orders
// have filled, rather than rebuilding it from the configured initial margin.
func (s *MartingaleGrid) ProjectedFullGridWithAccountState(availableBalance, positionQuantity, averageEntryPrice decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	if s.Cycle == nil || !positionQuantity.GreaterThan(decimal.Zero) || !averageEntryPrice.GreaterThan(decimal.Zero) {
		return s.ProjectedFullGridWithAvailableBalance(availableBalance)
	}
	totalQuantity := positionQuantity.Abs()
	weightedCost := totalQuantity.Mul(averageEntryPrice)
	for _, intent := range s.ExpectedOrderIntents() {
		if (intent.Role != domain.OrderRoleInitial && intent.Role != domain.OrderRoleSafety) || !intent.Price.GreaterThan(decimal.Zero) || !intent.Quantity.GreaterThan(decimal.Zero) {
			continue
		}
		totalQuantity = totalQuantity.Add(intent.Quantity.Abs())
		weightedCost = weightedCost.Add(intent.Quantity.Abs().Mul(intent.Price))
	}
	return s.projectedGridMetrics(totalQuantity, weightedCost, availableBalance, true, decimal.Zero)
}

func (s *MartingaleGrid) projectedFullGrid(availableBalance decimal.Decimal, includeAvailableBalance bool) (decimal.Decimal, decimal.Decimal) {
	if s.Cycle == nil || !s.Cycle.InitialPrice.GreaterThan(decimal.Zero) {
		return decimal.Zero, decimal.Zero
	}
	initialQuantity := s.quantityForMargin(s.Config.InitialMargin, s.Cycle.InitialPrice)
	totalQuantity := initialQuantity
	weightedCost := initialQuantity.Mul(s.Cycle.InitialPrice)
	totalMargin := s.Config.InitialMargin
	for _, level := range s.SafetyLevels(s.Cycle.InitialPrice) {
		totalQuantity = totalQuantity.Add(level.Quantity)
		weightedCost = weightedCost.Add(level.Quantity.Mul(level.Price))
		totalMargin = totalMargin.Add(level.Margin)
	}
	return s.projectedGridMetrics(totalQuantity, weightedCost, availableBalance, includeAvailableBalance, totalMargin)
}

func (s *MartingaleGrid) projectedGridMetrics(totalQuantity, weightedCost, availableBalance decimal.Decimal, includeAvailableBalance bool, totalMargin decimal.Decimal) (decimal.Decimal, decimal.Decimal) {
	if includeAvailableBalance {
		// The account's available balance is the account-level buffer for the
		// projection. Do not add the configured strategy margins again: doing
		// so makes the estimate depend on every planned order instead of the
		// free balance that remains available after the hypothetical fills.
		totalMargin = availableBalance
	}
	if !totalQuantity.GreaterThan(decimal.Zero) {
		return decimal.Zero, decimal.Zero
	}
	averageEntry := weightedCost.Div(totalQuantity)
	contractMultiplier := s.Market.ContractMult
	if !contractMultiplier.GreaterThan(decimal.Zero) {
		contractMultiplier = decimal.NewFromInt(1)
	}
	liquidationDistance := totalMargin.Div(totalQuantity.Mul(contractMultiplier))
	liquidationPrice := averageEntry.Sub(liquidationDistance)
	if s.Config.Direction == domain.DirectionShort {
		liquidationPrice = averageEntry.Add(liquidationDistance)
	}
	if liquidationPrice.LessThan(decimal.Zero) {
		liquidationPrice = decimal.Zero
	}
	return s.quantizePrice(averageEntry), s.quantizePrice(liquidationPrice)
}

// ProjectedOrderMetrics describes the martingale position after one active
// order is filled in full. It is intentionally a planning estimate: the
// exchange remains authoritative for the actual position and liquidation
// price.
type ProjectedOrderMetrics struct {
	AverageEntryPrice decimal.Decimal
	TakeProfitPrice   decimal.Decimal
	LiquidationPrice  decimal.Decimal
	HasPosition       bool
}

// ProjectedFillMetrics estimates the position-related values shown beside an
// active martingale order. Entry and safety orders are treated as additions;
// reduce-only/take-profit orders are treated as reductions. A partial fill is
// refreshed from the next strategy view rather than simulated here.
func (s *MartingaleGrid) ProjectedFillMetrics(order domain.ExchangeOrder) ProjectedOrderMetrics {
	return s.projectedFillMetrics(order, decimal.Zero, false)
}

// ProjectedFillMetricsWithAvailableBalance uses the account's free
// cross-margin balance as the risk buffer for the hypothetical post-fill
// position shown beside an active order. It does not add configured margins.
func (s *MartingaleGrid) ProjectedFillMetricsWithAvailableBalance(order domain.ExchangeOrder, availableBalance decimal.Decimal) ProjectedOrderMetrics {
	return s.projectedFillMetrics(order, availableBalance, true)
}

// ProjectedFillMetricsWithAccountState uses the live account position as the
// base for a hypothetical fill. This keeps the per-order projection aligned
// with the exchange position when the local fill stream is delayed or a
// partial fill has not been fully reconciled yet.
func (s *MartingaleGrid) ProjectedFillMetricsWithAccountState(order domain.ExchangeOrder, availableBalance, positionQuantity, averageEntryPrice decimal.Decimal) ProjectedOrderMetrics {
	if !positionQuantity.GreaterThan(decimal.Zero) || !averageEntryPrice.GreaterThan(decimal.Zero) {
		return s.ProjectedFillMetricsWithAvailableBalance(order, availableBalance)
	}
	return s.projectedFillMetricsFromPosition(order, availableBalance, true, positionQuantity, averageEntryPrice, true)
}

func (s *MartingaleGrid) projectedFillMetrics(order domain.ExchangeOrder, availableBalance decimal.Decimal, includeAvailableBalance bool) ProjectedOrderMetrics {
	if s.Cycle == nil || !order.Price.GreaterThan(decimal.Zero) || !order.Quantity.GreaterThan(decimal.Zero) {
		return ProjectedOrderMetrics{}
	}
	return s.projectedFillMetricsFromPosition(order, availableBalance, includeAvailableBalance, s.Cycle.PositionQuantity.Abs(), s.Cycle.AverageEntryPrice, false)
}

func (s *MartingaleGrid) projectedFillMetricsFromPosition(order domain.ExchangeOrder, availableBalance decimal.Decimal, includeAvailableBalance bool, currentQuantity, currentAverageEntryPrice decimal.Decimal, accountPosition bool) ProjectedOrderMetrics {
	if !order.Price.GreaterThan(decimal.Zero) || !order.Quantity.GreaterThan(decimal.Zero) {
		return ProjectedOrderMetrics{}
	}

	projectedQuantity := currentQuantity
	averageEntry := currentAverageEntryPrice
	margin := decimal.Zero
	if !accountPosition {
		margin = s.currentPositionMargin()
	}
	isReduction := order.ReduceOnly || order.Role == domain.OrderRoleTakeProfit || order.Role == domain.OrderRoleClose

	if isReduction {
		projectedQuantity = currentQuantity.Sub(decimal.Min(currentQuantity, order.Quantity.Abs()))
	} else {
		filledQuantity := order.Quantity.Abs()
		projectedQuantity = currentQuantity.Add(filledQuantity)
		if currentQuantity.IsZero() {
			averageEntry = order.Price
		} else if projectedQuantity.GreaterThan(decimal.Zero) {
			averageEntry = averageEntry.Mul(currentQuantity).Add(order.Price.Mul(filledQuantity)).Div(projectedQuantity)
		}
		margin = margin.Add(s.marginForProjectedEntry(order, currentQuantity))
	}
	if includeAvailableBalance {
		// For an account-backed projection, available balance is the complete
		// account-level buffer. It must replace the configured margin estimate,
		// not be added to it.
		margin = availableBalance
	}

	if !projectedQuantity.GreaterThan(decimal.Zero) || !averageEntry.GreaterThan(decimal.Zero) {
		return ProjectedOrderMetrics{}
	}
	averageEntry = s.quantizePrice(averageEntry)
	takeProfit := s.takeProfitPrice(averageEntry)
	liquidation := s.liquidationPrice(averageEntry, projectedQuantity, margin)
	return ProjectedOrderMetrics{
		AverageEntryPrice: averageEntry,
		TakeProfitPrice:   takeProfit,
		LiquidationPrice:  liquidation,
		HasPosition:       true,
	}
}

func (s *MartingaleGrid) currentPositionMargin() decimal.Decimal {
	if s.Cycle == nil || !s.Cycle.PositionQuantity.Abs().GreaterThan(decimal.Zero) {
		return decimal.Zero
	}
	total := s.Config.InitialMargin
	for level := 1; level <= s.Cycle.FilledSafetyOrders; level++ {
		total = total.Add(s.Config.SafetyMargin.Mul(pow(s.Config.AmountMultiplier, level-1)))
	}
	return total
}

func (s *MartingaleGrid) marginForProjectedEntry(order domain.ExchangeOrder, currentQuantity decimal.Decimal) decimal.Decimal {
	switch order.Role {
	case domain.OrderRoleInitial:
		if currentQuantity.IsZero() {
			return s.Config.InitialMargin
		}
	case domain.OrderRoleSafety:
		if order.Level > 0 {
			return s.Config.SafetyMargin.Mul(pow(s.Config.AmountMultiplier, order.Level-1))
		}
	}
	return decimal.Zero
}

func (s *MartingaleGrid) liquidationPrice(averageEntry, quantity, margin decimal.Decimal) decimal.Decimal {
	if !averageEntry.GreaterThan(decimal.Zero) || !quantity.GreaterThan(decimal.Zero) || !margin.GreaterThan(decimal.Zero) {
		return decimal.Zero
	}
	contractMultiplier := s.Market.ContractMult
	if !contractMultiplier.GreaterThan(decimal.Zero) {
		contractMultiplier = decimal.NewFromInt(1)
	}
	distance := margin.Div(quantity.Mul(contractMultiplier))
	liquidation := averageEntry.Sub(distance)
	if s.Config.Direction == domain.DirectionShort {
		liquidation = averageEntry.Add(distance)
	}
	if liquidation.LessThan(decimal.Zero) {
		liquidation = decimal.Zero
	}
	return s.quantizePrice(liquidation)
}

func (s *MartingaleGrid) SafetyLevels(initialPrice decimal.Decimal) []SafetyLevel {
	levels := make([]SafetyLevel, 0, s.Config.MaxSafetyOrders)
	for level := 1; level <= s.Config.MaxSafetyOrders; level++ {
		distance := decimal.Zero
		factor := decimal.NewFromInt(1)
		for index := 0; index < level; index++ {
			distance = distance.Add(s.Config.PriceStepPct.Mul(factor))
			factor = factor.Mul(s.Config.PriceStepMultiplier)
		}
		price := initialPrice
		if s.Config.Direction == domain.DirectionLong {
			price = initialPrice.Mul(decimal.NewFromInt(1).Sub(distance))
		} else {
			price = initialPrice.Mul(decimal.NewFromInt(1).Add(distance))
		}
		margin := s.Config.SafetyMargin.Mul(pow(s.Config.AmountMultiplier, level-1))
		quantity := s.quantityForMargin(margin, price)
		levels = append(levels, SafetyLevel{Level: level, Price: s.quantizePrice(price), Margin: margin, Quantity: quantity})
	}
	return levels
}

func (s *MartingaleGrid) resetCycle(initialPrice decimal.Decimal, marketEntry bool) {
	entryMode := domain.NormalizeEntryMode(string(s.Config.EntryMode), s.Config.InitialPrice != nil)
	if entryMode == domain.EntryModeLimitMaker && s.Config.InitialPrice != nil {
		initialPrice = *s.Config.InitialPrice
	}
	initialPrice = s.quantizePrice(initialPrice)
	tpPrice := initialPrice
	if s.Config.Direction == domain.DirectionLong {
		tpPrice = initialPrice.Mul(decimal.NewFromInt(1).Add(s.Config.TakeProfitPct))
	} else {
		tpPrice = initialPrice.Mul(decimal.NewFromInt(1).Sub(s.Config.TakeProfitPct))
	}
	s.Cycle = &domain.CycleSnapshot{
		CycleID:         s.CycleID,
		Status:          "running",
		InitialPrice:    initialPrice,
		TakeProfitPrice: s.quantizePrice(tpPrice),
		PlannedMargin:   s.plannedMargin(),
	}
	side := domain.OrderSideBuy
	if s.Config.Direction == domain.DirectionShort {
		side = domain.OrderSideSell
	}
	intent := domain.OrderIntent{
		ClientOrderID:    s.orderID("initial", 0),
		MarketIndex:      s.Config.MarketIndex,
		Symbol:           s.Config.Symbol,
		Side:             side,
		Role:             domain.OrderRoleInitial,
		Level:            0,
		Price:            initialPrice,
		Quantity:         s.quantityForMargin(s.Config.InitialMargin, initialPrice),
		OrderType:        domain.OrderTypeLimit,
		PostOnly:         entryMode == domain.EntryModeMaker || entryMode == domain.EntryModeLimitMaker,
		MaxSlippageRatio: s.Config.MarketMaxSlippage,
	}
	if entryMode == domain.EntryModeMarket || (entryMode == "" && marketEntry) {
		intent.OrderType = domain.OrderTypeMarket
		intent.PostOnly = false
	}
	s.initialIntent = &intent
	s.safetyIntents = nil
	s.takeProfitIntent = nil
	s.filledSafetyOrderID = make(map[string]struct{})
}

func (s *MartingaleGrid) consumeInitialFill(quantity decimal.Decimal) bool {
	if s.initialIntent == nil {
		return true
	}
	remaining := s.initialIntent.Quantity.Sub(quantity.Abs())
	if remaining.GreaterThan(decimal.Zero) {
		s.initialIntent.Quantity = remaining
		return false
	}
	s.initialIntent = nil
	return true
}

func (s *MartingaleGrid) makeSafetyIntents() []domain.OrderIntent {
	side := domain.OrderSideBuy
	if s.Config.Direction == domain.DirectionShort {
		side = domain.OrderSideSell
	}
	intents := make([]domain.OrderIntent, 0, len(s.SafetyLevels(s.Cycle.InitialPrice)))
	for _, level := range s.SafetyLevels(s.Cycle.InitialPrice) {
		intents = append(intents, domain.OrderIntent{
			ClientOrderID: s.orderID("safety", level.Level), MarketIndex: s.Config.MarketIndex,
			Symbol: s.Config.Symbol, Side: side, Role: domain.OrderRoleSafety,
			Level: level.Level, Price: level.Price, Quantity: level.Quantity, OrderType: domain.OrderTypeLimit,
		})
	}
	return intents
}

func (s *MartingaleGrid) makeTakeProfitIntent() domain.OrderIntent {
	side := domain.OrderSideSell
	if s.Config.Direction == domain.DirectionShort {
		side = domain.OrderSideBuy
	}
	trigger := s.Cycle.TakeProfitPrice
	return domain.OrderIntent{
		ClientOrderID: s.orderID("take-profit", s.Cycle.CycleID), MarketIndex: s.Config.MarketIndex,
		Symbol: s.Config.Symbol, Side: side, Role: domain.OrderRoleTakeProfit,
		Level: -1, Price: trigger, Quantity: s.Cycle.PositionQuantity.Abs(), ReduceOnly: true,
		TriggerPrice: &trigger, OrderType: domain.OrderTypeLimit,
	}
}

func (s *MartingaleGrid) replaceTakeProfit() Action {
	tp := s.makeTakeProfitIntent()
	if s.takeProfitIntent == nil || s.takeProfitIntent.ClientOrderID == "" {
		s.takeProfitIntent = &tp
		return Action{Place: []domain.OrderIntent{tp}}
	}
	// Keep one exchange order alive and modify it in place. This prevents a
	// partial fill or safety fill from creating overlapping take-profit orders.
	clientOrderID := s.takeProfitIntent.ClientOrderID
	tp.ClientOrderID = clientOrderID
	s.takeProfitIntent = &tp
	return Action{Modify: &ModifyAction{ClientOrderID: clientOrderID, Intent: tp}}
}

func (s *MartingaleGrid) applyEntryFill(fill domain.Fill) {
	if fill.Role == domain.OrderRoleInitial && s.Cycle.PositionQuantity.IsZero() {
		s.Cycle.InitialPrice = s.quantizePrice(fill.Price)
		tpPrice := fill.Price
		if s.Config.Direction == domain.DirectionLong {
			tpPrice = fill.Price.Mul(decimal.NewFromInt(1).Add(s.Config.TakeProfitPct))
		} else {
			tpPrice = fill.Price.Mul(decimal.NewFromInt(1).Sub(s.Config.TakeProfitPct))
		}
		s.Cycle.TakeProfitPrice = s.quantizePrice(tpPrice)
	}
	oldQuantity := s.Cycle.PositionQuantity.Abs()
	newQuantity := oldQuantity.Add(fill.Quantity)
	average := fill.Price
	if !oldQuantity.IsZero() {
		average = s.Cycle.AverageEntryPrice.Mul(oldQuantity).Add(fill.Price.Mul(fill.Quantity)).Div(newQuantity)
	}
	if s.Config.Direction == domain.DirectionLong {
		s.Cycle.PositionQuantity = s.Cycle.PositionQuantity.Add(fill.Quantity)
	} else {
		s.Cycle.PositionQuantity = s.Cycle.PositionQuantity.Sub(fill.Quantity)
	}
	s.Cycle.AverageEntryPrice = average
	s.Cycle.TakeProfitPrice = s.takeProfitPrice(average)
	if fill.Role == domain.OrderRoleSafety {
		if _, exists := s.filledSafetyOrderID[fill.ClientOrderID]; !exists {
			s.Cycle.FilledSafetyOrders++
			s.filledSafetyOrderID[fill.ClientOrderID] = struct{}{}
		}
	}
}

func (s *MartingaleGrid) applyExitFill(fill domain.Fill) {
	closeQuantity := decimal.Min(s.Cycle.PositionQuantity.Abs(), fill.Quantity)
	if s.Config.Direction == domain.DirectionLong {
		s.Cycle.RealizedPnl = s.Cycle.RealizedPnl.Add(fill.Price.Sub(s.Cycle.AverageEntryPrice).Mul(closeQuantity))
		s.Cycle.PositionQuantity = s.Cycle.PositionQuantity.Sub(closeQuantity)
	} else {
		s.Cycle.RealizedPnl = s.Cycle.RealizedPnl.Add(s.Cycle.AverageEntryPrice.Sub(fill.Price).Mul(closeQuantity))
		s.Cycle.PositionQuantity = s.Cycle.PositionQuantity.Add(closeQuantity)
	}
	if s.Cycle.PositionQuantity.IsZero() {
		s.Cycle.AverageEntryPrice = decimal.Zero
	}
}

func (s *MartingaleGrid) plannedMargin() decimal.Decimal {
	total := s.Config.InitialMargin
	for i := 0; i < s.Config.MaxSafetyOrders; i++ {
		total = total.Add(s.Config.SafetyMargin.Mul(pow(s.Config.AmountMultiplier, i)))
	}
	return total
}

func (s *MartingaleGrid) quantityForMargin(margin, price decimal.Decimal) decimal.Decimal {
	raw := margin.Mul(s.Config.Leverage).Div(price.Mul(s.Market.ContractMult))
	quantity := s.roundDown(raw, s.Market.SizeStep)
	if quantity.LessThan(s.Market.MinBaseAmount) {
		return decimal.Zero
	}
	if s.Market.MinQuoteAmount.GreaterThan(decimal.Zero) && quantity.Mul(price).LessThan(s.Market.MinQuoteAmount) {
		return decimal.Zero
	}
	return quantity
}

func (s *MartingaleGrid) validateConfig(initialPrice decimal.Decimal) error {
	if !initialPrice.GreaterThan(decimal.Zero) || !s.Config.Leverage.GreaterThan(decimal.Zero) {
		return errors.New("initial price and leverage must be positive")
	}
	if s.Config.Direction != domain.DirectionLong && s.Config.Direction != domain.DirectionShort {
		return errors.New("martingale direction must be long or short")
	}
	if !s.Config.InitialMargin.GreaterThan(decimal.Zero) || !s.Config.SafetyMargin.GreaterThan(decimal.Zero) {
		return errors.New("margins must be positive")
	}
	if !s.Config.PriceStepPct.GreaterThan(decimal.Zero) || !s.Config.PriceStepPct.LessThan(decimal.NewFromInt(1)) {
		return errors.New("price_step_pct must be between 0 and 1")
	}
	if !s.Config.TakeProfitPct.GreaterThan(decimal.Zero) || !s.Config.TakeProfitPct.LessThan(decimal.NewFromInt(1)) {
		return errors.New("take_profit_pct must be between 0 and 1")
	}
	if !s.Config.MarketMaxSlippage.GreaterThan(decimal.Zero) || !s.Config.MarketMaxSlippage.LessThanOrEqual(decimal.NewFromInt(1)) {
		return errors.New("market_max_slippage must be between 0 and 1")
	}
	if s.Config.MaxSafetyOrders < 0 || s.Config.MarketMaxRetries < 0 || s.Config.MarketMaxRetries > 20 {
		return errors.New("invalid retry or safety order count")
	}
	if s.Config.PriceStepMultiplier.LessThan(decimal.NewFromFloat(0.1)) || s.Config.PriceStepMultiplier.GreaterThan(decimal.NewFromInt(10)) {
		return errors.New("price_step_multiplier must be between 0.1 and 10")
	}
	if s.Config.AmountMultiplier.LessThan(decimal.NewFromFloat(0.1)) || s.Config.AmountMultiplier.GreaterThan(decimal.NewFromInt(10)) {
		return errors.New("amount_multiplier must be between 0.1 and 10")
	}
	if s.Config.MaxCycleMargin.GreaterThan(decimal.Zero) && s.plannedMargin().GreaterThan(s.Config.MaxCycleMargin) {
		return errors.New("planned cycle margin exceeds max_cycle_margin")
	}
	totalQuantity := s.quantityForMargin(s.Config.InitialMargin, initialPrice)
	if !totalQuantity.GreaterThan(decimal.Zero) {
		return errors.New("initial margin is below the market minimum order amount")
	}
	for _, level := range s.SafetyLevels(initialPrice) {
		if !level.Quantity.GreaterThan(decimal.Zero) {
			return fmt.Errorf("safety margin for level %d is below the market minimum order amount", level.Level)
		}
		totalQuantity = totalQuantity.Add(level.Quantity)
	}
	if s.Config.MaxPositionQuantity.GreaterThan(decimal.Zero) && totalQuantity.GreaterThan(s.Config.MaxPositionQuantity) {
		return errors.New("planned position exceeds max_position_quantity")
	}
	return nil
}

func (s *MartingaleGrid) takeProfitPrice(initialPrice decimal.Decimal) decimal.Decimal {
	if s.Config.Direction == domain.DirectionLong {
		return s.quantizePrice(initialPrice.Mul(decimal.NewFromInt(1).Add(s.Config.TakeProfitPct)))
	}
	return s.quantizePrice(initialPrice.Mul(decimal.NewFromInt(1).Sub(s.Config.TakeProfitPct)))
}

func (s *MartingaleGrid) quantizePrice(value decimal.Decimal) decimal.Decimal {
	return s.roundDown(value, s.Market.PriceStep)
}

func (s *MartingaleGrid) roundDown(value, step decimal.Decimal) decimal.Decimal {
	if step.IsZero() {
		return value
	}
	return value.Div(step).Truncate(0).Mul(step)
}

func (s *MartingaleGrid) orderID(role string, level int) string {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		return fmt.Sprintf("mg-%d-%s-%d", s.CycleID, role, level)
	}
	return fmt.Sprintf("mg-%d-%s-%d-%s", s.CycleID, role, level, hex.EncodeToString(random))
}

func pow(value decimal.Decimal, exponent int) decimal.Decimal {
	result := decimal.NewFromInt(1)
	for i := 0; i < exponent; i++ {
		result = result.Mul(value)
	}
	return result
}
