package strategy

import (
	"github.com/shopspring/decimal"

	"dex-grid-arbitr/internal/domain"
)

// Strategy is the lifecycle contract shared by the martingale and ordinary
// grid implementations. Runtime owns exchange I/O and delegates only
// strategy decisions to this interface.
type Strategy interface {
	Start(decimal.Decimal, bool) (Action, error)
	OnFill(domain.Fill) (Action, error)
	OnPrice(decimal.Decimal) (Action, error)
	Stop()
	EmergencyStop()
	SetStatus(domain.StrategyStatus)
	StrategyStatus() domain.StrategyStatus
	StrategyType() string
	Direction() domain.Direction
	MarketIndex() int
	Symbol() string
	CycleSnapshot() *domain.CycleSnapshot
	ProjectedFullGrid() (decimal.Decimal, decimal.Decimal)
	GridSnapshot() *domain.GridSnapshot
	ExpectedOrderIntents() []domain.OrderIntent
	PendingInitialOrder() (domain.OrderIntent, bool)
	UpdatePendingInitialOrder(domain.OrderIntent)
	Continuous() bool
	BindOrderID(domain.OrderIntent, domain.ExchangeOrder)
}
