package exchange

import (
	"errors"
	"fmt"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

// takeProfitMakerPrice keeps a triggered limit take-profit order on the
// non-marketable side of the trigger. A long position closes with a sell, so
// its maker price is one tick above the trigger; a short position closes with
// a buy, so its maker price is one tick below the trigger.
func takeProfitMakerPrice(trigger decimal.Decimal, side domain.OrderSide, priceStep decimal.Decimal) (decimal.Decimal, error) {
	if !trigger.GreaterThan(decimal.Zero) {
		return decimal.Zero, errors.New("take-profit trigger price must be positive")
	}
	if !priceStep.GreaterThan(decimal.Zero) {
		return decimal.Zero, errors.New("market price step must be positive")
	}

	price := trigger.Add(priceStep)
	if side == domain.OrderSideBuy {
		price = trigger.Sub(priceStep)
	}
	if !price.GreaterThan(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("maker take-profit price is not positive: trigger=%s step=%s", trigger, priceStep)
	}
	return price, nil
}
