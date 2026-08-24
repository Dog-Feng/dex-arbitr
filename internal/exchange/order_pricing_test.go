package exchange

import (
	"testing"

	"dex-grid-arbitr/internal/domain"
	"github.com/shopspring/decimal"
)

func TestTakeProfitMakerPriceStaysOutsideTrigger(t *testing.T) {
	step := decimal.RequireFromString("0.001")

	sell, err := takeProfitMakerPrice(decimal.RequireFromString("73.500"), domain.OrderSideSell, step)
	if err != nil || !sell.Equal(decimal.RequireFromString("73.501")) {
		t.Fatalf("unexpected sell maker price: price=%s err=%v", sell, err)
	}

	buy, err := takeProfitMakerPrice(decimal.RequireFromString("73.500"), domain.OrderSideBuy, step)
	if err != nil || !buy.Equal(decimal.RequireFromString("73.499")) {
		t.Fatalf("unexpected buy maker price: price=%s err=%v", buy, err)
	}
}

func TestTakeProfitMakerPriceRejectsInvalidStep(t *testing.T) {
	if _, err := takeProfitMakerPrice(decimal.NewFromInt(100), domain.OrderSideSell, decimal.Zero); err == nil {
		t.Fatal("expected invalid price step error")
	}
}
