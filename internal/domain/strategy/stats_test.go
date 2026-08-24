package strategy

import (
	"testing"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testCOID() order.ClientOrderID {
	return order.MustEncode(order.Ref{Slot: 1, Epoch: 1, Cell: 0, Purpose: order.PurposeClose, Seq: 1})
}

func TestForViewUsesGridProfitWhenVenueMissing(t *testing.T) {
	s := Stats{GridProfit: d("0.7448"), CycleFee: d("0.144"), FeePaid: d("0.216")}
	got := s.ForView().RealizedPnL
	if !got.Equal(d("0.7448")) {
		t.Fatalf("realized = %s, want 0.7448 (must not subtract estimated fees again)", got)
	}
}

func TestNoteVenueTradeZeroPnLDoesNotSwitchPath(t *testing.T) {
	stats := &Stats{GridProfit: d("10"), CycleFee: d("1"), FeePaid: d("3")}
	seen := map[int64]struct{}{}
	NoteVenueTrade(stats, seen, 1, 1, order.Trade{ID: 1, ClientOrderID: testCOID(), RealizedPnL: decimal.Zero})
	if stats.VenueRealized {
		t.Fatal("zero venue pnl must not set venue_realized")
	}
	if !stats.ForView().RealizedPnL.Equal(d("10")) {
		t.Fatalf("realized = %s, want 10", stats.ForView().RealizedPnL)
	}
}

func TestNoteVenueTradeNonZeroOverrides(t *testing.T) {
	stats := &Stats{GridProfit: d("10"), FeePaid: d("1")}
	seen := map[int64]struct{}{}
	NoteVenueTrade(stats, seen, 1, 1, order.Trade{ID: 1, ClientOrderID: testCOID(), RealizedPnL: d("24.5")})
	if !stats.VenueRealized {
		t.Fatal("non-zero venue pnl should set venue_realized")
	}
	if !stats.ForView().RealizedPnL.Equal(d("24.5")) {
		t.Fatalf("realized = %s, want 24.5", stats.ForView().RealizedPnL)
	}
}

func TestNoteVenueTradeNegativeStillOverrides(t *testing.T) {
	stats := &Stats{GridProfit: d("10"), FeePaid: d("1")}
	seen := map[int64]struct{}{}
	NoteVenueTrade(stats, seen, 1, 1, order.Trade{ID: 1, ClientOrderID: testCOID(), RealizedPnL: d("-3.2")})
	if !stats.VenueRealized {
		t.Fatal("negative venue pnl is real data and should switch path")
	}
	if !stats.ForView().RealizedPnL.Equal(d("-3.2")) {
		t.Fatalf("realized = %s, want -3.2", stats.ForView().RealizedPnL)
	}
}
