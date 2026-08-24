package fake

import (
	"context"
	"testing"

	"dex-grid/internal/domain/market"
	"dex-grid/internal/domain/order"
	"dex-grid/internal/exchange"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func testMarket() market.Market {
	return market.Market{
		Symbol:        "BTC",
		TickSize:      d("0.1"),
		LotSize:       d("0.001"),
		MinQty:        d("0.001"),
		MinNotional:   d("10"),
		MaxLeverage:   50,
		PriceDecimals: 1,
		SizeDecimals:  3,
	}
}

func coid(seq uint8) order.ClientOrderID {
	return order.MustEncode(order.Ref{Slot: 0, Epoch: 1, Cell: 0, Purpose: order.PurposeOpen, Seq: seq})
}

func TestPlaceRestingAndTrade(t *testing.T) {
	ex := New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))

	ctx := context.Background()
	_, err := ex.PlaceOrders(ctx, []exchange.PlaceRequest{{
		Symbol:        "BTC",
		ClientOrderID: coid(1),
		Side:          order.Buy,
		Type:          order.Limit,
		Price:         d("125"),
		Quantity:      d("1"),
		TIF:           order.PostOnly,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ex.Resting()); n != 1 {
		t.Fatalf("resting = %d, want 1", n)
	}

	ex.Trade(d("125"))
	if n := len(ex.Resting()); n != 0 {
		t.Fatalf("after trade resting = %d, want 0", n)
	}
	pos, _ := ex.Position(ctx, "BTC")
	if !pos.Size.Equal(d("1")) {
		t.Fatalf("position = %s, want 1", pos.Size)
	}
}

func TestPostOnlyCrossingIsRejected(t *testing.T) {
	ex := New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := ex.Subscribe(ctx, "BTC")
	if err != nil {
		t.Fatal(err)
	}
	// 丢掉订阅时的初始 ticker。
	<-ch

	res, err := ex.PlaceOrders(ctx, []exchange.PlaceRequest{{
		Symbol:        "BTC",
		ClientOrderID: coid(2),
		Side:          order.Buy,
		Type:          order.Limit,
		Price:         d("150.1"),
		Quantity:      d("1"),
		TIF:           order.PostOnly,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res[0].Err != nil {
		t.Fatalf("submit should succeed, got %v", res[0].Err)
	}
	ev := <-ch
	if ev.Order == nil || ev.Order.State != order.StateRejected {
		t.Fatalf("expected rejected order event, got %+v", ev.Order)
	}
	if len(ex.Resting()) != 0 {
		t.Fatal("rejected order must not rest")
	}
}

func TestMarketFillsImmediately(t *testing.T) {
	ex := New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))

	ctx := context.Background()
	_, err := ex.PlaceOrders(ctx, []exchange.PlaceRequest{{
		Symbol:        "BTC",
		ClientOrderID: coid(3),
		Side:          order.Buy,
		Type:          order.Market,
		Price:         d("151"),
		Quantity:      d("2"),
		TIF:           order.IOC,
	}})
	if err != nil {
		t.Fatal(err)
	}
	pos, _ := ex.Position(ctx, "BTC")
	if !pos.Size.Equal(d("2")) {
		t.Fatalf("position = %s, want 2", pos.Size)
	}
}

func TestModifyRestingOrder(t *testing.T) {
	ex := New(testMarket())
	ex.SetBook(d("99"), d("101"))
	ctx := context.Background()
	id := coid(4)
	_, err := ex.PlaceOrders(ctx, []exchange.PlaceRequest{{
		Symbol: "BTC", ClientOrderID: id, Side: order.Sell,
		Type: order.Limit, Price: d("110"), Quantity: d("1"), TIF: order.PostOnly,
	}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := ex.ModifyOrders(ctx, []exchange.ModifyRequest{{
		Symbol: "BTC", ClientOrderID: id, Price: d("108"), Quantity: d("2"),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 || res[0].Err != nil {
		t.Fatalf("modify = %+v", res)
	}
	resting := ex.Resting()
	if len(resting) != 1 || !resting[0].Price.Equal(d("108")) || !resting[0].Quantity.Equal(d("2")) {
		t.Fatalf("resting = %+v", resting)
	}
}

func TestCancelAll(t *testing.T) {
	ex := New(testMarket())
	ex.SetBook(d("99"), d("101"))
	ctx := context.Background()
	_, _ = ex.PlaceOrders(ctx, []exchange.PlaceRequest{{
		Symbol: "BTC", ClientOrderID: coid(1), Side: order.Buy,
		Type: order.Limit, Price: d("90"), Quantity: d("1"), TIF: order.PostOnly,
	}, {
		Symbol: "BTC", ClientOrderID: coid(2), Side: order.Sell,
		Type: order.Limit, Price: d("110"), Quantity: d("1"), TIF: order.PostOnly,
	}})
	if err := ex.CancelAll(ctx, "BTC"); err != nil {
		t.Fatal(err)
	}
	if n := len(ex.Resting()); n != 0 {
		t.Fatalf("resting = %d after cancel-all", n)
	}
}
