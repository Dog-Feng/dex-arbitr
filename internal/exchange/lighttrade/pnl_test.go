package lighttrade

import (
	"encoding/json"
	"testing"

	"dex-grid/internal/domain/order"

	"github.com/shopspring/decimal"
)

func TestFillForAccountUsesVenuePnLAlreadyNetOfFees(t *testing.T) {
	raw := `{
		"trade_id": 99,
		"size": "0.0002",
		"price": "64225.4",
		"ask_account_id": 7,
		"bid_account_id": 8,
		"ask_client_id": 1001,
		"bid_client_id": 1002,
		"ask_account_pnl": "0.0123",
		"bid_account_pnl": "0",
		"is_maker_ask": true,
		"maker_fee": "0.001",
		"taker_fee": "0.002",
		"timestamp": 1700000000000
	}`
	var tr APITrade
	if err := json.Unmarshal([]byte(raw), &tr); err != nil {
		t.Fatal(err)
	}

	ask, ok := FillForAccount(tr, 7)
	if !ok {
		t.Fatal("ask account should match")
	}
	if ask.Side != order.Sell || ask.ClientOrderID != 1001 {
		t.Fatalf("ask fill = %+v", ask)
	}
	if !ask.RealizedPnL.Equal(decimal.RequireFromString("0.0123")) {
		t.Fatalf("ask realized = %s, want venue ask_account_pnl", ask.RealizedPnL)
	}
	if !ask.IsMaker || !ask.Fee.Equal(decimal.RequireFromString("0.001")) {
		t.Fatalf("ask maker/fee = %v %s", ask.IsMaker, ask.Fee)
	}

	bid, ok := FillForAccount(tr, 8)
	if !ok {
		t.Fatal("bid account should match")
	}
	if bid.Side != order.Buy || bid.ClientOrderID != 1002 {
		t.Fatalf("bid fill = %+v", bid)
	}
	if !bid.RealizedPnL.IsZero() {
		t.Fatalf("bid realized = %s, want 0 (open)", bid.RealizedPnL)
	}
	if bid.IsMaker {
		t.Fatal("bid should be taker when is_maker_ask")
	}

	if _, ok := FillForAccount(tr, 9); ok {
		t.Fatal("foreign account must not match")
	}
}

func TestFillForAccountJSONNumberFees(t *testing.T) {
	raw := `{"trade_id":1,"size":"1","price":"10","ask_account_id":1,"bid_account_id":2,
		"ask_client_id":3,"bid_client_id":4,"ask_account_pnl":"-0.5","bid_account_pnl":"0",
		"is_maker_ask":false,"maker_fee":0,"taker_fee":0}`
	var tr APITrade
	if err := json.Unmarshal([]byte(raw), &tr); err != nil {
		t.Fatal(err)
	}
	ask, ok := FillForAccount(tr, 1)
	if !ok {
		t.Fatal("expected ask fill")
	}
	if !ask.RealizedPnL.Equal(decimal.RequireFromString("-0.5")) {
		t.Fatalf("realized = %s", ask.RealizedPnL)
	}
}
