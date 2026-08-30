package main

import (
	"encoding/json"
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseSodexOrderUpdatesArray(t *testing.T) {
	raw := json.RawMessage(`[
		{"c":"arb-1","i":58119,"z":"0","L":""},
		{"c":"arb-2","i":58117,"z":"0.00043","L":"102514"}
	]`)
	got := parseSodexOrderUpdates(raw)
	if len(got) != 2 {
		t.Fatalf("len %d", len(got))
	}
	if got[1].ClOrdID != "arb-2" || got[1].FilledQty != "0.00043" {
		t.Fatalf("%+v", got[1])
	}
}

func TestParseSodexOrderUpdatesObject(t *testing.T) {
	raw := json.RawMessage(`{"c":"arb-1","i":9,"z":"0.01","L":"100"}`)
	got := parseSodexOrderUpdates(raw)
	if len(got) != 1 || got[0].ClOrdID != "arb-1" {
		t.Fatalf("%+v", got)
	}
}

func TestSodexAccountSubIncludesAccountID(t *testing.T) {
	p := sodexAccountSubParams("accountOrderUpdate", "0xabc", 6222)
	if p["user"] != "0xabc" || p["channel"] != "accountOrderUpdate" {
		t.Fatalf("%+v", p)
	}
	id, ok := p["accountID"].(uint64)
	if !ok || id != 6222 {
		t.Fatalf("accountID %+v", p["accountID"])
	}
	p0 := sodexAccountSubParams("accountTrade", "0xabc", 0)
	if _, has := p0["accountID"]; has {
		t.Fatal("omit accountID when 0 so primary is used")
	}
}

func TestSanitizeSodexClOrdID(t *testing.T) {
	got := sanitizeSodexClOrdID("arb-1756560000000-1")
	if got != "arb-1756560000000-1" {
		t.Fatalf("%s", got)
	}
	long := "arb-1756560000000-18446744073709551615"
	got = sanitizeSodexClOrdID(long)
	if len(got) > 36 {
		t.Fatalf("len %d %s", len(got), got)
	}
	if sanitizeSodexClOrdID("bad id!") != "badid" {
		t.Fatalf("charset %s", sanitizeSodexClOrdID("bad id!"))
	}
}

func TestUnmarshalSodexOrdersNested(t *testing.T) {
	raw := json.RawMessage(`{"blockTime":1,"orders":[{"orderID":12,"clOrdID":"arb-1","executedQty":"0.001"}]}`)
	orders, err := unmarshalSodexOrders(raw)
	if err != nil || len(orders) != 1 || orders[0].OrderID != 12 {
		t.Fatalf("nested %+v %v", orders, err)
	}
}

func TestSodexFillNotePicksMax(t *testing.T) {
	n := sodexFillNote{
		fromOrder: decimal.RequireFromString("0.001"),
		fromFills: decimal.RequireFromString("0.0004"),
	}
	if !n.qty().Equal(decimal.RequireFromString("0.001")) {
		t.Fatalf("%s", n.qty())
	}
}
