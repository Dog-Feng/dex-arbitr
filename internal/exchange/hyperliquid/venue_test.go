package hyperliquid

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// TestInstrumentsMapping serves the io meta and perpDexs fixtures through a
// stub info server and verifies asset ids, precision and fees.
func TestInstrumentsMapping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		switch req["type"] {
		case "perpDexs":
			_, _ = w.Write([]byte(`[null,{"name":"xyz"},null,null,null,null,null,null,null,null,{"name":"io"}]`))
		case "meta":
			if req["dex"] != "io" {
				t.Errorf("dex = %v", req["dex"])
			}
			_, _ = w.Write([]byte(`{"universe":[
				{"szDecimals":3,"name":"io:OAI","maxLeverage":6,"onlyIsolated":true,"marginMode":"noCross","growthMode":"enabled","deployerFeeScale":"1.0"},
				{"szDecimals":3,"name":"io:ANTH","maxLeverage":6,"onlyIsolated":true,"marginMode":"strictIsolated","growthMode":"enabled","deployerFeeScale":"1.0"},
				{"szDecimals":4,"name":"io:SNDK","maxLeverage":15,"onlyIsolated":true,"marginMode":"strictIsolated","growthMode":"enabled","deployerFeeScale":"1.0"},
				{"szDecimals":2,"name":"io:SBE","maxLeverage":6,"onlyIsolated":true,"isDelisted":true,"marginMode":"strictIsolated","deployerFeeScale":"1.0"}
			]}`))
		default:
			t.Errorf("unexpected info request: %v", req["type"])
		}
	}))
	defer srv.Close()
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: srv.URL, ExchangeURL: srv.URL,
		WSURL: "wss://example.invalid/ws", BookDepth: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	insts, err := v.Instruments(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(insts) != 3 {
		t.Fatalf("expected 3 instruments, got %d", len(insts))
	}
	want := map[string]domain.Instrument{
		"OAI":  {Symbol: "OAI", MarketID: 200000, SizeDecimals: 3, PriceDecimals: 3, TakerFee: 0.00009, MakerFee: 0.00003},
		"ANTH": {Symbol: "ANTH", MarketID: 200001, SizeDecimals: 3, PriceDecimals: 3, TakerFee: 0.00009, MakerFee: 0.00003},
		"SNDK": {Symbol: "SNDK", MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2, TakerFee: 0.00009, MakerFee: 0.00003},
	}
	for _, inst := range insts {
		exp, ok := want[inst.Symbol]
		if !ok {
			t.Fatalf("unexpected instrument %+v", inst)
		}
		if inst.MarketID != exp.MarketID || inst.SizeDecimals != exp.SizeDecimals ||
			inst.PriceDecimals != exp.PriceDecimals || math.Abs(inst.TakerFee-exp.TakerFee) > 1e-12 {
			t.Fatalf("instrument mismatch: %+v want %+v", inst, exp)
		}
	}
}

// TestOnBook publishes a full book snapshot into the venue's book state.
func TestOnBook(t *testing.T) {
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws", BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	v.assetsMu.Lock()
	v.assets["io:SNDK"] = 200002
	v.assetsMu.Unlock()
	v.books[200002] = &bookState{marketID: 200002, depth: 5}
	raw := []byte(`{"channel":"l2Book","data":{"coin":"io:SNDK","time":1754450974231,"levels":[
		[{"px":"1355.8","sz":"2.0"},{"px":"1355.7","sz":"1.5"}],
		[{"px":"1356.1","sz":"5.1"}]
	]}}`)
	v.onBook(raw)
	bs := v.books[200002]
	if !bs.synced || len(bs.bids) != 2 || len(bs.asks) != 1 {
		t.Fatalf("book state: %+v", bs)
	}
	if bs.bids[0].Price != 1355.8 || bs.asks[0].Price != 1356.1 || bs.seq != 1754450974231 {
		t.Fatalf("book levels: %+v", bs)
	}
	if bs.updated.IsZero() || time.Since(bs.updated) > time.Second {
		t.Fatalf("book timestamp not set")
	}
}

// TestOnBbo verifies the top-of-book feed merges into a synced book state
// without disturbing deeper levels.
func TestOnBbo(t *testing.T) {
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws", BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	v.assetsMu.Lock()
	v.assets["io:ANTH"] = 200001
	v.assetsMu.Unlock()
	// Before the first full snapshot the book is not synced: bbo must be
	// ignored instead of seeding a partial book.
	unsynced := &bookState{marketID: 200001, depth: 5}
	v.books[200001] = unsynced
	v.onBbo([]byte(`{"channel":"bbo","data":{"coin":"io:ANTH","time":1754450974000,"bbo":[{"px":"1963.35","sz":"2.0"},{"px":"1963.45","sz":"0.002"}]}}`))
	if unsynced.synced || len(unsynced.bids) != 0 || len(unsynced.asks) != 0 {
		t.Fatalf("bbo seeded an unsynced book: %+v", unsynced)
	}
	bs := &bookState{
		marketID: 200001, depth: 5, synced: true,
		bids:    []domain.Level{{Price: 1963.3, Size: 1.336}, {Price: 1963.2, Size: 0.5}},
		asks:    []domain.Level{{Price: 1963.4, Size: 0.001}},
		updated: time.Now().Add(-2 * time.Second),
	}
	v.books[200001] = bs
	v.onBbo([]byte(`{"channel":"bbo","data":{"coin":"io:ANTH","time":1754450974231,"bbo":[{"px":"1963.35","sz":"2.0","n":3},{"px":"1963.45","sz":"0.002","n":1}]}}`))
	if bs.bids[0].Price != 1963.35 || bs.bids[0].Size != 2.0 || len(bs.bids) != 2 || bs.bids[1].Price != 1963.2 {
		t.Fatalf("bids after bbo: %+v", bs.bids)
	}
	if bs.asks[0].Price != 1963.45 || bs.asks[0].Size != 0.002 || len(bs.asks) != 1 {
		t.Fatalf("asks after bbo: %+v", bs.asks)
	}
	if bs.seq != 1754450974231 || time.Since(bs.updated) > time.Second {
		t.Fatalf("bbo did not refresh book seq/time")
	}
	// A crossed bbo is discarded, leaving the previous book intact.
	before := bs.bids[0]
	v.onBbo([]byte(`{"channel":"bbo","data":{"coin":"io:ANTH","time":1754450974300,"bbo":[{"px":"1964.0","sz":"1.0"},{"px":"1963.0","sz":"1.0"}]}}`))
	if bs.bids[0] != before {
		t.Fatalf("crossed bbo applied: %+v", bs.bids[0])
	}
	// A null side only updates the other side.
	v.onBbo([]byte(`{"channel":"bbo","data":{"coin":"io:ANTH","time":1754450974400,"bbo":[null,{"px":"1963.40","sz":"0.01"}]}}`))
	if bs.asks[0].Price != 1963.40 || bs.bids[0] != before {
		t.Fatalf("null-side bbo mishandled: bids %+v asks %+v", bs.bids[0], bs.asks[0])
	}
}

// TestOrderWireTightening verifies the native-precision rounding never widens
// the worst-acceptable price.
func TestOrderWireTightening(t *testing.T) {
	inst := domain.Instrument{MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2}
	_, c, price, size, _, err := orderWire(domain.OrderReq{
		ClientID: 7, Inst: inst, Side: domain.Buy, Qty: 0.123456, Price: 1356.129, ReduceOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if price != "1356.1" {
		t.Fatalf("buy price %q not tightened down", price)
	}
	if size != "0.1234" {
		t.Fatalf("size %q not floored", size)
	}
	_, c2, price2, _, _, err := orderWire(domain.OrderReq{
		ClientID: 8, Inst: inst, Side: domain.Sell, Qty: 0.1, Price: 1356.121,
	})
	if err != nil {
		t.Fatal(err)
	}
	if price2 != "1356.2" {
		t.Fatalf("sell price %q not tightened up", price2)
	}
	if c != c2 && c == "" {
		t.Fatalf("cloids missing")
	}
}

// TestSigFigTighten verifies the 5-significant-figure rule never widens a
// worst-acceptable price.
func TestSigFigTighten(t *testing.T) {
	cases := []struct {
		in   float64
		dec  int
		buy  bool
		want float64
	}{
		{1356.129, 2, true, 1356.1},  // 6 sig figs -> 5, floored
		{1356.121, 2, false, 1356.2}, // ceiled
		{135.61, 2, true, 135.61},    // exactly 5 sig figs, no change
		{12.34, 2, true, 12.34},      // few sig figs: grid finer than precision
		{0.0012345, 6, true, 0.001234},
		{100000.0, 0, false, 100000.0},
	}
	for _, tc := range cases {
		if got := sigFigTighten(tc.in, tc.dec, tc.buy); got != tc.want {
			t.Fatalf("sigFigTighten(%v,%d,buy=%v) = %v, want %v", tc.in, tc.dec, tc.buy, got, tc.want)
		}
	}
}

// TestPartialIOC pins the normalization of Hyperliquid partial IOC fills: a
// FILLED terminal below the requested size becomes Canceled (partial) so the
// executor's fill verification sees a consistent terminal state.
func TestPartialIOC(t *testing.T) {
	cases := []struct {
		status domain.OrderStatus
		filled float64
		req    float64
		want   domain.OrderStatus
	}{
		{domain.StatusFilled, 0.01, 0.01, domain.StatusFilled},
		{domain.StatusFilled, 0.003, 0.01, domain.StatusCanceled},
		{domain.StatusFilled, 0, 0.01, domain.StatusFilled}, // no fill facts: stay unresolved upstream
		{domain.StatusCanceled, 0.003, 0.01, domain.StatusCanceled},
		{domain.StatusRejected, 0, 0.01, domain.StatusRejected},
		{domain.StatusFilled, 0.003, 0, domain.StatusFilled}, // unknown request: leave as-is
	}
	for _, tc := range cases {
		if got := partialIOC(tc.status, tc.filled, tc.req); got != tc.want {
			t.Fatalf("partialIOC(%v, %v, %v) = %v, want %v", tc.status, tc.filled, tc.req, got, tc.want)
		}
	}
}

// TestQueryOrderTerminalFillFacts pins the QueryOrder contract: terminal
// states are only reported together with their fill facts.
func TestQueryOrderTerminalFillFacts(t *testing.T) {
	inst := domain.Instrument{MarketID: 200001, SizeDecimals: 3, PriceDecimals: 3}
	const oid = "77"

	t.Run("canceled with fills", func(t *testing.T) {
		v := queryStubVenue(t, map[string]string{
			"orderStatus": `{"status":"order","order":{"order":{"coin":"io:ANTH","oid":` + oid + `},"status":"canceled","statusTimestamp":1000}}`,
			"fills":       `[{"coin":"io:ANTH","px":"1958.4","sz":"0.004","oid":` + oid + `},{"coin":"io:ANTH","px":"1958.4","sz":"0.006","oid":` + oid + `}]`,
		})
		u, err := v.QueryOrder(context.Background(), inst, 7)
		if err != nil {
			t.Fatal(err)
		}
		if u.Status != domain.StatusCanceled || u.FilledQty != 0.01 {
			t.Fatalf("canceled update: %+v", u)
		}
	})

	t.Run("filled without fills stays unresolved", func(t *testing.T) {
		v := queryStubVenue(t, map[string]string{
			"orderStatus": `{"status":"order","order":{"order":{"coin":"io:ANTH","oid":` + oid + `},"status":"filled","statusTimestamp":1000}}`,
			"fills":       `[]`,
		})
		u, err := v.QueryOrder(context.Background(), inst, 7)
		if err != nil {
			t.Fatal(err)
		}
		if u.Status != domain.StatusUnknown {
			t.Fatalf("filled without fills must stay unresolved: %+v", u)
		}
	})

	t.Run("rejected is terminal with zero fills", func(t *testing.T) {
		v := queryStubVenue(t, map[string]string{
			"orderStatus": `{"status":"order","order":{"order":{"coin":"io:ANTH","oid":` + oid + `},"status":"rejected","statusTimestamp":1000}}`,
		})
		u, err := v.QueryOrder(context.Background(), inst, 7)
		if err != nil {
			t.Fatal(err)
		}
		if u.Status != domain.StatusRejected || u.FilledQty != 0 {
			t.Fatalf("rejected update: %+v", u)
		}
	})
}

// queryStubVenue serves the orderStatus and userFillsByTime info requests a
// QueryOrder performs.
func queryStubVenue(t *testing.T, responses map[string]string) *Venue {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
			return
		}
		switch req["type"] {
		case "orderStatus":
			_, _ = w.Write([]byte(responses["orderStatus"]))
		case "userFillsByTime":
			if responses["fills"] != "" {
				_, _ = w.Write([]byte(responses["fills"]))
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
		default:
			t.Errorf("unexpected info request: %v", req["type"])
		}
	}))
	t.Cleanup(srv.Close)
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: srv.URL, ExchangeURL: srv.URL,
		WSURL: "wss://example.invalid/ws", Account: testAccount, BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

const testAccount = "0xc01303fffd03b36666656b809808635d011a9197"
