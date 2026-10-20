package hyperliquid

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
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

// TestFillStateDedupAndEffective verifies tid-based deduplication and the
// synchronous-total floor from /exchange.
func TestFillStateDedupAndEffective(t *testing.T) {
	s := newFillState()
	if !s.add(1, 0.01, 100, 0.01) {
		t.Fatal("first add should be new")
	}
	if s.add(1, 0.01, 100, 0.01) {
		t.Fatal("same tid must deduplicate")
	}
	s.syncQty, s.syncQuote = 0.02, 2.0
	// Below the sync total only part of the trades (and fees) are known, so
	// the fee is reported as unknown rather than understated.
	if q, qu, fee := s.effective(); q != 0.02 || qu != 2.0 || fee != 0 {
		t.Fatalf("effective below sync total: %v/%v/%v", q, qu, fee)
	}
	if !s.add(2, 0.01, 100, 0) {
		t.Fatal("new tid must merge")
	}
	if q, qu, fee := s.effective(); q != 0.02 || qu != 2.0 || fee != 0.01 {
		t.Fatalf("effective past sync total: %v/%v/%v", q, qu, fee)
	}
}

// TestOnUserFillsDedupAndSnapshot verifies replay/snapshot trades are merged
// but never double counted or re-published.
func TestOnUserFillsDedupAndSnapshot(t *testing.T) {
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws", BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	ch := make(chan domain.OrderUpdate, 64)
	v.ordersMu.Lock()
	v.orderSubs = append(v.orderSubs, ch)
	v.orderLookup[1] = 7
	v.orderOrig[1] = 0.01
	v.knownCIDs[7] = time.Now()
	v.ordersMu.Unlock()
	feed := func(tid int64, isSnapshot bool) {
		raw := []byte(`{"channel":"userFills","data":{"user":"0x00","isSnapshot":` +
			`false` +
			`,"fills":[{"coin":"io:SNDK","px":"100","sz":"0.004","oid":1,"tid":` +
			strconv.FormatInt(tid, 10) + `}]}}`)
		if isSnapshot {
			raw = []byte(`{"channel":"userFills","data":{"user":"0x00","isSnapshot":true,"fills":[{"coin":"io:SNDK","px":"100","sz":"0.004","oid":1,"tid":` +
				strconv.FormatInt(tid, 10) + `}]}}`)
		}
		v.onUserFills(raw)
	}
	feed(1, false)
	u := <-ch
	if u.FilledQty != 0.004 {
		t.Fatalf("first fill published %v", u.FilledQty)
	}
	feed(1, false) // duplicate delivery: nothing new
	feed(1, true)  // snapshot replay: nothing new
	select {
	case u := <-ch:
		t.Fatalf("duplicate fill re-published: %+v", u)
	default:
	}
	feed(2, false)
	u = <-ch
	if u.FilledQty != 0.008 {
		t.Fatalf("cumulative fill %v, want 0.008", u.FilledQty)
	}
}

// TestOnUserFillsRegistersByCloid verifies a fill arriving before the submit
// ack is attributed through its cloid instead of being dropped.
func TestOnUserFillsRegistersByCloid(t *testing.T) {
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws", BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	ch := make(chan domain.OrderUpdate, 64)
	cl, err := cloid(7)
	if err != nil {
		t.Fatal(err)
	}
	v.ordersMu.Lock()
	v.orderSubs = append(v.orderSubs, ch)
	v.knownCIDs[7] = time.Now()
	v.ordersMu.Unlock()
	v.onUserFills([]byte(`{"channel":"userFills","data":{"user":"0x00","fills":[{"coin":"io:SNDK","px":"100","sz":"0.004","oid":1,"tid":1,"cloid":"` + cl + `"}]}}`))
	u := <-ch
	if u.ClientID != 7 || u.FilledQty != 0.004 {
		t.Fatalf("early fill misattributed: %+v", u)
	}
}

// tradeVenue builds a venue with trading credentials and a stub exchange
// endpoint controlled by the handler.
func tradeVenue(t *testing.T, handler http.HandlerFunc) *Venue {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: srv.URL, ExchangeURL: srv.URL,
		WSURL: "wss://example.invalid/ws", BookDepth: 20,
		Account:  "0xc01303fffd03b36666656b809808635d011a9197",
		AgentKey: "0x1111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return v
}

func TestSubmitActionLevelErrorIsRejected(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"err","response":"Order has invalid price."}`)
	})
	_, err := v.Submit(context.Background(), domain.OrderReq{
		ClientID: 1, Inst: domain.Instrument{MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2},
		Side: domain.Buy, Qty: 0.01, Price: 100, Type: domain.Limit,
	})
	if !errors.Is(err, domain.ErrRejected) {
		t.Fatalf("action-level error not rejected: %v", err)
	}
}

func TestSubmitRateLimitIsRejected(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	})
	_, err := v.Submit(context.Background(), domain.OrderReq{
		ClientID: 1, Inst: domain.Instrument{MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2},
		Side: domain.Buy, Qty: 0.01, Price: 100, Type: domain.Limit,
	})
	if !errors.Is(err, domain.ErrRejected) {
		t.Fatalf("429 not rejected: %v", err)
	}
}

func TestSubmitTimeoutStaysUnknown(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"status":"ok"}`)
	})
	v.rest.sendClient.Timeout = 20 * time.Millisecond
	_, err := v.Submit(context.Background(), domain.OrderReq{
		ClientID: 1, Inst: domain.Instrument{MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2},
		Side: domain.Buy, Qty: 0.01, Price: 100, Type: domain.Limit,
	})
	if err == nil || errors.Is(err, domain.ErrRejected) {
		t.Fatalf("timeout must stay unknown: %v", err)
	}
}

func TestCancelAlreadyTerminalIsIdempotent(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"err","response":"Order was never placed, already canceled, or filled."}`)
	})
	if err := v.CancelOrder(context.Background(), domain.Instrument{MarketID: 200002}, 1); err != nil {
		t.Fatalf("idempotent cancel failed: %v", err)
	}
}

func TestQueryOrderExpiredUnknownOidRejected(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"status":"unknownOid"}`)
	})
	v.ordersMu.Lock()
	v.orderExpires[1] = time.Now().Add(-time.Minute).UnixMilli()
	v.ordersMu.Unlock()
	u, err := v.QueryOrder(context.Background(), domain.Instrument{MarketID: 200002}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if u.Status != domain.StatusRejected {
		t.Fatalf("expired unknown oid: %+v", u)
	}
}

func TestQueryOrderCanceledWithPendingFillsStaysUnknown(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		switch req["type"] {
		case "orderStatus":
			fmt.Fprint(w, `{"status":"order","order":{"order":{"coin":"io:SNDK","oid":"42","sz":"0.004","origSz":"0.01"},"status":"canceled","statusTimestamp":1754450974231}}`)
		case "userFillsByTime":
			fmt.Fprint(w, `[]`)
		default:
			t.Errorf("unexpected request: %v", req["type"])
		}
	})
	u, err := v.QueryOrder(context.Background(), domain.Instrument{MarketID: 200002}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if u.Status != domain.StatusUnknown {
		t.Fatalf("canceled with unindexed fills must stay unknown: %+v", u)
	}
}

func TestQueryOrderCanceledZeroFillTerminal(t *testing.T) {
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		switch req["type"] {
		case "orderStatus":
			fmt.Fprint(w, `{"status":"order","order":{"order":{"coin":"io:SNDK","oid":"42","sz":"0.01","origSz":"0.01"},"status":"canceled","statusTimestamp":1754450974231}}`)
		case "userFillsByTime":
			fmt.Fprint(w, `[]`)
		default:
			t.Errorf("unexpected request: %v", req["type"])
		}
	})
	u, err := v.QueryOrder(context.Background(), domain.Instrument{MarketID: 200002}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if u.Status != domain.StatusCanceled || u.FilledQty != 0 {
		t.Fatalf("zero-fill cancel must be terminal: %+v", u)
	}
}

func TestCheckKeyRejectsDerivedAddress(t *testing.T) {
	s, err := newSigner("0x1111111111111111111111111111111111111111111111111111111111111111")
	if err != nil {
		t.Fatal(err)
	}
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws",
		BookDepth: 20, Account: strings.ToLower(s.address.Hex()),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	v.signer = s
	if err := v.CheckKey(context.Background()); err == nil {
		t.Fatal("derived agent address must be rejected regardless of case")
	}
}

// TestAccountSnapshotDelistedAndSelfSufficient verifies a delisted coin does
// not fail the snapshot and that the snapshot resolves the universe itself
// when Instruments was never called.
func TestAccountSnapshotDelistedAndSelfSufficient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode: %v", err)
		}
		switch req["type"] {
		case "perpDexs":
			fmt.Fprint(w, `[null,{"name":"xyz"},null,null,null,null,null,null,null,null,{"name":"io"}]`)
		case "meta":
			fmt.Fprint(w, `{"universe":[
				{"szDecimals":4,"name":"io:SNDK","maxLeverage":15,"onlyIsolated":true,"marginMode":"strictIsolated"},
				{"szDecimals":2,"name":"io:SBE","maxLeverage":6,"onlyIsolated":true,"isDelisted":true,"marginMode":"strictIsolated"}
			]}`)
		case "clearinghouseState":
			fmt.Fprint(w, `{"assetPositions":[{"position":{"coin":"io:SBE","entryPx":"10","liquidationPx":"0","szi":"0.5","positionValue":"5","leverage":{"type":"isolated"}}}],"marginSummary":{}}`)
		default:
			t.Errorf("unexpected info request: %v", req["type"])
		}
	}))
	defer srv.Close()
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: srv.URL, ExchangeURL: srv.URL,
		WSURL: "wss://example.invalid/ws", BookDepth: 20,
		Account: "0xc01303fffd03b36666656b809808635d011a9197",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	// No Instruments call: the snapshot must bootstrap the universe itself.
	snap, err := v.AccountSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Positions) != 1 || snap.Positions[0].Symbol != "SBE" || snap.Positions[0].MarketID < 0 {
		t.Fatalf("delisted position not surfaced: %+v", snap.Positions)
	}
}

// TestSetLeverageWire verifies the updateLeverage action body and that a
// request above the market maximum is rejected locally.
func TestSetLeverageWire(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Errorf("decode: %v", err)
		}
		fmt.Fprint(w, `{"status":"ok","response":{"type":"default"}}`)
	}))
	defer srv.Close()
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: srv.URL, ExchangeURL: srv.URL,
		WSURL: "wss://example.invalid/ws", BookDepth: 20,
		Account:  "0xc01303fffd03b36666656b809808635d011a9197",
		AgentKey: "0x1111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	v.levMu.Lock()
	v.maxLev[200002] = 6
	v.levMu.Unlock()
	if err := v.SetLeverage(context.Background(), domain.Instrument{MarketID: 200002}, 7); !errors.Is(err, domain.ErrLeverageRejected) {
		t.Fatalf("above-max leverage must be rejected: %v", err)
	}
	if err := v.SetLeverage(context.Background(), domain.Instrument{MarketID: 200002}, 3); err != nil {
		t.Fatal(err)
	}
	action, ok := gotBody["action"].(map[string]any)
	if !ok {
		t.Fatalf("missing action: %+v", gotBody)
	}
	if action["type"] != "updateLeverage" || action["asset"] != float64(200002) || action["isCross"] != false || action["leverage"] != float64(3) {
		t.Fatalf("unexpected action: %+v", action)
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
		{101234.5, 1, true, 101234.0}, // integer grid for six-digit prices
		{101234.5, 1, false, 101235.0},
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
