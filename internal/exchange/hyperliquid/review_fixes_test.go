package hyperliquid

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// A send that times out returns before any oid is known. The expiry must
// already be recorded so a later unknownOid becomes a definitive rejection
// once the window has elapsed, instead of an unknown that never resolves.
func TestSubmitTimeoutRecordsExpiryForUnknownOid(t *testing.T) {
	var sentExpiry atomic.Uint64
	v := tradeVenue(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Type         string `json:"type"`
			ExpiresAfter uint64 `json:"expiresAfter"`
		}
		_ = json.Unmarshal(body, &req)
		if req.Type == "orderStatus" {
			_, _ = io.WriteString(w, `{"status":"unknownOid"}`)
			return
		}
		sentExpiry.Store(req.ExpiresAfter)
		time.Sleep(200 * time.Millisecond)
		_, _ = io.WriteString(w, `{"status":"ok"}`)
	})
	v.rest.sendClient.Timeout = 20 * time.Millisecond
	inst := domain.Instrument{MarketID: 200002, SizeDecimals: 4, PriceDecimals: 2}
	_, err := v.Submit(context.Background(), domain.OrderReq{
		ClientID: 7, Inst: inst, Side: domain.Buy, Qty: 0.01, Price: 100, Type: domain.Limit,
	})
	if err == nil {
		t.Fatal("timed-out send must report an unknown outcome")
	}
	v.ordersMu.Lock()
	exp, recorded := v.orderExpires[7]
	_, known := v.knownCIDs[7]
	v.ordersMu.Unlock()
	if !recorded || !known || exp == 0 || uint64(exp) != sentExpiry.Load() {
		t.Fatalf("expiry not recorded before send: recorded=%v known=%v exp=%d sent=%d", recorded, known, exp, sentExpiry.Load())
	}
	// Inside the window an unknown oid may still execute.
	u, err := v.QueryOrder(context.Background(), inst, 7)
	if err != nil {
		t.Fatal(err)
	}
	if u.Status == domain.StatusRejected {
		t.Fatalf("unknown oid inside the expiry window must not be rejected: %+v", u)
	}
	// Once the recorded window has elapsed the same answer is definitive.
	v.ordersMu.Lock()
	v.orderExpires[7] = time.Now().Add(-time.Minute).UnixMilli()
	v.ordersMu.Unlock()
	u, err = v.QueryOrder(context.Background(), inst, 7)
	if err != nil {
		t.Fatal(err)
	}
	if u.Status != domain.StatusRejected {
		t.Fatalf("expired unknown oid: %+v", u)
	}
}

func readOnlyVenue(t *testing.T) *Venue {
	t.Helper()
	v, err := NewWithConfig(Config{
		Name: "entropy", Dex: "io", InfoURL: "https://example.invalid/info",
		ExchangeURL: "https://example.invalid/exchange", WSURL: "wss://example.invalid/ws", BookDepth: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = v.Close() })
	return v
}

// A filled order whose trades have not arrived is terminal but unpublished.
// Cleanup must keep it so the later user fill still resolves the order.
func TestCleanupKeepsTerminalOrderAwaitingFills(t *testing.T) {
	v := readOnlyVenue(t) // no account: the REST backfill goroutine is off
	ch := make(chan domain.OrderUpdate, 16)
	v.orderSubs = append(v.orderSubs, ch)
	c, _ := cloid(42)
	v.onOrderUpdate([]byte(`{"channel":"orderUpdates","data":[{"order":{"coin":"io:ANTH","oid":900,"origSz":"0.01","cloid":"` + c + `"},"status":"filled","statusTimestamp":1}]}`))
	v.cleanupOrders()
	v.ordersMu.Lock()
	_, kept := v.orderLookup[900]
	v.ordersMu.Unlock()
	if !kept {
		t.Fatal("terminal order awaiting fills dropped by cleanup")
	}
	v.onUserFills([]byte(`{"channel":"userFills","data":{"user":"x","fills":[{"coin":"io:ANTH","px":"100","sz":"0.01","oid":900,"tid":5,"time":1}]}}`))
	var got domain.OrderUpdate
	for {
		select {
		case u := <-ch:
			got = u
			continue
		default:
		}
		break
	}
	if got.ClientID != 42 || got.Status != domain.StatusFilled || got.FilledQty != 0.01 {
		t.Fatalf("late fill not published after cleanup: %+v", got)
	}
	// The retention window starts at the terminal state, so the entry still
	// ages out once it is older than the window.
	v.ordersMu.Lock()
	v.finalTouched[900] = time.Now().Add(-time.Hour)
	v.ordersMu.Unlock()
	v.cleanupOrders()
	v.ordersMu.Lock()
	_, still := v.orderLookup[900]
	v.ordersMu.Unlock()
	if still {
		t.Fatal("old terminal order not cleaned")
	}
}

// Entries that never become terminal, such as a fill state rebuilt by a REST
// query after its oid was cleaned, age out instead of leaking forever.
func TestCleanupAgesOutNonTerminalEntries(t *testing.T) {
	v := readOnlyVenue(t)
	v.ordersMu.Lock()
	v.fillState(901).add(1, 0.01, 100, 0)
	v.orderLookup[902] = 5
	v.ordersMu.Unlock()
	v.cleanupOrders()
	v.ordersMu.Lock()
	_, fs := v.fillStates[901]
	_, lk := v.orderLookup[902]
	v.staleSeen[901] = time.Now().Add(-2 * time.Hour)
	v.staleSeen[902] = time.Now().Add(-2 * time.Hour)
	v.ordersMu.Unlock()
	if !fs || !lk {
		t.Fatal("fresh non-terminal entries must survive their first cleanup")
	}
	v.cleanupOrders()
	v.ordersMu.Lock()
	defer v.ordersMu.Unlock()
	if _, ok := v.fillStates[901]; ok {
		t.Fatal("orphan fill state leaked")
	}
	if _, ok := v.orderLookup[902]; ok {
		t.Fatal("non-terminal lookup leaked")
	}
	if len(v.staleSeen) != 0 {
		t.Fatalf("stale markers leaked: %v", v.staleSeen)
	}
}

func TestSigningNetwork(t *testing.T) {
	cases := []struct {
		network, url string
		mainnet      bool
	}{
		{"", "https://api.hyperliquid.xyz/exchange", true},
		{"", "https://api.hyperliquid.xyz/exchange/", true},
		{"", "https://hl-proxy.internal:8443/exchange", true},
		{"", "https://api.hyperliquid-testnet.xyz/exchange", false},
		{"testnet", "https://api.hyperliquid.xyz/exchange", false},
		{"MAINNET", "https://api.hyperliquid-testnet.xyz/exchange", true},
	}
	for _, c := range cases {
		v, err := NewWithConfig(Config{
			Name: "e", Dex: "io", InfoURL: "https://api.hyperliquid.xyz/info", ExchangeURL: c.url,
			WSURL: "wss://api.hyperliquid.xyz/ws", BookDepth: 5, Network: c.network,
			Account:  "0xab000000000000000000000000000000000000cd",
			AgentKey: "0x1111111111111111111111111111111111111111111111111111111111111111",
		})
		if err != nil {
			t.Fatalf("%q %s: %v", c.network, c.url, err)
		}
		if v.signer.mainnet != c.mainnet {
			t.Fatalf("%q %s: mainnet=%v want %v", c.network, c.url, v.signer.mainnet, c.mainnet)
		}
		_ = v.Close()
	}
	_, err := New("e", "entropy", map[string]string{"network": "devnet"})
	if err == nil || !strings.Contains(err.Error(), "network") {
		t.Fatalf("invalid network accepted: %v", err)
	}
}
