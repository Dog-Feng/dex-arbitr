package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
)

// TestLiveSignatureAcceptedByExchange is a manual, opt-in probe: it signs a
// deliberately tiny order with a throwaway agent key and posts it to the
// mainnet exchange endpoint. The order is never valid for trading, but the
// server's rejection must not be "Invalid signature": any agent/permission
// error proves the EIP-712 encoding and signature scheme are correct.
// Run with: HL_LIVE_PROBE=1 go test ./internal/exchange/hyperliquid/ -run LiveSignature -v
func TestLiveSignatureAcceptedByExchange(t *testing.T) {
	if os.Getenv("HL_LIVE_PROBE") != "1" {
		t.Skip("set HL_LIVE_PROBE=1 to run the live signature probe")
	}
	keyHex := os.Getenv("HL_PROBE_KEY")
	if keyHex == "" {
		keyHex = "0x1111111111111111111111111111111111111111111111111111111111111111"
	}
	s, err := newSigner(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	const asset = 200002 // io:SNDK
	c := "0x" + strings.Repeat("0", 31) + "1"
	nonce := uint64(time.Now().UnixMilli())
	sig, err := s.signOrder(asset, true, false, "1.0", "0.01", c, nonce, 0)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(map[string]any{
		"action": map[string]any{
			"type": "order",
			"orders": []map[string]any{{
				"a": asset, "b": true, "p": "1.0", "s": "0.01", "r": false,
				"t": map[string]any{"limit": map[string]any{"tif": "Ioc"}}, "c": c,
			}},
			"grouping": "na",
		},
		"nonce":     nonce,
		"signature": sig,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, defaultExchangeURL, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("http %d: %s", resp.StatusCode, raw)
	var out struct {
		Status   string          `json:"status"`
		Response json.RawMessage `json:"response"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("invalid response JSON: %v", err)
	}
	responseText := string(out.Response)
	if strings.Contains(strings.ToLower(responseText), "invalid signature") {
		t.Fatalf("exchange rejected the signature itself: %s", out.Response)
	}
	if out.Status != "err" {
		t.Fatalf("probe unexpectedly succeeded or returned an unknown shape: %s", raw)
	}
	t.Logf("signature verified by exchange; rejected for: %s", out.Response)
}

// TestLiveInstrumentsAndBook is an opt-in end-to-end check against mainnet:
// instrument discovery (meta + perpDexs) and a live l2Book subscription for
// io:SNDK.
// Run with: HL_LIVE_PROBE=1 go test ./internal/exchange/hyperliquid/ -run LiveInstruments -v
func TestLiveInstrumentsAndBook(t *testing.T) {
	if os.Getenv("HL_LIVE_PROBE") != "1" {
		t.Skip("set HL_LIVE_PROBE=1 to run the live instruments/book probe")
	}
	v, err := New("entropy", "entropy", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	insts, err := v.Instruments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(insts) == 0 {
		t.Fatal("no instruments")
	}
	var sndk *domain.Instrument
	for i := range insts {
		if insts[i].Symbol == "SNDK" {
			sndk = &insts[i]
		}
	}
	if sndk == nil || sndk.MarketID != 200002 {
		t.Fatalf("io:SNDK not mapped to asset 200002: %+v", sndk)
	}
	ch, err := v.SubscribeBook(ctx, *sndk)
	if err != nil {
		t.Fatal(err)
	}
	// The venue publishes an explicit invalid book on reconnect; drain until
	// the first real snapshot arrives.
	for {
		select {
		case b := <-ch:
			if b.Valid() {
				t.Logf("book mid %.2f, bids %d asks %d", b.Mid(), len(b.Bids), len(b.Asks))
				return
			}
		case <-ctx.Done():
			t.Fatalf("no valid book within timeout: %v", ctx.Err())
		}
	}
}

// TestLiveAccountSnapshot is an opt-in check of the real configured account:
// venue construction, credentials presence and a clearinghouseState read.
// Run with: HL_LIVE_PROBE=1 HL_ACCOUNT=<0x..> HL_KEY=<agent key>
func TestLiveAccountSnapshot(t *testing.T) {
	if os.Getenv("HL_LIVE_PROBE") != "1" {
		t.Skip("set HL_LIVE_PROBE=1 to run the live account probe")
	}
	acct, key := os.Getenv("HL_ACCOUNT"), os.Getenv("HL_KEY")
	if acct == "" || key == "" {
		t.Fatal("HL_ACCOUNT and HL_KEY must be set")
	}
	v, err := New("entropy", "entropy", map[string]string{"account": acct, "private_key": key})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = v.Close() }()
	if !v.CanTrade() {
		t.Fatal("venue is not tradeable with the provided credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	snap, err := v.AccountSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("positions: %d, cross margin: %v", len(snap.Positions), snap.CrossMargin != nil)
	for _, p := range snap.Positions {
		t.Logf("  %s qty %.8f entry %.2f liq %.2f mode %d", p.Symbol, p.Qty, p.AvgEntry, p.LiquidationPrice, p.MarginMode)
	}
}
