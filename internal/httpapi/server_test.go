package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"dex-grid-arbitr/internal/config"
	"dex-grid-arbitr/internal/exchange"
	"dex-grid-arbitr/internal/persistence"
	"dex-grid-arbitr/internal/runtime"
)

func TestPaperHTTPCompatibility(t *testing.T) {
	store, err := persistence.OpenEventStore(filepath.Join(t.TempDir(), "events.db"))
	if err != nil {
		t.Fatal(err)
	}
	rt := runtime.New(config.Settings{Mode: "paper", LighterMarketIndex: 0}, exchange.NewPaper(), store)
	if err := rt.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	server := httptest.NewServer(NewServer(rt, config.Settings{Mode: "paper"}).Handler())
	defer server.Close()

	response, err := http.Get(server.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status: %d", response.StatusCode)
	}
	_ = response.Body.Close()

	body := `{"market_index":0,"symbol":"BTC-USD","initial_price":"100"}`
	response, err = http.Post(server.URL+"/api/strategy/start", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("strategy start status: %d body=%s", response.StatusCode, data)
	}
	_ = response.Body.Close()

	response, err = http.Get(server.URL + "/api/strategy")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var payload map[string]any
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["status"] != "running" {
		t.Fatalf("unexpected strategy response: %+v", payload)
	}
}
