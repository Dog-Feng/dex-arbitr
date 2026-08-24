package api

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"dex-grid/internal/app/supervisor"
	"dex-grid/internal/config"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/exchange/fake"
	"dex-grid/internal/infra/store"

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
		MakerFeeRate:  d("0.0002"),
		TakerFeeRate:  d("0.0005"),
	}
}

func setupAPI(t *testing.T) (*Server, *fake.Exchange) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{
		Server: config.Server{Addr: "127.0.0.1:0"},
	}
	sup := supervisor.New(cfg, st, nil, slog.Default())
	ex := fake.New(testMarket())
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))
	sup.Attach(config.Exchange{Name: "fake", Enabled: true, MaxRetries: 2}, ex, 0)
	return New(sup, cfg.Server), ex
}

func doJSON(t *testing.T, h http.Handler, method, path string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(raw)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var env map[string]any
	if rec.Body.Len() > 0 {
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatalf("json: %v body=%s", err, rec.Body.String())
		}
	}
	return rec.Code, env
}

func validParams() map[string]any {
	return map[string]any{
		"symbol":    "BTC",
		"direction": "neutral",
		"leverage":  5,
		"grid": map[string]any{
			"lower_price":  "100",
			"upper_price":  "200",
			"grid_count":   4,
			"sizing_mode":  "per_grid_qty",
			"per_grid_qty": "1",
		},
	}
}

func TestRootServesConsole(t *testing.T) {
	s, _ := setupAPI(t)
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "网格交易") {
		t.Fatalf("index.html missing title, body=%s", rec.Body.String()[:min(200, rec.Body.Len())])
	}

	req = httptest.NewRequest("GET", "/css/console.css", nil)
	rec = httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("css status = %d, want 200", rec.Code)
	}
}

func TestHealthzAndExchanges(t *testing.T) {
	s, _ := setupAPI(t)
	code, env := doJSON(t, s.Handler(), "GET", "/healthz", nil)
	if code != 200 || env["ok"] != true {
		t.Fatalf("healthz %d %+v", code, env)
	}
	code, env = doJSON(t, s.Handler(), "GET", "/api/exchanges", nil)
	if code != 200 || env["ok"] != true {
		t.Fatalf("exchanges %d %+v", code, env)
	}
}

func TestPreviewAndConfigRoundTrip(t *testing.T) {
	s, _ := setupAPI(t)
	code, env := doJSON(t, s.Handler(), "POST", "/api/exchanges/fake/preview", validParams())
	if code != 200 || env["ok"] != true {
		t.Fatalf("preview %d %+v", code, env)
	}
	data := env["data"].(map[string]any)
	if data["grid_count"].(float64) != 4 {
		t.Fatalf("grid_count = %v", data["grid_count"])
	}

	code, env = doJSON(t, s.Handler(), "PUT", "/api/exchanges/fake/config", validParams())
	if code != 200 || env["ok"] != true {
		t.Fatalf("put config %d %+v", code, env)
	}
	code, env = doJSON(t, s.Handler(), "GET", "/api/exchanges/fake/config", nil)
	if code != 200 || env["ok"] != true {
		t.Fatalf("get config %d %+v", code, env)
	}
}

func TestPreviewRejectsInvalidRange(t *testing.T) {
	s, _ := setupAPI(t)
	p := validParams()
	p["grid"].(map[string]any)["upper_price"] = "50"
	code, env := doJSON(t, s.Handler(), "POST", "/api/exchanges/fake/preview", p)
	if code != 400 || env["ok"] != false {
		t.Fatalf("expected 400, got %d %+v", code, env)
	}
	err := env["error"].(map[string]any)
	if err["code"] != "INVALID_RANGE" {
		t.Fatalf("code = %v", err["code"])
	}
}

func TestAuthRequired(t *testing.T) {
	s, _ := setupAPI(t)
	s.cfg.Auth.Enabled = true
	s.cfg.Auth.Token = "secret"
	code, _ := doJSON(t, s.Handler(), "GET", "/api/exchanges", nil)
	if code != 401 {
		t.Fatalf("status = %d, want 401", code)
	}
}

func TestStatusUsesConfigSymbol(t *testing.T) {
	s, _ := setupAPI(t)
	code, env := doJSON(t, s.Handler(), "PUT", "/api/exchanges/fake/config", validParams())
	if code != 200 || env["ok"] != true {
		t.Fatalf("put config %d %+v", code, env)
	}
	code, env = doJSON(t, s.Handler(), "GET", "/api/exchanges/fake/status", nil)
	if code != 200 || env["ok"] != true {
		t.Fatalf("status %d %+v", code, env)
	}
	data := env["data"].(map[string]any)
	if data["symbol"] != "BTC" {
		t.Fatalf("status symbol = %v, want BTC", data["symbol"])
	}
	if data["mark"] == nil || data["mark"] == "0" || data["mark"] == 0 {
		t.Fatalf("status mark empty: %+v", data["mark"])
	}
}

func TestKlinesUsesConfigSymbol(t *testing.T) {
	s, _ := setupAPI(t)
	code, env := doJSON(t, s.Handler(), "PUT", "/api/exchanges/fake/config", validParams())
	if code != 200 {
		t.Fatalf("put config %d %+v", code, env)
	}
	code, env = doJSON(t, s.Handler(), "GET", "/api/exchanges/fake/klines?interval=1h&limit=8", nil)
	if code != 200 || env["ok"] != true {
		t.Fatalf("klines %d %+v", code, env)
	}
	list, ok := env["data"].([]any)
	if !ok || len(list) != 8 {
		t.Fatalf("klines data = %+v", env["data"])
	}
}

func TestStartRequiresConfig(t *testing.T) {
	s, _ := setupAPI(t)
	code, env := doJSON(t, s.Handler(), "POST", "/api/exchanges/fake/start", nil)
	if code != 409 {
		t.Fatalf("start without config: %d %+v", code, env)
	}
}

func TestIPWhitelist(t *testing.T) {
	s, _ := setupAPI(t)
	s.cfg.IPWhitelist.Enabled = true
	s.cfg.IPWhitelist.Allow = []string{"203.0.113.10", "10.0.0.0/8"}
	h := s.Handler()

	assertHealth := func(remote string, want int) {
		t.Helper()
		req := httptest.NewRequest("GET", "/healthz", nil)
		req.RemoteAddr = remote
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("remote %s: status = %d, want %d", remote, rec.Code, want)
		}
	}

	assertHealth("127.0.0.1:1234", 200)
	assertHealth("[::1]:1234", 200)
	assertHealth("203.0.113.10:9", 200)
	assertHealth("10.1.2.3:9", 200)
	assertHealth("198.51.100.1:9", 403)
}
