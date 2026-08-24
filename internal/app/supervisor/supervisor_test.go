package supervisor

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dex-grid/internal/app/engine"
	"dex-grid/internal/config"
	"dex-grid/internal/domain/market"
	"dex-grid/internal/exchange/fake"
	"dex-grid/internal/infra/store"

	"github.com/shopspring/decimal"
)

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func TestStartStopNeutralGrid(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	sup := New(cfg, st, nil, slog.Default())
	ex := fake.New(market.Market{
		Symbol: "BTC", TickSize: d("0.1"), LotSize: d("0.001"),
		MinQty: d("0.001"), MinNotional: d("10"), MaxLeverage: 50,
		PriceDecimals: 1, SizeDecimals: 3,
		MakerFeeRate: d("0.0002"), TakerFeeRate: d("0.0005"),
	})
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))
	sup.Attach(config.Exchange{Name: "fake", Enabled: true, MaxRetries: 2}, ex, 0)
	t.Cleanup(func() { sup.Close(context.Background()) })

	params := map[string]any{
		"symbol": "BTC", "direction": "neutral", "leverage": 5,
		"grid": map[string]any{
			"lower_price": "100", "upper_price": "200", "grid_count": 4,
			"sizing_mode": "per_grid_qty", "per_grid_qty": "1",
		},
		"entry": map[string]any{"mode": "market", "slice_count": 1},
	}
	raw, _ := json.Marshal(params)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := sup.PutConfig(ctx, "fake", raw); err != nil {
		t.Fatal(err)
	}
	view, err := sup.Start(ctx, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != engine.StatusRunning.String() && view.Status != engine.StatusStarting.String() {
		t.Fatalf("status after start = %s", view.Status)
	}
	// 给事件流一点时间确认挂单。
	time.Sleep(50 * time.Millisecond)
	if n := len(ex.Resting()); n == 0 {
		t.Fatal("expected grid orders after start")
	}

	view, err = sup.Stop(ctx, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != engine.StatusStopped.String() {
		t.Fatalf("status after stop = %s", view.Status)
	}
}

func TestLoadStrategyFileAndAutostart(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	file := filepath.Join(t.TempDir(), "btc.yaml")
	body := []byte(`
symbol: BTC
direction: long
leverage: 5
grid:
  lower_price: "100"
  upper_price: "200"
  grid_count: 4
  sizing_mode: per_grid_qty
  per_grid_qty: "1"
entry:
  mode: market
  slice_count: 1
`)
	if err := os.WriteFile(file, body, 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{Exchanges: []config.Exchange{{
		Name: "fake", Enabled: true, MaxRetries: 2,
		StrategyFile: file, Autostart: true,
	}}}
	sup := New(cfg, st, nil, slog.Default())
	ex := fake.New(market.Market{
		Symbol: "BTC", TickSize: d("0.1"), LotSize: d("0.001"),
		MinQty: d("0.001"), MinNotional: d("10"), MaxLeverage: 50,
		PriceDecimals: 1, SizeDecimals: 3,
		MakerFeeRate: d("0.0002"), TakerFeeRate: d("0.0005"),
	})
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))
	sup.Attach(cfg.Exchanges[0], ex, 0)
	t.Cleanup(func() { sup.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sup.LoadStrategyFiles(ctx); err != nil {
		t.Fatal(err)
	}
	sup.Autostart(ctx)
	view, err := sup.Status(ctx, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != engine.StatusRunning.String() && view.Status != engine.StatusStarting.String() {
		t.Fatalf("autostart status = %s", view.Status)
	}
}

func TestLoadStrategyFileSkipsExistingConfig(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	file := filepath.Join(t.TempDir(), "sol.yaml")
	if err := os.WriteFile(file, []byte("symbol: SOL\ndirection: long\nleverage: 10\ngrid:\n  lower_price: \"72\"\n  upper_price: \"77\"\n  grid_count: 4\n  sizing_mode: per_grid_qty\n  per_grid_qty: \"1\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{Exchanges: []config.Exchange{{
		Name: "fake", Enabled: true, MaxRetries: 2,
		StrategyFile: file, Autostart: false,
	}}}
	sup := New(cfg, st, nil, slog.Default())
	ex := fake.New(market.Market{
		Symbol: "BTC", TickSize: d("0.1"), LotSize: d("0.001"),
		MinQty: d("0.001"), MinNotional: d("10"), MaxLeverage: 50,
		PriceDecimals: 1, SizeDecimals: 3,
		MakerFeeRate: d("0.0002"), TakerFeeRate: d("0.0005"),
	})
	ex.SetBook(d("149.9"), d("150.1"))
	ex.SetMark(d("150"))
	sup.Attach(cfg.Exchanges[0], ex, 0)
	t.Cleanup(func() { sup.Close(context.Background()) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	saved := []byte(`{"symbol":"BTC","direction":"neutral","leverage":5,"grid":{"lower_price":"100","upper_price":"200","grid_count":4,"sizing_mode":"per_grid_qty","per_grid_qty":"1"}}`)
	if _, err := sup.PutConfig(ctx, "fake", saved); err != nil {
		t.Fatal(err)
	}
	if err := sup.LoadStrategyFiles(ctx); err != nil {
		t.Fatal(err)
	}
	raw, err := sup.GetConfig("fake")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if got["symbol"] != "BTC" {
		t.Fatalf("saved config overwritten by strategy_file: %+v", got)
	}

	sup.Autostart(ctx)
	view, err := sup.Status(ctx, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != engine.StatusStopped.String() {
		t.Fatalf("autostart=false should stay stopped, got %s", view.Status)
	}
}

func TestStartStopMartingale(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "gridbot.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	sup := New(&config.Config{}, st, nil, slog.Default())
	ex := fake.New(market.Market{
		Symbol: "SOL", TickSize: d("0.01"), LotSize: d("0.001"),
		MinQty: d("0.001"), MinNotional: d("1"), MaxLeverage: 50,
		PriceDecimals: 2, SizeDecimals: 3,
		MakerFeeRate: d("0.0002"), TakerFeeRate: d("0.0005"),
		MaintMarginRate: d("0.025"),
	})
	ex.SetBook(d("99.9"), d("100.1"))
	ex.SetMark(d("100"))
	sup.Attach(config.Exchange{Name: "fake", Enabled: true, MaxRetries: 2}, ex, 0)
	t.Cleanup(func() { sup.Close(context.Background()) })

	params := map[string]any{
		"strategy": "martingale", "symbol": "SOL", "direction": "long", "leverage": 10,
		"martingale": map[string]any{
			"add_drop_pct": "2", "take_profit_pct": "1.5",
			"initial_margin": "50", "add_margin": "50",
			"max_add_times": 3, "add_multiplier": "1",
		},
		"entry": map[string]any{"mode": "market", "slice_count": 1},
	}
	raw, _ := json.Marshal(params)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := sup.PutConfig(ctx, "fake", raw); err != nil {
		t.Fatal(err)
	}
	view, err := sup.Start(ctx, "fake")
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != engine.StatusRunning.String() && view.Status != engine.StatusStarting.String() {
		t.Fatalf("status after start = %s", view.Status)
	}
	time.Sleep(80 * time.Millisecond)
	if n := len(ex.Resting()); n == 0 {
		t.Fatal("expected martingale orders after start")
	}
	if _, err := sup.Stop(ctx, "fake"); err != nil {
		t.Fatal(err)
	}
}
