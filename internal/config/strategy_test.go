package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadStrategyFile(t *testing.T) {
	path := findStrategyFile(t)
	raw, err := LoadStrategyFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if doc["symbol"] != "SOL" {
		t.Fatalf("symbol = %v", doc["symbol"])
	}
	if doc["leverage"].(float64) != 10 {
		t.Fatalf("leverage = %v", doc["leverage"])
	}
	grid := doc["grid"].(map[string]any)
	if grid["grid_count"].(float64) != 25 {
		t.Fatalf("grid_count = %v", grid["grid_count"])
	}
	if grid["margin"] != "1000" {
		t.Fatalf("margin = %v", grid["margin"])
	}
}

func TestValidateIPWhitelistRequiresAllow(t *testing.T) {
	cfg := &Config{}
	cfg.Exchanges = []Exchange{{
		Name: "lighter", Enabled: true, Network: "mainnet",
		Credentials: Credentials{AccountIndex: 1, APIKeyPrivateKey: "x"},
	}}
	cfg.applyDefaults()
	cfg.Server.IPWhitelist.Enabled = true
	if err := cfg.Validate(); err == nil {
		t.Fatal("白名单开启但 allow 为空时应当拒绝")
	}
	cfg.Server.IPWhitelist.Allow = []string{"1.2.3.4"}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
}

func findStrategyFile(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		filepath.Join(dir, "..", "..", "config", "lighter-sol.yaml"),
		filepath.Join(dir, "config", "lighter-sol.yaml"),
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	t.Fatal("找不到 config/lighter-sol.yaml")
	return ""
}
