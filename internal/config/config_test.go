package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadUsesDotEnvValuesInsteadOfProcessOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := "APP_MODE=lighter_mainnet\nAPP_HOST=127.0.0.1\nAPP_PORT=8123\nLIGHTER_ACCOUNT_INDEX=411813\nLIGHTER_ENABLE_TRADING=true\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APP_MODE", "paper")
	t.Setenv("LIGHTER_ENABLE_TRADING", "false")
	settings, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if settings.Mode != "lighter_mainnet" || !settings.LighterEnableTrading || settings.Port != 8123 || settings.LighterAccountIndex != 411813 {
		t.Fatalf(".env was not authoritative: %+v", settings)
	}
}

func TestLoadSodexMainnetValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := "APP_MODE=sodex_mainnet\nSODEX_ACCOUNT_ADDRESS=0xabc\nSODEX_ACCOUNT_ID=17\nSODEX_MARKET_INDEX=2\nSODEX_ENABLE_TRADING=true\nSODEX_REQUEST_TIMEOUT=45\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	settings, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if settings.Mode != "sodex_mainnet" || settings.SodexBaseURL != "https://mainnet-gw.sodex.dev" || settings.SodexChainID != 286623 {
		t.Fatalf("unexpected SoDEX mainnet defaults: %+v", settings)
	}
	if settings.SodexAccountAddress != "0xabc" || settings.SodexAccountID != 17 || settings.SodexMarketIndex != 2 || !settings.SodexEnableTrading || settings.SodexRequestTimeout != 45 {
		t.Fatalf("unexpected SoDEX values: %+v", settings)
	}
}

func TestLoadMultiDEXDefault(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	content := "APP_MODE=multi\nAPP_DEFAULT_DEX=SoDEX\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	settings, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if settings.Mode != "multi" || settings.DefaultDEX != "sodex" {
		t.Fatalf("unexpected multi-DEX settings: %+v", settings)
	}
}

func TestResolveConfigPathFindsProjectEnvFromDistExecutable(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "dist"), 0o755); err != nil {
		t.Fatal(err)
	}
	envPath := filepath.Join(root, ".env")
	if err := os.WriteFile(envPath, []byte("APP_MODE=paper\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	got := resolveConfigPathFrom(".env", filepath.Join(root, "dist"))
	if got != envPath {
		t.Fatalf("unexpected resolved config path: got=%s want=%s", got, envPath)
	}
}
