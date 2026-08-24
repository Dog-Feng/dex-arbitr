package config

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Settings struct {
	Mode                  string
	DefaultDEX            string
	Host                  string
	Port                  int
	DBPath                string
	LighterBaseURL        string
	LighterWSURL          string
	LighterAccountIndex   int64
	LighterAPIKeyIndex    uint8
	LighterPrivateKey     string
	LighterMarketIndex    int
	LighterEnableTrading  bool
	LighterRequestTimeout int
	LighterWSMaxReconnect int
	LighterWSInitialDelay int
	LighterWSMaxDelay     int
	LighterChainID        uint32
	SodexBaseURL          string
	SodexAccountAddress   string
	SodexAccountID        uint64
	SodexPrivateKey       string
	SodexAPIKeyName       string
	SodexSymbolID         int
	// SodexMarketIndex is kept as a backward-compatible alias for older .env files.
	SodexMarketIndex    int
	SodexEnableTrading  bool
	SodexRequestTimeout int
	SodexChainID        uint64
	// SoDEX normal-grid initialization is rate-limited because a batch is
	// counted by its number of orders against the per-account order limit.
	SodexGridBatchSize       int
	SodexGridBatchIntervalMS int
}

func Load(path string) (Settings, error) {
	if path == "" {
		path = ".env"
	}
	path = resolveConfigPath(path)
	values, err := readDotEnv(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Settings{}, fmt.Errorf("configuration file not found: %s", path)
		}
		return Settings{}, err
	}
	mode := strings.ToLower(strings.TrimSpace(valueOr(values, "APP_MODE", "paper")))
	chainID := uint32(304)
	if mode == "lighter_testnet" {
		chainID = 300
	}
	sodexBaseURL := valueOr(values, "SODEX_BASE_URL", "https://mainnet-gw.sodex.dev")
	sodexChainID := uint64(286623)
	if mode == "sodex_testnet" {
		sodexBaseURL = valueOr(values, "SODEX_BASE_URL", "https://testnet-gw.sodex.dev")
		sodexChainID = 138565
	}
	return Settings{
		Mode: mode, DefaultDEX: strings.ToLower(strings.TrimSpace(valueOr(values, "APP_DEFAULT_DEX", ""))), Host: valueOr(values, "APP_HOST", "127.0.0.1"),
		Port: intValue(values, "APP_PORT", 8000), DBPath: valueOr(values, "APP_DB_PATH", "data/strategy.db"),
		LighterBaseURL:           valueOr(values, "LIGHTER_BASE_URL", "https://testnet.zklighter.elliot.ai"),
		LighterWSURL:             valueOr(values, "LIGHTER_WS_URL", "wss://testnet.zklighter.elliot.ai/stream"),
		LighterAccountIndex:      int64Value(values, "LIGHTER_ACCOUNT_INDEX", 0),
		LighterAPIKeyIndex:       uint8(intValue(values, "LIGHTER_API_KEY_INDEX", 0)),
		LighterPrivateKey:        valueOr(values, "LIGHTER_API_PRIVATE_KEY", ""),
		LighterMarketIndex:       intValue(values, "LIGHTER_MARKET_INDEX", 0),
		LighterEnableTrading:     boolValue(values, "LIGHTER_ENABLE_TRADING", false),
		LighterRequestTimeout:    intValue(values, "LIGHTER_REQUEST_TIMEOUT", 10),
		LighterWSMaxReconnect:    intValue(values, "LIGHTER_WS_MAX_RECONNECT_ATTEMPTS", 10),
		LighterWSInitialDelay:    intValue(values, "LIGHTER_WS_INITIAL_RECONNECT_DELAY", 1),
		LighterWSMaxDelay:        intValue(values, "LIGHTER_WS_MAX_RECONNECT_DELAY", 30),
		LighterChainID:           chainID,
		SodexBaseURL:             sodexBaseURL,
		SodexAccountAddress:      valueOr(values, "SODEX_ACCOUNT_ADDRESS", ""),
		SodexAccountID:           uint64Value(values, "SODEX_ACCOUNT_ID", 0),
		SodexPrivateKey:          valueOr(values, "SODEX_PRIVATE_KEY", ""),
		SodexAPIKeyName:          valueOr(values, "SODEX_API_KEY_NAME", ""),
		SodexSymbolID:            intValue(values, "SODEX_SYMBOL_ID", intValue(values, "SODEX_MARKET_INDEX", 0)),
		SodexMarketIndex:         intValue(values, "SODEX_SYMBOL_ID", intValue(values, "SODEX_MARKET_INDEX", 0)),
		SodexEnableTrading:       boolValue(values, "SODEX_ENABLE_TRADING", false),
		SodexRequestTimeout:      intValue(values, "SODEX_REQUEST_TIMEOUT", 30),
		SodexChainID:             uint64Value(values, "SODEX_CHAIN_ID", sodexChainID),
		SodexGridBatchSize:       intValue(values, "SODEX_GRID_BATCH_SIZE", 10),
		SodexGridBatchIntervalMS: intValue(values, "SODEX_GRID_BATCH_INTERVAL_MS", 1000),
	}, nil
}

// resolveConfigPath keeps the documented "run from the project root" flow,
// while also allowing the packaged executable under dist/ to be launched by
// double-click. In that case the working directory is commonly dist/, so
// walk upward from the executable location when .env is not in the cwd.
func resolveConfigPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	if _, err := os.Stat(path); err == nil {
		return path
	}
	executable, err := os.Executable()
	if err != nil {
		return path
	}
	return resolveConfigPathFrom(filepath.Clean(path), filepath.Dir(executable))
}

func resolveConfigPathFrom(path, executableDir string) string {
	dir := filepath.Clean(executableDir)
	for {
		candidate := filepath.Join(dir, path)
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return path
}

func readDotEnv(path string) (map[string]string, error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	values := make(map[string]string)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), "\"'")
		if key != "" {
			values[key] = value
		}
	}
	return values, scanner.Err()
}

func valueOr(values map[string]string, key, fallback string) string {
	if value, exists := values[key]; exists && value != "" {
		return value
	}
	return fallback
}

func intValue(values map[string]string, key string, fallback int) int {
	value, err := strconv.Atoi(values[key])
	if err != nil {
		return fallback
	}
	return value
}

func int64Value(values map[string]string, key string, fallback int64) int64 {
	value, err := strconv.ParseInt(values[key], 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func uint64Value(values map[string]string, key string, fallback uint64) uint64 {
	value, err := strconv.ParseUint(values[key], 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func boolValue(values map[string]string, key string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(values[key]))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
