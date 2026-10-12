package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadExecutionPriceTicks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		yaml    string
		want    int
		wantErr bool
	}{
		{name: "omitted", yaml: "dry_run: true\n", want: 30},
		{name: "zero", yaml: "execution:\n  price_ticks: 0\n", want: 0},
		{name: "custom", yaml: "execution:\n  price_ticks: 7\n", want: 7},
		{name: "negative", yaml: "execution:\n  price_ticks: -1\n", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if tc.wantErr {
				if err == nil || !strings.Contains(err.Error(), "execution.price_ticks") {
					t.Fatalf("expected price_ticks validation error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Execution.PriceTicks != tc.want {
				t.Fatalf("price_ticks=%d, want %d", cfg.Execution.PriceTicks, tc.want)
			}
		})
	}
}
