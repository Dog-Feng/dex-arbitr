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

func TestLoadVolumeLossTolerance(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("dry_run: true\nvolume:\n  loss_tolerance_bps: 0.5\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Volume.LossToleranceBps != 0.5 {
		t.Fatalf("loss_tolerance_bps=%v, want 0.5", cfg.Volume.LossToleranceBps)
	}

	bad := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(bad, []byte("dry_run: true\nvolume:\n  loss_tolerance_bps: -1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(bad); err == nil || !strings.Contains(err.Error(), "loss_tolerance_bps") {
		t.Fatalf("expected loss_tolerance_bps validation error, got %v", err)
	}
}
