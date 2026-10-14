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

func TestLoadPairSelection(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		want    string
		wantErr string
	}{
		{name: "defaults to manual", yaml: "dry_run: true\n", want: "manual"},
		{name: "auto", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\n", want: "auto"},
		{name: "bad mode", yaml: "dry_run: true\nstrategy:\n  pair_selection: nope\n", wantErr: "pair_selection"},
		{name: "auto needs top_y", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\nauto:\n  top_y: -1\n", wantErr: "top_y"},
		{name: "auto needs notional", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\nauto:\n  notional_per_leg: -5\n", wantErr: "notional_per_leg"},
		{name: "auto depth ratio bounds", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\nauto:\n  depth_use_ratio: 2\n", wantErr: "depth_use_ratio"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected %q validation error, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Strategy.PairSelection != tc.want {
				t.Fatalf("pair_selection=%q, want %q", cfg.Strategy.PairSelection, tc.want)
			}
			if tc.want == "auto" {
				if cfg.Auto.TopY != 5 || cfg.Auto.MaxSteps != 3 || cfg.Auto.NotionalPerLeg != 200 {
					t.Fatalf("unexpected auto defaults: %+v", cfg.Auto)
				}
			}
		})
	}
}
