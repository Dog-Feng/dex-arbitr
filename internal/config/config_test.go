package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

func loadTestConfig(t *testing.T, data string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	return Load(path)
}

func TestSingleSlotEstimatorWarmsForAllStrategySelections(t *testing.T) {
	for _, kind := range []string{"arbitrage", "volume"} {
		for _, selection := range []string{"manual", "auto"} {
			t.Run(kind+"/"+selection, func(t *testing.T) {
				data := fmt.Sprintf("strategy:\n  kind: %s\n  pair_selection: %s\nestimator:\n  window: 2s\n  min_warmup: 2s\n  interval: 2s\n  min_coverage: 1\n", kind, selection)
				c, err := loadTestConfig(t, data)
				if err != nil {
					t.Fatal(err)
				}
				e, err := estimator.New(c.Estimator.Kind, estimator.Params{Window: c.Estimator.Window, MinWarmup: c.Estimator.MinWarmup, MinCoverage: c.Estimator.MinCoverage, Interval: c.Estimator.Interval})
				if err != nil {
					t.Fatal(err)
				}
				start := time.Unix(1000, 0)
				e.Update(start, 1)
				if estimator.ReadyAt(e, start) {
					t.Fatal("single sample bypassed elapsed warmup")
				}
				e.Update(start.Add(2*time.Second), 2)
				if st := estimator.StatsAt(e, start.Add(2*time.Second)); !st.Ready || st.Mean != 2 || st.Samples != 1 {
					t.Fatalf("single-slot configuration did not warm: %+v", st)
				}
				e.Update(start.Add(time.Minute), 3)
				if estimator.ReadyAt(e, start.Add(time.Minute)) {
					t.Fatal("long sampling gap did not restart warmup")
				}
			})
		}
	}
}

func TestLoadStrictValidation(t *testing.T) {
	for _, tc := range []struct {
		name, data, want string
	}{
		{"unknown root field", "dry_rnu: true\n", "dry_rnu"},
		{"unknown nested field", "strategy:\n  max_books_age: 1s\n", "max_books_age"},
		{"duplicate YAML key", "dry_run: true\ndry_run: false\n", "already defined"},
		{"second YAML document", "dry_run: true\n---\ndry_run: false\n", "multiple YAML documents"},
		{"nan threshold", "strategy:\n  open_bps: .nan\n", "strategy.open_bps must be finite"},
		{"bad session spec", "monitor:\n  analytics:\n    sessions:\n      x: 9:30-04:00\n", "sessions"},
		{"nonfinite inactive option", "auto:\n  notional_per_leg: -.inf\n", "auto.notional_per_leg must be finite"},
		{"zero estimator interval", "estimator:\n  interval: 0s\n", "estimator.interval must be > 0"},
		{"negative estimator window", "estimator:\n  window: -1s\n", "estimator.window must be > 0"},
		{"zero warmup", "estimator:\n  min_warmup: 0s\n", "estimator.min_warmup must be > 0"},
		{"warmup beyond window", "estimator:\n  window: 1m\n  min_warmup: 2m\n", "must not exceed window"},
		{"interval beyond window", "estimator:\n  window: 1m\n  min_warmup: 1m\n  interval: 2m\n", "must not exceed window"},
		{"unreachable warmup span", "estimator:\n  window: 5s\n  min_warmup: 5s\n  interval: 2s\n", "unreachable"},
		{"coverage above one", "estimator:\n  min_coverage: 1.1\n", "min_coverage"},
		{"zero action interval", "strategy:\n  interval_min: 0s\n", "strategy.interval_min must be > 0"},
		{"negative book age", "strategy:\n  max_book_age: -1s\n", "strategy.max_book_age must be > 0"},
		{"reversed action interval", "strategy:\n  interval_min: 11s\n  interval_max: 10s\n", "interval_max < interval_min"},
		{"zero auto dwell", "auto:\n  dwell: 0s\n", "auto.dwell must be > 0"},
		{"zero volatility cadence", "volatility:\n  interval: 0s\n", "volatility"},
		{"horizon below cadence", "volatility:\n  horizon: 10ms\n", "horizon >= interval"},
		{"negative margin refresh", "risk:\n  position_refresh: -1s\n", "position_refresh"},
		{"zero emergency limit", "risk:\n  emergency_market_bps: 0\n", "emergency_market_bps"},
		{"adaptive window below horizon", "execution:\n  adaptive_latency:\n    enabled: true\n    window_max: 200ms\n", "window_max must be >= volatility.horizon"},
		{"adaptive window below interval", "execution:\n  adaptive_latency:\n    enabled: true\n    window_min: 50ms\n", "window_min must be >= volatility.interval"},
		{"empty matching suffix", "matching:\n  strip_suffixes: [USD, '']\n", "empty suffix"},
		{"blank matching suffix", "matching:\n  strip_suffixes: [' ']\n", "empty suffix"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadTestConfig(t, tc.data); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestLoadVenueAndPairIdentity(t *testing.T) {
	venues := "venues:\n  - {name: L, kind: lighter}\n  - {name: R, kind: extended}\n  - {name: C, kind: lighter}\n"
	for _, tc := range []struct {
		name, data, want string
	}{
		{"duplicate venue", "venues:\n  - {name: L, kind: lighter}\n  - {name: L, kind: extended}\n", "duplicate venue"},
		{"blank venue", "venues:\n  - {name: ' ', kind: lighter}\n", "name and kind"},
		{"same venue on both legs", venues + "pairs:\n  - {base: BTC, left: L, right: L, step_qty: 1, max_steps: 1}\n", "different venues"},
		{"unknown venue", venues + "pairs:\n  - {base: BTC, left: L, right: Missing, step_qty: 1, max_steps: 1}\n", "unknown venue"},
		{"empty base", venues + "pairs:\n  - {base: '', left: L, right: R, step_qty: 1, max_steps: 1}\n", "base must not be empty"},
		{"base normalizes empty", venues + "pairs:\n  - {base: '-', left: L, right: R, step_qty: 1, max_steps: 1}\n", "empty base"},
		{"duplicate reversed normalized pair", venues + "pairs:\n  - {base: BTC-USD, left: L, right: R, step_qty: 1, max_steps: 1}\n  - {base: BTC, left: R, right: L, step_qty: 1, max_steps: 1}\n", "duplicate pair"},
		{"shared market between combinations", venues + "pairs:\n  - {base: BTC, left: L, right: R, step_qty: 1, max_steps: 1}\n  - {base: BTCUSD, left: L, right: C, step_qty: 1, max_steps: 1}\n", "share market"},
		{"nonfinite pair size", venues + "pairs:\n  - {base: BTC, left: L, right: R, step_qty: .nan, max_steps: 1}\n", "pairs[0].step_qty must be finite"},
		{"zero pair size", venues + "pairs:\n  - {base: BTC, left: L, right: R, step_qty: 0, max_steps: 1}\n", "step_qty and max_steps"},
		{"volume floor at pair max steps", venues + "strategy:\n  kind: volume\nvolume:\n  min_inventory: 2\npairs:\n  - {base: BTC, left: L, right: R, step_qty: 1, max_steps: 2}\n", "min_inventory"},
		{"volume floor at auto max steps", venues + "strategy:\n  kind: volume\n  pair_selection: auto\nvolume:\n  min_inventory: 3\nauto:\n  top_y: 1\n  max_steps: 3\n", "min_inventory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadTestConfig(t, tc.data); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error=%v, want %q", err, tc.want)
			}
		})
	}
	if _, err := loadTestConfig(t, venues+"pairs:\n  - {base: BTC, left: L, right: R, step_qty: 1, max_steps: 1}\n  - {base: ETH, left: L, right: C, step_qty: 1, max_steps: 1}\n"); err != nil {
		t.Fatalf("disjoint markets should be accepted: %v", err)
	}
}

func TestLoadDefaultsAndExplicitZero(t *testing.T) {
	c, err := loadTestConfig(t, "dry_run: true\nstrategy:\n  open_bps: 0\n  close_bps: 0\nexecution:\n  price_ticks: 0\n  min_ticks: 0\nrisk:\n  margin_hysteresis: 0\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Strategy.OpenBps != 0 || c.Strategy.CloseBps != 0 || c.Execution.PriceTicks != 0 || c.Execution.TickMin != 0 || c.Risk.MarginHysteresis != 0 {
		t.Fatalf("explicit zeros replaced by defaults: %+v", c)
	}
	if c.Estimator.Interval != time.Second || c.Volatility.Interval != 100*time.Millisecond || c.Strategy.IntervalMin != 7*time.Second {
		t.Fatalf("omitted values lost defaults: %+v", c)
	}
	t.Setenv("DEX_TEST_MONITOR_TOKEN", "expanded-token")
	c, err = loadTestConfig(t, "monitor:\n  token: ${DEX_TEST_MONITOR_TOKEN}\n")
	if err != nil || c.Monitor.Token != "expanded-token" {
		t.Fatalf("environment expansion: %+v, %v", c, err)
	}
}

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

func TestLoadZScoreSwitch(t *testing.T) {
	c, err := loadTestConfig(t, "estimator:\n  kind: zscore\n  window: 2h\n  min_warmup: 15m\n  interval: 1s\n  min_coverage: 0.8\nstrategy:\n  kind: zscore\n  z_open: 2.5\n  step_z: 0.5\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Estimator.MuHalfLife != 15*time.Minute || c.Estimator.SigmaHalfLife != 3*time.Hour || c.Estimator.SigmaFloor != 0.5 {
		t.Fatalf("zscore estimator defaults not applied: %+v", c.Estimator)
	}
	if c.Strategy.ZOpen != 2.5 {
		t.Fatalf("z_open = %v, want 2.5", c.Strategy.ZOpen)
	}
	if c.Strategy.StepZ != 0.5 {
		t.Fatalf("step_z = %v, want 0.5", c.Strategy.StepZ)
	}
	if _, err := strategy.New(c.Strategy.Kind); err != nil {
		t.Fatalf("zscore strategy not registered: %v", err)
	}
}

func TestLoadZScoreValidation(t *testing.T) {
	for _, tc := range []struct{ name, data, want string }{
		{"negative floor", "estimator:\n  kind: zscore\n  sigma_floor: -1\n", "sigma_floor"},
		{"negative z open", "estimator:\n  kind: zscore\nstrategy:\n  kind: zscore\n  z_open: -1\n", "z_open"},
		{"negative step z", "estimator:\n  kind: zscore\nstrategy:\n  kind: zscore\n  step_z: -0.5\n", "step_z"},
		{"zscore strategy with sma estimator", "strategy:\n  kind: zscore\n", "requires estimator.kind zscore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadTestConfig(t, tc.data); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestLoadFlipHysteresisDefaultAndZero(t *testing.T) {
	c, err := loadTestConfig(t, "dry_run: true\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Strategy.FlipHysteresisSigma != 1.0 {
		t.Fatalf("default flip_hysteresis_sigma=%v, want 1", c.Strategy.FlipHysteresisSigma)
	}
	c, err = loadTestConfig(t, "strategy:\n  flip_hysteresis_sigma: 0\n")
	if err != nil || c.Strategy.FlipHysteresisSigma != 0 {
		t.Fatalf("explicit zero should disable the hysteresis: %v %v", c, err)
	}
	if _, err := loadTestConfig(t, "strategy:\n  flip_hysteresis_sigma: -1\n"); err == nil || !strings.Contains(err.Error(), "flip_hysteresis_sigma") {
		t.Fatalf("negative flip_hysteresis_sigma accepted: %v", err)
	}
}

func TestLoadCapacityHysteresisDefaultAndZero(t *testing.T) {
	c, err := loadTestConfig(t, "dry_run: true\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Strategy.CapBand != 2 || c.Strategy.CapHysteresisSigma != 1.0 {
		t.Fatalf("default cap hysteresis wrong: band=%d sigma=%v", c.Strategy.CapBand, c.Strategy.CapHysteresisSigma)
	}
	c, err = loadTestConfig(t, "strategy:\n  cap_band: 0\n  cap_hysteresis_sigma: 0\n")
	if err != nil || c.Strategy.CapBand != 0 || c.Strategy.CapHysteresisSigma != 0 {
		t.Fatalf("explicit zeros should disable the capacity hysteresis: %+v %v", c.Strategy, err)
	}
	if _, err := loadTestConfig(t, "strategy:\n  cap_band: -1\n"); err == nil || !strings.Contains(err.Error(), "cap_band") {
		t.Fatalf("negative cap_band accepted: %v", err)
	}
}

func TestLoadMonitorAnalyticsDefaults(t *testing.T) {
	c, err := loadTestConfig(t, "dry_run: true\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Monitor.Analytics.Bucket != 15*time.Minute || c.Monitor.Analytics.RetainBuckets != 672 {
		t.Fatalf("analytics defaults wrong: %+v", c.Monitor.Analytics)
	}
	a := c.Monitor.Analytics.Anomalies
	if a.ConsecutiveFails != 3 || a.FlipsWindowMin != 10 || a.FlipsThreshold != 3 ||
		a.FullLoadHeadroom != 2 || a.FullLoadMinutes != 30 || a.OldestStepMinutes != 60 ||
		a.LossBucketCount != 3 || a.LossPerBucketUSD != 0.05 || a.RescueSlipUSD != 0.01 ||
		a.LatencyP95Ms != 600 || a.SilentMinutes != 30 || a.RiskFlapsPerHour != 8 || a.MuJumpSigma != 3 {
		t.Fatalf("anomaly defaults wrong: %+v", a)
	}
	c, err = loadTestConfig(t, "monitor:\n  analytics:\n    bucket: 1h\n    anomalies:\n      consecutive_fails: -1\n")
	if err != nil {
		t.Fatal(err)
	}
	if c.Monitor.Analytics.Bucket != time.Hour || c.Monitor.Analytics.Anomalies.ConsecutiveFails != -1 {
		t.Fatalf("explicit analytics values not honored: %+v", c.Monitor.Analytics)
	}
	c, err = loadTestConfig(t, "monitor:\n  analytics:\n    sessions:\n      us_cash: 21:30-04:00\n")
	if err != nil || c.Monitor.Analytics.Sessions["us_cash"] != "21:30-04:00" {
		t.Fatalf("sessions not parsed: %+v %v", c.Monitor.Analytics.Sessions, err)
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
		{name: "auto fail rate bounds", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\nauto:\n  max_fail_rate: 1.5\n", wantErr: "max_fail_rate"},
		{name: "auto notional cap", yaml: "dry_run: true\nstrategy:\n  pair_selection: auto\nauto:\n  max_total_notional: 10000\n", want: "auto"},
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
				if cfg.Auto.TopY != 5 || cfg.Auto.MaxSteps != 3 || cfg.Auto.NotionalPerLeg != 200 || cfg.Auto.MaxFailRate != 0.3 {
					t.Fatalf("unexpected auto defaults: %+v", cfg.Auto)
				}
			}
		})
	}
}

func TestLoadMatchingAliases(t *testing.T) {
	c, err := loadTestConfig(t, "matching:\n  aliases:\n    ANTH: ANTHROPIC\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Matching.Aliases["ANTH"]; got != "ANTHROPIC" {
		t.Fatalf("alias not retained: %q", got)
	}
	matcher := symbols.DefaultMatcher()
	if len(c.Matching.Aliases) > 0 {
		matcher.Aliases = c.Matching.Aliases
	}
	if got := matcher.Normalize("ANTH"); got != "ANTHROPIC" {
		t.Fatalf("alias normalization: %q", got)
	}
	for _, bad := range []string{
		"matching:\n  aliases:\n    ANTH: ANTH\n",
		"matching:\n  aliases:\n    '': X\n",
	} {
		if _, err := loadTestConfig(t, bad); err == nil {
			t.Fatalf("invalid aliases accepted: %q", bad)
		}
	}
}

func TestStrategyKindsUseCommonWindowAndExplicitVolumePolicy(t *testing.T) {
	for _, kind := range []string{"arbitrage", "volume"} {
		for _, mode := range []string{"manual", "auto"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				c, err := loadTestConfig(t, "strategy:\n  kind: "+kind+"\n  pair_selection: "+mode+"\n  vol_k: 0\n")
				if err != nil {
					t.Fatal(err)
				}
				if c.Estimator.Window != 2*time.Hour || c.Estimator.MinWarmup != time.Hour || c.Estimator.Interval != time.Second || c.Estimator.MinCoverage != .8 {
					t.Fatalf("strategy/selection changed sampling defaults: %+v", c.Estimator)
				}
				if c.Strategy.VolK != 0 {
					t.Fatal("explicitly disabled volatility buffer was overwritten")
				}
			})
		}
	}
	for _, tc := range []struct{ name, data, want string }{
		{"unknown strategy", "strategy:\n  kind: typo\n  pair_selection: auto\n", "strategy.kind"},
		{"negative volatility multiplier", "strategy:\n  vol_k: -1\n", "vol_k"},
		{"volume negative edge", "strategy:\n  kind: volume\nvolume:\n  loss_tolerance_bps: 0.5\n", "volume.loss_tolerance_bps must be 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := loadTestConfig(t, tc.data); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Load=%v, want %s", err, tc.want)
			}
		})
	}
}
