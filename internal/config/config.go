// Package config loads the YAML config (with ${ENV} expansion).
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Venue config. Opts are passed to the adapter factory (e.g. account_index, private_key).
type Venue struct {
	Name string            `yaml:"name"`
	Kind string            `yaml:"kind"`
	Opts map[string]string `yaml:"opts"`
}

// Pair config.
type Pair struct {
	Base     string  `yaml:"base"`
	Left     string  `yaml:"left"`
	Right    string  `yaml:"right"`
	StepQty  float64 `yaml:"step_qty"`
	MaxSteps int     `yaml:"max_steps"`
}

// Config root.
type Config struct {
	DryRun   bool   `yaml:"dry_run"`
	StateDir string `yaml:"state_dir"`
	Leverage int    `yaml:"leverage"` // account leverage, applied to every venue
	// SamplesDir, when set, appends spread samples (100ms) for offline calibration.
	SamplesDir string  `yaml:"samples_dir"`
	Venues     []Venue `yaml:"venues"`
	Matching   struct {
		StripSuffixes []string `yaml:"strip_suffixes"`
		Separators    string   `yaml:"separators"`
	} `yaml:"matching"`
	Estimator struct {
		Kind        string        `yaml:"kind"`
		Window      time.Duration `yaml:"window"`
		MinWarmup   time.Duration `yaml:"min_warmup"`
		MinCoverage float64       `yaml:"min_coverage"`
		Interval    time.Duration `yaml:"interval"`
	} `yaml:"estimator"`
	Volatility struct {
		Interval time.Duration `yaml:"interval"`  // sampling interval
		Horizon  time.Duration `yaml:"horizon"`   // increment horizon
		HalfLife time.Duration `yaml:"half_life"` // EWMA half-life
		MaxAge   time.Duration `yaml:"max_age"`   // book freshness gate for sampling
	} `yaml:"volatility"`
	Risk struct {
		MarginFloorRatio   float64       `yaml:"margin_floor_ratio"`   // <= ratio stops opening
		MarginTriggerRatio float64       `yaml:"margin_trigger_ratio"` // <= ratio triggers emergency unwind
		MarginHysteresis   float64       `yaml:"margin_hysteresis"`    // recovery band above the floor
		PositionRefresh    time.Duration `yaml:"position_refresh"`     // liquidation price refresh interval
		EmergencyMarketBps float64       `yaml:"emergency_market_bps"` // protective bound of the emergency unwind
		MaxNotional        float64       `yaml:"max_notional"`         // per-venue aggregate notional cap (USD); 0 = unlimited
	} `yaml:"risk"`
	Strategy struct {
		Kind        string        `yaml:"kind"` // arbitrage | volume (registered strategies)
		OpenBps     float64       `yaml:"open_bps"`
		CloseBps    float64       `yaml:"close_bps"`
		DepthMult   float64       `yaml:"depth_mult"`
		CloseOrder  string        `yaml:"close_order"`
		VolK        float64       `yaml:"vol_k"` // one-sided sigma buffer on top of open_bps
		IntervalMin time.Duration `yaml:"interval_min"`
		IntervalMax time.Duration `yaml:"interval_max"`
		MaxBookAge  time.Duration `yaml:"max_book_age"`
	} `yaml:"strategy"`
	Execution struct {
		PriceTicks   int     `yaml:"price_ticks"`   // fixed allowance; fallback while sigma is not ready
		DynamicTicks bool    `yaml:"dynamic_ticks"` // scale the allowance by volatility
		TickK        float64 `yaml:"tick_k"`        // one-sided coverage coefficient
		TickMin      int     `yaml:"min_ticks"`     // floor for the dynamic allowance
		TickMax      int     `yaml:"max_ticks"`     // cap for the dynamic allowance; 0 = no cap
	} `yaml:"execution"`
	Pairs []Pair `yaml:"pairs"`
}

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	// Seed before decoding so an explicit 0 remains distinct from an omitted value.
	c.Execution.PriceTicks = 30
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.defaults()
	return &c, c.validate()
}

func (c *Config) defaults() {
	if c.StateDir == "" {
		c.StateDir = "state"
	}
	if c.Leverage == 0 {
		c.Leverage = 10
	}
	x := &c.Execution
	if x.TickK == 0 {
		x.TickK = 1.28
	}
	if x.TickMin == 0 {
		x.TickMin = 1
	}
	e := &c.Estimator
	if e.Kind == "" {
		e.Kind = "sma"
	}
	if e.Window == 0 {
		e.Window = 2 * time.Hour
	}
	if e.MinWarmup == 0 {
		e.MinWarmup = time.Hour
	}
	if e.MinCoverage == 0 {
		e.MinCoverage = 0.8
	}
	if e.Interval == 0 {
		e.Interval = time.Second
	}
	v := &c.Volatility
	if v.Interval == 0 {
		v.Interval = 100 * time.Millisecond
	}
	if v.Horizon == 0 {
		v.Horizon = 500 * time.Millisecond
	}
	if v.HalfLife == 0 {
		v.HalfLife = time.Minute
	}
	if v.MaxAge == 0 {
		v.MaxAge = 50 * time.Millisecond
	}
	r := &c.Risk
	if r.MarginFloorRatio == 0 {
		r.MarginFloorRatio = 0.2
	}
	if r.MarginTriggerRatio == 0 {
		r.MarginTriggerRatio = 0.1
	}
	if r.MarginHysteresis == 0 {
		r.MarginHysteresis = 0.05
	}
	if r.PositionRefresh == 0 {
		r.PositionRefresh = 15 * time.Second
	}
	if r.EmergencyMarketBps == 0 {
		r.EmergencyMarketBps = 100
	}
	s := &c.Strategy
	if s.Kind == "" {
		s.Kind = "arbitrage"
	}
	if s.OpenBps == 0 {
		s.OpenBps = 1
	}
	if s.CloseBps == 0 {
		s.CloseBps = 1
	}
	if s.DepthMult == 0 {
		s.DepthMult = 2
	}
	if s.CloseOrder == "" {
		s.CloseOrder = "best"
	}
	if s.VolK == 0 {
		s.VolK = 1.28
	}
	if s.IntervalMin == 0 {
		s.IntervalMin = 7 * time.Second
	}
	if s.IntervalMax == 0 {
		s.IntervalMax = 10 * time.Second
	}
	if s.MaxBookAge == 0 {
		s.MaxBookAge = 10 * time.Second
	}
}

func (c *Config) validate() error {
	if c.Execution.PriceTicks < 0 {
		return fmt.Errorf("execution.price_ticks must be >= 0")
	}
	if c.Execution.TickK <= 0 {
		return fmt.Errorf("execution.tick_k must be > 0")
	}
	if c.Execution.TickMin < 0 || c.Execution.TickMax < 0 {
		return fmt.Errorf("execution.min_ticks / max_ticks must be >= 0")
	}
	if c.Execution.TickMax > 0 && c.Execution.TickMax < c.Execution.TickMin {
		return fmt.Errorf("execution.max_ticks must be >= min_ticks")
	}
	if c.Leverage < 1 {
		return fmt.Errorf("leverage must be >= 1")
	}
	if c.Volatility.Interval <= 0 || c.Volatility.Horizon < c.Volatility.Interval || c.Volatility.HalfLife <= 0 || c.Volatility.MaxAge <= 0 {
		return fmt.Errorf("volatility: interval/half_life/max_age must be > 0 and horizon >= interval")
	}
	if c.Risk.MarginTriggerRatio <= 0 || c.Risk.MarginTriggerRatio > c.Risk.MarginFloorRatio || c.Risk.MarginFloorRatio >= 1 {
		return fmt.Errorf("risk: require 0 < margin_trigger_ratio <= margin_floor_ratio < 1")
	}
	if c.Risk.MarginHysteresis < 0 || c.Risk.MarginFloorRatio+c.Risk.MarginHysteresis >= 1 {
		return fmt.Errorf("risk: margin_hysteresis must keep floor+hysteresis < 1")
	}
	if c.Risk.PositionRefresh <= 0 || c.Risk.EmergencyMarketBps <= 0 {
		return fmt.Errorf("risk: position_refresh and emergency_market_bps must be > 0")
	}
	if c.Risk.MaxNotional < 0 {
		return fmt.Errorf("risk: max_notional must be >= 0")
	}
	names := map[string]bool{}
	for _, v := range c.Venues {
		if v.Name == "" || v.Kind == "" {
			return fmt.Errorf("venue needs name and kind")
		}
		names[v.Name] = true
	}
	for _, p := range c.Pairs {
		if !names[p.Left] || !names[p.Right] {
			return fmt.Errorf("pair %s references unknown venue", p.Base)
		}
		if p.StepQty <= 0 || p.MaxSteps <= 0 {
			return fmt.Errorf("pair %s: step_qty and max_steps must be > 0", p.Base)
		}
	}
	if c.Strategy.IntervalMax < c.Strategy.IntervalMin {
		return fmt.Errorf("strategy.interval_max < interval_min")
	}
	switch c.Strategy.CloseOrder {
	case "best", "lifo", "fifo":
	default:
		return fmt.Errorf("strategy.close_order must be best|lifo|fifo")
	}
	if c.Strategy.VolK <= 0 {
		return fmt.Errorf("strategy.vol_k must be > 0")
	}
	return nil
}
