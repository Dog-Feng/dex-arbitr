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
		Kind          string        `yaml:"kind"`           // arbitrage | volume (registered strategies)
		PairSelection string        `yaml:"pair_selection"` // manual | auto
		OpenBps       float64       `yaml:"open_bps"`
		CloseBps      float64       `yaml:"close_bps"`
		DepthMult     float64       `yaml:"depth_mult"`
		CloseOrder    string        `yaml:"close_order"`
		VolK          float64       `yaml:"vol_k"` // one-sided sigma buffer on top of open_bps
		IntervalMin   time.Duration `yaml:"interval_min"`
		IntervalMax   time.Duration `yaml:"interval_max"`
		MaxBookAge    time.Duration `yaml:"max_book_age"`
	} `yaml:"strategy"`
	Volume struct {
		MinInventory      int     `yaml:"min_inventory"`       // floor steps held while churning
		DailyVolumeTarget float64 `yaml:"daily_volume_target"` // USD; 0 = unlimited
		DailyCostBudget   float64 `yaml:"daily_cost_budget"`   // USD max daily loss; 0 = unlimited
		LossToleranceBps  float64 `yaml:"loss_tolerance_bps"`  // per-trade allowed loss (bps) on open
	} `yaml:"volume"`
	Execution struct {
		PriceTicks    int     `yaml:"price_ticks"`     // fixed allowance; fallback while sigma is not ready
		DynamicTicks  bool    `yaml:"dynamic_ticks"`   // scale the allowance by volatility
		TickK         float64 `yaml:"tick_k"`          // one-sided coverage coefficient
		TickMin       int     `yaml:"min_ticks"`       // floor for the dynamic allowance
		TickMax       int     `yaml:"max_ticks"`       // cap for the dynamic allowance; 0 = no cap
		DepthMinMult  float64 `yaml:"depth_min_mult"`  // depth multiple below which allowance widens
		DepthMaxScale float64 `yaml:"depth_max_scale"` // max multiplier on thin books
	} `yaml:"execution"`
	Adaptive struct {
		Window     int     `yaml:"window"`      // outcomes before adjusting; 0 = disabled
		FailHigh   float64 `yaml:"fail_high"`   // failure rate above which to widen
		FailLow    float64 `yaml:"fail_low"`    // failure rate below which to tighten
		UpFactor   float64 `yaml:"up_factor"`   // widen factor (>1)
		DownFactor float64 `yaml:"down_factor"` // tighten factor (<1)
		MinK       float64 `yaml:"min_k"`
		MaxK       float64 `yaml:"max_k"`
	} `yaml:"adaptive"`
	Monitor struct {
		Addr  string `yaml:"addr"`  // HTTP metrics endpoint; empty = disabled
		Token string `yaml:"token"` // bearer token; empty = no auth
	} `yaml:"monitor"`
	Auto  Auto   `yaml:"auto"`
	Pairs []Pair `yaml:"pairs"`
}

// Auto configures the auto pair-selection loop (only used when
// strategy.pair_selection = auto).
type Auto struct {
	TopY             int           `yaml:"top_y"`              // active pair count
	MaxSteps         int           `yaml:"max_steps"`          // steps allowed per active pair
	NotionalPerLeg   float64       `yaml:"notional_per_leg"`   // USD notional cap per leg
	DepthUseRatio    float64       `yaml:"depth_use_ratio"`    // max fraction of top depth used
	MinChurnScore    float64       `yaml:"min_churn_score"`    // below this a pair is not selected
	PromoteMargin    float64       `yaml:"promote_margin"`     // relative score lead required to swap
	Dwell            time.Duration `yaml:"dwell"`              // sustained lead/lag before swapping
	MaxFailRate      float64       `yaml:"max_fail_rate"`      // one-leg failure rate above which a pair is disabled
	MaxTotalNotional float64       `yaml:"max_total_notional"` // account-level notional budget; 0 = unlimited
	Candidates       []string      `yaml:"candidates"`         // optional whitelist; empty = all matched
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
		x.TickK = 2.5
	}
	if x.TickMin == 0 {
		x.TickMin = 1
	}
	if x.DepthMinMult == 0 {
		x.DepthMinMult = 5
	}
	if x.DepthMaxScale == 0 {
		x.DepthMaxScale = 4
	}
	a := &c.Adaptive
	if a.FailHigh == 0 {
		a.FailHigh = 0.10
	}
	if a.FailLow == 0 {
		a.FailLow = 0.02
	}
	if a.UpFactor == 0 {
		a.UpFactor = 1.3
	}
	if a.DownFactor == 0 {
		a.DownFactor = 0.95
	}
	if a.MinK == 0 {
		a.MinK = 0.5
	}
	if a.MaxK == 0 {
		a.MaxK = 10
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
	if s.PairSelection == "" {
		s.PairSelection = "manual"
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
	au := &c.Auto
	if au.TopY == 0 {
		au.TopY = 5
	}
	if au.MaxSteps == 0 {
		au.MaxSteps = 3
	}
	if au.NotionalPerLeg == 0 {
		au.NotionalPerLeg = 200
	}
	if au.DepthUseRatio == 0 {
		au.DepthUseRatio = 0.25
	}
	if au.MinChurnScore == 0 {
		au.MinChurnScore = 1
	}
	if au.PromoteMargin == 0 {
		au.PromoteMargin = 0.05
	}
	if au.Dwell == 0 {
		au.Dwell = time.Minute
	}
	if au.MaxFailRate == 0 {
		au.MaxFailRate = 0.3
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
	if c.Execution.DepthMinMult <= 0 {
		return fmt.Errorf("execution.depth_min_mult must be > 0")
	}
	if c.Execution.DepthMaxScale < 1 {
		return fmt.Errorf("execution.depth_max_scale must be >= 1")
	}
	if c.Adaptive.Window < 0 {
		return fmt.Errorf("adaptive.window must be >= 0")
	}
	if c.Adaptive.Window > 0 {
		if c.Adaptive.FailLow < 0 || c.Adaptive.FailLow >= c.Adaptive.FailHigh || c.Adaptive.FailHigh > 1 {
			return fmt.Errorf("adaptive: require 0 <= fail_low < fail_high <= 1")
		}
		if c.Adaptive.UpFactor <= 1 || c.Adaptive.DownFactor <= 0 || c.Adaptive.DownFactor >= 1 {
			return fmt.Errorf("adaptive: up_factor > 1 and 0 < down_factor < 1")
		}
		if c.Adaptive.MinK <= 0 || c.Adaptive.MaxK < c.Adaptive.MinK {
			return fmt.Errorf("adaptive: require 0 < min_k <= max_k")
		}
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
	if c.Volume.MinInventory < 0 {
		return fmt.Errorf("volume.min_inventory must be >= 0")
	}
	if c.Volume.DailyVolumeTarget < 0 || c.Volume.DailyCostBudget < 0 {
		return fmt.Errorf("volume.daily_volume_target / daily_cost_budget must be >= 0")
	}
	if c.Volume.LossToleranceBps < 0 {
		return fmt.Errorf("volume.loss_tolerance_bps must be >= 0")
	}
	switch c.Strategy.PairSelection {
	case "manual", "auto":
	default:
		return fmt.Errorf("strategy.pair_selection must be manual|auto")
	}
	if c.Strategy.PairSelection == "auto" {
		if c.Auto.TopY <= 0 {
			return fmt.Errorf("auto.top_y must be > 0")
		}
		if c.Auto.MaxSteps <= 0 {
			return fmt.Errorf("auto.max_steps must be > 0")
		}
		if c.Auto.NotionalPerLeg <= 0 {
			return fmt.Errorf("auto.notional_per_leg must be > 0")
		}
		if c.Auto.DepthUseRatio <= 0 || c.Auto.DepthUseRatio > 1 {
			return fmt.Errorf("auto.depth_use_ratio must be in (0, 1]")
		}
		if c.Auto.MinChurnScore < 0 {
			return fmt.Errorf("auto.min_churn_score must be >= 0")
		}
		if c.Auto.PromoteMargin < 0 {
			return fmt.Errorf("auto.promote_margin must be >= 0")
		}
		if c.Auto.Dwell < 0 {
			return fmt.Errorf("auto.dwell must be >= 0")
		}
		if c.Auto.MaxFailRate <= 0 || c.Auto.MaxFailRate > 1 {
			return fmt.Errorf("auto.max_fail_rate must be in (0, 1]")
		}
		if c.Auto.MaxTotalNotional < 0 {
			return fmt.Errorf("auto.max_total_notional must be >= 0")
		}
	}
	return nil
}
