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
	DryRun   bool    `yaml:"dry_run"`
	StateDir string  `yaml:"state_dir"`
	Venues   []Venue `yaml:"venues"`
	Matching struct {
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
	Strategy struct {
		OpenBps     float64       `yaml:"open_bps"`
		CloseBps    float64       `yaml:"close_bps"`
		DepthMult   float64       `yaml:"depth_mult"`
		CloseOrder  string        `yaml:"close_order"`
		IntervalMin time.Duration `yaml:"interval_min"`
		IntervalMax time.Duration `yaml:"interval_max"`
		MaxBookAge  time.Duration `yaml:"max_book_age"`
	} `yaml:"strategy"`
	Pairs []Pair `yaml:"pairs"`
}

// Load reads and validates a config file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
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
	s := &c.Strategy
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
	return nil
}
