// Command scanboard evaluates multiple strategy lenses over every matched
// pair and appends one JSONL line per lens per evaluation to a file, so the
// same market instant can be compared under arbitrage, volume and z-score
// criteria with different thresholds for backtesting.
//
// Example:
//
//	go run ./cmd/scanboard -config configs/config.yaml -lenses configs/lenses.example.json -out scanboard.jsonl
//
// It is strictly read-only: credentials are stripped before venues are built
// and no lens ever submits an order.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/hyperliquid"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
)

func main() {
	path := flag.String("config", "configs/config.yaml", "base config (venues, matching)")
	lenses := flag.String("lenses", "", "lens definitions JSON file (required)")
	out := flag.String("out", "scanboard.jsonl", "JSONL output path (append)")
	interval := flag.Duration("interval", time.Second, "mean sampling / evaluation interval")
	flag.Parse()
	if err := validateFlags(*path, *lenses, *out, *interval); err != nil {
		slog.Error("flags", "err", err)
		os.Exit(2)
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, *path, *lenses, *out, *interval); err != nil {
		slog.Error("scanboard", "err", err)
		os.Exit(1)
	}
}

func validateFlags(path, lenses, out string, interval time.Duration) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("config path must not be empty")
	}
	if strings.TrimSpace(lenses) == "" {
		return fmt.Errorf("lenses file is required")
	}
	if strings.TrimSpace(out) == "" {
		return fmt.Errorf("output path must not be empty")
	}
	if interval <= 0 {
		return fmt.Errorf("interval must be positive")
	}
	return nil
}

// lensSpec is the JSON lens file entry. Durations are human-readable strings
// parsed with time.ParseDuration.
type lensSpec struct {
	Name          string  `json:"name"`
	Strategy      string  `json:"strategy"`
	EstimatorKind string  `json:"estimator_kind"`
	Window        string  `json:"window,omitempty"`
	MinWarmup     string  `json:"min_warmup,omitempty"`
	MinCoverage   float64 `json:"min_coverage,omitempty"`
	Interval      string  `json:"interval,omitempty"`
	MuHalfLife    string  `json:"mu_half_life,omitempty"`
	SigmaHalfLife string  `json:"sigma_half_life,omitempty"`
	SigmaFloor    float64 `json:"sigma_floor,omitempty"`
	OpenBps       float64 `json:"open_bps,omitempty"`
	VolK          float64 `json:"vol_k,omitempty"`
	ZOpen         float64 `json:"z_open,omitempty"`
	StepZ         float64 `json:"step_z,omitempty"`
	MaxSteps      int     `json:"max_steps,omitempty"`
	DepthMult     float64 `json:"depth_mult,omitempty"`
	StepQty       float64 `json:"step_qty,omitempty"`
	MinInventory  int     `json:"min_inventory,omitempty"` // volume-only, informational
}

func dur(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d, nil
}

func loadLenses(r io.Reader) ([]scan.Lens, error) {
	var file struct {
		Lenses []lensSpec `json:"lenses"`
	}
	if err := json.NewDecoder(r).Decode(&file); err != nil {
		return nil, fmt.Errorf("decode lenses: %w", err)
	}
	if len(file.Lenses) == 0 {
		return nil, fmt.Errorf("no lenses defined")
	}
	out := make([]scan.Lens, 0, len(file.Lenses))
	seen := map[string]bool{}
	for _, s := range file.Lenses {
		if s.Name == "" || seen[s.Name] {
			return nil, fmt.Errorf("lens names must be unique and non-empty (duplicate %q)", s.Name)
		}
		seen[s.Name] = true
		switch s.Strategy {
		case "arbitrage", "volume", "zscore":
		default:
			return nil, fmt.Errorf("lens %s: unknown strategy %q", s.Name, s.Strategy)
		}
		window, err := dur(s.Window)
		if err != nil {
			return nil, fmt.Errorf("lens %s: window: %w", s.Name, err)
		}
		minWarmup, err := dur(s.MinWarmup)
		if err != nil {
			return nil, fmt.Errorf("lens %s: min_warmup: %w", s.Name, err)
		}
		interval, err := dur(s.Interval)
		if err != nil {
			return nil, fmt.Errorf("lens %s: interval: %w", s.Name, err)
		}
		muHalf, err := dur(s.MuHalfLife)
		if err != nil {
			return nil, fmt.Errorf("lens %s: mu_half_life: %w", s.Name, err)
		}
		sigmaHalf, err := dur(s.SigmaHalfLife)
		if err != nil {
			return nil, fmt.Errorf("lens %s: sigma_half_life: %w", s.Name, err)
		}
		out = append(out, scan.Lens{
			Name: s.Name, Strategy: s.Strategy, EstimatorKind: s.EstimatorKind,
			Estimator: estimator.Params{
				Window: window, MinWarmup: minWarmup, MinCoverage: s.MinCoverage, Interval: interval,
				MuHalfLife: muHalf, SigmaHalfLife: sigmaHalf, SigmaFloor: s.SigmaFloor,
			},
			OpenBps: s.OpenBps, VolK: s.VolK, ZOpen: s.ZOpen, StepZ: s.StepZ,
			MaxSteps: s.MaxSteps, DepthMult: s.DepthMult, StepQty: s.StepQty,
		})
	}
	return out, nil
}

// jsonlSink serializes lens results behind one mutex and flushes every line.
type jsonlSink struct {
	mu sync.Mutex
	w  *bufio.Writer
	f  *os.File
}

func newJSONLSink(path string) (*jsonlSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &jsonlSink{f: f, w: bufio.NewWriter(f)}, nil
}

func (s *jsonlSink) Write(r scan.LensResult) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		return err
	}
	return s.w.Flush()
}

func (s *jsonlSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Flush(); err != nil {
		_ = s.f.Close()
		return err
	}
	return s.f.Close()
}

// readOnlyOptions strips trading credentials so the board can never trade.
func readOnlyOptions(opts map[string]string) map[string]string {
	copy := make(map[string]string, len(opts))
	for key, value := range opts {
		switch key {
		case "private_key", "account_index", "api_key_index":
			continue
		}
		copy[key] = value
	}
	return copy
}

func run(ctx context.Context, configPath, lensesPath, outPath string, interval time.Duration) (err error) {
	c, err := config.Load(configPath)
	if err != nil {
		return err
	}
	lf, err := os.Open(lensesPath)
	if err != nil {
		return err
	}
	lenses, loadErr := loadLenses(lf)
	closeErr := lf.Close()
	if loadErr != nil {
		return loadErr
	}
	if closeErr != nil {
		return closeErr
	}
	sink, err := newJSONLSink(outPath)
	if err != nil {
		return fmt.Errorf("open %s: %w", outPath, err)
	}
	defer func() {
		if closeSinkErr := sink.Close(); closeSinkErr != nil {
			err = errors.Join(err, closeSinkErr)
		}
	}()
	venues := map[string]exchange.Venue{}
	defer func() {
		for name, v := range venues {
			if closeErr := v.Close(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close %s: %w", name, closeErr))
			}
		}
	}()
	for _, vc := range c.Venues {
		v, err := exchange.New(vc.Kind, vc.Name, readOnlyOptions(vc.Opts))
		if err != nil {
			return fmt.Errorf("venue %s: %w", vc.Name, err)
		}
		venues[vc.Name] = v
	}
	matches, err := scan.MatchAll(ctx, c.BuildMatcher(), venues)
	if err != nil {
		return err
	}
	if len(matches) == 0 {
		return fmt.Errorf("no matched pairs")
	}
	slog.Info("scanboard started", "pairs", len(matches), "lenses", len(lenses), "out", outPath)
	var wg sync.WaitGroup
	errs := make(chan error, len(matches))
	for _, m := range matches {
		m := m
		wg.Add(1)
		go func() {
			defer wg.Done()
			feeBps := 2 * (m.Pair.Left.TakerFee + m.Pair.Right.TakerFee) * 1e4
			if werr := scan.WatchLenses(ctx, m.Pair.Base, m.L, m.R, m.Pair.Left, m.Pair.Right, lenses, feeBps,
				interval, c.Volatility.Interval, c.Strategy.MaxBookAge, c.Volatility.MaxAge,
				func(r scan.LensResult) {
					if werr := sink.Write(r); werr != nil {
						select {
						case errs <- fmt.Errorf("write %s: %w", m.Pair.Base, werr):
						default:
						}
					}
				}); werr != nil {
				select {
				case errs <- fmt.Errorf("scan %s: %w", m.Pair.Base, werr):
				default:
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		err = errors.Join(err, e)
	}
	return err
}
