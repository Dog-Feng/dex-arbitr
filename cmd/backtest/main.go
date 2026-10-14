// Command backtest sweeps thresholds over persisted samples with ideal fills.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/Dog-Feng/dex-arbitr/internal/backtest"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if _, writeErr := fmt.Fprintln(os.Stderr, err); writeErr != nil {
			os.Exit(1)
		}
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("backtest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	path := flags.String("samples", "", "4-column legacy or 15-column book samples file")
	stepQty := flags.Float64("step_qty", 0.01, "step quantity")
	maxSteps := flags.Int("max_steps", 10, "max steps")
	depthMult := flags.Float64("depth_mult", 2, "minimum top-book depth / step quantity")
	feeBps := flags.Float64("fee_bps", 0, "round-trip proxy, split equally over four fills; overridden by per-leg rates")
	feeL := flags.Float64("taker_fee_l", 0, "left per-fill taker fee rate (e.g. 0.0005)")
	feeR := flags.Float64("taker_fee_r", 0, "right per-fill taker fee rate")
	volK := flags.Float64("vol_k", 1.28, "one-sided sigma buffer")
	allowGaps := flags.Bool("allow_gaps", false, "allow missing samples, resetting volatility warmup")
	legacyRef := flags.Float64("legacy_ref", 100, "synthetic USD price reference for approximate legacy replay")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: backtest -samples <file>")
	}
	if *depthMult <= 0 || *legacyRef <= 0 {
		return fmt.Errorf("depth_mult and legacy_ref must be positive")
	}
	samples, err := backtest.Load(*path)
	if err != nil {
		return err
	}
	p := backtest.Params{StepQty: *stepQty, MaxSteps: *maxSteps, DepthMult: *depthMult, VolK: *volK, FeeBps: *feeBps, TakerFeeL: *feeL, TakerFeeR: *feeR, AllowGaps: *allowGaps, LegacyRefPrice: *legacyRef}
	// Validate the complete dataset before emitting a successful-looking report.
	first, err := backtest.ReplayChecked(samples, p)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "samples=%d approximate=%t fees=%s\n", len(samples), first.Approximate, first.FeeModel); err != nil {
		return err
	}
	if first.Approximate {
		if _, err := fmt.Fprintf(out, "legacy proxy: fixed synthetic reference %.8g USD, assumed unlimited depth; USD values do not represent actual fills\n", *legacyRef); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(out, "ideal top-book fills; mean=1s window=2h warmup=1h coverage=80%; volatility=100ms/500ms decision=200ms; no IOC, rescue, execution delay/jitter, funding or risk model"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "terminal stats: mean_samples=%d mean_ready=%t volatility_ready=%t\n", first.MeanSamples, first.MeanReady, first.VolReady); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(out, "MTM includes remaining inventory at executable closing sides and estimated fees; inventory is not forcibly closed"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "%-6s %-6s %-7s %-10s %-10s %-12s %-12s %-9s %-10s %-6s %-7s\n", "open", "close", "trips", "net_bps", "avg_bps", "realized_usd", "mtm_usd", "end_steps", "end_qty", "gaps", "skipped"); err != nil {
		return err
	}
	for _, open := range []float64{0, 0.5, 1, 1.5, 2} {
		for _, close := range []float64{0, 0.5, 1} {
			p.OpenBps, p.CloseBps = open, close
			r, err := backtest.ReplayChecked(samples, p)
			if err != nil {
				return err
			}
			mark := "unavailable"
			if r.MTMValid {
				mark = fmt.Sprintf("%.6f", r.MTMUSD)
			}
			if _, err := fmt.Fprintf(out, "%-6.2f %-6.2f %-7d %-10.3f %-10.4f %-12.6f %-12s %-9d %-10.6f %-6d %-7d\n", open, close, r.RoundTrips, r.NetBps, r.AvgBps, r.RealizedUSD, mark, r.EndInventory, r.EndQty, r.GapCount, r.SkippedBooks); err != nil {
				return err
			}
		}
	}
	return nil
}
