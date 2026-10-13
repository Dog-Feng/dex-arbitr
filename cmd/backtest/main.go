// Command backtest replays the strategy over persisted samples and sweeps the
// open/close thresholds to report the realized net edge per combination.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Dog-Feng/dex-arbitr/internal/backtest"
)

func main() {
	path := flag.String("samples", "", "samples file (unixNano,midBps,lsBps,slBps)")
	stepQty := flag.Float64("step_qty", 0.01, "step quantity")
	maxSteps := flag.Int("max_steps", 10, "max steps")
	feeBps := flag.Float64("fee_bps", 0, "round-trip fee in bps")
	volK := flag.Float64("vol_k", 1.28, "one-sided sigma buffer")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "usage: backtest -samples <file>")
		os.Exit(2)
	}
	samples, err := backtest.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("samples=%d\n", len(samples))
	fmt.Printf("%-8s %-8s %-10s %-10s %-10s\n", "open", "close", "trips", "net_bps", "avg_bps")
	for _, open := range []float64{0, 0.5, 1, 1.5, 2} {
		for _, close := range []float64{0, 0.5, 1} {
			r := backtest.Replay(samples, backtest.Params{StepQty: *stepQty, MaxSteps: *maxSteps,
				OpenBps: open, CloseBps: close, VolK: *volK, FeeBps: *feeBps})
			fmt.Printf("%-8.2f %-8.2f %-10d %-10.2f %-10.4f\n", open, close, r.RoundTrips, r.NetBps, r.AvgBps)
		}
	}
}
