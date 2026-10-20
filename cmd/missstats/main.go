// Command missstats classifies one-leg failures from the trade log and the
// persisted 100ms book samples, and estimates the cost of the alternative
// "unwind the filled leg" policy. It is offline and read-only: it opens data
// files and never touches the trading engine.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/Dog-Feng/dex-arbitr/internal/analytics"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("missstats", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trades := flags.String("trades", "state/trades/trades.jsonl", "monitor trade-event JSONL")
	samples := flags.String("samples", "state/samples", "sample files directory")
	relBps := flags.Float64("rel_bps", 0.26, "relative-dislocation threshold in bps")
	comBps := flags.Float64("com_bps", 0.26, "common-jump threshold in bps")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *trades == "" || *samples == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: missstats -trades <jsonl> -samples <dir> [-rel_bps 0.26] [-com_bps 0.26]")
	}
	f, err := os.Open(*trades)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintln(os.Stderr, closeErr)
		}
	}()
	events, err := analytics.ParseTrades(f)
	if err != nil {
		return err
	}
	rows, sum, err := analytics.AnalyzeMisses(events, *samples, *relBps, *comBps)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "events=%d misses=%d (relative=%d common=%d ambiguous=%d no_samples=%d)\n",
		len(events), sum.Total, sum.Relative, sum.Common, sum.Ambiguous, sum.NoSamples); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "rescue total=%.6f unwind-est total=%.6f estimated saving=%.6f USD\n",
		sum.RescueUSD, sum.UnwindUSD, sum.SavingUSD); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "avg rescue: relative=%.6f common=%.6f USD\n\n",
		sum.AvgRescueRelative, sum.AvgRescueCommon); err != nil {
		return err
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "time\tpair\tkind\tcommon_bps\trelative_bps\trescue_usd\tunwind_est\twould_save"); err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%.3f\t%.3f\t%.6f\t%.6f\t%.6f\n",
			r.Time.UTC().Format("2006-01-02T15:04:05Z"), r.Pair, r.Kind,
			r.CommonBps, r.RelativeBps, r.RescueUSD, r.UnwindUSD, r.SavingUSD); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	_, err = fmt.Fprintln(out, "\nNOTE: unwind estimate is a half-spread bound on the filled leg, not a fill simulation.")
	return err
}
