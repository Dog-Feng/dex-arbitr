// Command analytics reports per-bucket and per-session statistics from the
// persisted trade-event log written by the monitor. It is offline and
// read-only: it never touches the trading engine.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/analytics"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("analytics", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	trades := flags.String("trades", "state/trades/trades.jsonl", "monitor trade-event JSONL")
	from := flags.String("from", "", "inclusive start (RFC3339, UTC); empty = unbounded")
	to := flags.String("to", "", "exclusive end (RFC3339, UTC); empty = unbounded")
	bucket := flags.Duration("bucket", 15*time.Minute, "aggregation granularity")
	sessionSpecs := flags.String("sessions", "", `comma-separated "name:HH:MM-HH:MM" Beijing windows, e.g. us_cash:21:30-04:00`)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *trades == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: analytics -trades <file> [-from RFC3339] [-to RFC3339] [-bucket 15m] [-sessions name:HH:MM-HH:MM,...]")
	}
	sessions, err := parseSessions(*sessionSpecs)
	if err != nil {
		return err
	}
	var r analytics.Range
	if *from != "" {
		if r.From, err = time.Parse(time.RFC3339, *from); err != nil {
			return fmt.Errorf("from: %w", err)
		}
	}
	if *to != "" {
		if r.To, err = time.Parse(time.RFC3339, *to); err != nil {
			return fmt.Errorf("to: %w", err)
		}
	}
	if !r.From.IsZero() && !r.To.IsZero() && !r.From.Before(r.To) {
		return fmt.Errorf("from must be before to")
	}
	f, err := os.Open(*trades)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = closeErr
		}
	}()
	events, err := analytics.ParseTrades(f)
	if err != nil {
		return err
	}
	rows, err := analytics.Replay(events, *bucket, sessions, r)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "events=%d buckets=%d bucket=%s from=%s to=%s\n",
		len(events), len(rows), bucket.String(), bound(r.From), bound(r.To)); err != nil {
		return err
	}
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "start_utc\tsession\tpair\topens\tcloses\tflips\texec\tone_leg\trescues\tvolume\trealized\trealized_bps\tfail_rate\tlat_avg\tlat_p95\tmu\tsigma\ttick_k\tsteps_peak\toldest_s"); err != nil {
		return err
	}
	for _, b := range rows {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%.4f\t%.6f\t%.4f\t%.4f\t%.1f\t%.1f\t%.4f\t%.4f\t%.4f\t%d\t%.0f\n",
			b.StartUTC, b.Session, b.Pair, b.Opens, b.Closes, b.Flips, b.Executions, b.OneLegFails, b.Rescues,
			float64(b.VolumeUSD), float64(b.RealizedUSD), realizedBps(b), b.FailRate, b.LatencyAvgMs, b.LatencyP95Ms,
			b.MuBps, b.SigmaBps, b.TickK, b.OpenStepsPeak, b.OldestStepAgeS); err != nil {
			return err
		}
	}
	if err := w.Flush(); err != nil {
		return err
	}
	sums := analytics.SessionSummaries(rows, *bucket)
	if len(sums) > 0 {
		if _, err := fmt.Fprintln(out, "\nsession summaries:"); err != nil {
			return err
		}
		w2 := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
		if _, err := fmt.Fprintln(w2, "session\thours\tbuckets\topens\tcloses\tflips\texec\tone_leg\trescues\tvolume\trealized\tbps\tfail_rate\tlat_avg\tmu\tsigma\ttick_k\trescue_cost"); err != nil {
			return err
		}
		for _, s := range sums {
			if _, err := fmt.Fprintf(w2, "%s\t%.1f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%.4f\t%.6f\t%.4f\t%.4f\t%.1f\t%.4f\t%.4f\t%.4f\t%.6f\n",
				s.Session, s.Hours, s.Buckets, s.Opens, s.Closes, s.Flips, s.Executions, s.OneLegFails, s.Rescues,
				s.VolumeUSD, s.RealizedUSD, s.RealizedBps, s.FailRate, s.LatencyAvgMs, s.MuBps, s.SigmaBps, s.TickK, s.RescueCostUSD); err != nil {
				return err
			}
		}
		if err := w2.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func realizedBps(b monitor.BucketMetrics) float64 {
	if float64(b.VolumeUSD) <= 0 {
		return 0
	}
	return float64(b.RealizedUSD) / float64(b.VolumeUSD) * 1e4
}

func bound(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339)
}

func parseSessions(s string) (map[string]string, error) {
	sessions := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return sessions, nil
	}
	for _, part := range strings.Split(s, ",") {
		name, spec, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok || name == "" || !monitor.ValidSessionSpec(strings.TrimSpace(spec)) {
			return nil, fmt.Errorf("invalid session %q (want name:HH:MM-HH:MM)", part)
		}
		sessions[name] = strings.TrimSpace(spec)
	}
	return sessions, nil
}
