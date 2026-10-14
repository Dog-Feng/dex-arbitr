// Command volcal analyzes persisted spread samples for calibration: it prints
// a volatility signature plot and the empirical distribution of horizon moves.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		slog.Error("volcal", "err", err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	fs := flag.NewFlagSet("volcal", flag.ContinueOnError)
	path := fs.String("f", "", "samples file (4 legacy or 15 quote columns)")
	interval := fs.Duration("interval", 100*time.Millisecond, "expected sample interval; gaps over 1.5 intervals are excluded")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*path) == "" {
		return fmt.Errorf("usage: volcal -f <samples file> [-interval 100ms]")
	}
	if len(fs.Args()) != 0 {
		return fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if *interval <= 0 || *interval > time.Duration(math.MaxInt64)-*interval/2 {
		return fmt.Errorf("interval must be positive and its gap limit must fit a duration")
	}
	ts, mid, err := load(*path)
	if err != nil {
		return err
	}
	if len(ts) == 0 {
		return fmt.Errorf("no samples loaded")
	}
	return report(output, ts, mid, *interval)
}

func load(path string) (ts []int64, mid []float64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("open samples: %w", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close samples: %w", closeErr))
		}
	}()
	return readSamples(f)
}

func readSamples(r io.Reader) ([]int64, []float64, error) {
	var ts []int64
	var mid []float64
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		p := strings.Split(sc.Text(), ",")
		if len(p) != 4 && len(p) != 15 {
			return nil, nil, fmt.Errorf("sample line %d: expected 4 or 15 columns, got %d", line, len(p))
		}
		t, err := strconv.ParseInt(strings.TrimSpace(p[0]), 10, 64)
		if err != nil {
			return nil, nil, fmt.Errorf("sample line %d timestamp: %w", line, err)
		}
		if len(ts) > 0 && t <= ts[len(ts)-1] {
			return nil, nil, fmt.Errorf("sample line %d: timestamps must strictly increase", line)
		}
		var m float64
		for i := 1; i < len(p); i++ {
			if i >= 13 { // the quote format ends in the two book timestamps
				if _, err := strconv.ParseInt(strings.TrimSpace(p[i]), 10, 64); err != nil {
					return nil, nil, fmt.Errorf("sample line %d column %d: %w", line, i+1, err)
				}
				continue
			}
			v, err := strconv.ParseFloat(strings.TrimSpace(p[i]), 64)
			if err != nil {
				return nil, nil, fmt.Errorf("sample line %d column %d: %w", line, i+1, err)
			}
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, nil, fmt.Errorf("sample line %d column %d: value must be finite", line, i+1)
			}
			if i == 1 {
				m = v
			}
		}
		ts = append(ts, t)
		mid = append(mid, m)
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read samples after line %d: %w", line, err)
	}
	return ts, mid, nil
}

func report(output io.Writer, ts []int64, mid []float64, interval time.Duration) error {
	maxGap := int64(interval + interval/2)
	gaps := 0
	for i := 1; i < len(ts); i++ {
		if elapsed(ts[i-1], ts[i]) > uint64(maxGap) {
			gaps++
		}
	}
	if _, err := fmt.Fprintf(output, "samples=%d span=%s interval=%s gaps=%d (over %s excluded)\n",
		len(ts), timeSpan(ts), interval, gaps, time.Duration(maxGap)); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(output, "== signature plot (RMS mid-spread moves, first sample at least the horizon ahead) =="); err != nil {
		return err
	}
	for _, ms := range []int64{50, 100, 250, 500, 1000} {
		h := ms * int64(time.Millisecond)
		var incs []float64
		if h >= int64(interval) {
			var err error
			incs, err = increments(ts, mid, h, maxGap)
			if err != nil {
				return err
			}
		}
		if len(incs) == 0 {
			if _, err := fmt.Fprintf(output, "  %5dms: unavailable (0 usable increments)\n", ms); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(output, "  %5dms: %.4f bps (increments=%d)\n", ms, rms(incs), len(incs)); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(output, "== 500ms increment distribution =="); err != nil {
		return err
	}
	var incs []float64
	if interval <= 500*time.Millisecond {
		var err error
		incs, err = increments(ts, mid, int64(500*time.Millisecond), maxGap)
		if err != nil {
			return err
		}
	}
	if len(incs) == 0 {
		_, err := fmt.Fprintln(output, "  unavailable (0 usable increments)")
		return err
	}
	if _, err := fmt.Fprintf(output, "  increments=%d sigma=%.4f bps, mean|d|=%.4f bps\n", len(incs), rms(incs), meanAbs(incs)); err != nil {
		return err
	}
	for _, q := range []float64{50, 90, 95, 97.5, 99} {
		if _, err := fmt.Fprintf(output, "  p%4.1f: %+.4f bps\n", q, quantile(incs, q/100)); err != nil {
			return err
		}
	}
	return nil
}

// elapsed avoids signed subtraction overflow for a historical timestamp series
// spanning both sides of the Unix epoch. Input timestamps are strictly ordered.
func elapsed(start, end int64) uint64 { return uint64(end) - uint64(start) }

func timeSpan(ts []int64) string {
	return fmt.Sprintf("%.0fs", float64(elapsed(ts[0], ts[len(ts)-1]))/1e9)
}

// increments returns signed moves to the first sample at least h ahead within
// one uninterrupted segment. It never uses a long data gap as a short move.
func increments(ts []int64, mid []float64, h, maxGap int64) ([]float64, error) {
	if len(ts) != len(mid) || h <= 0 || maxGap <= 0 {
		return nil, fmt.Errorf("invalid increment parameters")
	}
	out := make([]float64, 0, len(ts))
	for start := 0; start < len(ts); {
		end := start + 1
		for end < len(ts) && elapsed(ts[end-1], ts[end]) <= uint64(maxGap) {
			end++
		}
		j := start + 1
		for i := start; i < end; i++ {
			if j <= i {
				j = i + 1
			}
			for j < end && elapsed(ts[i], ts[j]) < uint64(h) {
				j++
			}
			if j >= end {
				break
			}
			d := mid[j] - mid[i]
			if math.IsNaN(d) || math.IsInf(d, 0) {
				return nil, fmt.Errorf("increment at sample %d is not finite", i+1)
			}
			out = append(out, d)
		}
		start = end
	}
	return out, nil
}

func rms(xs []float64) float64 {
	scale := 0.0
	for _, x := range xs {
		scale = math.Max(scale, math.Abs(x))
	}
	if scale == 0 {
		return 0
	}
	sum := 0.0
	for _, x := range xs {
		sum += (x / scale) * (x / scale)
	}
	return scale * math.Sqrt(sum/float64(len(xs)))
}

func meanAbs(xs []float64) float64 {
	sum := 0.0
	for _, x := range xs {
		sum += math.Abs(x) / float64(len(xs))
	}
	return sum
}

func quantile(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(math.Round(p * float64(len(s)-1)))
	return s[i]
}
