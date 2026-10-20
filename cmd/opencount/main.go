// Command opencount replays persisted 100ms book samples through the same
// mean/volatility estimators the live actor uses, counts how often the opening
// conditions qualify, and simulates how many actions different order-interval
// settings would admit. It is offline and read-only.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("opencount", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	samples := flags.String("samples", "state/samples/SPY_lighter_rhlighter.jsonl", "15-column sample file")
	volK := flags.Float64("vol_k", 1.0, "one-sided sigma buffer")
	stepQty := flags.Float64("step_qty", 0.02, "step quantity")
	depthMult := flags.Float64("depth_mult", 3.0, "top-book depth multiple")
	curInt := flags.Duration("cur_interval", 1500*time.Millisecond, "current average action interval")
	newInt := flags.Duration("new_interval", 500*time.Millisecond, "proposed action interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *samples == "" || flags.NArg() != 0 {
		return fmt.Errorf("usage: opencount -samples <file>")
	}
	f, err := os.Open(*samples)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintln(os.Stderr, closeErr)
		}
	}()

	sma := estimator.NewSMA(estimator.Params{Window: 2 * time.Hour, MinWarmup: time.Hour, MinCoverage: 0.8, Interval: time.Second})
	vol := volatility.New(volatility.Params{Interval: 100 * time.Millisecond, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})

	needDepth := *stepQty * *depthMult
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var qualify []time.Time
	var rows, goodRows int
	for sc.Scan() {
		rows++
		p := strings.Split(sc.Text(), ",")
		if len(p) < 15 {
			continue
		}
		ns, err := strconv.ParseInt(p[0], 10, 64)
		if err != nil {
			continue
		}
		t := time.Unix(0, ns)
		mid, e1 := strconv.ParseFloat(p[1], 64)
		ls, e2 := strconv.ParseFloat(p[2], 64)
		sl, e3 := strconv.ParseFloat(p[3], 64)
		lbidQ, e4 := strconv.ParseFloat(p[8], 64)
		laskQ, e5 := strconv.ParseFloat(p[9], 64)
		rbidQ, e6 := strconv.ParseFloat(p[10], 64)
		raskQ, e7 := strconv.ParseFloat(p[11], 64)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil {
			continue
		}
		goodRows++
		sma.Update(t, mid)
		vol.AddAt(t, mid)
		if !sma.ReadyAt(t) || !vol.ReadyAt(t) {
			continue
		}
		mu := sma.MeanAt(t)
		sigma := vol.VolAt(t)
		if math.IsNaN(mu) || math.IsNaN(sigma) {
			continue
		}
		thr := *volK * sigma
		if ls-mu >= thr && rbidQ >= needDepth && laskQ >= needDepth ||
			sl+mu >= thr && lbidQ >= needDepth && raskQ >= needDepth {
			qualify = append(qualify, t)
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	sort.Slice(qualify, func(i, j int) bool { return qualify[i].Before(qualify[j]) })

	perMin := map[int64]int{}
	maxPerMin := 0
	for _, t := range qualify {
		k := t.Unix() / 60
		perMin[k]++
		if perMin[k] > maxPerMin {
			maxPerMin = perMin[k]
		}
	}
	sim := func(spacing time.Duration) int {
		n, last := 0, time.Time{}
		for _, t := range qualify {
			if last.IsZero() || t.Sub(last) >= spacing {
				n++
				last = t
			}
		}
		return n
	}
	cur := sim(*curInt)
	pro := sim(*newInt)
	if _, err := fmt.Fprintf(out, "samples=%d parsed=%d qualifying_ticks=%d active_minutes=%d max_per_minute=%d\n",
		rows, goodRows, len(qualify), len(perMin), maxPerMin); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(out, "actions @%s=%d  actions @%s=%d  gain=%.1f%%\n",
		curInt.String(), cur, newInt.String(), pro, float64(pro-cur)/float64(cur+1)*100); err != nil {
		return err
	}
	return nil
}
