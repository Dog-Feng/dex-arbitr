// Command volcal analyzes persisted spread samples for calibration: it prints
// a volatility signature plot (realized vol at several horizons) and the
// empirical distribution of horizon increments, used to pick the coverage
// coefficient k and to confirm the sampling interval is above the noise floor.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	path := flag.String("f", "", "samples file (lines: unixNano,midBps,lsBps,slBps)")
	flag.Parse()
	if *path == "" {
		fmt.Fprintln(os.Stderr, "usage: volcal -f <samples file>")
		os.Exit(2)
	}
	ts, mid := load(*path)
	if len(ts) == 0 {
		fmt.Fprintln(os.Stderr, "no samples loaded")
		os.Exit(1)
	}
	fmt.Printf("samples=%d span=%s\n", len(ts), (timeSpan(ts)))
	fmt.Println("== signature plot (realized vol of mid spread, bps) ==")
	for _, ms := range []int64{50, 100, 250, 500, 1000} {
		fmt.Printf("  %5dms: %.4f\n", ms, horizonVol(ts, mid, ms*1e6))
	}
	incs := increments(ts, mid, 500*1e6)
	fmt.Println("== 500ms increment distribution ==")
	fmt.Printf("  sigma=%.4f bps, mean|d|=%.4f bps\n", horizonVol(ts, mid, 500*1e6), meanAbs(incs))
	for _, q := range []float64{50, 90, 95, 97.5, 99} {
		fmt.Printf("  p%4.1f: %+.4f bps\n", q, quantile(incs, q/100))
	}
}

func load(path string) ([]int64, []float64) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()
	var ts []int64
	var mid []float64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		if len(p) < 2 {
			continue
		}
		t, err1 := strconv.ParseInt(p[0], 10, 64)
		m, err2 := strconv.ParseFloat(p[1], 64)
		if err1 != nil || err2 != nil {
			continue
		}
		ts = append(ts, t)
		mid = append(mid, m)
	}
	return ts, mid
}

func timeSpan(ts []int64) string {
	d := float64(ts[len(ts)-1]-ts[0]) / 1e9
	return fmt.Sprintf("%.0fs", d)
}

// horizonVol estimates the volatility of the H-horizon increment (bps) using
// the first sample at least H ahead of each sample.
func horizonVol(ts []int64, mid []float64, h int64) float64 {
	var sum, n float64
	for i := range ts {
		j := sort.Search(len(ts), func(k int) bool { return ts[k]-ts[i] >= h })
		if j >= len(ts) {
			break
		}
		d := mid[j] - mid[i]
		sum += d * d
		n++
	}
	if n == 0 {
		return 0
	}
	return math.Sqrt(sum / n)
}

// increments returns the H-horizon signed increments.
func increments(ts []int64, mid []float64, h int64) []float64 {
	out := make([]float64, 0, len(ts))
	for i := range ts {
		j := sort.Search(len(ts), func(k int) bool { return ts[k]-ts[i] >= h })
		if j >= len(ts) {
			break
		}
		out = append(out, mid[j]-mid[i])
	}
	return out
}

func meanAbs(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	var s float64
	for _, x := range xs {
		s += math.Abs(x)
	}
	return s / float64(len(xs))
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
