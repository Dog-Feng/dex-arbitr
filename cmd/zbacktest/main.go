// Command zbacktest replays a persisted 100ms sample file through the
// strategy decision logic and reports trading behavior and PnL estimates.
// It is offline and read-only; no venue is contacted and no order is placed.
//
// The sample file layout matches store.SampleSink.AddQuote:
//
//	ns, mid, ls, sl, lbid, lask, rbid, rask, refmid, lbidQ, laskQ, rbidQ, raskQ, lTs, rTs
package main

import (
	"bufio"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

type sim struct {
	kind        string
	p           strategy.Params
	steps       []strategy.Step
	nextID      int64
	lastDir     strategy.Dir
	capEngaged  bool
	opens       int
	closes      int
	execs       int
	gross       float64
	fees        float64
	net         float64
	volume      float64
	peakSteps   int
	firstOpen   time.Time
	capAt       time.Time
	lastExec    time.Time
	freezeMax   time.Duration
	capTime     time.Duration
	lastAction  time.Time
	noOpenUntil time.Time
	coolReset   bool
	forced      int
}

func (s *sim) counter() int {
	c := 0
	for _, st := range s.steps {
		c += int(st.Dir)
	}
	return c
}

func (s *sim) syncCap(maxSteps, band int) {
	n := absInt(s.counter())
	if n > s.peakSteps {
		s.peakSteps = n
	}
	if n >= maxSteps {
		s.capEngaged = true
		if s.capAt.IsZero() {
			s.capAt = time.Now()
		}
	}
	if band > 0 && n <= maxSteps-band {
		s.capEngaged = false
	}
}

func (s *sim) apply(act strategy.Action, t time.Time, feeL, feeR float64) {
	switch act.Kind {
	case strategy.Open:
		entryFees := act.Qty * (act.L.Price*feeL + act.R.Price*feeR)
		s.volume += act.Qty * (act.L.Price + act.R.Price)
		s.nextID++
		s.steps = append(s.steps, strategy.Step{
			ID: s.nextID, Dir: act.Dir, Qty: act.Qty, OriginalQty: act.Qty,
			OpenL: act.L.Price, OpenR: act.R.Price, EntryBps: strategy.EntryBps(act.Dir, act.L.Price, act.R.Price),
			EntryFeesUSD: entryFees, OpenedAt: t,
		})
		s.fees += entryFees
		s.net -= entryFees
		s.opens++
		s.execs++
		s.lastDir = act.Dir
	case strategy.Close:
		gross := 0.0
		for i := range s.steps {
			st := &s.steps[i]
			if st.ID != act.StepID {
				continue
			}
			gross = strategy.GrossProfitUSD(*st, act.L.Price, act.R.Price, act.Qty)
			exitFees := act.Qty * (act.L.Price*feeL + act.R.Price*feeR)
			s.gross += gross
			s.fees += exitFees
			s.net += gross - exitFees
			s.volume += act.Qty * (act.L.Price + act.R.Price)
			s.steps = append(s.steps[:i], s.steps[i+1:]...)
			break
		}
		s.closes++
		s.execs++
	}
	s.lastExec = t
	s.lastAction = t
}

func absInt(v int) int {
	if v < 0 {
		return -v
	}
	return v
}

// statPoint is a 1s-downsampled spread observation for multi-window stats.
type statPoint struct {
	t   int64 // unix seconds
	mid float64
}

// sigmaTracker keeps one EWMA dispersion per configured half-life.
type sigmaTracker struct {
	half []float64 // seconds
	mean []float64
	mx2  []float64
	last time.Time
	n    int
}

func newSigmaTracker(half []time.Duration) *sigmaTracker {
	s := &sigmaTracker{}
	for _, h := range half {
		s.half = append(s.half, h.Seconds())
		s.mean = append(s.mean, 0)
		s.mx2 = append(s.mx2, 0)
	}
	return s
}

func (s *sigmaTracker) update(t time.Time, x float64) {
	if s.last.IsZero() {
		s.last = t
		s.n = 1
		for i := range s.mean {
			s.mean[i] = x
			s.mx2[i] = x * x
		}
		return
	}
	dt := t.Sub(s.last).Seconds()
	if dt <= 0 {
		return
	}
	s.n++
	s.last = t
	for i, h := range s.half {
		a := 1 - math.Exp(-dt/h)
		s.mean[i] += a * (x - s.mean[i])
		s.mx2[i] += a * (x*x - s.mx2[i])
	}
}

func (s *sigmaTracker) sigma(i int) float64 {
	v := s.mx2[i] - s.mean[i]*s.mean[i]
	if v < 0 {
		v = 0
	}
	return math.Sqrt(v)
}

func printStats(pts []statPoint, sig *sigmaTracker, rawMin, rawMax float64) {
	if len(pts) == 0 {
		fmt.Println("no samples")
		return
	}
	last := pts[len(pts)-1].t
	sum, minV, maxV := 0.0, math.Inf(1), math.Inf(-1)
	for _, p := range pts {
		sum += p.mid
		if p.mid < minV {
			minV = p.mid
		}
		if p.mid > maxV {
			maxV = p.mid
		}
	}
	fmt.Printf("span=%.2fh samples=%d full: mean=%.2f bps range=[%.2f, %.2f]\n",
		(float64(last-pts[0].t))/3600, len(pts), sum/float64(len(pts)), minV, maxV)
	fmt.Printf("raw 100ms range=[%.2f, %.2f] bps (incl. transient spikes)\n", rawMin, rawMax)
	for _, days := range []int{1, 2, 3, 4, 5} {
		cut := last - int64(days)*86400
		n, sum, minV, maxV := 0, 0.0, math.Inf(1), math.Inf(-1)
		for _, p := range pts {
			if p.t < cut {
				continue
			}
			n++
			sum += p.mid
			if p.mid < minV {
				minV = p.mid
			}
			if p.mid > maxV {
				maxV = p.mid
			}
		}
		if n == 0 {
			fmt.Printf("%dd window: no data\n", days)
			continue
		}
		coverage := float64(n) / float64(days*86400)
		note := ""
		if coverage < 0.9 {
			note = fmt.Sprintf(" (coverage %.0f%%, partial)", coverage*100)
		}
		fmt.Printf("%dd trailing: mean=%.2f bps range=[%.2f, %.2f]%s\n", days, sum/float64(n), minV, maxV, note)
	}
	for i, h := range sig.half {
		fmt.Printf("sigma_long(half=%.0fh)=%.2f bps  mean=%.2f\n", h/3600, sig.sigma(i), sig.mean[i])
	}
}

func main() {
	samples := flag.String("samples", "", "15-column sample file (SampleSink format)")
	kind := flag.String("kind", "volume", "strategy kind: arbitrage | volume | zscore")
	qty := flag.Float64("qty", 0.01, "step quantity")
	maxSteps := flag.Int("steps", 50, "max steps")
	depth := flag.Float64("depth", 3, "depth multiplier")
	volK := flag.Float64("volk", 1, "one-sided sigma buffer")
	openBps := flag.Float64("open", 0, "open_bps")
	closeBps := flag.Float64("close", 0, "close_bps")
	flip := flag.Float64("flip", 1, "flip hysteresis sigma")
	capBand := flag.Int("capband", 2, "capacity band")
	capHyst := flag.Float64("caphyst", 1, "cap hysteresis sigma")
	zOpen := flag.Float64("zopen", 2, "z-score open threshold")
	stepZ := flag.Float64("stepz", 0, "z-score ladder spacing per open step (0 = off)")
	timeStop := flag.Duration("timestop", 0, "simulated forced close after this holding time (0 = off)")
	lossStop := flag.Float64("lossstop", 0, "simulated forced close when a step's gross <= -X bps (0 = off)")
	cool := flag.Duration("cool", 0, "suppress opens for this long after a forced close (0 = off)")
	coolZ := flag.Float64("coolz", 0, "after a forced close, reopen only once |z| falls back to <= X (0 = off)")
	feeL := flag.Float64("feel", 0, "left-leg taker fee rate")
	feeR := flag.Float64("feer", 0.00009, "right-leg taker fee rate")
	window := flag.Duration("window", 2*time.Hour, "sma window")
	warmup := flag.Duration("warmup", time.Hour, "estimator min warmup")
	muHalf := flag.Duration("muhalf", 15*time.Minute, "zscore mean half-life")
	sigmaHalf := flag.Duration("sigmahalf", 3*time.Hour, "zscore dispersion half-life")
	sigmaFloor := flag.Float64("sigmafloor", 0.5, "zscore dispersion floor (bps)")
	interval := flag.Duration("interval", time.Second, "decision cadence")
	stats := flag.Bool("stats", false, "print multi-window statistics instead of a backtest")
	flag.Parse()
	if *samples == "" {
		fmt.Fprintln(os.Stderr, "-samples is required")
		os.Exit(2)
	}

	f, err := os.Open(*samples)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			fmt.Fprintln(os.Stderr, cerr)
		}
	}()

	params := strategy.Params{
		StepQty: *qty, MaxSteps: *maxSteps, OpenBps: *openBps, CloseBps: *closeBps,
		DepthMult: *depth, CloseOrder: "best", VolK: *volK, FeeBps: 0,
		TakerFeeL: *feeL, TakerFeeR: *feeR, MinInventory: 0,
		FlipHysteresisSigma: *flip, CapBand: *capBand, CapHysteresisSigma: *capHyst, ZOpen: *zOpen,
		StepZ: *stepZ,
	}
	strat, err := strategy.New(*kind)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	params = strategy.Parameters(strat, params)

	var est estimator.Estimator
	if *kind == "zscore" {
		est = estimator.NewZScore(estimator.Params{Interval: time.Second, MinWarmup: *warmup,
			MuHalfLife: *muHalf, SigmaHalfLife: *sigmaHalf, SigmaFloor: *sigmaFloor})
	} else {
		est = estimator.NewSMA(estimator.Params{Window: *window, MinWarmup: *warmup, MinCoverage: 0.8, Interval: time.Second})
	}

	s := &sim{kind: *kind, p: params}
	var vol float64
	var volT, volLast time.Time
	var ring [8]float64
	var ringN int
	var volVar float64
	var lastDecision time.Time
	var lastQuote spread.Quote
	lastMid := 0.0
	lastRef := 1.0
	var pts []statPoint
	lastSec := int64(0)
	sig := newSigmaTracker([]time.Duration{3 * time.Hour, 12 * time.Hour, 24 * time.Hour})
	rawMin, rawMax := math.Inf(1), math.Inf(-1)

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	rows := 0
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		p := strings.Split(line, ",")
		if len(p) < 15 {
			continue
		}
		ns, err := strconv.ParseInt(p[0], 10, 64)
		if err != nil {
			continue
		}
		t := time.Unix(0, ns)
		var v [15]float64
		ok := true
		for i := 1; i < 13; i++ {
			v[i], err = strconv.ParseFloat(p[i], 64)
			if err != nil || math.IsNaN(v[i]) || math.IsInf(v[i], 0) {
				ok = false
				break
			}
		}
		if !ok {
			continue
		}
		rows++
		if qm := v[1]; qm < rawMin {
			rawMin = qm
		}
		if qm := v[1]; qm > rawMax {
			rawMax = qm
		}
		q := spread.Quote{
			Mid: v[1], LS: v[2], SL: v[3], RefMid: v[8],
			L: domain.Book{Bids: []domain.Level{{Price: v[4], Size: v[9]}}, Asks: []domain.Level{{Price: v[5], Size: v[10]}}},
			R: domain.Book{Bids: []domain.Level{{Price: v[6], Size: v[11]}}, Asks: []domain.Level{{Price: v[7], Size: v[12]}}},
		}
		est.Update(t, q.Mid)
		// Short-horizon dispersion over ~0.5s increments, EWMA half-life 60s,
		// mirroring the live volatility chain used by arbitrage/volume.
		if volT.IsZero() {
			volT = t
		}
		idx := ringN % len(ring)
		if ringN >= len(ring) {
			old := ring[idx]
			inc := q.Mid - old
			dt := t.Sub(volLast).Seconds()
			if dt > 0 {
				a := 1 - math.Exp(-dt/60)
				volVar += a * (inc*inc - volVar)
			}
			vol = math.Sqrt(volVar)
		}
		ring[idx] = q.Mid
		ringN++
		volLast = t
		volReady := ringN >= len(ring)
		lastQuote = q
		lastMid, lastRef = q.Mid, q.RefMid
		if sec := t.Unix(); sec > lastSec {
			pts = append(pts, statPoint{t: sec, mid: q.Mid})
			lastSec = sec
		}
		sig.update(t, q.Mid)
		if *stats {
			continue
		}
		forced := 0
		for i := 0; i < len(s.steps); i++ {
			st := s.steps[i]
			ol, or := st.Dir.LegSides()
			ls, rs := ol.Opposite(), or.Opposite()
			cl, cr := lastQuote.L.Bids[0].Price, lastQuote.R.Bids[0].Price
			if ls == domain.Buy {
				cl = lastQuote.L.Asks[0].Price
			}
			if rs == domain.Buy {
				cr = lastQuote.R.Asks[0].Price
			}
			gross := strategy.GrossProfitUSD(st, cl, cr, st.Qty)
			grossBps := gross / st.Qty / ((st.OpenL + st.OpenR) / 2) * 1e4
			if (*timeStop > 0 && t.Sub(st.OpenedAt) >= *timeStop) || (*lossStop > 0 && grossBps <= -*lossStop) {
				act := strategy.Action{Kind: strategy.Close, Dir: st.Dir, StepID: st.ID, Qty: st.Qty,
					L: strategy.Leg{Side: ls, Price: cl}, R: strategy.Leg{Side: rs, Price: cr}}
				s.apply(act, t, *feeL, *feeR)
				s.syncCap(*maxSteps, *capBand)
				if *cool > 0 && t.Add(*cool).After(s.noOpenUntil) {
					s.noOpenUntil = t.Add(*cool)
				}
				if *coolZ > 0 {
					s.coolReset = true
				}
				forced++
				s.forced++
				i--
			}
		}

		if lastDecision.IsZero() || t.Sub(lastDecision) >= *interval {
			lastDecision = t
			mu := est.Mean()
			muReady := estimator.ReadyAt(est, t)
			v := vol
			if *kind == "zscore" {
				if lv, ok := est.(estimator.LongVol); ok {
					v = lv.LongVol()
				}
			}
			st := strategy.State{Mode: strategy.ModeRunning, Steps: s.steps, LastDir: s.lastDir, CapEngaged: s.capEngaged}
			if !muReady || !volReady {
				st.Mode = strategy.ModeWarmup
			}
			act := strat.Decide(s.p, st, lastQuote, mu, muReady, v)
			if act.Kind == strategy.Open && t.Before(s.noOpenUntil) {
				act = strategy.Action{Kind: strategy.None, Reason: "cooling down after a forced close"}
			}
			if act.Kind == strategy.Open && s.coolReset {
				act = strategy.Action{Kind: strategy.None, Reason: "waiting for z to reset after a forced close"}
			}
			if s.coolReset && v > 0 {
				zmax := math.Max(math.Abs((lastQuote.LS-mu)/v), math.Abs((lastQuote.SL+mu)/v))
				if zmax <= *coolZ {
					s.coolReset = false
				}
			}
			if act.Kind != strategy.None {
				if s.firstOpen.IsZero() && act.Kind == strategy.Open {
					s.firstOpen = t
				}
				s.apply(act, t, *feeL, *feeR)
				s.syncCap(*maxSteps, *capBand)
			} else if !s.lastAction.IsZero() {
				if d := t.Sub(s.lastAction); d > s.freezeMax {
					s.freezeMax = d
				}
			}
			if s.capEngaged {
				s.capTime += *interval
			}
		}
	}
	if err := sc.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *stats {
		printStats(pts, sig, rawMin, rawMax)
		return
	}

	fmt.Printf("kind=%s rows=%d\n", *kind, rows)
	fmt.Printf("first_open=%s last_exec=%s cap_at=%s (UTC)\n",
		s.firstOpen.UTC().Format("15:04:05"), s.lastExec.UTC().Format("15:04:05"), s.capAt.UTC().Format("15:04:05"))
	fmt.Printf("opens=%d closes=%d executions=%d steps=%d peak=%d\n", s.opens, s.closes, s.execs, len(s.steps), s.peakSteps)
	fmt.Printf("gross=%.6f fees=%.6f net=%.6f\n", s.gross, s.fees, s.net)
	fmt.Printf("volume_usd=%.2f\n", s.volume)
	fmt.Printf("freeze_max=%s cap_time=%s forced=%d\n", s.freezeMax.Round(time.Second), s.capTime.Round(time.Second), s.forced)
	if len(s.steps) > 0 {
		minB, maxB := math.Inf(1), math.Inf(-1)
		unrealized := 0.0
		for _, st := range s.steps {
			if st.EntryBps < minB {
				minB = st.EntryBps
			}
			if st.EntryBps > maxB {
				maxB = st.EntryBps
			}
			// DirSL steps earn the spread's rise above their entry; DirLS the
			// fall below it.
			if st.Dir == strategy.DirSL {
				unrealized += st.Qty * lastRef * (lastMid + st.EntryBps) / 1e4
			} else {
				unrealized += st.Qty * lastRef * (st.EntryBps - lastMid) / 1e4
			}
		}
		fmt.Printf("frozen_entries: n=%d span=%.2f bps (%.2f..%.2f) unrealized_end=%.3f usd\n",
			len(s.steps), maxB-minB, minB, maxB, unrealized)
	}
}
