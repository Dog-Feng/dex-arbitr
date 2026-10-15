// Package backtest replays the pure strategy offline with ideal top-book fills.
// It does not model IOC failures, rescue, execution latency, funding or risk.
package backtest

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// Sample contains four legacy spread columns or a recorded book pair. Legacy
// rows cannot recover actual prices, depth or book freshness.
type Sample struct {
	Ts          int64
	Mid, LS, SL float64
	L, R        domain.Book
	RefMid      float64
}

type Params struct {
	StepQty                            float64
	MaxSteps                           int
	DepthMult, OpenBps, CloseBps, VolK float64
	FeeBps                             float64 // equal per-fill fee-rate proxy when per-leg rates are absent
	TakerFeeL, TakerFeeR               float64
	Estimator                          estimator.Params
	Volatility                         volatility.Params
	DecisionInterval                   time.Duration
	MaxBookAge, MaxVolBookAge          time.Duration
	LegacyRefPrice                     float64 // fixed synthetic USD reference, default 100
	AllowGaps                          bool    // gaps restart sampling; false rejects incomplete series
}

// Result separates closed-trade PnL from terminal inventory liquidation marks.
// NetBps sums per-trip net returns using each step's fixed entry reference.
type Result struct {
	RoundTrips, Opens      int
	NetBps, AvgBps         float64
	RealizedUSD            float64
	UnrealizedUSD          float64 // open inventory, net of entry and estimated exit fees
	MTMUSD                 float64 // realized plus unrealized, valid only when MTMValid
	MTMValid               bool
	EndInventory           int
	EndQty                 float64
	Approximate            bool // a legacy row used synthetic prices/depth
	GapCount, SkippedBooks int
	MeanReady, VolReady    bool
	MeanSamples            int
	FeeModel               string
	Error                  string // compatibility Replay reports validation failures here
}

// Load rejects malformed, nonfinite, unsorted and empty files. New rows have
// ts,mid,ls,sl,Lbid,Lask,Rbid,Rask,ref,LbidQty,LaskQty,RbidQty,RaskQty,LTs,RTs.
func Load(path string) (samples []Sample, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open samples: %w", err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return readSamples(f)
}

func readSamples(r io.Reader) ([]Sample, error) {
	var out []Sample
	sc := bufio.NewScanner(r)
	line := 0
	for sc.Scan() {
		line++
		cols := strings.Split(sc.Text(), ",")
		if len(cols) != 4 && len(cols) != 15 {
			return nil, fmt.Errorf("samples line %d: want 4 or 15 columns, got %d", line, len(cols))
		}
		ts, err := strconv.ParseInt(strings.TrimSpace(cols[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("samples line %d timestamp: %w", line, err)
		}
		values := make([]float64, len(cols)-1)
		lastFloat := len(cols)
		if len(cols) == 15 {
			lastFloat = 13
		}
		for i := 1; i < lastFloat; i++ {
			v, err := strconv.ParseFloat(strings.TrimSpace(cols[i]), 64)
			if err != nil || !finite(v) {
				return nil, fmt.Errorf("samples line %d column %d: invalid finite number %q", line, i+1, cols[i])
			}
			values[i-1] = v
		}
		s := Sample{Ts: ts, Mid: values[0], LS: values[1], SL: values[2]}
		if len(cols) == 15 {
			lt, err := strconv.ParseInt(strings.TrimSpace(cols[13]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("samples line %d left timestamp: %w", line, err)
			}
			rt, err := strconv.ParseInt(strings.TrimSpace(cols[14]), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("samples line %d right timestamp: %w", line, err)
			}
			s.L = domain.Book{Ts: time.Unix(0, lt), Bids: []domain.Level{{Price: values[3], Size: values[8]}}, Asks: []domain.Level{{Price: values[4], Size: values[9]}}}
			s.R = domain.Book{Ts: time.Unix(0, rt), Bids: []domain.Level{{Price: values[5], Size: values[10]}}, Asks: []domain.Level{{Price: values[6], Size: values[11]}}}
			s.RefMid = values[7]
		}
		if err := validateSample(s); err != nil {
			return nil, fmt.Errorf("samples line %d: %w", line, err)
		}
		if len(out) > 0 && ts <= out[len(out)-1].Ts {
			return nil, fmt.Errorf("samples line %d: timestamps must strictly increase", line)
		}
		if len(out) > 0 && legacy(s) != legacy(out[0]) {
			return nil, fmt.Errorf("samples line %d: cannot mix legacy proxy and recorded book prices", line)
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read samples: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("samples are empty")
	}
	return out, nil
}

func legacy(s Sample) bool {
	return len(s.L.Bids) == 0 && len(s.L.Asks) == 0 && len(s.R.Bids) == 0 && len(s.R.Asks) == 0 && s.RefMid == 0
}

func validateSample(s Sample) error {
	if !finite(s.Mid) || !finite(s.LS) || !finite(s.SL) || time.Unix(0, s.Ts).IsZero() {
		return fmt.Errorf("invalid spread values or timestamp")
	}
	if legacy(s) {
		if s.LS+s.SL > 1e-8 || math.Abs(s.LS) >= 20000 || math.Abs(s.SL) >= 20000 {
			return fmt.Errorf("legacy spreads cannot form positive uncrossed proxy prices")
		}
		if math.Abs(s.Mid-(s.LS-s.SL)/2) > 1e-7*math.Max(1, math.Abs(s.Mid)) {
			return fmt.Errorf("legacy mid disagrees with executable spreads")
		}
		return nil
	}
	q, ok := spread.Compute(s.L, s.R)
	if !ok || s.L.Ts.IsZero() || s.R.Ts.IsZero() || s.L.Ts.After(time.Unix(0, s.Ts)) || s.R.Ts.After(time.Unix(0, s.Ts)) {
		return fmt.Errorf("invalid book pair or future book timestamp")
	}
	for _, pair := range [][2]float64{{q.Mid, s.Mid}, {q.LS, s.LS}, {q.SL, s.SL}, {q.RefMid, s.RefMid}} {
		if !finite(pair[1]) || math.Abs(pair[0]-pair[1]) > 1e-7*math.Max(1, math.Abs(pair[0])) {
			return fmt.Errorf("recorded spread/reference disagrees with book prices")
		}
	}
	return nil
}

func normalize(p Params) (Params, error) {
	if !finite(p.StepQty) || p.StepQty <= 0 || p.MaxSteps <= 0 || !finite(p.OpenBps) || !finite(p.CloseBps) || !finite(p.VolK) || p.VolK < 0 || !finite(p.FeeBps) || p.FeeBps < 0 || !finite(p.TakerFeeL) || p.TakerFeeL < 0 || !finite(p.TakerFeeR) || p.TakerFeeR < 0 {
		return p, fmt.Errorf("invalid replay quantity, steps, thresholds or fees")
	}
	if p.DepthMult == 0 {
		p.DepthMult = 2
	}
	if p.LegacyRefPrice == 0 {
		p.LegacyRefPrice = 100
	}
	if !finite(p.DepthMult) || p.DepthMult <= 0 || !finite(p.LegacyRefPrice) || p.LegacyRefPrice <= 0 {
		return p, fmt.Errorf("depth multiplier and legacy reference must be positive and finite")
	}
	if p.Estimator.Interval == 0 {
		p.Estimator.Interval = time.Second
	}
	if p.Estimator.Window == 0 {
		p.Estimator.Window = 2 * time.Hour
	}
	if p.Estimator.MinWarmup == 0 {
		p.Estimator.MinWarmup = time.Hour
	}
	if p.Estimator.MinCoverage == 0 {
		p.Estimator.MinCoverage = 0.8
	}
	if p.Volatility.Interval == 0 {
		p.Volatility.Interval = 100 * time.Millisecond
	}
	if p.Volatility.Horizon == 0 {
		p.Volatility.Horizon = 500 * time.Millisecond
	}
	if p.Volatility.HalfLife == 0 {
		p.Volatility.HalfLife = time.Minute
	}
	if p.DecisionInterval == 0 {
		p.DecisionInterval = 200 * time.Millisecond
	}
	if p.MaxBookAge == 0 {
		p.MaxBookAge = 10 * time.Second
	}
	if p.MaxVolBookAge == 0 {
		p.MaxVolBookAge = 50 * time.Millisecond
	}
	if p.Estimator.Interval <= 0 || p.Estimator.Window < p.Estimator.Interval || p.Estimator.MinWarmup < 0 || p.Estimator.MinWarmup > p.Estimator.Window || !finite(p.Estimator.MinCoverage) || p.Estimator.MinCoverage <= 0 || p.Estimator.MinCoverage > 1 || p.Volatility.Interval <= 0 || p.Volatility.Horizon < p.Volatility.Interval || p.Volatility.HalfLife <= 0 || p.DecisionInterval <= 0 || p.MaxBookAge <= 0 || p.MaxVolBookAge <= 0 {
		return p, fmt.Errorf("invalid replay sampling or freshness parameters")
	}
	if maxSpan := (p.Estimator.Window / p.Estimator.Interval) * p.Estimator.Interval; p.Estimator.MinWarmup > maxSpan {
		return p, fmt.Errorf("estimator.min_warmup (%s) is unreachable: a %s window sampled every %s can span at most %s", p.Estimator.MinWarmup, p.Estimator.Window, p.Estimator.Interval, maxSpan)
	}
	return p, nil
}

// Replay is retained for compatibility and reports validation failures in Error.
// New callers should use ReplayChecked.
func Replay(samples []Sample, p Params) Result {
	r, err := ReplayChecked(samples, p)
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

// ReplayChecked uses actual historical timestamps, 1s mean sampling, 100ms
// volatility and 200ms decisions by default. Missing ticks are never invented.
func ReplayChecked(samples []Sample, p Params) (Result, error) {
	p, err := normalize(p)
	if err != nil {
		return Result{}, err
	}
	if len(samples) == 0 {
		return Result{}, fmt.Errorf("samples are empty")
	}
	for i, s := range samples {
		if err := validateSample(s); err != nil {
			return Result{}, fmt.Errorf("sample %d: %w", i+1, err)
		}
		if i > 0 && s.Ts <= samples[i-1].Ts {
			return Result{}, fmt.Errorf("sample %d: timestamps must strictly increase", i+1)
		}
		if legacy(s) != legacy(samples[0]) {
			return Result{}, fmt.Errorf("sample %d: cannot mix legacy proxy and recorded book prices", i+1)
		}
	}
	mu := estimator.NewSMA(p.Estimator)
	vol := volatility.New(p.Volatility)
	feeL, feeR := p.TakerFeeL, p.TakerFeeR
	res := Result{FeeModel: "per-leg taker rates"}
	if feeL == 0 && feeR == 0 {
		feeL, feeR = p.FeeBps/40000, p.FeeBps/40000
		res.FeeModel = "equal per-fill rate proxy from fee_bps"
	}
	sp := strategy.Params{StepQty: p.StepQty, MaxSteps: p.MaxSteps, OpenBps: p.OpenBps, CloseBps: p.CloseBps, DepthMult: p.DepthMult, CloseOrder: "best", VolK: p.VolK, TakerFeeL: feeL, TakerFeeR: feeR}
	st := strategy.State{Mode: strategy.ModeRunning}
	var nextMean, nextVol, nextDecision time.Time
	var finalQuote spread.Quote
	finalValid := false
	// Persisted ticker timestamps jitter around their scheduled phase. Keep
	// that phase fixed; a slightly early row must not become a missing tick.
	tolerance := p.Volatility.Interval / 2
	for i, s := range samples {
		now := time.Unix(0, s.Ts)
		if i > 0 && now.Sub(time.Unix(0, samples[i-1].Ts)) > p.Volatility.Interval+p.Volatility.Interval/2 {
			if !p.AllowGaps {
				return Result{}, fmt.Errorf("sample %d: sampling gap %s exceeds %s; explicitly allow gaps to restart warmup", i+1, now.Sub(time.Unix(0, samples[i-1].Ts)), p.Volatility.Interval+p.Volatility.Interval/2)
			}
			res.GapCount++
			vol.Reset()
			nextMean, nextVol, nextDecision = time.Time{}, time.Time{}, time.Time{}
		}
		q := sampleQuote(s, p.LegacyRefPrice)
		res.Approximate = res.Approximate || legacy(s)
		fresh := booksFresh(q, now, p.MaxBookAge, false)
		finalQuote, finalValid = q, fresh
		if !fresh {
			res.SkippedBooks++
			continue
		}
		if sampleDue(now, &nextMean, p.Estimator.Interval, tolerance) {
			mu.Update(now, s.Mid)
		}
		if sampleDue(now, &nextVol, p.Volatility.Interval, tolerance) {
			if booksFresh(q, now, p.MaxVolBookAge, true) {
				vol.AddAt(now, s.Mid)
			}
		}
		if !sampleDue(now, &nextDecision, p.DecisionInterval, tolerance) {
			continue
		}
		stats := mu.StatsAt(now)
		act := (strategy.Arbitrage{}).Decide(sp, st, q, stats.Mean, stats.Ready, vol.VolAt(now))
		switch act.Kind {
		case strategy.Open:
			res.Opens++
			st.Steps = append(st.Steps, strategy.Step{ID: int64(res.Opens), Dir: act.Dir, Qty: act.Qty, OriginalQty: act.Qty, EntryBps: strategy.EntryBps(act.Dir, act.L.Price, act.R.Price), OpenL: act.L.Price, OpenR: act.R.Price, EntryFeesUSD: act.Qty * (act.L.Price*feeL + act.R.Price*feeR), OpenedAt: now, PriceEstimated: legacy(s)})
		case strategy.Close:
			for j, step := range st.Steps {
				if step.ID != act.StepID {
					continue
				}
				net := strategy.GrossProfitUSD(step, act.L.Price, act.R.Price, step.Qty) - strategy.RoundTripFeeUSD(step, act.L.Price, act.R.Price, step.Qty, feeL, feeR)
				res.RealizedUSD += net
				res.NetBps += net / (step.Qty * (step.OpenL + step.OpenR) / 2) * 1e4
				res.RoundTrips++
				st.Steps = append(st.Steps[:j], st.Steps[j+1:]...)
				break
			}
		}
	}
	if res.RoundTrips > 0 {
		res.AvgBps = res.NetBps / float64(res.RoundTrips)
	}
	end := time.Unix(0, samples[len(samples)-1].Ts)
	stats := mu.StatsAt(end)
	res.MeanReady, res.MeanSamples, res.VolReady = stats.Ready, stats.Samples, vol.ReadyAt(end)
	res.EndInventory = len(st.Steps)
	res.MTMValid = finalValid || res.EndInventory == 0
	for _, step := range st.Steps {
		res.EndQty += step.Qty
		if !finalValid {
			continue
		}
		l, r := finalQuote.L.Bids[0].Price, finalQuote.R.Asks[0].Price
		if step.Dir == strategy.DirSL {
			l, r = finalQuote.L.Asks[0].Price, finalQuote.R.Bids[0].Price
		}
		res.UnrealizedUSD += strategy.GrossProfitUSD(step, l, r, step.Qty) - strategy.RoundTripFeeUSD(step, l, r, step.Qty, feeL, feeR)
	}
	if res.MTMValid {
		res.MTMUSD = res.RealizedUSD + res.UnrealizedUSD
	}
	return res, nil
}

func sampleQuote(s Sample, ref float64) spread.Quote {
	if !legacy(s) {
		return spread.Quote{Mid: s.Mid, LS: s.LS, SL: s.SL, RefMid: s.RefMid, L: s.L, R: s.R}
	}
	// Legacy rows define only a constant-reference proxy. A tiny width permits
	// locked legacy spreads without pretending to know real depth.
	pad := ref * 1e-12
	l := domain.Book{Ts: time.Unix(0, s.Ts), Bids: []domain.Level{{Price: ref*(1+s.SL/20000) - pad, Size: 1e18}}, Asks: []domain.Level{{Price: ref*(1-s.LS/20000) + pad, Size: 1e18}}}
	r := domain.Book{Ts: l.Ts, Bids: []domain.Level{{Price: ref*(1+s.LS/20000) - pad, Size: 1e18}}, Asks: []domain.Level{{Price: ref*(1-s.SL/20000) + pad, Size: 1e18}}}
	return spread.Quote{Mid: s.Mid, LS: s.LS, SL: s.SL, RefMid: ref, L: l, R: r}
}

func booksFresh(q spread.Quote, now time.Time, age time.Duration, checkSkew bool) bool {
	if now.Sub(q.L.Ts) > age || now.Sub(q.R.Ts) > age || q.L.Ts.After(now) || q.R.Ts.After(now) {
		return false
	}
	skew := q.L.Ts.Sub(q.R.Ts)
	return !checkSkew || (skew <= age && -skew <= age)
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func sampleDue(now time.Time, next *time.Time, interval, tolerance time.Duration) bool {
	if next.IsZero() {
		*next = now
	}
	if tolerance > interval/2 {
		tolerance = interval / 2
	}
	through := now.Add(tolerance)
	if through.Before(*next) {
		return false
	}
	steps := through.Sub(*next)/interval + 1
	*next = next.Add(steps * interval)
	return true
}
