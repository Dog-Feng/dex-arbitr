package scan

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// Lens is one evaluation view over the same pair: a strategy kind plus its own
// estimator and threshold parameters. Parameter sweeps are expressed as
// multiple lenses sharing one book stream, so the same market instant can be
// scored under arbitrage, volume and z-score criteria side by side.
//
// This package is strictly read-only: lens evaluation never submits orders
// and never mutates the trading engine's decision inputs.
type Lens struct {
	Name          string // unique label, echoed in every result line
	Strategy      string // arbitrage | volume | zscore
	EstimatorKind string // sma | zscore ("" defaults to sma)
	Estimator     estimator.Params
	OpenBps       float64 // arbitrage threshold above fees
	VolK          float64 // one-sided sigma buffer
	ZOpen         float64 // z-score open threshold (0 = strategy default 2)
	StepZ         float64 // z-score ladder spacing
	MaxSteps      int     // ladder cap for the steps figure
	DepthMult     float64 // depth gate multiple (need = StepQty*DepthMult)
	StepQty       float64 // depth gate quantity
}

// LensResult is one evaluation of one lens at one instant. A line is emitted
// even when the lens is not ready or the signal is absent, so backtests can
// reconstruct the full decision surface from the JSONL stream.
type LensResult struct {
	Ts            time.Time `json:"ts"`
	Pair          string    `json:"pair"`
	Left          string    `json:"left"`
	Right         string    `json:"right"`
	Lens          string    `json:"lens"`
	Strategy      string    `json:"strategy"`
	MidBps        float64   `json:"mid_bps"`
	MuBps         float64   `json:"mu_bps,omitempty"`
	DevBps        float64   `json:"dev_bps,omitempty"`
	SigmaShortBps float64   `json:"sigma_short_bps,omitempty"`
	LongVolBps    float64   `json:"longvol_bps,omitempty"`
	Z             float64   `json:"z,omitempty"`
	NetBps        float64   `json:"net_bps,omitempty"`
	Steps         int       `json:"steps,omitempty"`
	MuReady       bool      `json:"mu_ready"`
	VolReady      bool      `json:"vol_ready"`
	DepthOK       bool      `json:"depth_ok,omitempty"`
	Reason        string    `json:"reason,omitempty"`
}

// evaluateLens renders one lens against the current books and statistics.
// arbitrage/volume use strategy.OpeningMargins, the exact threshold function
// the core Decide path uses; z-score mirrors the core formula
// z = (executable spread ∓ mu) / LongVol with the same fee gate and ladder
// spacing. Both go through strategy.Parameters so per-strategy parameter
// policies (e.g. volume forcing open_bps to zero) stay identical to trading.
func evaluateLens(l Lens, q spread.Quote, mu float64, muReady bool, sigmaShort float64, sigmaReady bool, longVol float64, feeBps float64, base, left, right string, now time.Time) LensResult {
	res := LensResult{
		Ts: now, Pair: base, Left: left, Right: right,
		Lens: l.Name, Strategy: l.Strategy,
		MidBps: q.Mid, MuBps: mu, SigmaShortBps: sigmaShort,
		MuReady: muReady, VolReady: sigmaReady,
	}
	if !muReady {
		res.Reason = "mean not ready"
		return res
	}
	if !sigmaReady {
		res.Reason = "sigma not ready"
		return res
	}
	// Depth gate: the minimum top size across both sides of both legs must
	// cover StepQty*DepthMult, the same requirement Decide applies.
	need := l.StepQty * l.DepthMult
	res.DepthOK = need <= 0 || lensDepthOK(q, need)

	strat, err := strategy.New(l.Strategy)
	if err != nil {
		res.Reason = "unknown strategy"
		return res
	}
	params := strategy.Parameters(strat, strategy.Params{
		OpenBps: l.OpenBps, VolK: l.VolK, FeeBps: feeBps,
		ZOpen: l.ZOpen, StepZ: l.StepZ, MaxSteps: l.MaxSteps,
		DepthMult: l.DepthMult,
	})

	if l.Strategy == "zscore" {
		if longVol <= 0 || math.IsNaN(longVol) || math.IsInf(longVol, 0) {
			res.Reason = "longvol unavailable"
			return res
		}
		// Directional z, compared exactly like the core strategy: the richer
		// raw z wins, and only a positive z (spread rich vs the mean) opens.
		zLS := (q.LS - mu) / longVol
		zSL := (q.SL + mu) / longVol
		z, dev := zLS, q.LS-mu
		if zSL > zLS {
			z, dev = zSL, q.SL+mu
		}
		res.Z, res.LongVolBps, res.DevBps = z, longVol, dev
		res.NetBps = dev - feeBps
		if dev < feeBps {
			res.Reason = "dev below fees"
			return res
		}
		zOpen := params.ZOpen
		if zOpen <= 0 {
			zOpen = 2
		}
		if z < zOpen {
			res.Reason = "z below z_open"
			return res
		}
		steps := 1
		if params.StepZ > 0 {
			steps = int(math.Floor((z-zOpen)/params.StepZ)) + 1
		}
		if params.MaxSteps > 0 && steps > params.MaxSteps {
			steps = params.MaxSteps
		}
		res.Steps = steps
		return res
	}

	// arbitrage and volume share the executable-margin formula; volume's
	// parameter policy already forced OpenBps to zero above.
	ls, sl := strategy.OpeningMargins(params, q, mu, sigmaShort)
	if math.IsNaN(ls) && math.IsNaN(sl) {
		res.Reason = "invalid margins"
		return res
	}
	dev := q.LS - mu
	net := ls
	if sl > ls {
		dev, net = q.SL+mu, sl
	}
	res.DevBps, res.NetBps = dev, net
	if net <= 0 {
		res.Reason = "below threshold"
	}
	return res
}

// lensDepthOK reports whether both books carry the required size at their
// tops, mirroring the strategy's depth check.
func lensDepthOK(q spread.Quote, need float64) bool {
	if len(q.L.Bids) == 0 || len(q.L.Asks) == 0 || len(q.R.Bids) == 0 || len(q.R.Asks) == 0 {
		return false
	}
	min4 := q.L.Bids[0].Size
	for _, s := range []float64{q.L.Asks[0].Size, q.R.Bids[0].Size, q.R.Asks[0].Size} {
		if s < min4 {
			min4 = s
		}
	}
	return min4+1e-12 >= need
}

// WatchLenses streams one pair's books and evaluates every lens on a shared
// cadence: the mean estimators sample at sampleEvery, the short-horizon
// volatility chain samples at volEvery, and evaluation runs once per
// sampleEvery interval. Book freshness and the two-leg time-skew gate follow
// the same rules as Watch, so the statistics a lens sees match the engine's
// sampling semantics. Results are emitted for every lens at every evaluation,
// ready or not.
func WatchLenses(ctx context.Context, base string, l, r exchange.Venue, li, ri domain.Instrument, lenses []Lens, feeBps float64, sampleEvery, volEvery, maxBookAge, maxVolBookAge time.Duration, on func(LensResult)) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if sampleEvery <= 0 {
		sampleEvery = time.Second
	}
	if volEvery <= 0 {
		volEvery = 100 * time.Millisecond
	}
	if maxBookAge <= 0 {
		maxBookAge = 10 * time.Second
	}
	if maxVolBookAge <= 0 {
		maxVolBookAge = 50 * time.Millisecond
	}
	type lensState struct {
		lens  Lens
		est   estimator.Estimator
		strat strategy.Strategy
	}
	states := make([]lensState, 0, len(lenses))
	for _, lens := range lenses {
		kind := lens.EstimatorKind
		if kind == "" {
			kind = "sma"
		}
		est, err := estimator.New(kind, lens.Estimator)
		if err != nil {
			return fmt.Errorf("lens %s estimator: %w", lens.Name, err)
		}
		states = append(states, lensState{lens: lens, est: est})
	}
	chL, err := l.SubscribeBook(ctx, li)
	if err != nil {
		return fmt.Errorf("%s subscribe: %w", l.Name(), err)
	}
	chR, err := r.SubscribeBook(ctx, ri)
	if err != nil {
		return fmt.Errorf("%s subscribe: %w", r.Name(), err)
	}
	vol := volatility.New(volatility.Params{Interval: volEvery, Horizon: 500 * time.Millisecond, HalfLife: time.Minute})
	var bl, br domain.Book
	quote := func(now time.Time, age time.Duration) (spread.Quote, bool) {
		if bl.Venue != l.Name() || br.Venue != r.Name() || bl.MarketID != li.MarketID || br.MarketID != ri.MarketID ||
			bl.Ts.IsZero() || br.Ts.IsZero() || now.Sub(bl.Ts) > age || now.Sub(br.Ts) > age || bl.Ts.After(now) || br.Ts.After(now) {
			return spread.Quote{}, false
		}
		return spread.Compute(bl, br)
	}
	sample := time.NewTicker(sampleEvery)
	defer sample.Stop()
	fast := time.NewTicker(volEvery)
	defer fast.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case b, ok := <-chL:
			if !ok {
				return fmt.Errorf("%s book subscription closed", l.Name())
			}
			if !b.Valid() || !b.Ts.Before(bl.Ts) {
				bl = b
			}
		case b, ok := <-chR:
			if !ok {
				return fmt.Errorf("%s book subscription closed", r.Name())
			}
			if !b.Valid() || !b.Ts.Before(br.Ts) {
				br = b
			}
		case <-fast.C:
			now := time.Now()
			if q, ok := quote(now, maxVolBookAge); ok {
				if d := bl.Ts.Sub(br.Ts); d <= maxVolBookAge && -d <= maxVolBookAge {
					vol.AddAt(now, q.Mid)
				}
			}
		case <-sample.C:
			now := time.Now()
			q, ok := quote(now, maxBookAge)
			if !ok {
				continue // no evaluation without fresh books
			}
			for _, st := range states {
				st.est.Update(now, q.Mid)
				mu := estimator.MeanAt(st.est, now)
				muReady := estimator.ReadyAt(st.est, now)
				sigmaReady := vol.ReadyAt(now)
				sigmaShort := vol.VolAt(now)
				longVol := 0.0
				if lv, isLV := st.est.(estimator.LongVol); isLV {
					longVol = lv.LongVol()
				}
				on(evaluateLens(st.lens, q, mu, muReady, sigmaShort, sigmaReady, longVol, feeBps, base, l.Name(), r.Name(), now))
			}
		}
	}
}
