package engine

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/monitor"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/store"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
	"github.com/Dog-Feng/dex-arbitr/internal/volatility"
)

// candidate is the shared, continuously-updated market view of one matched
// pair. It implements PairView so a promoted actor reads warm stats instead of
// re-warming its own.
type candidate struct {
	base string
	key  string
	pair symbols.Pair
	l, r exchange.Venue

	mu         sync.RWMutex
	bookL      domain.Book
	bookR      domain.Book
	healthLast scan.Health
	est        estimator.Estimator
	vol        *volatility.Estimator
	volL       *volatility.Estimator
	volR       *volatility.Estimator
	health     *scan.HealthTracker
	notional   float64
	depthRatio float64
	tick       float64
	maxBookAge time.Duration
	samples    *store.SampleSink
}

func (c *candidate) Book(venue string) (domain.Book, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b := c.bookR
	if venue == c.l.Name() {
		b = c.bookL
	}
	return b, b.Valid()
}

func (c *candidate) Quote(now time.Time, maxAge time.Duration) (spread.Quote, bool) {
	c.mu.RLock()
	l, r := c.bookL, c.bookR
	c.mu.RUnlock()
	if now.Sub(l.Ts) > maxAge || now.Sub(r.Ts) > maxAge {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
}

func (c *candidate) Mean() (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.est.Mean(), c.est.Ready()
}

func (c *candidate) VolBps() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vol.Vol()
}

func (c *candidate) LegVol(venue string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if venue == c.l.Name() {
		return c.volL.Vol()
	}
	if venue == c.r.Name() {
		return c.volR.Vol()
	}
	return 0
}

func (c *candidate) healthSnapshot() scan.Health {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.healthLast
}

func (c *candidate) Stats() estimator.Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.est.Stats()
}

// collect subscribes both books and keeps the pair's estimators warm. It runs
// for every candidate (active or standby) so promotion needs no re-warmup.
func (c *candidate) collect(ctx context.Context, sampleEvery, volEvery, volMaxAge time.Duration) {
	if c.samples != nil {
		defer c.samples.Close()
	}
	chL, err := c.l.SubscribeBook(ctx, c.pair.Left)
	if err != nil {
		slog.Warn("auto subscribe", "pair", c.base, "venue", c.l.Name(), "err", err)
		return
	}
	chR, err := c.r.SubscribeBook(ctx, c.pair.Right)
	if err != nil {
		slog.Warn("auto subscribe", "pair", c.base, "venue", c.r.Name(), "err", err)
		return
	}
	if sampleEvery == 0 {
		sampleEvery = time.Second
	}
	if volEvery == 0 {
		volEvery = 100 * time.Millisecond
	}
	if volMaxAge == 0 {
		volMaxAge = 50 * time.Millisecond
	}
	sample := time.NewTicker(sampleEvery)
	defer sample.Stop()
	volT := time.NewTicker(volEvery)
	defer volT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case b, ok := <-chL:
			if !ok {
				return
			}
			c.mu.Lock()
			c.bookL = b
			c.mu.Unlock()
		case b, ok := <-chR:
			if !ok {
				return
			}
			c.mu.Lock()
			c.bookR = b
			c.mu.Unlock()
		case now := <-volT.C:
			if q, ok := c.Quote(now, volMaxAge); ok {
				c.mu.Lock()
				c.vol.Add(q.Mid)
				c.volL.Add(q.L.Mid())
				c.volR.Add(q.R.Mid())
				c.mu.Unlock()
				if c.samples != nil {
					c.samples.Add(now, q.Mid, q.LS, q.SL)
				}
			}
		case now := <-sample.C:
			q, ok := c.Quote(now, c.maxBookAge)
			if !ok {
				continue
			}
			c.mu.Lock()
			c.est.Update(now, q.Mid)
			c.mu.Unlock()
			mid := q.RefMid
			if mid <= 0 {
				mid = (q.L.Mid() + q.R.Mid()) / 2
			}
			need := c.notional / mid
			h := c.health.Add(q, c.vol.Vol(), need, c.tick/mid*1e4)
			c.mu.Lock()
			c.healthLast = h
			c.mu.Unlock()
		}
	}
}

// sizedQty derives the order quantity from the notional cap and the usable
// top-of-book depth, floored to the pair's size precision and checked against
// the minimum quantity.
func (c *candidate) sizedQty() (float64, bool) {
	c.mu.RLock()
	l, r := c.bookL, c.bookR
	c.mu.RUnlock()
	if !l.Valid() || !r.Valid() {
		return 0, false
	}
	mid := (l.Mid() + r.Mid()) / 2
	top := min4(l.Bids[0].Size, l.Asks[0].Size, r.Bids[0].Size, r.Asks[0].Size)
	capNotional := c.depthRatio * top * mid
	n := math.Min(c.notional, capNotional)
	qty := c.pair.AlignQty(n / mid)
	if ok, _ := c.pair.ValidQty(qty, mid); !ok {
		return 0, false
	}
	return qty, true
}

// activeRun is a promoted actor plus its cancellation.
type activeRun struct {
	actor  *PairActor
	cancel context.CancelFunc
	budget float64 // reserved worst-case notional (X * max_steps)
}

// matchAll resolves every matched pair across the configured venues, in a
// deterministic venue order (lighter before rhlighter).
func (rt *runtime) matchAll(ctx context.Context) ([]*candidate, error) {
	type ven struct {
		name  string
		venue exchange.Venue
		inst  []domain.Instrument
	}
	var vs []ven
	names := make([]string, 0, len(rt.venues))
	for name := range rt.venues {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		slog.Info("auto: fetching instruments", "venue", name)
		inst, err := rt.venues[name].Instruments(ctx)
		if err != nil {
			return nil, fmt.Errorf("%s instruments: %w", name, err)
		}
		vs = append(vs, ven{name: name, venue: rt.venues[name], inst: inst})
	}
	whitelist := map[string]bool{}
	for _, b := range rt.cfg.Auto.Candidates {
		whitelist[rt.matcher.Normalize(b)] = true
	}
	var out []*candidate
	for i := 0; i < len(vs); i++ {
		for j := i + 1; j < len(vs); j++ {
			for _, p := range symbols.Match(rt.matcher, vs[i].inst, vs[j].inst) {
				if len(whitelist) > 0 && !whitelist[p.Base] {
					continue
				}
				est, vol, volL, volR, err := rt.newStats()
				if err != nil {
					return nil, err
				}
				var samples *store.SampleSink
				if rt.cfg.SamplesDir != "" {
					samples, err = store.NewSampleSink(filepath.Join(rt.cfg.SamplesDir, fmt.Sprintf("%s_%s_%s.jsonl", p.Base, vs[i].name, vs[j].name)))
					if err != nil {
						return nil, err
					}
				}
				out = append(out, &candidate{
					base: p.Base, key: pairKey(p.Base, vs[i].name, vs[j].name, rt.cfg.DryRun),
					pair: p, l: vs[i].venue, r: vs[j].venue,
					est: est, vol: vol, volL: volL, volR: volR,
					health:   scan.NewHealthTracker(600),
					notional: rt.cfg.Auto.NotionalPerLeg, depthRatio: rt.cfg.Auto.DepthUseRatio,
					tick:       maxTick(p.Left, p.Right),
					maxBookAge: rt.cfg.Strategy.MaxBookAge, samples: samples,
				})
			}
		}
	}
	return out, nil
}

// maxTick is the coarser of the two legs' price ticks (the binding constraint).
func maxTick(l, r domain.Instrument) float64 {
	if l.Tick() > r.Tick() {
		return l.Tick()
	}
	return r.Tick()
}

// runAuto drives the shared-stats picker loop.
func (rt *runtime) runAuto(ctx context.Context) error {
	cands, err := rt.matchAll(ctx)
	if err != nil {
		return err
	}
	if len(cands) == 0 {
		return fmt.Errorf("auto pair selection: no matched pairs")
	}
	slog.Info("auto pair selection", "candidates", len(cands), "top_y", rt.cfg.Auto.TopY)
	collectCtx, cancelCollect := context.WithCancel(ctx)
	defer cancelCollect()
	for _, c := range cands {
		go c.collect(collectCtx, rt.cfg.Estimator.Interval, rt.cfg.Volatility.Interval, rt.cfg.Volatility.MaxAge)
	}
	slog.Info("auto collectors started", "warmup", rt.cfg.Estimator.MinWarmup)
	active := map[string]*activeRun{}
	promoteSince := map[string]time.Time{}
	demoteSince := map[string]time.Time{}
	var wg sync.WaitGroup
	defer func() {
		for _, ar := range active {
			ar.cancel()
		}
		wg.Wait()
	}()
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	status := time.NewTicker(30 * time.Second)
	defer status.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			rt.selectPairs(ctx, cands, active, promoteSince, demoteSince, &wg)
		case <-status.C:
			ready, warm := 0, 0
			for _, c := range cands {
				if _, ok := c.Mean(); ok {
					ready++
				} else {
					warm++
				}
			}
			slog.Info("picker status", "candidates", len(cands), "ready", ready, "warming", warm, "active", len(active))
		}
	}
}

// selectPairs computes desired membership and applies margin+dwell hysteresis.
func (rt *runtime) selectPairs(ctx context.Context, cands []*candidate, active map[string]*activeRun,
	promoteSince, demoteSince map[string]time.Time, wg *sync.WaitGroup) {
	now := time.Now()
	for _, c := range cands {
		st := c.Stats()
		rt.mon.Record(monitor.Event{Time: now, Pair: c.key, Kind: monitor.KindPairWarmup,
			Samples: st.Samples, Coverage: st.Coverage, Ready: st.Ready})
		h := c.healthSnapshot()
		rt.mon.Record(monitor.Event{Time: now, Pair: c.key, Kind: monitor.KindPairHealth,
			Score: h.Score, DepthQ: h.DepthQuality, TickQ: h.TickQuality, Stability: h.Stability, Reverts: h.RevertsPerMin})
	}
	byBase := map[string]*candidate{}
	type entry struct {
		c     *candidate
		score float64
		ready bool
	}
	entries := make([]entry, 0, len(cands))
	for _, c := range cands {
		byBase[c.base] = c
		h := c.healthSnapshot()
		_, ready := c.Mean()
		entries = append(entries, entry{c: c, score: h.Score, ready: ready})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].score > entries[j].score })

	desired := map[string]bool{}
	for base := range active {
		desired[base] = true
	}
	// A pair whose one-leg failure rate exceeded the cap is disabled: active
	// ones drain and demote once flat, standbys are never promoted.
	for base := range desired {
		if c := byBase[base]; rt.mon.RollingFailRate(c.key) > rt.cfg.Auto.MaxFailRate {
			delete(desired, base)
		}
	}
	var standbys []entry
	for _, e := range entries {
		if !e.ready || e.score < rt.cfg.Auto.MinChurnScore {
			continue
		}
		if rt.mon.RollingFailRate(e.c.key) > rt.cfg.Auto.MaxFailRate {
			continue
		}
		if _, ok := active[e.c.base]; !ok {
			standbys = append(standbys, e)
		}
	}
	// Fill empty slots with the best eligible standbys, then displace the
	// weakest incumbent only when a standby leads by the configured margin.
	for _, e := range standbys {
		if len(desired) >= rt.cfg.Auto.TopY {
			break
		}
		desired[e.c.base] = true
	}
	if len(desired) >= rt.cfg.Auto.TopY {
		for _, e := range standbys {
			if desired[e.c.base] {
				continue
			}
			weakBase, weakScore := "", math.MaxFloat64
			for base := range desired {
				if s := byBase[base].healthSnapshot().Score; s < weakScore {
					weakBase, weakScore = base, s
				}
			}
			if weakBase == "" || e.score <= weakScore*(1+rt.cfg.Auto.PromoteMargin) {
				continue
			}
			delete(desired, weakBase)
			desired[e.c.base] = true
		}
	}

	for base, ar := range active {
		if desired[base] {
			delete(demoteSince, base)
			continue
		}
		if !ar.actor.Flat() {
			continue
		}
		if t, ok := demoteSince[base]; !ok {
			demoteSince[base] = now
			continue
		} else if now.Sub(t) < rt.cfg.Auto.Dwell {
			continue
		}
		ar.cancel()
		delete(active, base)
		delete(demoteSince, base)
		rt.mon.Record(monitor.Event{Time: now, Pair: ar.actor.key, Kind: monitor.KindPairDemoted})
		slog.Info("pair demoted", "base", base)
	}

	for base := range desired {
		if _, ok := active[base]; ok {
			delete(promoteSince, base)
			continue
		}
		c := byBase[base]
		if t, ok := promoteSince[base]; !ok {
			promoteSince[base] = now
			continue
		} else if now.Sub(t) < rt.cfg.Auto.Dwell {
			continue
		}
		if err := rt.promote(ctx, c, active, wg); err != nil {
			slog.Warn("pair promote failed", "base", base, "err", err)
			promoteSince[base] = now // back off one dwell before retrying
			continue
		}
		delete(promoteSince, base)
	}
	for base := range promoteSince {
		if !desired[base] {
			delete(promoteSince, base)
		}
	}
}

// promote sizes the pair and starts its actor on the shared warm stats.
func (rt *runtime) promote(ctx context.Context, c *candidate, active map[string]*activeRun, wg *sync.WaitGroup) error {
	budget := rt.cfg.Auto.NotionalPerLeg * float64(rt.cfg.Auto.MaxSteps)
	if rt.cfg.Auto.MaxTotalNotional > 0 {
		used := 0.0
		for _, ar := range active {
			used += ar.budget
		}
		if used+budget > rt.cfg.Auto.MaxTotalNotional {
			return fmt.Errorf("notional budget exceeded")
		}
	}
	qty, ok := c.sizedQty()
	if !ok {
		return fmt.Errorf("size unavailable")
	}
	if !rt.cfg.DryRun {
		rt.applyLeverage(ctx, c.l, c.pair.Left)
		rt.applyLeverage(ctx, c.r, c.pair.Right)
	}
	a, err := rt.buildActor(c.pair, qty, rt.cfg.Auto.MaxSteps, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		return err
	}
	a.cfg.View = c
	actx, cancel := context.WithCancel(ctx)
	active[c.base] = &activeRun{actor: a, cancel: cancel, budget: budget}
	rt.mon.Record(monitor.Event{Time: time.Now(), Pair: c.key, Kind: monitor.KindPairPromoted, Score: c.healthSnapshot().Score})
	slog.Info("pair promoted", "base", c.base, "qty", qty)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := a.Run(actx); err != nil {
			slog.Error("pair actor stopped", "pair", c.base, "err", err)
		}
	}()
	return nil
}

func min4(a, b, c, d float64) float64 {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	if d < m {
		m = d
	}
	return m
}
