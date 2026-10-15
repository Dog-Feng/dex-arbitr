package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"sort"
	"strings"
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
	excluded   bool // whitelist applies to new trading, never to recovery
	failRate   float64
}

func (c *candidate) Book(venue string) (domain.Book, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var b domain.Book
	var marketID int
	switch venue {
	case c.l.Name():
		b, marketID = c.bookL, c.pair.Left.MarketID
	case c.r.Name():
		b, marketID = c.bookR, c.pair.Right.MarketID
	default:
		return domain.Book{}, false
	}
	return b, b.Valid() && b.Venue == venue && b.MarketID == marketID && !b.Ts.IsZero() && !time.Now().Before(b.Ts)
}

func (c *candidate) Quote(now time.Time, maxAge time.Duration) (spread.Quote, bool) {
	c.mu.RLock()
	l, r := c.bookL, c.bookR
	c.mu.RUnlock()
	if !c.bookIdentities(l, r) || l.Ts.IsZero() || r.Ts.IsZero() || now.Before(l.Ts) || now.Before(r.Ts) || now.Sub(l.Ts) > maxAge || now.Sub(r.Ts) > maxAge {
		return spread.Quote{}, false
	}
	return spread.Compute(l, r)
}

func (c *candidate) bookIdentities(l, r domain.Book) bool {
	return l.Venue == c.l.Name() && l.MarketID == c.pair.Left.MarketID &&
		r.Venue == c.r.Name() && r.MarketID == c.pair.Right.MarketID
}

func (c *candidate) Mean() (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	st := estimator.StatsAt(c.est, time.Now())
	return st.Mean, st.Ready
}

func (c *candidate) VolBps() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vol.VolAt(time.Now())
}

func (c *candidate) VolReady() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.vol.ReadyAt(time.Now())
}

func (c *candidate) LegVol(venue string) float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if venue == c.l.Name() {
		return c.volL.VolAt(time.Now())
	}
	if venue == c.r.Name() {
		return c.volR.VolAt(time.Now())
	}
	return 0
}

func (c *candidate) healthSnapshot() scan.Health {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	if !c.bookIdentities(c.bookL, c.bookR) || !c.bookL.Valid() || !c.bookR.Valid() || c.bookL.Ts.IsZero() || c.bookR.Ts.IsZero() ||
		now.Before(c.bookL.Ts) || now.Before(c.bookR.Ts) ||
		now.Sub(c.bookL.Ts) > c.maxBookAge || now.Sub(c.bookR.Ts) > c.maxBookAge {
		return scan.Health{}
	}
	if c.health != nil {
		mid := (c.bookL.Mid() + c.bookR.Mid()) / 2
		if mid <= 0 {
			return scan.Health{}
		}
		return c.health.SnapshotAt(now, c.tick/mid*1e4)
	}
	return c.healthLast
}

func (c *candidate) Stats() estimator.Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return estimator.StatsAt(c.est, time.Now())
}

// collect subscribes both books and keeps the pair's estimators warm. It runs
// for every candidate (active or standby) so promotion needs no re-warmup.
func (c *candidate) collect(ctx context.Context, sampleEvery, volEvery, volMaxAge time.Duration) error {
	chL, err := c.l.SubscribeBook(ctx, c.pair.Left)
	if err != nil {
		return fmt.Errorf("%s book subscription: %w", c.l.Name(), err)
	}
	chR, err := c.r.SubscribeBook(ctx, c.pair.Right)
	if err != nil {
		return fmt.Errorf("%s book subscription: %w", c.r.Name(), err)
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
			return nil
		case b, ok := <-chL:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("%s book stream closed", c.l.Name())
			}
			c.mu.Lock()
			c.bookL = b
			c.mu.Unlock()
		case b, ok := <-chR:
			if !ok {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("%s book stream closed", c.r.Name())
			}
			c.mu.Lock()
			c.bookR = b
			c.mu.Unlock()
		case <-volT.C:
			now := time.Now()
			if q, ok := c.Quote(now, volMaxAge); ok {
				c.mu.Lock()
				c.vol.AddAt(now, q.Mid)
				c.volL.AddAt(now, q.L.Mid())
				c.volR.AddAt(now, q.R.Mid())
				c.mu.Unlock()
				if c.samples != nil {
					if err := c.samples.AddQuote(now, q); err != nil {
						return fmt.Errorf("write samples: %w", err)
					}
				}
			}
		case <-sample.C:
			now := time.Now()
			q, ok := c.Quote(now, c.maxBookAge)
			if !ok {
				continue
			}
			mid := q.RefMid
			if mid <= 0 {
				mid = (q.L.Mid() + q.R.Mid()) / 2
			}
			need := c.notional / mid
			c.mu.Lock()
			c.est.Update(now, q.Mid)
			c.healthLast = c.health.AddAt(now, q, c.vol.VolAt(now), need, c.tick/mid*1e4)
			c.mu.Unlock()
		}
	}
}

// SizedQty derives the order quantity from the notional cap and the usable
// top-of-book depth, floored to the pair's size precision and checked against
// the minimum quantity.
func (c *candidate) SizedQty() (float64, bool) {
	c.mu.RLock()
	l, r := c.bookL, c.bookR
	c.mu.RUnlock()
	if !l.Valid() || !r.Valid() {
		return 0, false
	}
	mid := (l.Mid() + r.Mid()) / 2
	// Either venue can be the buy leg. Using the more expensive ask keeps
	// each leg within the configured notional before the executor applies
	// its final IOC-price admission check.
	price := math.Max(l.Asks[0].Price, r.Asks[0].Price)
	top := min4(l.Bids[0].Size, l.Asks[0].Size, r.Bids[0].Size, r.Asks[0].Size)
	capNotional := c.depthRatio * top * price
	n := math.Min(c.notional, capNotional)
	qty := c.pair.AlignQty(n / price)
	if ok, _ := c.pair.ValidQty(qty, mid); !ok {
		return 0, false
	}
	return qty, true
}

// activeRun is a promoted actor plus its cancellation.
type activeRun struct {
	actor    *PairActor
	cancel   context.CancelFunc
	budget   float64 // reserved worst-case notional (X * max_steps)
	done     chan error
	draining bool
	stopping bool
}

// matchAll resolves every matched pair across the configured venues, in a
// deterministic venue order (lighter before rhlighter).
func (rt *runtime) matchAll(ctx context.Context) (cands []*candidate, matchErr error) {
	var out []*candidate
	defer func() {
		if matchErr != nil {
			for _, c := range out {
				if c.samples != nil {
					if err := c.samples.Close(); err != nil {
						matchErr = errors.Join(matchErr, fmt.Errorf("%s unused samples close: %w", c.key, err))
					}
				}
			}
		}
	}()
	matches, err := scan.MatchAll(ctx, rt.matcher, rt.venues)
	if err != nil {
		return nil, err
	}
	whitelist := map[string]bool{}
	for _, b := range rt.cfg.Auto.Candidates {
		whitelist[rt.matcher.Normalize(b)] = true
	}
	for _, match := range matches {
		p, l, r := match.Pair, match.L, match.R
		est, vol, volL, volR, err := rt.newStats()
		if err != nil {
			return nil, err
		}
		var samples *store.SampleSink
		if rt.cfg.SamplesDir != "" {
			samples, err = store.NewSampleSink(filepath.Join(rt.cfg.SamplesDir, fmt.Sprintf("%s_%s_%s.jsonl", p.Base, l.Name(), r.Name())))
			if err != nil {
				return nil, err
			}
		}
		out = append(out, &candidate{
			base: p.Base, key: pairKey(p.Base, l.Name(), r.Name(), rt.cfg.DryRun),
			pair: p, l: l, r: r,
			est: est, vol: vol, volL: volL, volR: volR,
			health:   scan.NewHealthTracker(600),
			notional: rt.cfg.Auto.NotionalPerLeg, depthRatio: rt.cfg.Auto.DepthUseRatio,
			tick:       maxTick(p.Left, p.Right),
			maxBookAge: rt.cfg.Strategy.MaxBookAge, samples: samples,
			excluded: len(whitelist) > 0 && !whitelist[p.Base],
		})
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

// runAuto restores all persisted exposure before permitting fresh promotions.
func (rt *runtime) runAuto(ctx context.Context) (runErr error) {
	cands, err := rt.matchAll(ctx)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	var collectors, actors sync.WaitGroup
	active := map[string]*activeRun{}
	collectorErr := make(chan error, len(cands))
	defer func() {
		cancel()
		for _, ar := range active {
			ar.cancel()
		}
		collectors.Wait()
		actors.Wait()
		for key, ar := range active {
			select {
			case err := <-ar.done:
				if err != nil {
					runErr = errors.Join(runErr, fmt.Errorf("pair %s stopped: %w", key, err))
				}
			default:
			}
		}
		close(collectorErr)
		for err := range collectorErr {
			runErr = errors.Join(runErr, err)
		}
		for _, c := range cands {
			if c.samples != nil {
				if err := c.samples.Close(); err != nil {
					runErr = errors.Join(runErr, fmt.Errorf("%s samples close: %w", c.key, err))
				}
			}
		}
	}()
	for _, c := range cands {
		collectors.Add(1)
		go func() {
			defer collectors.Done()
			if err := c.collect(runCtx, rt.cfg.Estimator.Interval, rt.cfg.Volatility.Interval, rt.cfg.Volatility.MaxAge); err != nil {
				collectorErr <- fmt.Errorf("candidate %s: %w", c.key, err)
				cancel()
			}
		}()
	}
	if err := rt.restorePairs(runCtx, cands, active, &actors); err != nil {
		return err
	}
	if len(cands) == 0 {
		return fmt.Errorf("auto pair selection: no matched pairs")
	}
	promoteSince, demoteSince := map[string]time.Time{}, map[string]time.Time{}
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	status := time.NewTicker(30 * time.Second)
	defer status.Stop()
	slog.Info("auto collectors started", "candidates", len(cands), "restored", len(active), "top_y", rt.cfg.Auto.TopY)
	for {
		select {
		case <-runCtx.Done():
			select {
			case err := <-collectorErr:
				return err
			default:
				return nil
			}
		case err := <-collectorErr:
			return err
		case <-tick.C:
			if err := rt.selectPairs(runCtx, cands, active, promoteSince, demoteSince, &actors); err != nil {
				return err
			}
		case <-status.C:
			ready := 0
			for _, c := range cands {
				if _, ok := c.Mean(); ok {
					ready++
				}
			}
			slog.Info("picker status", "candidates", len(cands), "ready", ready, "warming", len(cands)-ready, "active", len(active))
		}
	}
}

func (rt *runtime) restorePairs(ctx context.Context, cands []*candidate, active map[string]*activeRun, wg *sync.WaitGroup) error {
	states, err := rt.store.List()
	if err != nil {
		return fmt.Errorf("list saved pairs: %w", err)
	}
	byKey := map[string]*candidate{}
	for _, c := range cands {
		byKey[c.key] = c
	}
	keys := make([]string, 0, len(states))
	for key, st := range states {
		if strings.HasSuffix(key, "_paper") != rt.cfg.DryRun || (len(st.Steps) == 0 && !st.Unresolved) {
			continue
		}
		if byKey[key] == nil {
			return fmt.Errorf("saved open pair %s has no configured matched markets", key)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// Check all recovery plans before starting an actor. Shared-market ledgers
	// cannot be reconciled independently without an account-level allocation.
	for i, key := range keys {
		for _, prior := range keys[:i] {
			if candidatesOverlap(byKey[key], byKey[prior]) {
				return fmt.Errorf("saved pairs %s and %s share a market", key, prior)
			}
		}
	}
	positionNotionals := map[string]float64{}
	if !rt.cfg.DryRun {
		covered := map[string]bool{}
		for _, key := range keys {
			c := byKey[key]
			covered[marketKey(c.l.Name(), c.pair.Left.MarketID)] = true
			covered[marketKey(c.r.Name(), c.pair.Right.MarketID)] = true
		}
		for name, v := range rt.venues {
			ps, err := v.Positions(ctx)
			if err != nil {
				return fmt.Errorf("%s startup positions: %w", name, err)
			}
			for _, p := range ps {
				if p.Qty != 0 && !covered[marketKey(name, p.MarketID)] {
					return fmt.Errorf("%s market %d has a position without a recoverable step ledger", name, p.MarketID)
				}
				if p.Qty != 0 && finitePositive(p.AvgEntry) {
					positionNotionals[marketKey(name, p.MarketID)] = math.Abs(p.Qty) * p.AvgEntry
				}
			}
		}
	}
	type recovery struct {
		c        *candidate
		actor    *PairActor
		used     float64
		budget   float64
		draining bool
	}
	plans := make([]recovery, 0, len(keys))
	reserved := 0.0
	for _, key := range keys {
		c, st := byKey[key], states[key]
		qty := 0.0
		for _, step := range st.Steps {
			qty = math.Max(qty, step.Qty)
		}
		if qty == 0 {
			// An unresolved execution may have changed positions before adding
			// a step. Restore its actor even when the last ledger is empty.
			qty = math.Max(c.pair.MinQty, math.Pow10(-c.pair.SizeDecimals))
		}
		a, err := rt.buildActor(c.pair, qty, rt.cfg.Auto.MaxSteps, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
		if err != nil {
			return fmt.Errorf("restore %s: %w", key, err)
		}
		used, budget, priced := recoveryReservation(st, rt.cfg.Auto.NotionalPerLeg, rt.cfg.Auto.MaxSteps)
		// A position query can reveal more exposure than the saved ledger,
		// even when this actor will remain paused for reconciliation. It must
		// still consume capacity before any other actor is allowed to open.
		actual := math.Max(positionNotionals[marketKey(c.l.Name(), c.pair.Left.MarketID)],
			positionNotionals[marketKey(c.r.Name(), c.pair.Right.MarketID)])
		if actual > used {
			budget += actual - used
			used = actual
		}
		// A legacy ledger without entry prices cannot quantify its exposure.
		// Keep it in recovery and occupy the account cap until it exits.
		if !priced && rt.cfg.Auto.MaxTotalNotional > 0 {
			used = math.Max(used, rt.cfg.Auto.MaxTotalNotional)
			budget = math.Max(budget, used)
		}
		plans = append(plans, recovery{c: c, actor: a, used: used, budget: budget, draining: c.excluded || !priced})
		if st.Unresolved {
			rt.recovering = append(rt.recovering, a)
		}
		reserved += used
	}
	// Count every retained position before granting any actor room to open.
	// Reservations never grow while actors run: a close retains its allocation
	// for replacement steps, and a draining actor keeps its actual exposure
	// allocated until the picker receives its confirmed exit.
	trading := 0
	for i := range plans {
		p := &plans[i]
		extra := p.budget - p.used
		overBudget := rt.cfg.Auto.MaxTotalNotional > 0 && reserved+extra > rt.cfg.Auto.MaxTotalNotional
		p.draining = p.draining || trading >= rt.cfg.Auto.TopY || overBudget
		if p.draining {
			p.budget = p.used
			p.actor.Drain()
		} else {
			reserved += extra
			trading++
		}
	}
	// Do not start an actor until every recovery reservation is final.
	for _, p := range plans {
		ar := rt.startActor(ctx, p.c, p.actor, p.budget, active, wg)
		ar.draining = p.draining
	}
	return nil
}

func marketKey(venue string, marketID int) string { return fmt.Sprintf("%s#%d", venue, marketID) }

func candidatesOverlap(a, b *candidate) bool {
	aLegs := [2]string{marketKey(a.l.Name(), a.pair.Left.MarketID), marketKey(a.r.Name(), a.pair.Right.MarketID)}
	bLegs := [2]string{marketKey(b.l.Name(), b.pair.Left.MarketID), marketKey(b.r.Name(), b.pair.Right.MarketID)}
	for _, al := range aLegs {
		for _, bl := range bLegs {
			if al == bl {
				return true
			}
		}
	}
	return false
}

func (rt *runtime) candidateFailRate(c *candidate, active map[string]*activeRun) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ar := active[c.key]; ar != nil {
		c.failRate = ar.actor.FailRate()
	}
	return c.failRate
}

// selectPairs keeps unique market combinations and waits for a drained actor
// to exit before releasing either its slot or its reserved notional budget.
func (rt *runtime) selectPairs(ctx context.Context, cands []*candidate, active map[string]*activeRun,
	promoteSince, demoteSince map[string]time.Time, wg *sync.WaitGroup) error {
	now := time.Now()
	byKey := map[string]*candidate{}
	type entry struct {
		c      *candidate
		score  float64
		ready  bool
		failed bool
	}
	entries := make([]entry, 0, len(cands))
	for _, c := range cands {
		byKey[c.key] = c
		st, h := c.Stats(), c.healthSnapshot()
		if rt.mon != nil {
			rt.mon.Record(monitor.Event{Time: now, Pair: c.key, Kind: monitor.KindPairWarmup, Samples: st.Samples, Coverage: st.Coverage, Ready: st.Ready})
			rt.mon.Record(monitor.Event{Time: now, Pair: c.key, Kind: monitor.KindPairHealth, Score: h.Score, DepthQ: h.DepthQuality, TickQ: h.TickQuality, Stability: h.Stability, Reverts: h.RevertsPerMin})
		}
		entries = append(entries, entry{c: c, score: h.Score, ready: st.Ready, failed: rt.candidateFailRate(c, active) > rt.cfg.Auto.MaxFailRate})
	}
	// Unexpected termination is an engine failure, including nil termination
	// without a drain request; otherwise stale membership would hide lost actors.
	for key, ar := range active {
		if ar.done == nil {
			continue
		}
		select {
		case err := <-ar.done:
			if err != nil {
				return fmt.Errorf("pair %s stopped: %w", key, err)
			}
			if !ar.stopping {
				return fmt.Errorf("pair %s stopped before draining", key)
			}
			delete(active, key)
			delete(demoteSince, key)
			if rt.mon != nil {
				rt.mon.Record(monitor.Event{Time: now, Pair: key, Kind: monitor.KindPairDemoted})
			}
		default:
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].score == entries[j].score {
			return entries[i].c.key < entries[j].c.key
		}
		return entries[i].score > entries[j].score
	})
	desired := map[string]bool{}
	for _, e := range entries {
		if ar := active[e.c.key]; ar != nil && !ar.draining && !e.c.excluded && !e.failed {
			desired[e.c.key] = true
		}
	}
	// Recovery may temporarily exceed TopY. Those actors retain risk management,
	// but the weakest extras drain and never create additional trading capacity.
	for len(desired) > rt.cfg.Auto.TopY {
		weak := ""
		for i := len(entries) - 1; i >= 0; i-- {
			if desired[entries[i].c.key] {
				weak = entries[i].c.key
				break
			}
		}
		delete(desired, weak)
	}
	eligible := func(e entry) bool {
		return !e.c.excluded && !e.failed && e.ready && e.score >= rt.cfg.Auto.MinChurnScore
	}
	overlapsDesired := func(c *candidate, except string) bool {
		for key := range desired {
			if key != except && candidatesOverlap(c, byKey[key]) {
				return true
			}
		}
		return false
	}
	for _, e := range entries {
		if !eligible(e) || desired[e.c.key] || active[e.c.key] != nil {
			continue
		}
		if len(desired) < rt.cfg.Auto.TopY && !overlapsDesired(e.c, "") {
			desired[e.c.key] = true
			continue
		}
		weak, score := "", math.MaxFloat64
		for key := range desired {
			if overlapsDesired(e.c, key) {
				continue
			}
			s := byKey[key].healthSnapshot().Score
			if s < score || (s == score && (weak == "" || key < weak)) {
				weak, score = key, s
			}
		}
		if weak != "" && e.score > score*(1+rt.cfg.Auto.PromoteMargin) {
			delete(desired, weak)
			desired[e.c.key] = true
		}
	}
	for key, ar := range active {
		c := byKey[key]
		if c == nil {
			return fmt.Errorf("active pair %s missing candidate", key)
		}
		if desired[key] && !ar.draining {
			delete(demoteSince, key)
			continue
		}
		if !ar.draining {
			hard := c.excluded || rt.candidateFailRate(c, active) > rt.cfg.Auto.MaxFailRate
			if !hard {
				if t, ok := demoteSince[key]; !ok {
					demoteSince[key] = now
					continue
				} else if now.Sub(t) < rt.cfg.Auto.Dwell {
					continue
				}
			}
			ar.draining = true
			ar.actor.Drain()
		}
		if !ar.stopping && ar.actor.Drained() {
			ar.stopping = true
			ar.cancel()
		}
	}
	for _, e := range entries {
		key := e.c.key
		if !desired[key] {
			delete(promoteSince, key)
			continue
		}
		if active[key] != nil {
			delete(promoteSince, key)
			continue
		}
		if t, ok := promoteSince[key]; !ok {
			promoteSince[key] = now
			continue
		} else if now.Sub(t) < rt.cfg.Auto.Dwell {
			continue
		}
		if len(active) >= rt.cfg.Auto.TopY {
			continue
		}
		if err := rt.promote(ctx, e.c, active, wg); err != nil {
			slog.Warn("pair promote deferred", "pair", key, "err", err)
			promoteSince[key] = now
			continue
		}
		delete(promoteSince, key)
	}
	for key := range promoteSince {
		if !desired[key] {
			delete(promoteSince, key)
		}
	}
	return nil
}

// pairBudget is a per-leg reservation, preserving the existing configuration
// meaning: NotionalPerLeg * MaxSteps, without adding a second-leg multiplier.
func (rt *runtime) pairBudget() float64 {
	return rt.cfg.Auto.NotionalPerLeg * float64(rt.cfg.Auto.MaxSteps)
}

func (rt *runtime) promote(ctx context.Context, c *candidate, active map[string]*activeRun, wg *sync.WaitGroup) error {
	if !rt.startupRecoveryReady() {
		return fmt.Errorf("startup orders remain unresolved")
	}
	if len(active) >= rt.cfg.Auto.TopY {
		return fmt.Errorf("active slots occupied")
	}
	if c.excluded {
		return fmt.Errorf("pair excluded by whitelist")
	}
	for _, ar := range active {
		ac := &candidate{l: ar.actor.cfg.L, r: ar.actor.cfg.R, pair: ar.actor.cfg.Pair}
		if candidatesOverlap(c, ac) {
			return fmt.Errorf("market already managed by %s", ar.actor.key)
		}
	}
	budget := rt.pairBudget()
	if rt.cfg.Auto.MaxTotalNotional > 0 {
		used := 0.0
		for _, ar := range active {
			used += ar.budget
		}
		if used+budget > rt.cfg.Auto.MaxTotalNotional {
			return fmt.Errorf("notional budget exceeded")
		}
	}
	qty, ok := c.SizedQty()
	if !ok {
		return fmt.Errorf("size unavailable")
	}
	if _, ok := c.Quote(time.Now(), rt.cfg.Strategy.MaxBookAge); !ok {
		return fmt.Errorf("books stale")
	}
	if !rt.cfg.DryRun {
		rt.applyLeverage(ctx, c.l, c.pair.Left)
		rt.applyLeverage(ctx, c.r, c.pair.Right)
	}
	a, err := rt.buildActor(c.pair, qty, rt.cfg.Auto.MaxSteps, c.l, c.r, c.est, c.vol, c.volL, c.volR, nil)
	if err != nil {
		return err
	}
	rt.startActor(ctx, c, a, budget, active, wg)
	return nil
}

func (rt *runtime) startActor(ctx context.Context, c *candidate, a *PairActor, budget float64, active map[string]*activeRun, wg *sync.WaitGroup) *activeRun {
	a.cfg.View = c
	a.cfg.MayOpen = rt.startupRecoveryReady
	a.cfg.MaxOpenNotional = rt.cfg.Auto.NotionalPerLeg
	actx, cancel := context.WithCancel(ctx)
	ar := &activeRun{actor: a, cancel: cancel, budget: budget, done: make(chan error, 1)}
	active[c.key] = ar
	if rt.mon != nil {
		rt.mon.Record(monitor.Event{Time: time.Now(), Pair: c.key, Kind: monitor.KindPairPromoted, Score: c.healthSnapshot().Score})
	}
	wg.Add(1)
	go func() { defer wg.Done(); ar.done <- a.Run(actx); close(ar.done) }()
	return ar
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
