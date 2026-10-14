// Command selfcheck verifies the full flow against live mainnet.
//
//	go run ./cmd/selfcheck                    # read-only checks (no keys needed)
//	go run ./cmd/selfcheck -live              # real open/close round trip on flat markets
//	go run ./cmd/selfcheck -live -rescue      # local one-leg rejection -> real rescue flatten
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/engine"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

type options struct {
	base   string
	qty    float64
	watch  time.Duration
	live   bool
	rescue bool
}

type checks struct {
	out        io.Writer
	pass, fail int
}

func (c *checks) check(ok bool, name, detail string, args ...any) {
	mark := "PASS"
	if ok {
		c.pass++
	} else {
		mark = "FAIL"
		c.fail++
	}
	fmt.Fprintf(c.out, "[%s] %-38s %s\n", mark, name, fmt.Sprintf(detail, args...))
}

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config")
	base := flag.String("symbol", "SNDK", "base symbol")
	qty := flag.Float64("qty", 0.01, "test qty")
	watch := flag.Duration("watch", 15*time.Second, "order book watch duration")
	live := flag.Bool("live", false, "place real orders (round trip; both markets must be flat)")
	rescue := flag.Bool("rescue", false, "with -live: reject the right order locally and verify real rescue flatten")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	os.Exit(runMain(*cfgPath, options{base: *base, qty: *qty, watch: *watch, live: *live, rescue: *rescue}))
}

func runMain(path string, opts options) int {
	cfg, err := config.Load(path)
	if err != nil {
		fmt.Println("config:", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	venues, err := engine.BuildVenues(cfg)
	if err != nil {
		fmt.Println("build venues:", err)
		return 1
	}
	return runChecks(ctx, cfg, opts, venues, os.Stdout)
}

func selectPair(cfg *config.Config, venues map[string]exchange.Venue) (config.Pair, exchange.Venue, exchange.Venue, error) {
	var pc config.Pair
	if len(cfg.Pairs) > 0 {
		pc = cfg.Pairs[0]
	} else if len(cfg.Venues) == 2 {
		pc = config.Pair{Left: cfg.Venues[0].Name, Right: cfg.Venues[1].Name, MaxSteps: cfg.Auto.MaxSteps}
	} else {
		return pc, nil, nil, fmt.Errorf("selfcheck needs a configured pair or exactly two venues")
	}
	if pc.Left == pc.Right || venues[pc.Left] == nil || venues[pc.Right] == nil {
		return pc, nil, nil, fmt.Errorf("selfcheck needs two distinct configured venues")
	}
	if pc.MaxSteps <= 0 {
		pc.MaxSteps = 1
	}
	return pc, venues[pc.Left], venues[pc.Right], nil
}

func runChecks(ctx context.Context, cfg *config.Config, opts options, venues map[string]exchange.Venue, out io.Writer) (code int) {
	c := &checks{out: out}
	defer func() {
		for name, v := range venues {
			if v != nil {
				if err := v.Close(); err != nil {
					c.check(false, name+" close", "%v", err)
				}
			}
		}
		fmt.Fprintf(out, "\nRESULT: %d passed, %d failed\n", c.pass, c.fail)
		if c.fail > 0 {
			code = 1
		}
	}()
	if opts.watch <= 0 || (opts.rescue && !opts.live) {
		c.check(false, "arguments", "watch must be positive; rescue requires live")
		return 1
	}
	pc, L, R, err := selectPair(cfg, venues)
	c.check(err == nil, "select venues", "%v", err)
	if err != nil {
		return 1
	}
	fmt.Fprintln(out, "== 1. venues & instruments ==")
	li, errL := L.Instruments(ctx)
	ri, errR := R.Instruments(ctx)
	c.check(errL == nil && len(li) > 0, L.Name()+" perp instruments", "%d perps %v", len(li), errL)
	c.check(errR == nil && len(ri) > 0, R.Name()+" perp instruments", "%d perps %v", len(ri), errR)
	if c.fail > 0 {
		return 1
	}
	spotLeak := false
	for _, instruments := range [][]domain.Instrument{li, ri} {
		for _, i := range instruments {
			spotLeak = spotLeak || strings.Contains(i.Symbol, "/")
		}
	}
	c.check(!spotLeak, "spot markets filtered out", "")
	m := engine.Matcher(cfg)
	pairs := symbols.Match(m, li, ri)
	c.check(len(pairs) > 0, "cross-venue matched pairs", "%d common perps", len(pairs))
	var pair symbols.Pair
	found := false
	for _, p := range pairs {
		if p.Base == m.Normalize(opts.base) {
			pair, found = p, true
			break
		}
	}
	c.check(found, "resolve "+opts.base, "")
	if c.fail > 0 {
		return 1
	}
	fmt.Fprintln(out, "== 2. symbol match & precision ==")
	c.check(pair.SizeDecimals == min(pair.Left.SizeDecimals, pair.Right.SizeDecimals), "aligned size decimals = min", "%d", pair.SizeDecimals)
	c.check(pair.MinQty >= math.Max(pair.Left.MinBase, pair.Right.MinBase), "aligned min qty = max", "%.8f", pair.MinQty)
	okQty, why := pair.ValidQty(opts.qty, 0)
	c.check(okQty, "aligned quantity", "%s", why)
	if c.fail > 0 {
		return 1
	}
	fmt.Fprintln(out, "== 3. live order books (WS) ==")
	wctx, cancel := context.WithTimeout(ctx, opts.watch)
	defer cancel()
	books, err := subscribeBooks(wctx, L, R, pair, cfg.Strategy.MaxBookAge)
	c.check(err == nil, "book streams subscribed", "%v", err)
	if err != nil {
		return 1
	}
	est := estimator.NewSMA(estimator.Params{Window: 2 * time.Hour, MinWarmup: time.Hour, MinCoverage: 0.8, Interval: time.Second})
	err = books.observe(wctx, ctx, est)
	c.check(err == nil, "book streams observed", "%v", err)
	c.check(books.nL > 0 && books.fresh(books.bl, L.Name(), pair.Left.MarketID), L.Name()+" book stream", "%d updates", books.nL)
	c.check(books.nR > 0 && books.fresh(books.br, R.Name(), pair.Right.MarketID), R.Name()+" book stream", "%d updates", books.nR)
	c.check(books.bad == 0, "no crossed/empty books", "%d bad snapshots", books.bad)
	cancel()
	if c.fail > 0 {
		return 1
	}
	fmt.Fprintln(out, "== 4. spread, mean, qty, strategy ==")
	q, ok := spread.Compute(books.bl, books.br)
	c.check(ok, "spread computed", "LS %+.2f SL %+.2f mid %+.2f bps", q.LS, q.SL, q.Mid)
	st := est.Stats()
	c.check(st.Samples > 0 && !st.Ready, "SMA sampling (warmup expected)", "samples %d ready=%v", st.Samples, st.Ready)
	err = validPairQty(pair, opts.qty, books.bl, books.br)
	c.check(err == nil, "qty valid on both venues", "%v", err)
	if c.fail > 0 {
		return 1
	}
	p := strategy.Params{StepQty: opts.qty, MaxSteps: pc.MaxSteps, OpenBps: cfg.Strategy.OpenBps, CloseBps: cfg.Strategy.CloseBps,
		DepthMult: cfg.Strategy.DepthMult, CloseOrder: cfg.Strategy.CloseOrder, VolK: cfg.Strategy.VolK}
	strat, err := strategy.New(cfg.Strategy.Kind)
	c.check(err == nil, "strategy", "%v", err)
	if err != nil {
		return 1
	}
	a := strat.Decide(p, strategy.State{Mode: strategy.ModeRunning}, q, est.Mean(), true, 0)
	c.check(true, "decide (using short-window mean)", "%s %s", a.Kind, a.Reason)
	fmt.Fprintln(out, "== 5. signing ==")
	for _, side := range []struct {
		v    exchange.Venue
		inst domain.Instrument
		book domain.Book
	}{{L, pair.Left, books.bl}, {R, pair.Right, books.br}} {
		err := verifySigning(ctx, side.v, domain.OrderReq{ClientID: 1, Inst: side.inst, Side: domain.Buy, Qty: opts.qty, Price: side.book.Asks[0].Price})
		c.check(err == nil, side.v.Name()+" signing", "%v", err)
	}
	if c.fail > 0 {
		return 1
	}
	if opts.live {
		err := liveFlow(ctx, L, R, pair, opts.qty, cfg.Strategy.DepthMult, cfg.Strategy.MaxBookAge, opts.rescue, engine.ExecutionConfig(cfg), out)
		c.check(err == nil, "live flow and final flat positions", "%v", err)
	} else {
		fmt.Fprintln(out, "== 6. live orders: skipped (run with -live and API keys) ==")
	}
	return 0
}

type signingVenue interface {
	CheckKey(context.Context) error
	SignOnly(domain.OrderReq, int64) (string, error)
	SignEphemeral(domain.OrderReq, int64) (string, error)
}

func verifySigning(ctx context.Context, v exchange.Venue, req domain.OrderReq) error {
	s, ok := v.(signingVenue)
	if !ok {
		return fmt.Errorf("venue %s does not support offline signing checks", v.Name())
	}
	if v.CanTrade() {
		if err := s.CheckKey(ctx); err != nil {
			return fmt.Errorf("verify configured key: %w", err)
		}
		if _, err := s.SignOnly(req, 0); err != nil {
			return fmt.Errorf("sign configured order: %w", err)
		}
	} else if _, err := s.SignEphemeral(req, 0); err != nil {
		return fmt.Errorf("sign ephemeral order for configured chain: %w", err)
	}
	return nil
}
