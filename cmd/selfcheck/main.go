// Command selfcheck verifies the full flow against live mainnet.
//
//	go run ./cmd/selfcheck                    # read-only checks (no keys needed)
//	go run ./cmd/selfcheck -live              # + real 0.01 open/close round trip
//	go run ./cmd/selfcheck -live -rescue      # + forced one-leg failure -> rescue flatten
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"time"

	lclient "github.com/elliottech/lighter-go/client"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/engine"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/execution"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
	"github.com/Dog-Feng/dex-arbitr/internal/spread/estimator"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
	"github.com/Dog-Feng/dex-arbitr/internal/symbols"
)

var (
	pass, fail int
)

func check(ok bool, name string, detail string, args ...any) {
	mark := "PASS"
	if ok {
		pass++
	} else {
		mark = "FAIL"
		fail++
	}
	fmt.Printf("[%s] %-38s %s\n", mark, name, fmt.Sprintf(detail, args...))
}

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config")
	base := flag.String("symbol", "SNDK", "base symbol")
	qty := flag.Float64("qty", 0.01, "test qty")
	watch := flag.Duration("watch", 15*time.Second, "order book watch duration")
	live := flag.Bool("live", false, "place real orders (round trip)")
	rescue := flag.Bool("rescue", false, "with -live: force a one-leg failure and verify rescue flatten")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Println("config:", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	fmt.Println("== 1. venues & instruments ==")
	venues, err := engine.BuildVenues(cfg)
	check(err == nil, "build venues", "%v", err)
	if err != nil {
		os.Exit(1)
	}
	pc := cfg.Pairs[0]
	L, R := venues[pc.Left], venues[pc.Right]
	m := engine.Matcher(cfg)
	li, err1 := L.Instruments(ctx)
	ri, err2 := R.Instruments(ctx)
	check(err1 == nil && len(li) > 0, L.Name()+" perp instruments", "%d perps %v", len(li), err1)
	check(err2 == nil && len(ri) > 0, R.Name()+" perp instruments", "%d perps %v", len(ri), err2)
	spotLeak := false
	for _, i := range append(li, ri...) {
		if strings.Contains(i.Symbol, "/") {
			spotLeak = true
		}
	}
	check(!spotLeak, "spot markets filtered out", "")
	pairs := symbols.Match(m, li, ri)
	check(len(pairs) > 0, "cross-venue matched pairs", "%d common perps", len(pairs))

	fmt.Println("== 2. symbol match & precision ==")
	pair, err := engine.ResolvePair(ctx, m, *base, L, R)
	check(err == nil, "resolve "+*base, "%v", err)
	if err != nil {
		os.Exit(1)
	}
	fmt.Printf("     %s: %s#%d (size_dec %d, px_dec %d, min %.4f)  %s#%d (size_dec %d, px_dec %d, min %.4f)\n", pair.Base,
		L.Name(), pair.Left.MarketID, pair.Left.SizeDecimals, pair.Left.PriceDecimals, pair.Left.MinBase,
		R.Name(), pair.Right.MarketID, pair.Right.SizeDecimals, pair.Right.PriceDecimals, pair.Right.MinBase)
	check(pair.SizeDecimals == min(pair.Left.SizeDecimals, pair.Right.SizeDecimals), "aligned size decimals = min", "%d", pair.SizeDecimals)
	check(pair.MinQty >= math.Max(pair.Left.MinBase, pair.Right.MinBase)-1e-12, "aligned min qty = max", "%.4f", pair.MinQty)

	fmt.Println("== 3. live order books (WS) ==")
	wctx, wcancel := context.WithTimeout(ctx, *watch)
	chL, _ := L.SubscribeBook(wctx, pair.Left)
	chR, _ := R.SubscribeBook(wctx, pair.Right)
	var bl, br domain.Book
	nL, nR, crossed := 0, 0, 0
	est := estimator.NewSMA(estimator.Params{Window: 2 * time.Hour, MinWarmup: time.Hour, MinCoverage: 0.8, Interval: time.Second})
	tick := time.NewTicker(time.Second)
loop:
	for {
		select {
		case b := <-chL:
			bl = b
			nL++
			if !b.Valid() {
				crossed++
			}
		case b := <-chR:
			br = b
			nR++
			if !b.Valid() {
				crossed++
			}
		case now := <-tick.C:
			if q, ok := spread.Compute(bl, br); ok {
				est.Update(now, q.Mid)
				fmt.Printf("     %s  L %.2f/%.2f (%.4f/%.4f)  R %.2f/%.2f (%.4f/%.4f)  LS %+.2fbps SL %+.2fbps mid %+.2fbps\n",
					now.Format("15:04:05"), bl.Bids[0].Price, bl.Asks[0].Price, bl.Bids[0].Size, bl.Asks[0].Size,
					br.Bids[0].Price, br.Asks[0].Price, br.Bids[0].Size, br.Asks[0].Size, q.LS, q.SL, q.Mid)
			}
		case <-wctx.Done():
			break loop
		}
	}
	tick.Stop()
	wcancel()
	check(nL > 0 && bl.Valid(), L.Name()+" book stream", "%d updates, %d levels", nL, len(bl.Bids))
	check(nR > 0 && br.Valid(), R.Name()+" book stream", "%d updates, %d levels", nR, len(br.Bids))
	check(crossed == 0, "no crossed/empty books", "%d bad snapshots", crossed)

	fmt.Println("== 4. spread, mean, qty, strategy ==")
	q, ok := spread.Compute(bl, br)
	check(ok, "spread computed", "LS %+.2f SL %+.2f mid %+.2f bps", q.LS, q.SL, q.Mid)
	st := est.Stats()
	check(st.Samples > 0 && !st.Ready, "SMA sampling (warmup expected)", "samples %d mean %+.2fbps ready=%v (needs 60m)", st.Samples, st.Mean, st.Ready)
	okQty, why := pair.ValidQty(*qty, q.RefMid)
	check(okQty, fmt.Sprintf("qty %.4f valid on both", *qty), "notional %.2f USD %s", *qty*q.RefMid, why)
	p := strategy.Params{StepQty: *qty, MaxSteps: pc.MaxSteps, OpenBps: cfg.Strategy.OpenBps, CloseBps: cfg.Strategy.CloseBps,
		DepthMult: cfg.Strategy.DepthMult, CloseOrder: cfg.Strategy.CloseOrder, VolK: cfg.Strategy.VolK}
	strat, err := strategy.New(cfg.Strategy.Kind)
	if err != nil {
		check(false, "strategy", "%v", err)
	} else {
		a := strat.Decide(p, strategy.State{Mode: strategy.ModeRunning}, q, est.Mean(), true, 0)
		check(true, "decide (using short-window mean)", "%s %s", a.Kind, a.Reason)
	}
	need := *qty * cfg.Strategy.DepthMult
	fmt.Printf("     depth need %.4f: L bid %.4f ask %.4f | R bid %.4f ask %.4f\n", need, bl.Bids[0].Size, bl.Asks[0].Size, br.Bids[0].Size, br.Asks[0].Size)

	fmt.Println("== 5. signing ==")
	for _, v := range []exchange.Venue{L, R} {
		lv := v.(*lighter.Venue)
		inst := pair.Left
		if v == R {
			inst = pair.Right
		}
		req := domain.OrderReq{ClientID: 1, Inst: inst, Side: domain.Buy, Qty: *qty, Price: bl.Bids[0].Price}
		if v.CanTrade() {
			err := lv.CheckKey()
			check(err == nil, v.Name()+" API key matches chain", "%v", err)
			_, err = lv.SignOnly(req, 0)
			check(err == nil, v.Name()+" sign IOC order", "%v", err)
		} else {
			// ephemeral key: proves signer + tx encoding for this chain id (never sent)
			sk, _, _ := lclient.GenerateAPIKey()
			opts := map[string]string{"account_index": "1", "api_key_index": "4", "private_key": sk}
			kind := "lighter"
			if strings.Contains(v.Name(), "rh") {
				kind = "rhlighter"
			}
			tv, err := lighter.New(v.Name()+"-ephemeral", kind, opts)
			if err == nil {
				var js string
				js, err = tv.SignOnly(req, 0)
				if err == nil {
					fmt.Printf("     %s signed tx: %.120s...\n", v.Name(), js)
				}
				_ = tv.Close()
			}
			check(err == nil, v.Name()+" sign IOC order (ephemeral key)", "%v", err)
		}
	}

	if *live {
		liveFlow(ctx, L, R, pair, *qty, cfg.Strategy.DepthMult, *rescue, engine.ExecutionConfig(cfg))
	} else {
		fmt.Println("== 6. live orders: skipped (run with -live and API keys) ==")
	}
	for _, v := range venues {
		_ = v.Close()
	}
	fmt.Printf("\nRESULT: %d passed, %d failed\n", pass, fail)
	if fail > 0 {
		os.Exit(1)
	}
}

func liveFlow(ctx context.Context, L, R exchange.Venue, pair symbols.Pair, qty, depthMult float64, rescue bool, execCfg execution.Config) {
	fmt.Println("== 6. LIVE round trip ==")
	if !L.CanTrade() || !R.CanTrade() {
		check(false, "credentials", "both venues need keys (env LIGHTER_* / RH_*)")
		return
	}
	tr, err := execution.NewTracker(ctx, L, R)
	check(err == nil, "order stream subscribed", "%v", err)
	if err != nil {
		return
	}
	books := map[string]<-chan domain.Book{}
	books[L.Name()], _ = L.SubscribeBook(ctx, pair.Left)
	books[R.Name()], _ = R.SubscribeBook(ctx, pair.Right)
	latest := map[string]domain.Book{}
	refresh := func() {
		deadline := time.After(3 * time.Second)
		for len(latest) < 2 || !latest[L.Name()].Valid() || !latest[R.Name()].Valid() {
			select {
			case b := <-books[L.Name()]:
				latest[L.Name()] = b
			case b := <-books[R.Name()]:
				latest[R.Name()] = b
			case <-deadline:
				return
			}
		}
		for n, ch := range books { // drain to newest
			select {
			case b := <-ch:
				latest[n] = b
			default:
			}
		}
	}
	bookFn := func(v string, _ int) (domain.Book, bool) {
		refresh()
		b, ok := latest[v]
		return b, ok && b.Valid()
	}
	ex := execution.NewExecutor(execCfg, tr, execution.NewIDGen(), bookFn, nil, nil)
	time.Sleep(2 * time.Second) // let WS subscriptions settle

	pl0, e1 := execution.MarketPosition(ctx, L, pair.Left.MarketID)
	pr0, e2 := execution.MarketPosition(ctx, R, pair.Right.MarketID)
	check(e1 == nil && e2 == nil, "baseline positions", "L %.4f R %.4f %v %v", pl0, pr0, e1, e2)
	if e1 != nil || e2 != nil {
		return
	}

	leg := func(v exchange.Venue, inst domain.Instrument, side domain.Side) execution.LegSpec {
		b := latest[v.Name()]
		px := b.Bids[0].Price
		if side == domain.Buy {
			px = b.Asks[0].Price
		}
		return execution.LegSpec{Venue: v, Inst: inst, Side: side, Price: px}
	}

	// pick the direction whose top-of-book depth is sufficient
	refresh()
	q, _ := spread.Compute(latest[L.Name()], latest[R.Name()])
	dir := strategy.DirLS
	if q.SL > q.LS {
		dir = strategy.DirSL
	}
	ls, rs := dir.LegSides()
	fmt.Printf("     OPEN %s qty %.4f (LS %+.2f SL %+.2f bps)\n", dir, qty, q.LS, q.SL)
	res := ex.Execute(ctx, leg(L, pair.Left, ls), leg(R, pair.Right, rs), qty, false, pl0, pr0)
	report(res)
	check(math.Abs(res.PosL+res.PosR-(pl0+pr0)) < 1e-9, "open: hedged (net exposure 0)", "posL %.4f posR %.4f", res.PosL, res.PosR)
	hedged := math.Min(res.L.Filled, res.R.Filled)
	check(hedged > 0, "open: both legs filled", "fillL %.4f fillR %.4f", res.L.Filled, res.R.Filled)

	time.Sleep(3 * time.Second)
	if hedged > 0 {
		refresh()
		fmt.Printf("     CLOSE qty %.4f (reduce-only)\n", hedged)
		res2 := ex.Execute(ctx, leg(L, pair.Left, ls.Opposite()), leg(R, pair.Right, rs.Opposite()), domain.Round(hedged, pair.SizeDecimals), true, res.PosL, res.PosR)
		report(res2)
		check(math.Abs(res2.PosL-pl0) < 1e-9 && math.Abs(res2.PosR-pr0) < 1e-9, "close: back to baseline", "posL %.4f posR %.4f", res2.PosL, res2.PosR)
	}

	if rescue {
		time.Sleep(3 * time.Second)
		refresh()
		fmt.Println("     RESCUE test: L leg normal, R leg priced 2% away (will not fill) -> must flatten L")
		pl, _ := execution.MarketPosition(ctx, L, pair.Left.MarketID)
		pr, _ := execution.MarketPosition(ctx, R, pair.Right.MarketID)
		rl := leg(R, pair.Right, domain.Sell)
		rl.Price = domain.Round(latest[R.Name()].Asks[0].Price*1.02, pair.Right.PriceDecimals) // sell IOC far above bid: no fill
		res3 := ex.Execute(ctx, leg(L, pair.Left, domain.Buy), rl, qty, false, pl, pr)
		report(res3)
		check(res3.R.Filled == 0, "rescue: R leg did not fill", "fillR %.4f", res3.R.Filled)
		check(math.Abs(res3.PosL-pl) < 1e-9 && math.Abs(res3.PosR-pr) < 1e-9, "rescue: flattened to baseline", "posL %.4f posR %.4f orders %d", res3.PosL, res3.PosR, res3.RescueOrders)
	}
}

func report(r execution.Result) {
	fmt.Printf("     L: cid %d %s filled %.4f avg %.4f resolved=%v err=%v\n", r.L.ClientID, r.L.Status, r.L.Filled, r.L.AvgPrice, r.L.Resolved, r.L.SubmitErr)
	fmt.Printf("     R: cid %d %s filled %.4f avg %.4f resolved=%v err=%v\n", r.R.ClientID, r.R.Status, r.R.Filled, r.R.AvgPrice, r.R.Resolved, r.R.SubmitErr)
	fmt.Printf("     positions L %.4f R %.4f | rescue orders %d qty %.4f | %s\n", r.PosL, r.PosR, r.RescueOrders, r.RescueQty, r.Duration.Round(time.Millisecond))
}
