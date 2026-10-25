// Command pairlist prints every matched pair across the configured venues
// using the engine's own symbol matcher (symbols.Matcher via scan.MatchAll),
// together with the aligned precision, minimum quantity and round-trip fee
// basis each pair would trade with. It is read-only: trading credentials are
// stripped before venues are built and no order is ever submitted.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/hyperliquid"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
	"github.com/Dog-Feng/dex-arbitr/internal/scan"
)

// readOnlyOptions strips trading credentials so the listing can never trade.
func readOnlyOptions(opts map[string]string) map[string]string {
	copy := make(map[string]string, len(opts))
	for key, value := range opts {
		switch key {
		case "private_key", "account_index", "api_key_index":
			continue
		}
		copy[key] = value
	}
	return copy
}

func main() {
	path := flag.String("config", "configs/config.yaml", "config file")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	c, err := config.Load(*path)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	venues := map[string]exchange.Venue{}
	defer func() {
		for _, v := range venues {
			_ = v.Close()
		}
	}()
	for _, vc := range c.Venues {
		v, err := exchange.New(vc.Kind, vc.Name, readOnlyOptions(vc.Opts))
		if err != nil {
			slog.Error("venue", "name", vc.Name, "err", err)
			os.Exit(1)
		}
		venues[vc.Name] = v
	}
	names := make([]string, 0, len(venues))
	for name := range venues {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		insts, err := venues[name].Instruments(ctx)
		if err != nil {
			slog.Error("instruments", "venue", name, "err", err)
			os.Exit(1)
		}
		fmt.Printf("venue %-10s perpetuals: %d\n", name, len(insts))
	}
	matches, err := scan.MatchAll(ctx, c.BuildMatcher(), venues)
	if err != nil {
		slog.Error("match", "err", err)
		os.Exit(1)
	}
	if len(matches) == 0 {
		fmt.Println("no matched pairs")
		return
	}
	fmt.Printf("\nmatched pairs: %d\n\n", len(matches))
	fmt.Printf("%-12s %-26s %-26s %-8s %-12s %-16s %-10s %-10s\n",
		"base", "left", "right", "sz_dec", "min_qty", "px_dec l/r", "taker_l", "taker_r")
	for _, m := range matches {
		feeBps := 2 * (m.Pair.Left.TakerFee + m.Pair.Right.TakerFee) * 1e4
		fmt.Printf("%-12s %-12s#%-13d %-12s#%-13d %-8d %-12s %-16s %-9s %-10s fee_bps=%.2f\n",
			m.Pair.Base, m.L.Name(), m.Pair.Left.MarketID, m.R.Name(), m.Pair.Right.MarketID,
			m.Pair.SizeDecimals, fmt.Sprintf("%.6f", m.Pair.MinQty),
			fmt.Sprintf("%d/%d", m.Pair.Left.PriceDecimals, m.Pair.Right.PriceDecimals),
			fmt.Sprintf("%.4f%%", m.Pair.Left.TakerFee*100), fmt.Sprintf("%.4f%%", m.Pair.Right.TakerFee*100), feeBps)
	}
}
