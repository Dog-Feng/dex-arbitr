// Command hltransfer moves collateral between perp dexs (or spot) for a
// Hyperliquid venue, e.g. funding the Entropy (io) dex from the default dex:
//
//	go run ./cmd/hltransfer -config configs/anth-live.yaml -venue entropy -from "" -to io -amount 30
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/exchange"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/hyperliquid"
)

// sender is the transfer surface a hyperliquid venue exposes.
type sender interface {
	exchange.Venue
	SendAsset(ctx context.Context, sourceDex, destinationDex, amount string) error
}

func main() {
	cfgPath := flag.String("config", "configs/config.yaml", "config file")
	venue := flag.String("venue", "entropy", "venue name to transfer through")
	from := flag.String("from", "", "source dex (empty = default perps dex, spot = spot)")
	to := flag.String("to", "io", "destination dex")
	amount := flag.String("amount", "", "USDC amount as a decimal string")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if *amount == "" {
		slog.Error("flags", "err", "amount is required")
		os.Exit(2)
	}
	if err := validateAmount(*amount); err != nil {
		slog.Error("flags", "err", err)
		os.Exit(2)
	}
	c, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	var kind string
	var opts map[string]string
	for _, vc := range c.Venues {
		if vc.Name == *venue {
			kind, opts = vc.Kind, vc.Opts
			break
		}
	}
	if opts == nil {
		slog.Error("venue not configured", "venue", *venue)
		os.Exit(1)
	}
	v, err := exchange.New(kind, *venue, opts)
	if err != nil {
		slog.Error("build venue", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := v.Close(); err != nil {
			slog.Warn("venue close", "err", err)
		}
	}()
	s, ok := v.(sender)
	if !ok || !v.CanTrade() {
		slog.Error("venue has no transfer credentials", "venue", *venue)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := s.SendAsset(ctx, *from, *to, *amount); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			slog.Error("transfer outcome unknown; check balances before retrying", "err", err)
		} else {
			slog.Error("transfer", "err", err)
		}
		os.Exit(1)
	}
	slog.Info("transfer accepted", "from", *from, "to", *to, "amount", *amount)
}

// validateAmount requires a finite positive decimal that fits the USDC wire
// precision (8 decimals on Hyperliquid).
func validateAmount(s string) error {
	f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f <= 0 {
		return errors.New("amount must be a finite positive decimal")
	}
	parts := strings.Split(strings.TrimSpace(s), ".")
	if len(parts) == 2 && len(parts[1]) > 8 {
		return errors.New("amount has more than 8 decimal places")
	}
	return nil
}
