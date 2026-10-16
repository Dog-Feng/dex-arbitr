// Command arbd runs the cross-DEX perpetual spread step-arbitrage engine.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/engine"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
)

// chinaZone is UTC+8 without daylight saving (China Standard Time).
var chinaZone = time.FixedZone("CST", 8*3600)

// replaceTimeAttr rewrites the handler's time attribute into China Standard
// Time so logs show the operator's wall clock instead of UTC.
func replaceTimeAttr(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey && a.Value.Kind() == slog.KindTime {
		a.Value = slog.TimeValue(a.Value.Time().In(chinaZone))
	}
	return a
}

func main() {
	path := flag.String("config", "configs/config.yaml", "config file")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: replaceTimeAttr,
	})))

	cfg, err := config.Load(*path)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := engine.Run(ctx, cfg); err != nil {
		slog.Error("engine", "err", err)
		os.Exit(1)
	}
}
