// Command arbd runs the cross-DEX perpetual spread step-arbitrage engine.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/engine"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/lighter"
)

func main() {
	path := flag.String("config", "configs/config.yaml", "config file")
	flag.Parse()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

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
