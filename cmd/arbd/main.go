// Command arbd runs the cross-DEX perpetual spread step-arbitrage engine.
package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/config"
	"github.com/Dog-Feng/dex-arbitr/internal/engine"
	_ "github.com/Dog-Feng/dex-arbitr/internal/exchange/hyperliquid"
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
	if !cfg.DryRun {
		release, err := lockAgentKeys(cfg)
		if err != nil {
			slog.Error("agent key lock", "err", err)
			os.Exit(1)
		}
		defer release()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := engine.Run(ctx, cfg); err != nil {
		slog.Error("engine", "err", err)
		os.Exit(1)
	}
}

// lockAgentKeys pins one lock file per configured private key in the state
// directory, so two live processes can never submit with the same signing key
// and nonce space. The lock file stores the holder pid; the returned function
// releases every lock.
func lockAgentKeys(cfg *config.Config) (func(), error) {
	if err := os.MkdirAll(cfg.StateDir, 0o755); err != nil {
		return nil, fmt.Errorf("create state dir: %w", err)
	}
	var held []*os.File
	release := func() {
		for _, f := range held {
			_ = os.Remove(f.Name())
			_ = f.Close()
		}
	}
	seen := map[string]bool{}
	for _, v := range cfg.Venues {
		key := strings.TrimPrefix(strings.TrimSpace(v.Opts["private_key"]), "0x")
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		sum := sha256.Sum256([]byte(key))
		name := filepath.Join(cfg.StateDir, fmt.Sprintf(".agent-%x.lock", sum[:8]))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				release()
				return nil, fmt.Errorf("signing key already in use by another process (lock %s)", name)
			}
			release()
			return nil, fmt.Errorf("lock %s: %w", name, err)
		}
		if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
			_ = f.Close()
			release()
			return nil, fmt.Errorf("write %s: %w", name, err)
		}
		held = append(held, f)
	}
	return release, nil
}
