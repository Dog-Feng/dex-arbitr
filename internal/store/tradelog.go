package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// TradeLog appends one serialized trade-event line per call. It implements the
// monitor's TradeSink so the engine can wire persistence at startup without
// importing monitor into store.
type TradeLog struct {
	mu     sync.Mutex
	f      *os.File
	closed bool
}

// NewTradeLog opens (or creates) the trade log in append mode.
func NewTradeLog(path string) (*TradeLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open trade log: %w", err)
	}
	return &TradeLog{f: f}, nil
}

// WriteTrade appends one pre-serialized line.
func (t *TradeLog) WriteTrade(line []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return os.ErrClosed
	}
	if _, err := t.f.Write(line); err != nil {
		return fmt.Errorf("append trade log: %w", err)
	}
	return nil
}

// Close flushes and closes the log.
func (t *TradeLog) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	if err := t.f.Close(); err != nil {
		return fmt.Errorf("close trade log: %w", err)
	}
	return nil
}
