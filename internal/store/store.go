// Package store persists pair step state as durable, atomic JSON files.
package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// PairState persisted per pair. Missing daily fields in older files mean zero.
type PairState struct {
	Unresolved     bool            `json:"unresolved,omitempty"`
	CheckpointExit bool            `json:"checkpoint_exit,omitempty"`
	PnLUncertain   bool            `json:"pnl_uncertain,omitempty"`
	NextID         int64           `json:"next_id"`
	Steps          []strategy.Step `json:"steps"`
	// Interventions records completed external-intervention recoveries, used
	// to cap automatic re-entry into trading within a rolling window.
	Interventions []time.Time `json:"interventions,omitempty"`
	Day           string      `json:"day,omitempty"`
	VolumeUSD     float64     `json:"volume_usd,omitempty"`
	PnLUSD        float64     `json:"pnl_usd,omitempty"`
}

// Store serializes access to a directory of pair-state JSON files.
type Store struct {
	dir    string
	mu     sync.RWMutex
	closed bool
	keysMu sync.Mutex
	keys   map[string]*sync.Mutex
}

// New creates the state directory.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	return &Store{dir: dir, keys: make(map[string]*sync.Mutex)}, nil
}

// Close marks the store closed; further Load/Save/List calls fail.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return nil
}

func validKey(key string) error {
	if key == "" || key == "." || key == ".." || strings.ContainsAny(key, "/\\\x00") {
		return fmt.Errorf("invalid state key %q", key)
	}
	return nil
}

func (s *Store) path(key string) string { return filepath.Join(s.dir, key+".json") }

// Load returns an empty state if none exists and validates persisted ledgers.
func (s *Store) Load(key string) (PairState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return PairState{}, os.ErrClosed
	}
	return s.load(key)
}

func (s *Store) load(key string) (PairState, error) {
	if err := validKey(key); err != nil {
		return PairState{}, err
	}
	b, err := os.ReadFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return PairState{NextID: 1}, nil
	}
	if err != nil {
		return PairState{}, fmt.Errorf("read state %s: %w", key, err)
	}
	var st PairState
	if err := json.Unmarshal(b, &st); err != nil {
		return PairState{}, fmt.Errorf("decode state %s: %w", key, err)
	}
	if err := validate(st); err != nil {
		return PairState{}, fmt.Errorf("invalid state %s: %w", key, err)
	}
	return st, nil
}

// List returns every saved state indexed by its actual file key, including
// empty ledgers whose daily budgets still need to be restored.
func (s *Store) List() (map[string]PairState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list states: %w", err)
	}
	states := make(map[string]PairState)
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		key := strings.TrimSuffix(e.Name(), ".json")
		st, err := s.load(key)
		if err != nil {
			return nil, err
		}
		states[key] = st
	}
	return states, nil
}

// Save validates and marshals before touching the old state, serializes writes
// per key, then syncs both the replacement file and the containing directory.
func (s *Store) Save(key string, st PairState) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return os.ErrClosed
	}
	if err := validKey(key); err != nil {
		return err
	}
	if err := validate(st); err != nil {
		return fmt.Errorf("invalid state %s: %w", key, err)
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("encode state %s: %w", key, err)
	}
	s.keysMu.Lock()
	mu := s.keys[key]
	if mu == nil {
		mu = new(sync.Mutex)
		s.keys[key] = mu
	}
	s.keysMu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	if err := s.write(key, b); err != nil {
		return fmt.Errorf("save state %s: %w", key, err)
	}
	return nil
}

func (s *Store) write(key string, b []byte) (retErr error) {
	f, err := os.CreateTemp(s.dir, key+".*.tmp")
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, f.Close())
		}
		if err := os.Remove(f.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, err)
		}
	}()
	if err := f.Chmod(0o644); err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(f.Name(), s.path(key)); err != nil {
		return err
	}
	return syncDir(s.dir)
}

func finite(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func validate(st PairState) error {
	if st.CheckpointExit && !st.Unresolved {
		return fmt.Errorf("checkpoint_exit requires unresolved execution")
	}
	if st.NextID < 1 {
		return fmt.Errorf("next_id must be positive")
	}
	if st.Day != "" {
		day, err := time.Parse("2006-01-02", st.Day)
		if err != nil || day.Format("2006-01-02") != st.Day {
			return fmt.Errorf("invalid UTC budget day %q", st.Day)
		}
	}
	if !finite(st.VolumeUSD) || st.VolumeUSD < 0 || !finite(st.PnLUSD) {
		return fmt.Errorf("daily budget values must be finite and volume nonnegative")
	}
	ids := make(map[int64]bool)
	var dir strategy.Dir
	for _, step := range st.Steps {
		if step.ID < 1 || step.ID >= st.NextID || ids[step.ID] {
			return fmt.Errorf("step id %d must be unique, positive, and below next_id", step.ID)
		}
		ids[step.ID] = true
		if step.Dir != strategy.DirLS && step.Dir != strategy.DirSL {
			return fmt.Errorf("step %d has invalid direction", step.ID)
		}
		if dir != 0 && step.Dir != dir {
			return fmt.Errorf("steps have mixed directions")
		}
		dir = step.Dir
		if !finite(step.Qty) || step.Qty <= 0 || !finite(step.EntryBps) || !finite(step.Realized) {
			return fmt.Errorf("step %d has invalid quantity or PnL", step.ID)
		}
		if !finite(step.OpenL) || !finite(step.OpenR) || step.OpenL < 0 || step.OpenR < 0 {
			return fmt.Errorf("step %d has invalid entry prices", step.ID)
		}
		if !finite(step.RealizedUSD) || !finite(step.EntryFeesUSD) || step.EntryFeesUSD < 0 {
			return fmt.Errorf("step %d has invalid settlement amounts", step.ID)
		}
		if !finite(step.OriginalQty) || step.OriginalQty < 0 || (step.OriginalQty > 0 && step.OriginalQty+math.Max(1e-9, step.Qty*1e-8) < step.Qty) {
			return fmt.Errorf("step %d has invalid original quantity", step.ID)
		}
	}
	return nil
}
