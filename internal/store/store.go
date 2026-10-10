// Package store persists pair step state as JSON files (atomic writes).
package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

// PairState persisted per pair.
type PairState struct {
	NextID int64           `json:"next_id"`
	Steps  []strategy.Step `json:"steps"`
}

// Store is a directory of pair state files.
type Store struct{ dir string }

// New creates the directory if needed.
func New(dir string) (*Store, error) {
	return &Store{dir: dir}, os.MkdirAll(dir, 0o755)
}

func (s *Store) path(key string) string { return filepath.Join(s.dir, key+".json") }

// Load returns an empty state if none exists.
func (s *Store) Load(key string) (PairState, error) {
	var st PairState
	b, err := os.ReadFile(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return PairState{NextID: 1}, nil
	}
	if err != nil {
		return st, err
	}
	return st, json.Unmarshal(b, &st)
}

// Save atomically.
func (s *Store) Save(key string, st PairState) error {
	b, _ := json.MarshalIndent(st, "", "  ")
	tmp := s.path(key) + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(key))
}
