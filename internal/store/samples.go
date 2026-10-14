package store

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

// SampleSink appends spread samples to a file for offline calibration
// (signature plots, horizon-move quantiles). Writes are buffered by the OS and
// flushed on Close. Add writes the four legacy columns; AddQuote appends
// execution prices, depth and book timestamps for reproducible backtests.
type SampleSink struct {
	mu     sync.Mutex
	f      *os.File
	closed bool
}

// NewSampleSink opens (or creates) the sample file in append mode.
func NewSampleSink(path string) (*SampleSink, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &SampleSink{f: f}, nil
}

// Add appends one sample.
func (s *SampleSink) Add(t time.Time, mid, ls, sl float64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if t.IsZero() || t.UnixNano() <= 0 {
		return fmt.Errorf("sample time must be after the Unix epoch")
	}
	for _, v := range []float64{mid, ls, sl} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("sample values must be finite")
		}
	}
	if _, err := fmt.Fprintf(s.f, "%d,%g,%g,%g\n", t.UnixNano(), mid, ls, sl); err != nil {
		return fmt.Errorf("append sample: %w", err)
	}
	return nil
}

// AddQuote appends execution-relevant prices, depth, and timestamps after the
// four legacy columns: Lbid,Lask,Rbid,Rask,ref,LbidQty,LaskQty,RbidQty,RaskQty,LTs,RTs.
func (s *SampleSink) AddQuote(t time.Time, q spread.Quote) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if !q.L.Valid() || !q.R.Valid() {
		return fmt.Errorf("sample quote has invalid books")
	}
	if t.IsZero() || t.UnixNano() <= 0 || q.L.Ts.IsZero() || q.L.Ts.UnixNano() <= 0 || q.R.Ts.IsZero() || q.R.Ts.UnixNano() <= 0 {
		return fmt.Errorf("sample and book times must be after the Unix epoch")
	}
	values := []float64{q.Mid, q.LS, q.SL, q.L.Bids[0].Price, q.L.Asks[0].Price,
		q.R.Bids[0].Price, q.R.Asks[0].Price, q.RefMid, q.L.Bids[0].Size,
		q.L.Asks[0].Size, q.R.Bids[0].Size, q.R.Asks[0].Size}
	for i, v := range values {
		if !finite(v) || (i >= 3 && i <= 7 && v <= 0) || (i >= 8 && v < 0) {
			return fmt.Errorf("sample quote contains invalid price or depth")
		}
	}
	if _, err := fmt.Fprintf(s.f, "%d,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,%g,%d,%d\n",
		t.UnixNano(), values[0], values[1], values[2], values[3], values[4], values[5], values[6],
		values[7], values[8], values[9], values[10], values[11], q.L.Ts.UnixNano(), q.R.Ts.UnixNano()); err != nil {
		return fmt.Errorf("append quote sample: %w", err)
	}
	return nil
}

// Close flushes and closes the file.
func (s *SampleSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return errors.Join(s.f.Sync(), s.f.Close())
}
