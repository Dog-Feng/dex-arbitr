package store

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// SampleSink appends spread samples to a file for offline calibration
// (signature plots, horizon-move quantiles). Writes are buffered by the OS and
// flushed on Close; each line is "unixNano,midBps,lsBps,slBps".
type SampleSink struct {
	mu sync.Mutex
	f  *os.File
}

// NewSampleSink opens (or creates) the sample file in append mode.
func NewSampleSink(path string) (*SampleSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &SampleSink{f: f}, nil
}

// Add appends one sample.
func (s *SampleSink) Add(t time.Time, mid, ls, sl float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.f, "%d,%g,%g,%g\n", t.UnixNano(), mid, ls, sl)
}

// Close flushes and closes the file.
func (s *SampleSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
