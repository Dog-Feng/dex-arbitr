package store

import (
	"encoding/csv"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/domain"
	"github.com/Dog-Feng/dex-arbitr/internal/spread"
)

func sampleQuote(t *testing.T, at time.Time) spread.Quote {
	t.Helper()
	l := domain.Book{Ts: at.Add(-time.Millisecond), Bids: []domain.Level{{Price: 100, Size: 0}}, Asks: []domain.Level{{Price: 101, Size: 2}}}
	r := domain.Book{Ts: at.Add(-2 * time.Millisecond), Bids: []domain.Level{{Price: 102, Size: 3}}, Asks: []domain.Level{{Price: 103, Size: 4}}}
	q, ok := spread.Compute(l, r)
	if !ok {
		t.Fatal("sample quote invalid")
	}
	return q
}

func TestSampleQuotePersistsBooksAndZeroDepth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "samples.csv")
	s, err := NewSampleSink(path)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_760_000_000, 42)
	q := sampleQuote(t, at)
	if err := s.AddQuote(at, q); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil || len(rows) != 1 || len(rows[0]) != 15 {
		t.Fatalf("quote row: %v, %v", rows, err)
	}
	row := rows[0]
	if row[0] != strconv.FormatInt(at.UnixNano(), 10) || row[4] != "100" || row[5] != "101" || row[6] != "102" || row[7] != "103" || row[9] != "0" || row[13] != strconv.FormatInt(q.L.Ts.UnixNano(), 10) || row[14] != strconv.FormatInt(q.R.Ts.UnixNano(), 10) {
		t.Fatalf("book information lost: %v", row)
	}
	if err := s.AddQuote(at, q); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("append after Close: %v", err)
	}
}

func TestSampleRejectsInvalidQuoteWithoutWriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.csv")
	s, err := NewSampleSink(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	at := time.Unix(1_760_000_000, 0)
	for _, tc := range []struct {
		name   string
		mutate func(*spread.Quote)
	}{
		{"empty book", func(q *spread.Quote) { q.L.Bids = nil }},
		{"nonfinite spread", func(q *spread.Quote) { q.Mid = math.NaN() }},
		{"nonfinite reference", func(q *spread.Quote) { q.RefMid = math.Inf(1) }},
		{"negative depth", func(q *spread.Quote) { q.L.Bids[0].Size = -1 }},
		{"missing book time", func(q *spread.Quote) { q.R.Ts = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := sampleQuote(t, at)
			tc.mutate(&q)
			if err := s.AddQuote(at, q); err == nil {
				t.Fatal("invalid quote accepted")
			}
		})
	}
	if err := s.Add(time.Time{}, 0, 0, 0); err == nil {
		t.Fatal("missing sample time accepted")
	}
	if err := s.Add(at, 0, math.NaN(), 0); err == nil {
		t.Fatal("nonfinite legacy value accepted")
	}
	data, err := os.ReadFile(path)
	if err != nil || len(data) != 0 {
		t.Fatalf("invalid values reached journal: %q, %v", data, err)
	}
}

func TestSamplePropagatesWriteAndCloseErrors(t *testing.T) {
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "samples.csv"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	s := &SampleSink{f: f}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Add(time.Now(), 0, 0, 0); err == nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("write failure hidden: %v", err)
	}
	if err := s.Close(); err == nil || !errors.Is(err, os.ErrClosed) {
		t.Fatalf("flush/close failure hidden: %v", err)
	}
}

func TestSampleConcurrentAppendsRemainWholeRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.csv")
	s, err := NewSampleSink(path)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Add(time.Now(), float64(i), 1, 2); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
	if err != nil || len(rows) != 16 {
		t.Fatalf("concurrent appends: %v, %v", rows, err)
	}
	for _, row := range rows {
		if len(row) != 4 {
			t.Fatalf("partial/interleaved row: %v", row)
		}
	}
}
