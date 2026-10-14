package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestReadSamplesAcceptsHistoricalFormats(t *testing.T) {
	for _, tc := range []struct {
		name string
		data string
		ts   []int64
		mid  []float64
	}{
		{"legacy", "-100000000, 1.5, 1, -1\n\n0,2.5,2,-2\n", []int64{-100000000, 0}, []float64{1.5, 2.5}},
		{"quote", "1600000000000000000,1.5,1,-1,100,101,102,103,101.5,1,1,2,2,1599999999999999999,1599999999999999998\n", []int64{1600000000000000000}, []float64{1.5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts, mid, err := readSamples(strings.NewReader(tc.data))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(ts, tc.ts) || !reflect.DeepEqual(mid, tc.mid) {
				t.Fatalf("ts=%v mid=%v", ts, mid)
			}
		})
	}
}

func TestReadSamplesRejectsCorruptionAndReordering(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  string
		want string
	}{
		{"missing fields", "2,1", "expected 4 or 15 columns"},
		{"bad timestamp", "bad,1,1,1", "timestamp"},
		{"bad mid", "2,bad,1,1", "column 2"},
		{"nan mid", "2,NaN,1,1", "finite"},
		{"infinite unused value", "2,1,+Inf,1", "finite"},
		{"duplicate timestamp", "1,1,1,1", "strictly increase"},
		{"backward timestamp", "0,1,1,1", "strictly increase"},
		{"bad book timestamp", "2,1,1,1,1,1,1,1,1,1,1,1,1,NaN,2", "column 14"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := readSamples(strings.NewReader("1,0,0,0\n" + tc.row + "\n"))
			if err == nil || !strings.Contains(err.Error(), "line 2") || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want line 2 and %s", err, tc.want)
			}
		})
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReadSamplesReportsReadErrors(t *testing.T) {
	want := errors.New("read failed")
	r := io.MultiReader(strings.NewReader("1,0,0,0\n"), failingReader{want})
	if _, _, err := readSamples(r); !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
	if _, _, err := readSamples(strings.NewReader(strings.Repeat("1", 70_000))); err == nil || !strings.Contains(err.Error(), "read samples") {
		t.Fatalf("scanner limit error=%v", err)
	}
}

func TestIncrementsExcludeLongGaps(t *testing.T) {
	const ms = int64(time.Millisecond)
	for _, tc := range []struct {
		name string
		ts   []int64
		mid  []float64
		want []float64
	}{
		{"continuous historical", []int64{-500 * ms, -400 * ms, -300 * ms, -200 * ms, -100 * ms, 0}, []float64{0, 1, 2, 3, 4, 5}, []float64{5}},
		{"gap cannot substitute horizon", []int64{0, 100 * ms, 1000 * ms, 1100 * ms}, []float64{0, 1, 100, 101}, []float64{}},
		{"restart after gap", []int64{0, 100 * ms, 1000 * ms, 1100 * ms, 1200 * ms, 1300 * ms, 1400 * ms, 1500 * ms}, []float64{0, 1, 100, 101, 102, 103, 104, 105}, []float64{5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := increments(tc.ts, tc.mid, 500*ms, 150*ms)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("increments=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestReportExplainsUnusableData(t *testing.T) {
	var output bytes.Buffer
	ts := []int64{0, int64(100 * time.Millisecond), int64(time.Second), int64(1100 * time.Millisecond)}
	if err := report(&output, ts, []float64{0, 1, 100, 101}, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"gaps=1", "50ms: unavailable", "100ms: 1.0000 bps (increments=2)", "500ms: unavailable", "500ms increment distribution ==\n  unavailable"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("report missing %q: %s", want, output.String())
		}
	}
}

func TestRunHistoricalCalibrationAndInvalidFlags(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.csv")
	var samples strings.Builder
	for i := 0; i < 6; i++ {
		if _, err := fmt.Fprintf(&samples, "%d,%d,0,0\n", int64(1_600_000_000_000_000_000)+int64(i)*int64(100*time.Millisecond), i); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path, []byte(samples.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"default", []string{"-f", path}, "500ms: 5.0000 bps (increments=1)"},
		{"missing path", nil, "usage"},
		{"missing file", []string{"-f", path + ".missing"}, "open samples"},
		{"zero interval", []string{"-f", path, "-interval", "0"}, "interval must be positive"},
		{"negative interval", []string{"-f", path, "-interval", "-1ms"}, "interval must be positive"},
		{"overflowing gap limit", []string{"-f", path, "-interval", "2500000h"}, "gap limit"},
		{"extra argument", []string{"-f", path, "extra"}, "unexpected arguments"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := run(tc.args, &out)
			if tc.name == "default" {
				if err != nil || !strings.Contains(out.String(), tc.want) {
					t.Fatalf("error=%v output=%s", err, out.String())
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestCalibrationRejectsOverflowingMoves(t *testing.T) {
	if _, err := increments([]int64{0, 1}, []float64{-math.MaxFloat64, math.MaxFloat64}, 1, 1); err == nil {
		t.Fatal("overflowing move accepted")
	}
	if got := rms([]float64{1e200, -1e200}); got != 1e200 {
		t.Fatalf("finite large-move RMS=%v", got)
	}
}
