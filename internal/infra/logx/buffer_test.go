package logx

import (
	"log/slog"
	"testing"
	"time"
)

func TestBufferWrapsAndFilters(t *testing.T) {
	b := NewBuffer(3)
	b.Append(Record{Time: time.Now(), Level: "INFO", Message: "a", Exchange: "lighter"})
	b.Append(Record{Time: time.Now(), Level: "WARN", Message: "b", Exchange: "lighter"})
	b.Append(Record{Time: time.Now(), Level: "INFO", Message: "c", Exchange: "other"})
	b.Append(Record{Time: time.Now(), Level: "INFO", Message: "d", Exchange: "lighter"})

	all := b.List("", "", 10)
	if len(all) != 3 || all[0].Message != "d" || all[2].Message != "b" {
		t.Fatalf("wrapped list = %+v", all)
	}
	only := b.List("lighter", "INFO", 10)
	if len(only) != 1 || only[0].Message != "d" {
		t.Fatalf("filtered = %+v", only)
	}
}

func TestHandlerWritesBuffer(t *testing.T) {
	buf := NewBuffer(10)
	h := NewHandler(slog.NewTextHandler(discard{}, nil), buf)
	log := slog.New(h)
	log.Info("hello", "exchange", "lighter")
	got := buf.List("lighter", "", 1)
	if len(got) != 1 || got[0].Message != "hello" {
		t.Fatalf("got %+v", got)
	}
}

func TestHandlerCapturesLoggerWithExchange(t *testing.T) {
	buf := NewBuffer(10)
	h := NewHandler(slog.NewTextHandler(discard{}, nil), buf)
	log := slog.New(h).With("exchange", "lighter")
	log.Info("epoch advanced", "from", 1, "to", 2)
	got := buf.List("lighter", "", 1)
	if len(got) != 1 {
		t.Fatalf("expected 1 log, got %d: %+v", len(got), buf.List("", "", 10))
	}
	if got[0].Message != "epoch advanced" || got[0].Exchange != "lighter" {
		t.Fatalf("got %+v", got[0])
	}
}

type discard struct{}

func (discard) Write(p []byte) (int, error) { return len(p), nil }
