package main

import (
	"log/slog"
	"testing"
	"time"
)

func TestReplaceTimeAttrUsesChinaTime(t *testing.T) {
	utc := time.Date(2026, 10, 16, 1, 2, 3, 123456789, time.UTC)
	got := replaceTimeAttr(nil, slog.Attr{Key: slog.TimeKey, Value: slog.TimeValue(utc)})
	if got.Value.Kind() != slog.KindTime {
		t.Fatal("time attribute lost its time kind")
	}
	gt := got.Value.Time()
	if !gt.Equal(utc) {
		t.Fatalf("instant changed: %v -> %v", utc, gt)
	}
	if _, off := gt.Zone(); off != 8*3600 {
		t.Fatalf("offset=%d, want +08:00", off)
	}
	if gt.Hour() != 9 {
		t.Fatalf("wall clock hour=%d, want 9", gt.Hour())
	}
	other := replaceTimeAttr(nil, slog.Attr{Key: "msg", Value: slog.StringValue("x")})
	if other.Key != "msg" || other.Value.String() != "x" {
		t.Fatalf("non-time attr mutated: %+v", other)
	}
}
