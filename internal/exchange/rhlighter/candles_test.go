package rhlighter

import "testing"

func TestCandleTime(t *testing.T) {
	ms := candleTime(1767700500000)
	if ms.Year() < 2025 {
		t.Fatalf("ms timestamp parsed as %s", ms)
	}
	sec := candleTime(1_700_000_000)
	if sec.Year() != 2023 {
		t.Fatalf("seconds timestamp parsed as %s", sec)
	}
}

func TestNormalizeResolution(t *testing.T) {
	res, dur, ok := normalizeResolution("")
	if !ok || res != "1h" || dur.Hours() != 1 {
		t.Fatalf("default = %s %s ok=%v", res, dur, ok)
	}
	if _, _, ok := normalizeResolution("3h"); ok {
		t.Fatal("3h should be rejected")
	}
}
