package main

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/Dog-Feng/dex-arbitr/internal/scan"
	"github.com/Dog-Feng/dex-arbitr/internal/strategy"
)

func TestBoardRequiresFreshMeanAndVolatility(t *testing.T) {
	now := time.Unix(1_600_000_000, 0)
	valid := scan.Score{Base: "fresh", Ts: now, Dir: strategy.DirLS, NetBps: 1, MuReady: true, VolReady: true}
	for _, tc := range []struct {
		name   string
		score  scan.Score
		status string
	}{
		{"missing mean", scan.Score{Ts: now, NetBps: 100, VolReady: true}, "unavailable"},
		{"missing volatility", scan.Score{Ts: now, NetBps: 100, MuReady: true}, "unavailable"},
		{"invalidated callback", scan.Score{Ts: now}, "unavailable"},
		{"expired", scan.Score{Ts: now.Add(-3 * time.Second), NetBps: 100, MuReady: true, VolReady: true}, "stale"},
		{"future", scan.Score{Ts: now.Add(time.Second), NetBps: 100, MuReady: true, VolReady: true}, "stale"},
		{"nonfinite", scan.Score{Ts: now, NetBps: math.NaN(), MuReady: true, VolReady: true}, "invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBoard([]pairSpec{{base: "broken"}, {base: "fresh"}})
			b.set(valid)
			broken := valid
			broken.Base, broken.NetBps = "broken", 100
			b.set(broken) // an old profitable callback must be replaced
			tc.score.Base = "broken"
			b.set(tc.score)
			if got := b.sorted(now)[0].Base; got != "fresh" {
				t.Fatalf("ranked %s ahead of fresh opportunity", got)
			}
			var out bytes.Buffer
			if err := render(&out, b, false, now); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.status) || strings.Contains(out.String(), "+100.00") {
				t.Fatalf("invalid score retained a numeric opportunity: %s", out.String())
			}
		})
	}
}

func TestBoardExpiresLastCallback(t *testing.T) {
	now := time.Unix(1_600_000_000, 0)
	b := newBoard([]pairSpec{{base: "pair"}})
	b.set(scan.Score{Base: "pair", Ts: now, Dir: strategy.DirLS, NetBps: 3, MuReady: true, VolReady: true})
	for _, tc := range []struct {
		name string
		at   time.Time
		want string
	}{
		{"live", now.Add(time.Second), "+3.00"},
		{"no more callbacks", now.Add(3 * time.Second), "stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := render(&out, b, false, tc.at); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Fatalf("display=%s, want %s", out.String(), tc.want)
			}
		})
	}
}

func TestValidateFlags(t *testing.T) {
	for _, tc := range []struct {
		name    string
		path    string
		refresh time.Duration
		args    []string
		wantErr bool
	}{
		{"valid", "config.yaml", time.Second, nil, false},
		{"zero refresh", "config.yaml", 0, nil, true},
		{"negative refresh", "config.yaml", -time.Second, nil, true},
		{"empty config", " ", time.Second, nil, true},
		{"extra argument", "config.yaml", time.Second, []string{"extra"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateFlags(tc.path, tc.refresh, tc.args); (err != nil) != tc.wantErr {
				t.Fatalf("error=%v, want error=%v", err, tc.wantErr)
			}
		})
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRenderReturnsOutputError(t *testing.T) {
	want := errors.New("output closed")
	if err := render(failingWriter{want}, newBoard(nil), false, time.Now()); !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
}

func TestReadOnlyOptionsKeepConfiguredMarketSource(t *testing.T) {
	input := map[string]string{"host": "https://example.test", "ws_url": "wss://example.test/ws", "chain_id": "304", "book_depth": "7", "private_key": "test-only-secret", "account_index": "12", "api_key_index": "4"}
	got := readOnlyOptions(input)
	for _, key := range []string{"host", "ws_url", "chain_id", "book_depth"} {
		if got[key] != input[key] {
			t.Fatalf("scanner lost source option %s", key)
		}
	}
	for _, key := range []string{"private_key", "account_index", "api_key_index"} {
		if _, exists := got[key]; exists {
			t.Fatalf("scanner received credential option %s", key)
		}
	}
	got["host"] = "changed"
	if input["host"] != "https://example.test" || input["private_key"] != "test-only-secret" {
		t.Fatal("read-only setup mutated trading configuration")
	}
}

func TestBoardPreservesSameBaseOnDifferentVenues(t *testing.T) {
	now := time.Now()
	b := newBoard(nil)
	for _, s := range []scan.Score{
		{Base: "BTC", Left: "A", Right: "B", Ts: now, NetBps: 3, MuReady: true, VolReady: true},
		{Base: "BTC", Left: "A", Right: "C", Ts: now, NetBps: 2, MuReady: true, VolReady: true},
	} {
		b.set(s)
	}
	rows := b.sorted(now)
	if len(rows) != 2 || rows[0].Right != "B" || rows[1].Right != "C" {
		t.Fatalf("venue combinations overwritten: %+v", rows)
	}
	var out bytes.Buffer
	if err := render(&out, b, false, now); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "A/B") || !strings.Contains(out.String(), "A/C") {
		t.Fatal("scanner does not identify venue combinations")
	}
}

func TestBoardAllowsExplicitlyDisabledVolatilityFilter(t *testing.T) {
	now := time.Now()
	s := scan.Score{Base: "TEST", Ts: now, MuReady: true, VolReady: false, VolOptional: true, NetBps: 1}
	if status := scoreStatus(s, now); status != "" {
		t.Fatalf("disabled volatility filter hides valid score: %s", status)
	}
	s.MuReady = false
	if status := scoreStatus(s, now); status != "unavailable" {
		t.Fatalf("disabled volatility filter bypassed mean warmup: %s", status)
	}
}
