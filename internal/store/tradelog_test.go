package store

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTradeLogAppendAndClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trades", "trades.jsonl")
	tl, err := NewTradeLog(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := tl.WriteTrade([]byte("{\"kind\":\"executed\"}\n")); err != nil {
		t.Fatal(err)
	}
	if err := tl.WriteTrade([]byte("{\"kind\":\"status\"}\n")); err != nil {
		t.Fatal(err)
	}
	if err := tl.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tl.WriteTrade([]byte("x\n")); err == nil {
		t.Fatal("write after close succeeded")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 2 || lines[0] != `{"kind":"executed"}` || lines[1] != `{"kind":"status"}` {
		t.Fatalf("unexpected log contents: %q", string(b))
	}
}
