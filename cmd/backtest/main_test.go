package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunReportsApproximateInventoryAndAssumptions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.csv")
	if err := os.WriteFile(path, []byte("1,3,3,-3\n100000001,3,3,-3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run([]string{"-samples", path}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"approximate=true", "legacy proxy", "decision=200ms", "end_steps", "end_qty", "mtm_usd", "not forcibly closed"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in %s", want, out.String())
		}
	}
}

func TestRunRejectsInvalidParamsAndGapsBeforeReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "samples.csv")
	if err := os.WriteFile(path, []byte("1,3,3,-3\n1000000001,3,3,-3\n"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"-samples", path}, {"-samples", path, "-step_qty", "0"}, {"-samples", path, "-vol_k", "NaN"}, {"-samples", path, "-unknown"}} {
		var out bytes.Buffer
		if err := run(args, &out); err == nil {
			t.Fatalf("accepted %v", args)
		}
		if out.Len() != 0 {
			t.Fatalf("partial success report=%q", out.String())
		}
	}
	var out bytes.Buffer
	if err := run([]string{"-samples", path, "-allow_gaps"}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "gaps") {
		t.Fatal("missing gap report")
	}
}
