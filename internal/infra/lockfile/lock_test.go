package lockfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAcquireAndRelease(t *testing.T) {
	dir := t.TempDir()
	l, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil {
		t.Fatal("second acquire should fail")
	}
	l.Release()
	l2, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	l2.Release()
}

func TestStaleLockIsTakenOver(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gridbot.lock")
	if err := os.WriteFile(path, []byte("999999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := Acquire(dir)
	if err != nil {
		t.Fatalf("stale pid 1 should be takeable: %v", err)
	}
	l.Release()
}
