//go:build windows

package store

// syncDir is a no-op on Windows: directory handles cannot be fsynced there.
// State-file durability on the Linux production target is unaffected.
func syncDir(string) error { return nil }
