//go:build !windows

package store

import (
	"errors"
	"os"
)

// syncDir flushes directory metadata so a just-renamed state file is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
