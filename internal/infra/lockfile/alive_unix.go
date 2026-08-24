//go:build unix

package lockfile

import (
	"os"
	"syscall"
)

func alive(p *os.Process) bool {
	return p.Signal(syscall.Signal(0)) == nil
}
