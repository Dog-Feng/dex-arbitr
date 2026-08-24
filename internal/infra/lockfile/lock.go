// Package lockfile 防止同一数据目录被两个进程打开。
//
// 两个进程共用一份 SQLite 和同一组 Lighter 密钥时，nonce 会立刻错乱。
package lockfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Lock 持有数据目录上的排他锁。
type Lock struct {
	path string
}

// Acquire 在 dir/gridbot.lock 上创建排他锁。已存在且进程仍在则失败。
func Acquire(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("lockfile: 创建数据目录失败: %w", err)
	}
	path := filepath.Join(dir, "gridbot.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return nil, fmt.Errorf("lockfile: %w", err)
		}
		if stale(path) {
			_ = os.Remove(path)
			return Acquire(dir)
		}
		return nil, fmt.Errorf("数据目录 %s 已被另一个进程占用（%s）。若确认没有在跑，删除该文件后重试", dir, path)
	}
	if _, err := fmt.Fprintf(f, "%d\n", os.Getpid()); err != nil {
		f.Close()
		_ = os.Remove(path)
		return nil, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return nil, err
	}
	return &Lock{path: path}, nil
}

// Release 删除锁文件。
func (l *Lock) Release() {
	if l == nil {
		return
	}
	_ = os.Remove(l.path)
}

func stale(path string) bool {
	raw, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 0 {
		return true
	}
	if pid == os.Getpid() {
		return false
	}
	proc, err := os.FindProcess(pid)
	if err != nil {
		return true
	}
	return !alive(proc)
}
