//go:build !windows

package platform

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type posixLock struct {
	fd *os.File
}

func (p *posixLock) Release() error {
	err := unix.Flock(int(p.fd.Fd()), unix.LOCK_UN)
	closeErr := p.fd.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// AcquireSingleInstanceLock obtém flock exclusivo em dir/name.lock.
// Retorna erro se já bloqueado por outro processo.
func AcquireSingleInstanceLock(dir, name string) (SingleInstanceLock, error) {
	path := filepath.Join(dir, name+".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open_lock: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("already_running lock=%s: %w", name, err)
	}
	return &posixLock{fd: f}, nil
}
