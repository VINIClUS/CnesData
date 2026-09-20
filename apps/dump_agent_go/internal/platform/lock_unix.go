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

// Release unlocks, closes, and best-effort unlinks the lock file. Removing
// the path is safe here: flock is bound to the open file description, not
// the directory entry, so deleting the name doesn't affect this (already
// released) handle. Absence of the file (os.IsNotExist) is not an error —
// idempotent Release matches the rest of this package's uninstall/cleanup
// conventions. See H8 in docs/edge-agent-audit-2026-09-20.md — previously the
// lock file survived every uninstall as unexplained residue.
func (p *posixLock) Release() error {
	unlockErr := unix.Flock(int(p.fd.Fd()), unix.LOCK_UN)
	path := p.fd.Name()
	closeErr := p.fd.Close()
	removeErr := os.Remove(path)
	if unlockErr != nil {
		return unlockErr
	}
	if closeErr != nil {
		return closeErr
	}
	if removeErr != nil && !os.IsNotExist(removeErr) {
		return removeErr
	}
	return nil
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
