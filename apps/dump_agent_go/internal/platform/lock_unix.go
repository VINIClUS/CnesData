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
	parent, err := lockDirectory(filepath.Dir(p.fd.Name()))
	if err != nil {
		return p.releaseWithoutUnlink(err)
	}
	path := p.fd.Name()
	unlockErr := unix.Flock(int(p.fd.Fd()), unix.LOCK_UN)
	closeErr := p.fd.Close()
	removeErr := os.Remove(path)
	dirErr := unlockDirectory(parent)
	return firstError(unlockErr, closeErr, ignoreMissing(removeErr), dirErr)
}

func (p *posixLock) releaseWithoutUnlink(parentErr error) error {
	unlockErr := unix.Flock(int(p.fd.Fd()), unix.LOCK_UN)
	closeErr := p.fd.Close()
	return firstError(parentErr, unlockErr, closeErr)
}

func lockDirectory(path string) (*os.File, error) {
	dir, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open_lock_dir=%w", err)
	}
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX); err != nil {
		_ = dir.Close()
		return nil, fmt.Errorf("lock_dir=%w", err)
	}
	return dir, nil
}

func unlockDirectory(dir *os.File) error {
	unlockErr := unix.Flock(int(dir.Fd()), unix.LOCK_UN)
	closeErr := dir.Close()
	return firstError(unlockErr, closeErr)
}

func firstError(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func ignoreMissing(err error) error {
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// AcquireSingleInstanceLock obtém flock exclusivo em dir/name.lock.
// Retorna erro se já bloqueado por outro processo.
func AcquireSingleInstanceLock(dir, name string) (SingleInstanceLock, error) {
	parent, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unlockDirectory(parent) }()

	path := filepath.Join(dir, name+".lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open_lock=%w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("already_running lock=%s cause=%w", name, err)
	}
	return &posixLock{fd: f}, nil
}
