//go:build !windows

package platform_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/stretchr/testify/require"
)

func TestAcquireLock_FirstSucceeds(t *testing.T) {
	dir := t.TempDir()
	lock, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)
	require.NoError(t, lock.Release())
}

func TestAcquireLock_SecondFails(t *testing.T) {
	dir := t.TempDir()
	lock1, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)
	defer lock1.Release()

	_, err = platform.AcquireSingleInstanceLock(dir, "test")
	require.Error(t, err)
}

func TestAcquireLock_AfterReleaseAllowed(t *testing.T) {
	dir := t.TempDir()
	lock1, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)
	require.NoError(t, lock1.Release())

	lock2, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)
	defer lock2.Release()
}

func TestAcquireLock_ReleaseRemoveArquivoDeLock(t *testing.T) {
	dir := t.TempDir()
	lock, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)

	lockPath := filepath.Join(dir, "test.lock")
	_, statErr := os.Stat(lockPath)
	require.NoError(t, statErr, "lock file must exist while held")

	require.NoError(t, lock.Release())

	_, statErr = os.Stat(lockPath)
	require.True(t, os.IsNotExist(statErr), "lock file must not survive Release (residue)")
}

func TestAcquireLock_ReleaseEhIdempotenteMesmoSeArquivoJaSumiu(t *testing.T) {
	dir := t.TempDir()
	lock, err := platform.AcquireSingleInstanceLock(dir, "test")
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, "test.lock")))

	require.NoError(t, lock.Release(), "Release must not fail when file was already removed")
}
