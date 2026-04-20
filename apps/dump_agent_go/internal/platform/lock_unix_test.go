//go:build !windows

package platform_test

import (
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
