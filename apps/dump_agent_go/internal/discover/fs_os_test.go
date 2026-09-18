package discover

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOSFS_StatExistingFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.txt")
	require.NoError(t, os.WriteFile(path, []byte("hello"), 0o644))
	info, err := OSFS{}.Stat(path)
	require.NoError(t, err)
	require.True(t, info.Exists)
	require.Equal(t, int64(5), info.Size)
	require.False(t, info.IsDir)
}

func TestOSFS_StatExistingDir(t *testing.T) {
	dir := t.TempDir()
	info, err := OSFS{}.Stat(dir)
	require.NoError(t, err)
	require.True(t, info.Exists)
	require.True(t, info.IsDir)
}

func TestOSFS_StatMissing(t *testing.T) {
	info, err := OSFS{}.Stat(filepath.Join(t.TempDir(), "missing"))
	require.NoError(t, err)
	require.False(t, info.Exists)
}

func TestOSFS_ReadDir(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.txt", "b.txt", "c.txt"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name),
			[]byte("x"), 0o644))
	}
	names, err := OSFS{}.ReadDir(dir)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"a.txt", "b.txt", "c.txt"}, names)
}

func TestOSFS_ReadDirMissing(t *testing.T) {
	_, err := OSFS{}.ReadDir(filepath.Join(t.TempDir(), "missing"))
	require.Error(t, err)
}
