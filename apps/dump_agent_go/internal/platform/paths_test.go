package platform_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/stretchr/testify/require"
)

func TestLogsDir_HonorsOverride(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DUMP_LOGS_DIR", dir)

	got, err := platform.LogsDir()
	require.NoError(t, err)
	require.Equal(t, dir, got)
}

func TestResolveMachineID_Envar(t *testing.T) {
	t.Setenv("MACHINE_ID", "abc12345")
	got, err := platform.ResolveMachineID(t.TempDir())
	require.NoError(t, err)
	require.Equal(t, "abc12345", got)
}

func TestResolveMachineID_PersistsGenerated(t *testing.T) {
	t.Setenv("MACHINE_ID", "")
	dir := t.TempDir()

	id1, err := platform.ResolveMachineID(dir)
	require.NoError(t, err)
	require.Len(t, id1, 8)

	id2, err := platform.ResolveMachineID(dir)
	require.NoError(t, err)
	require.Equal(t, id1, id2, "machine id should persist across calls")

	_, err = os.Stat(filepath.Join(dir, "machine_id"))
	require.NoError(t, err)
}
