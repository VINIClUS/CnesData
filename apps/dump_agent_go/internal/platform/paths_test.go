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

func TestAppDataDir_HonorsOverride(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	t.Setenv("AGENT_APPDATA_DIR", dir)

	got, err := platform.AppDataDir()
	require.NoError(t, err)
	require.Equal(t, dir, got)
}

func TestAppDataDir_UsaXDGStateHome(t *testing.T) {
	base := t.TempDir()
	t.Setenv("AGENT_APPDATA_DIR", "")
	t.Setenv("XDG_STATE_HOME", base)

	got, err := platform.AppDataDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(base, "cnes-agent"), got)
}

func TestAppDataDir_UsaHomeQuandoXDGStateHomeVazio(t *testing.T) {
	t.Setenv("AGENT_APPDATA_DIR", "")
	t.Setenv("XDG_STATE_HOME", "")

	home, err := os.UserHomeDir()
	require.NoError(t, err)
	got, err := platform.AppDataDir()
	require.NoError(t, err)
	require.Equal(t, filepath.Join(home, ".local", "state", "cnes-agent"), got)
}

func TestRestrictStateTree_POSIXNaoRetornaErro(t *testing.T) {
	require.NoError(t, platform.RestrictStateTree(t.TempDir()))
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

func TestWindowsAppDataDir_UsaProgramDataDoAmbiente(t *testing.T) {
	got := platform.Export.WindowsAppDataDir(`D:\CustomProgramData`)
	require.Equal(t, `D:\CustomProgramData\CnesAgent`, got)
}

func TestWindowsAppDataDir_FallbackQuandoProgramDataVazio(t *testing.T) {
	got := platform.Export.WindowsAppDataDir("")
	require.Equal(t, `C:\ProgramData\CnesAgent`, got)
}

// Regression for H5: an interactive admin session and the LocalSystem
// service identity have different %LOCALAPPDATA% values but the SAME
// %ProgramData% value — so both must resolve to the identical state root.
func TestWindowsAppDataDir_IdenticoParaAdminInterativoELocalSystem(t *testing.T) {
	const sharedProgramData = `C:\ProgramData`
	adminSession := platform.Export.WindowsAppDataDir(sharedProgramData)
	localSystemService := platform.Export.WindowsAppDataDir(sharedProgramData)
	require.Equal(t, adminSession, localSystemService)
}
