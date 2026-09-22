package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/service"
	"github.com/stretchr/testify/require"
)

// On POSIX, service.Uninstall is the handler_stub.go no-op (exit 2) — there
// is no Linux service manager integration yet. This locks the exit-code
// contract cmdUninstall exposes, so a future change to either side doesn't
// silently flip the CLI's reported exit code.
func TestCmdUninstall_DelegaParaServiceUninstall(t *testing.T) {
	want := service.Uninstall()
	got := cmdUninstall(nil)
	if got != want {
		t.Fatalf("cmdUninstall() = %d, want %d (must delegate to service.Uninstall unchanged)", got, want)
	}
}

func TestPurgeState_RemoveRaizDoEstadoQuandoPurgeAtivo(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "state-root")
	require.NoError(t, os.MkdirAll(stateDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(stateDir, "machine_id"), []byte("abc"), 0o644))
	t.Setenv("AGENT_APPDATA_DIR", stateDir)

	rc := purgeState()
	require.Equal(t, 0, rc)

	_, err := os.Stat(stateDir)
	require.True(t, os.IsNotExist(err), "state root must be removed after purge")
}

func TestPurgeState_IdempotenteQuandoDiretorioJaAusente(t *testing.T) {
	stateDir := filepath.Join(t.TempDir(), "never-created")
	t.Setenv("AGENT_APPDATA_DIR", stateDir)

	rc := purgeState()
	require.Equal(t, 0, rc, "purge must not fail when there is nothing to remove")
}

func TestCmdUninstall_ParsePurgeFlag(t *testing.T) {
	fs := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	purge := fs.Bool("purge", false, "")
	require.NoError(t, fs.Parse([]string{"--purge"}))
	require.True(t, *purge)

	fs2 := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	purge2 := fs2.Bool("purge", false, "")
	require.NoError(t, fs2.Parse(nil))
	require.False(t, *purge2, "purge must default to false (state-preserving uninstall)")
}
