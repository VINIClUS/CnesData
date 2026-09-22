package service

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeTempConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.env")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func TestParseEnvFile_IgnoraComentariosEBrancos(t *testing.T) {
	path := writeTempConfig(t, "# comentario\n\nTENANT_ID=354130\n   \nCENTRAL_API_URL=http://x\n")
	lines, err := parseEnvFile(path)
	require.NoError(t, err)
	require.Equal(t, []string{"TENANT_ID=354130", "CENTRAL_API_URL=http://x"}, lines)
}

func TestParseEnvFile_IgnoraLinhaSemIgual(t *testing.T) {
	path := writeTempConfig(t, "TENANT_ID=354130\nnotakeyvalue\n")
	lines, err := parseEnvFile(path)
	require.NoError(t, err)
	require.Equal(t, []string{"TENANT_ID=354130"}, lines)
}

func TestParseEnvFile_ArquivoAusente_RetornaErro(t *testing.T) {
	_, err := parseEnvFile(filepath.Join(t.TempDir(), "missing.env"))
	require.Error(t, err)
}

func TestSplitSecretLines_RejeitaChavesDeSenha(t *testing.T) {
	lines := []string{
		"TENANT_ID=354130",
		"DB_PASSWORD=hunter2",
		"CNES_DB_PASSWORD=hunter2",
		"CENTRAL_API_URL=http://x",
		"API_TOKEN=abc123",
		"SOME_SECRET=xyz",
		"AUTH_APIKEY=k",
	}
	safe, rejected := splitSecretLines(lines)
	require.Equal(t, []string{"TENANT_ID=354130", "CENTRAL_API_URL=http://x"}, safe)
	require.ElementsMatch(t,
		[]string{"DB_PASSWORD", "CNES_DB_PASSWORD", "API_TOKEN", "SOME_SECRET", "AUTH_APIKEY"},
		rejected)
}

func TestSplitSecretLines_SemChavesSecretas_NadaRejeitado(t *testing.T) {
	lines := []string{"TENANT_ID=354130", "COMPETENCIA_YYYYMM=202601"}
	safe, rejected := splitSecretLines(lines)
	require.Equal(t, lines, safe)
	require.Empty(t, rejected)
}

func TestIsSecretKey_CaseInsensitive(t *testing.T) {
	require.True(t, isSecretKey("db_password"))
	require.True(t, isSecretKey("Secret_Value"))
	require.False(t, isSecretKey("TENANT_ID"))
}
