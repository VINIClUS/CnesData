package main

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// runbookPath points at the install runbook from this package's directory.
// If this test starts failing with "no such file", the runbook moved —
// update the path, don't delete the test (see H6 in
// docs/edge-agent-audit-2026-09-20.md: the runbook previously documented bare
// DB_HOST/DB_USER/DB_PATH/DB_PASSWORD/DB_CHARSET, which resolveFB in
// path_config.go never reads — only <SOURCE>_DB_* is honored).
const runbookPath = "../../../../docs/runbooks/dumpagent-install-windows.md"

// nonDBEnvAllowlist are config.env keys the runbook may use that are not
// Firebird source config (and so aren't subject to the <SOURCE>_ prefix).
var nonDBEnvAllowlist = map[string]bool{
	"CENTRAL_API_URL":         true,
	"TENANT_ID":               true,
	"COMPETENCIA_YYYYMM":      true,
	"INTENT":                  true,
	"TIPO_EXTRACAO":           true,
	"COD_MUN_IBGE":            true,
	"DUMP_MAX_JITTER_SECONDS": true,
	"FIREBIRD_DLL":            true,
}

var prefixedDBVarPattern = regexp.MustCompile(`^(CNES|SIHD|BPA)_DB_(HOST|PORT|PATH|USER|CHARSET)$`)

func TestRunbook_ConfigEnvUsaNomesDeVariavelQueResolveFBLe(t *testing.T) {
	body, err := os.ReadFile(runbookPath)
	require.NoError(t, err, "install runbook not found at %s", runbookPath)

	block := extractFirstEnvCodeBlock(t, string(body))
	require.NotEmpty(t, block, "expected a ```env fenced block in the runbook")

	for _, line := range strings.Split(block, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		key, _, found := strings.Cut(line, "=")
		require.True(t, found, "malformed config.env line in runbook: %q", line)
		if nonDBEnvAllowlist[key] {
			continue
		}
		require.True(t, prefixedDBVarPattern.MatchString(key),
			"runbook documents %q, but resolveFB (path_config.go) only reads "+
				"<SOURCE>_DB_{HOST,PORT,PATH,USER,CHARSET} or a known non-DB var — "+
				"this exact drift (bare DB_HOST/DB_USER/...) previously shipped a "+
				"non-functional install procedure (H6)", key)
	}
}

func extractFirstEnvCodeBlock(t *testing.T, markdown string) string {
	t.Helper()
	const fence = "```env\n"
	start := strings.Index(markdown, fence)
	require.GreaterOrEqual(t, start, 0)
	rest := markdown[start+len(fence):]
	end := strings.Index(rest, "```")
	require.GreaterOrEqual(t, end, 0)
	return rest[:end]
}
