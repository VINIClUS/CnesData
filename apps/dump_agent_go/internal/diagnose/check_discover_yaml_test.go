package diagnose

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckDiscoverYAML_AbsentReturnsWARN(t *testing.T) {
	cfg := Config{DiscoverYAMLPath: filepath.Join(t.TempDir(), "config.yaml")}
	check := checkDiscoverYAML(context.Background(), cfg)
	require.Equal(t, "discover_yaml", check.Name)
	require.Equal(t, SeverityWarn, check.Severity)
	require.Contains(t, check.Message, "absent")
}

func TestCheckDiscoverYAML_PresentValidPasses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "cnes:\n  host: localhost\n  port: 3050\n  database_path: C:\\X.GDB\n  user: SYSDBA\n  charset: WIN1252\nsihd:\n  host: localhost\nbpa:\n  host: localhost\nsia:\n  dbf_dir: C:\\SIA\n"
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))
	cfg := Config{DiscoverYAMLPath: path}
	check := checkDiscoverYAML(context.Background(), cfg)
	require.Equal(t, SeverityPass, check.Severity)
	require.Contains(t, check.Fields, "sources_ready")
	require.Contains(t, check.Fields, "sources_unconfigured")
}

func TestCheckDiscoverYAML_BadYAMLFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte("not: yaml: "), 0o644))
	cfg := Config{DiscoverYAMLPath: path}
	check := checkDiscoverYAML(context.Background(), cfg)
	require.Equal(t, SeverityFail, check.Severity)
	require.Contains(t, check.Message, "parse")
}
