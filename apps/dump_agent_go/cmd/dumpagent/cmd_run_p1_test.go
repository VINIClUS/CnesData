package main

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cnesdata/dumpagent/internal/discover"
	"github.com/stretchr/testify/require"
)

func TestLoadDiscoverYAML_AbsentReturnsEmpty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	cfg, err := loadDiscoverYAML(path)
	require.NoError(t, err)
	require.Equal(t, discover.Config{}, cfg)
}

func TestLoadDiscoverYAML_PresentLoaded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := "cnes:\n  host: yaml-host\n  port: 3060\n" +
		"  database_path: C:\\X.GDB\n  user: SYSDBA\n" +
		"  charset: WIN1252\nsihd:\n  host: localhost\n" +
		"bpa:\n  host: localhost\nsia:\n  dbf_dir: C:\\SIA\n"
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))
	cfg, err := loadDiscoverYAML(path)
	require.NoError(t, err)
	require.Equal(t, "yaml-host", cfg.CNES.Host)
}

func TestLoadDiscoverYAML_DisabledByEnvBypass(t *testing.T) {
	t.Setenv("AGENT_DISABLE_DISCOVER", "true")
	cfg, err := loadDiscoverYAML("does-not-matter.yaml")
	require.NoError(t, err)
	require.Equal(t, discover.Config{}, cfg)
}

func TestLogOverrides_EmitsWARN(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(h)
	overrides := []OverrideRecord{
		{Layer: layerCLI, Source: "cnes", Field: "database_path"},
		{Layer: layerEnv, Source: "bpa", Field: "host"},
	}
	logOverrides(logger, overrides)
	out := buf.String()
	require.Contains(t, out, "config_override")
	require.Contains(t, out, "layer=cli")
	require.Contains(t, out, "source=cnes")
	require.Contains(t, out, "field=database_path")
	require.Contains(t, out, "layer=env")
	require.Contains(t, out, "source=bpa")
	require.True(t, strings.Count(out, "config_override") >= 2)
}

func TestLogPasswordSource_WarnOnMasterkey(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(h)
	logPasswordSource(logger, "cnes", PasswordSourceMasterkey)
	out := buf.String()
	require.Contains(t, out, "password_default_active")
	require.Contains(t, out, "source=cnes")
}

func TestLogPasswordSource_NoWarnOnEnv(t *testing.T) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(h)
	logPasswordSource(logger, "cnes", PasswordSourceEnv)
	out := buf.String()
	require.NotContains(t, out, "password_default_active")
	require.Contains(t, out, "password_source")
}
