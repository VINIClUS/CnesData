package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/discover"
	"github.com/stretchr/testify/require"
)

func TestRunDiscover_HappyPath(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")

	prev := discoverRunFn
	defer func() { discoverRunFn = prev }()
	discoverRunFn = func(_ context.Context, _ discover.RunConfig) []discover.SourceResult {
		return []discover.SourceResult{
			{Source: discover.SourceCNES,
				Top: discover.Candidate{Path: `C:\X.GDB`, Score: 80,
					Strategy: discover.StrategyRegistry}},
			{Source: discover.SourceSIHD},
			{Source: discover.SourceBPA},
			{Source: discover.SourceSIA},
		}
	}

	code := runDiscover([]string{"--out", out})
	require.Equal(t, 0, code)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(data), `C:\X.GDB`)
}

func TestRunDiscover_ZeroCandidatesExit3(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")

	prev := discoverRunFn
	defer func() { discoverRunFn = prev }()
	discoverRunFn = func(_ context.Context, _ discover.RunConfig) []discover.SourceResult {
		return []discover.SourceResult{
			{Source: discover.SourceCNES},
			{Source: discover.SourceSIHD},
			{Source: discover.SourceBPA},
			{Source: discover.SourceSIA},
		}
	}

	code := runDiscover([]string{"--out", out})
	require.Equal(t, 3, code)
	_, err := os.Stat(out)
	require.NoError(t, err, "writes empty scaffold even at exit 3")
}

func TestRunDiscover_ExistingFileNoForceExit2(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(out, []byte("preexisting"), 0o644))
	code := runDiscover([]string{"--out", out})
	require.Equal(t, 2, code)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Equal(t, "preexisting", string(data),
		"existing file untouched without --force")
}

func TestRunDiscover_ForceOverwrites(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(out, []byte("old"), 0o644))

	prev := discoverRunFn
	defer func() { discoverRunFn = prev }()
	discoverRunFn = func(_ context.Context, _ discover.RunConfig) []discover.SourceResult {
		return []discover.SourceResult{
			{Source: discover.SourceCNES,
				Top: discover.Candidate{Path: `C:\Y.GDB`, Score: 80,
					Strategy: discover.StrategyRegistry}},
			{Source: discover.SourceSIHD},
			{Source: discover.SourceBPA},
			{Source: discover.SourceSIA},
		}
	}

	code := runDiscover([]string{"--out", out, "--force"})
	require.Equal(t, 0, code)
	data, err := os.ReadFile(out)
	require.NoError(t, err)
	require.NotContains(t, string(data), "old")
	require.Contains(t, string(data), `C:\Y.GDB`)
}
