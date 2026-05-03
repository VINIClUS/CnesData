package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenDeltaStore_AbsentFlagOpens(t *testing.T) {
	t.Setenv("AGENT_DELTA_MODE", "")
	dir := t.TempDir()
	store := openDeltaStoreIfEnabled(dir)
	require.NotNil(t, store, "delta store enabled by default (Phase C)")
	defer store.Close()
	require.FileExists(t, filepath.Join(dir, "state", "delta.db"))
}

func TestOpenDeltaStore_FlagTrueOpens(t *testing.T) {
	t.Setenv("AGENT_DELTA_MODE", "true")
	dir := t.TempDir()
	store := openDeltaStoreIfEnabled(dir)
	require.NotNil(t, store)
	defer store.Close()
	require.FileExists(t, filepath.Join(dir, "state", "delta.db"))
}

func TestOpenDeltaStore_FlagFalseSkips(t *testing.T) {
	t.Setenv("AGENT_DELTA_MODE", "false")
	store := openDeltaStoreIfEnabled(t.TempDir())
	require.Nil(t, store, "AGENT_DELTA_MODE=false is the escape hatch")
}
