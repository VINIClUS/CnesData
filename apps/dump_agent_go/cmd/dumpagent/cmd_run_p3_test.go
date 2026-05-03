package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenDeltaStore_AbsentFlagSkips(t *testing.T) {
	t.Setenv("AGENT_DELTA_MODE", "")
	store := openDeltaStoreIfEnabled(t.TempDir())
	require.Nil(t, store, "delta store nil when AGENT_DELTA_MODE unset")
}

func TestOpenDeltaStore_FlagOnReturnsStore(t *testing.T) {
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
	require.Nil(t, store)
}
