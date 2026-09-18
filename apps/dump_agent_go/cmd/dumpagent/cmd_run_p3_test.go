package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenDeltaStore_OpensAndCreatesFile(t *testing.T) {
	dir := t.TempDir()
	store := openDeltaStore(dir)
	require.NotNil(t, store)
	defer store.Close()
	require.FileExists(t, filepath.Join(dir, "state", "delta.db"))
}
