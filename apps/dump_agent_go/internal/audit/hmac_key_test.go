package audit

import (
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/secrets"
	"github.com/stretchr/testify/require"
)

func TestLoadOrCreate_GeneratesOnFirstCall(t *testing.T) {
	dir := t.TempDir()
	store := secrets.NewStore(filepath.Join(dir, "secrets"))
	key, err := LoadOrCreate(store)
	require.NoError(t, err)
	require.Len(t, key, 32, "HMAC key must be 32 bytes")
}

func TestLoadOrCreate_IdempotentReload(t *testing.T) {
	dir := t.TempDir()
	store := secrets.NewStore(filepath.Join(dir, "secrets"))
	first, err := LoadOrCreate(store)
	require.NoError(t, err)
	second, err := LoadOrCreate(store)
	require.NoError(t, err)
	require.Equal(t, first, second, "reload must return same key")
}

func TestLoadOrCreate_DistinctAcrossDirs(t *testing.T) {
	store1 := secrets.NewStore(filepath.Join(t.TempDir(), "s1"))
	store2 := secrets.NewStore(filepath.Join(t.TempDir(), "s2"))
	k1, _ := LoadOrCreate(store1)
	k2, _ := LoadOrCreate(store2)
	require.NotEqual(t, k1, k2, "fresh stores -> distinct keys")
}
