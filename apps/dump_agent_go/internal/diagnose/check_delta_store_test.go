package diagnose

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/stretchr/testify/require"
)

func TestCheckDeltaStore_AbsentReturnsWARN(t *testing.T) {
	cfg := Config{DeltaDBPath: filepath.Join(t.TempDir(), "delta.db")}
	check := checkDeltaStore(context.Background(), cfg)
	require.Equal(t, "delta_store", check.Name)
	require.Equal(t, SeverityWarn, check.Severity)
}

func TestCheckDeltaStore_PathUnsetReturnsWARN(t *testing.T) {
	cfg := Config{}
	check := checkDeltaStore(context.Background(), cfg)
	require.Equal(t, "delta_store", check.Name)
	require.Equal(t, SeverityWarn, check.Severity)
}

func TestCheckDeltaStore_PresentEmptyPasses(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "delta.db")
	s, err := delta.Open(path)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	cfg := Config{DeltaDBPath: path}
	check := checkDeltaStore(context.Background(), cfg)
	require.Equal(t, SeverityPass, check.Severity)
	require.Contains(t, check.Fields, "committed_buckets")
	require.Contains(t, check.Fields, "pending_buckets")
}
