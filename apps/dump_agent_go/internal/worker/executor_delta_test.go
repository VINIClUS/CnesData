package worker

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/stretchr/testify/require"
)

func TestExecuteDelta_ColdStartCommitsAllInsert(t *testing.T) {
	dir := t.TempDir()
	store, err := delta.Open(filepath.Join(dir, "delta.db"))
	require.NoError(t, err)
	defer store.Close()

	key := delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	}
	rows := []delta.Row{
		{"CNES": "1", "NOME_FANTA": "A"},
		{"CNES": "2", "NOME_FANTA": "B"},
	}

	ds, pending, err := ComputeAndStagePending(
		context.Background(),
		DeltaStageRequest{Store: store, Key: key, JobID: "job-test", Current: rows},
	)
	require.NoError(t, err)
	require.Len(t, ds.Inserts, 2)
	require.Empty(t, ds.Updates)
	require.Empty(t, ds.Deletes)

	require.NoError(t, pending.Commit())
	got, err := store.GetCommitted(key)
	require.NoError(t, err)
	require.Len(t, got, 2)
}

func TestExecuteDelta_FailedJobAbortsPending(t *testing.T) {
	dir := t.TempDir()
	store, err := delta.Open(filepath.Join(dir, "delta.db"))
	require.NoError(t, err)
	defer store.Close()

	key := delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	}
	rows := []delta.Row{{"CNES": "1", "NOME_FANTA": "A"}}

	_, pending, err := ComputeAndStagePending(
		context.Background(),
		DeltaStageRequest{Store: store, Key: key, JobID: "job-test", Current: rows},
	)
	require.NoError(t, err)
	pending.Abort()

	got, err := store.GetCommitted(key)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestExecuteDelta_UnknownProfileError(t *testing.T) {
	dir := t.TempDir()
	store, err := delta.Open(filepath.Join(dir, "delta.db"))
	require.NoError(t, err)
	defer store.Close()

	key := delta.SourceKey{
		Source: "xyz", Intent: "abc", Competencia: "202605",
	}
	_, _, err = ComputeAndStagePending(
		context.Background(),
		DeltaStageRequest{Store: store, Key: key, JobID: "job-test", Current: []delta.Row{}},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown_profile")
}
