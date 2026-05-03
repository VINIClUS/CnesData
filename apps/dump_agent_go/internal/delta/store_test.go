package delta

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStore_OpenClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestStore_GetCommitted_Empty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	key := SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"}
	got, err := s.GetCommitted(key)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestStore_BeginPending_Commit_Promotes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	key := SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"}
	pending, err := s.BeginPending(key, "job-001")
	require.NoError(t, err)
	require.NoError(t, pending.Put("pk1", [32]byte{0x01}))
	require.NoError(t, pending.Put("pk2", [32]byte{0x02}))
	require.NoError(t, pending.Commit())

	committed, err := s.GetCommitted(key)
	require.NoError(t, err)
	require.Len(t, committed, 2)
	require.Equal(t, [32]byte{0x01}, committed["pk1"])
}

func TestStore_BeginPending_Abort_LeavesCommittedUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	key := SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"}

	p1, err := s.BeginPending(key, "job-001")
	require.NoError(t, err)
	require.NoError(t, p1.Put("pk1", [32]byte{0x01}))
	require.NoError(t, p1.Commit())

	p2, err := s.BeginPending(key, "job-002")
	require.NoError(t, err)
	require.NoError(t, p2.Put("pk2", [32]byte{0x02}))
	p2.Abort()

	committed, err := s.GetCommitted(key)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.Contains(t, committed, "pk1")
	require.NotContains(t, committed, "pk2")
}

func TestStore_BeginPending_ConcurrentReturnsErrPendingExists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	key := SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"}

	p1, err := s.BeginPending(key, "job-A")
	require.NoError(t, err)

	_, err = s.BeginPending(key, "job-B")
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrPendingExists))
	p1.Abort()
}

func TestStore_GarbageCollectStalePending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	key := SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"}

	p, err := s.BeginPending(key, "job-stale")
	require.NoError(t, err)
	require.NoError(t, p.Put("pk1", [32]byte{0x01}))
	// Don't commit and don't abort — leave pending bucket on disk

	count, err := s.GarbageCollectStalePending(0)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	count2, err := s.GarbageCollectStalePending(time.Hour)
	require.NoError(t, err)
	require.Equal(t, 0, count2, "second call has nothing left to GC")
}
