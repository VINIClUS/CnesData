package delta

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/stretchr/testify/require"
)

func TestStore_OpenClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	require.NoError(t, s.Close())
}

func TestFingerprintsPendentesSobrevivemReinicioAntesDaFila(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	ref := referenciaPendente("job-full", 1)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"pk": {7}}))
	require.NoError(t, s.Close())
	s, err = Open(path)
	require.NoError(t, err)
	defer s.Close()
	count, err := s.GarbageCollectStalePending(0)
	require.NoError(t, err)
	require.Zero(t, count)
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Empty(t, committed)
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
	committed, err = s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"pk": {7}}, committed)
}

func TestRetomaPendenteAposReinicioPreservandoDadosAteSubstituicao(t *testing.T) {
	for _, replace := range []bool{false, true} {
		name := "preserva"
		if replace {
			name = "substitui"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "delta.db")
			s, err := Open(path)
			require.NoError(t, err)
			ref := referenciaPendente("full", 9)
			pending, err := s.BeginPendingRef(ref)
			require.NoError(t, err)
			require.NoError(t, pending.Replace(map[string][32]byte{"original": {1}}))
			require.NoError(t, s.Close())
			s, err = Open(path)
			require.NoError(t, err)
			defer s.Close()
			resumed, err := s.ResumePendingRef(ref)
			require.NoError(t, err)
			require.NotNil(t, resumed)
			want := map[string][32]byte{"original": {1}}
			if replace {
				require.NoError(t, resumed.Replace(map[string][32]byte{"substituido": {2}}))
				want = map[string][32]byte{"substituido": {2}}
			}
			require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
			committed, err := s.GetCommitted(ref.SourceKey)
			require.NoError(t, err)
			require.Equal(t, want, committed)
		})
	}
}

func TestRetomadaAusenteNaoCriaPendenteNemAlteraOutraReferencia(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("full", 1)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"original": {1}}))
	otherSource, otherJob, otherFence := ref, ref, ref
	otherSource.SourceKey.Source = "sihd"
	otherJob.JobID = "outro"
	otherFence.FencingToken++
	for _, absent := range []PendingRef{otherSource, otherJob, otherFence} {
		resumed, err := s.ResumePendingRef(absent)
		require.ErrorIs(t, err, ErrPendingNotFound)
		require.Nil(t, resumed)
		created, err := s.BeginPendingRef(absent)
		require.NoError(t, err)
		created.Abort()
	}
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"original": {1}}, committed)
}

func TestRetomadaValidaReferenciaAntesDeConsultarPendente(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("job/invalido", 1)
	resumed, err := s.ResumePendingRef(ref)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrPendingNotFound)
	require.Nil(t, resumed)
	_, _, _, _, ok, err := s.ChainHead(ref.SourceKey)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestSubstituicaoPendenteAtomicaPreservaConjuntoAnteriorSeFalhar(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("full", 1)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"removido": {1}}))
	require.NoError(t, pending.Replace(map[string][32]byte{"preservado": {2}}))
	require.Error(t, pending.Replace(map[string][32]byte{"": {3}}))
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"preservado": {2}}, committed)
}

func TestDeltaAvancaHashDoServidorEPreservaBaseFull(t *testing.T) {
	s := abreStore(t)
	full := referenciaPendente("full", 1)
	confirmaFull(t, s, full)
	ref := referenciaPendente("delta", 2)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"nova": {2}}))
	raw := manifestoFull(ref.JobID)
	raw.SnapshotMode, raw.Sequence = manifest.SnapshotModeDelta, 2
	base, previous := "full", strings.Repeat("a", 64)
	raw.BaseSnapshotID, raw.PreviousManifestSHA256 = &base, &previous
	raw.CreatedAt = raw.CreatedAt.Add(time.Hour)
	require.NoError(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	require.NoError(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	snapshot, sequence, hash, created, ok, err := s.ChainHead(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "full", snapshot)
	require.Equal(t, uint32(2), sequence)
	require.Equal(t, strings.Repeat("b", 64), hash)
	require.Equal(t, manifestoFull("full").CreatedAt, created)
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"nova": {2}}, committed)
}

func TestResposta409DescartaSomenteFonteEFenceRejeitados(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("rejeitado", 1)
	confirmaFull(t, s, referenciaPendente("base", 1))
	otherFence := referenciaPendente("rejeitado", 2)
	otherSource := referenciaPendente("outra", 1)
	otherSource.SourceKey.Source = "sihd"
	for _, current := range []PendingRef{ref, otherFence, otherSource} {
		pending, err := s.BeginPendingRef(current)
		require.NoError(t, err)
		require.NoError(t, pending.Replace(map[string][32]byte{"pk": {9}}))
	}
	require.NoError(t, s.RequireFull(ref, "base_missing"))
	require.NoError(t, s.RequireFull(ref, "base_missing"))
	reason, required, err := s.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, required)
	require.Equal(t, "base_missing", reason)
	_, required, err = s.ForceFull(otherSource.SourceKey)
	require.NoError(t, err)
	require.False(t, required)
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"base": {1}}, committed)
	snapshot, _, _, _, ok, err := s.ChainHead(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "base", snapshot)
	require.Error(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
	for _, current := range []PendingRef{otherFence, otherSource} {
		err := s.ConfirmPending(current, manifestoFull(current.JobID), strings.Repeat("b", 64))
		require.NoError(t, err)
	}
	_, required, err = s.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.False(t, required)
}

func TestForcaFullSobreviveReinicio(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	ref := referenciaPendente("full", 1)
	require.NoError(t, s.RequireFull(ref, "sequence_gap"))
	require.NoError(t, s.Close())
	s, err = Open(path)
	require.NoError(t, err)
	defer s.Close()
	reason, required, err := s.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, required)
	require.Equal(t, "sequence_gap", reason)
}

func TestConfirmacaoInvalidaMantemCabecaEPendente(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("delta", 2)
	confirmaFull(t, s, referenciaPendente("base", 1))
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"nova": {2}}))
	raw := manifestoFull(ref.JobID)
	require.Error(t, s.ConfirmPending(ref, raw, "invalid"))
	wrong := ref
	wrong.FencingToken++
	require.Error(t, s.ConfirmPending(wrong, raw, strings.Repeat("b", 64)))
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"base": {1}}, committed)
	require.NoError(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
}

func TestReplayRecusaManifestoAlteradoComMesmaReferencia(t *testing.T) {
	s := abreStore(t)
	ref := referenciaPendente("full", 1)
	confirmaFull(t, s, ref)
	raw := manifestoFull(ref.JobID)
	raw.CreatedAt = raw.CreatedAt.Add(time.Hour)
	require.Error(t, s.ConfirmPending(ref, raw, strings.Repeat("a", 64)))
	raw = manifestoFull(ref.JobID)
	raw.SnapshotMode = manifest.SnapshotModeDelta
	require.Error(t, s.ConfirmPending(ref, raw, strings.Repeat("a", 64)))
}

func TestReinicioAposConfirmacaoPermiteReplaySemRecriarPendente(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delta.db")
	s, err := Open(path)
	require.NoError(t, err)
	ref := referenciaPendente("full", 1)
	confirmaFull(t, s, ref)
	require.NoError(t, s.Close())
	s, err = Open(path)
	require.NoError(t, err)
	defer s.Close()
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
	snapshot, sequence, hash, _, ok, err := s.ChainHead(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "full", snapshot)
	require.Equal(t, uint32(1), sequence)
	require.Equal(t, strings.Repeat("a", 64), hash)
}

func TestFullVazioSubstituiFingerprintsSomenteNaConfirmacao(t *testing.T) {
	s := abreStore(t)
	confirmaFull(t, s, referenciaPendente("base", 1))
	ref := referenciaPendente("vazio", 2)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(nil))
	require.Error(t, pending.Commit())
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Len(t, committed, 1)
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("b", 64)))
	committed, err = s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Empty(t, committed)
}

func TestFalhaAoPersistirCabecaRevertePromocaoEPermiteRetry(t *testing.T) {
	s := abreStore(t)
	confirmaFull(t, s, referenciaPendente("base", 1))
	ref := referenciaPendente("full", 2)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"nova": {2}}))
	raw := manifestoFull(ref.JobID)
	raw.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	require.Error(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	committed, err := s.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"base": {1}}, committed)
	snapshot, sequence, hash, _, ok, err := s.ChainHead(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "base", snapshot)
	require.Equal(t, uint32(1), sequence)
	require.Equal(t, strings.Repeat("a", 64), hash)
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("b", 64)))
}

func TestDeltaInvalidoPreservaEstadoEFullObrigatorio(t *testing.T) {
	s := abreStore(t)
	confirmaFull(t, s, referenciaPendente("base", 1))
	ref := referenciaPendente("delta", 2)
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"nova": {2}}))
	require.NoError(t, s.RequireFull(referenciaPendente("rejeitado", 1), "base_missing"))
	raw := manifestoFull(ref.JobID)
	raw.SnapshotMode, raw.Sequence = manifest.SnapshotModeDelta, 3
	base, previous := "base", strings.Repeat("a", 64)
	raw.BaseSnapshotID, raw.PreviousManifestSHA256 = &base, &previous
	require.Error(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	raw.Sequence = 2
	previous = strings.Repeat("c", 64)
	require.Error(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	previous = strings.Repeat("a", 64)
	require.NoError(t, s.ConfirmPending(ref, raw, strings.Repeat("b", 64)))
	reason, required, err := s.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, required)
	require.Equal(t, "base_missing", reason)
}

func abreStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "delta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func referenciaPendente(job string, fence uint64) PendingRef {
	return PendingRef{
		SourceKey: SourceKey{Source: "cnes", Intent: "estabelecimentos", Competencia: "202605"},
		JobID:     job, FencingToken: fence,
	}
}

func manifestoFull(job string) manifest.Raw {
	return manifest.Raw{
		ManifestID: job, SnapshotID: job, SnapshotMode: manifest.SnapshotModeFull,
		Sequence: 1, CreatedAt: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
	}
}

func confirmaFull(t *testing.T, s *Store, ref PendingRef) {
	t.Helper()
	pending, err := s.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(map[string][32]byte{"base": {1}}))
	require.NoError(t, s.ConfirmPending(ref, manifestoFull(ref.JobID), strings.Repeat("a", 64)))
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
