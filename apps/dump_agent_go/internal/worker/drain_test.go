package worker

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/breaker"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/stretchr/testify/require"
)

type stubJobAPI struct {
	registerResp error
	failResp     error
	registerN    int
	failN        int
	lastJob      Job
	lastSize     int64
}

func (s *stubJobAPI) MintUploadURL(_ context.Context, _ JobSpec) (*Job, error) {
	return nil, nil
}
func (s *stubJobAPI) RegisterJob(_ context.Context, job Job, sizeBytes int64) error {
	s.registerN++
	s.lastJob = job
	s.lastSize = sizeBytes
	return s.registerResp
}
func (s *stubJobAPI) FailJob(_ context.Context, _ Job, _ error) error {
	s.failN++
	return s.failResp
}
func (s *stubJobAPI) SendHeartbeat(_ context.Context, _ string) error { return nil }

func newDrainFixture(t *testing.T, registerResp error) (*Drainer, *queue.Outbox, *stubJobAPI) {
	t.Helper()
	ob, err := queue.Open(filepath.Join(t.TempDir(), "ob.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ob.Close() })
	stub := &stubJobAPI{registerResp: registerResp}
	br := breaker.New(5, 60*time.Second, "test")
	d := NewDrainer(ob, br, stub)
	return d, ob, stub
}

func TestDrain_HappyPathDeletesEnvelope(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-1", SizeBytes: 100})
	d.tick(context.Background())
	if stub.registerN != 1 {
		t.Errorf("inner RegisterJob calls=%d want 1", stub.registerN)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("envelope still present after success: %+v", items)
	}
}

func TestDrain_ReplaysSha256AndMinioKey(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{
		Type:      queue.TypeComplete,
		JobUUID:   "uuid-replay",
		SizeBytes: 2048,
		SHA256:    "deadbeef",
		MinioKey:  "354130/CNES_VINCULO/2026-01-01/x.parquet.gz",
	})
	d.tick(context.Background())
	if stub.registerN != 1 {
		t.Fatalf("RegisterJob calls=%d want 1", stub.registerN)
	}
	if stub.lastJob.ID != "uuid-replay" {
		t.Errorf("Job.ID=%q want uuid-replay", stub.lastJob.ID)
	}
	if stub.lastJob.Sha256 != "deadbeef" {
		t.Errorf("Job.Sha256=%q want deadbeef", stub.lastJob.Sha256)
	}
	if stub.lastJob.MinioKey != "354130/CNES_VINCULO/2026-01-01/x.parquet.gz" {
		t.Errorf("Job.MinioKey=%q lost on replay", stub.lastJob.MinioKey)
	}
	if stub.lastSize != 2048 {
		t.Errorf("sizeBytes=%d want 2048", stub.lastSize)
	}
}

func TestDrain_TerminalDropDeletes(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 404})
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-2"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("envelope still present after 404: %+v", items)
	}
}

func TestDrain_TransientRetainsEnvelope(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 503})
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-3"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("envelope dropped after transient: %+v", items)
	}
	if items[0].Envelope.Attempts != 1 {
		t.Errorf("attempts=%d want 1", items[0].Envelope.Attempts)
	}
}

func TestDrain_BreakerTripsAfterThreshold(t *testing.T) {
	d, ob, _ := newDrainFixture(t, &obs.HTTPError{StatusCode: 503})
	for i := 0; i < 10; i++ {
		_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-x"})
	}
	d.tick(context.Background())
	items, _ := ob.Peek(20)
	// After 5 transient failures (threshold), breaker OPEN; remaining envelopes untouched.
	if len(items) < 5 {
		t.Fatalf("expected at least 5 retained after breaker trip, got %d", len(items))
	}
}

func TestDrain_FailEnvelopeDispatched(t *testing.T) {
	d, ob, stub := newDrainFixture(t, nil)
	_ = ob.Append(queue.Envelope{
		Type: queue.TypeFail, JobUUID: "uuid-4", Cause: "oops",
	})
	d.tick(context.Background())
	if stub.failN != 1 {
		t.Errorf("inner FailJob calls=%d want 1", stub.failN)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Errorf("Fail envelope retained: %+v", items)
	}
}

func TestDrain_NetworkErrIsTransient(t *testing.T) {
	d, ob, _ := newDrainFixture(t, errors.New("dial: connection refused"))
	_ = ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "uuid-5"})
	d.tick(context.Background())
	items, _ := ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("envelope dropped after network err: %+v", items)
	}
}

func TestDrain_RunRespectsCtxCancel(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit on ctx cancel")
	}
}

func TestDrainer_TickIntervalJittered_Low(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 0.0 }) // bottom of jitter window
	got := d.NextInterval()
	want := 24 * time.Second
	if got != want {
		t.Errorf("rand=0.0 got %v want %v", got, want)
	}
}

func TestDrainer_TickIntervalJittered_High(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 1.0 })
	got := d.NextInterval()
	want := 36 * time.Second
	if got != want {
		t.Errorf("rand=1.0 got %v want %v", got, want)
	}
}

func TestDrainer_TickIntervalJittered_Mid(t *testing.T) {
	d, _, _ := newDrainFixture(t, nil)
	d.SetRand(func() float64 { return 0.5 })
	got := d.NextInterval()
	want := 30 * time.Second
	if got != want {
		t.Errorf("rand=0.5 got %v want %v", got, want)
	}
}

type rawClientFunc func(context.Context, queue.Envelope) (RawManifestResponse, error)

func (f rawClientFunc) SendRawManifest(
	ctx context.Context, envelope queue.Envelope,
) (RawManifestResponse, error) {
	return f(ctx, envelope)
}

type rawDrainFixture struct {
	store          *delta.Store
	out            *queue.Outbox
	env            queue.Envelope
	raw            manifest.Raw
	ref            delta.PendingRef
	storePath      string
	outPath        string
	spoolDirectory string
}

func newRawDrainFixture(t *testing.T) rawDrainFixture {
	t.Helper()
	storePath := filepath.Join(t.TempDir(), "state.db")
	store, err := delta.Open(storePath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	ref := delta.PendingRef{JobID: "new-job", FencingToken: 2, SourceKey: delta.SourceKey{
		Source: "cnes", Intent: "profissionais", Competencia: "202601",
	}}
	spoolDirectory, spool := newFixtureSpool(t)
	raw, err := manifest.Build(manifest.BuildRequest{
		JobID: ref.JobID, TenantID: "tenant", SourceType: manifest.SourceTypeCNESLocal,
		FileSubtype: "CNES_VINCULO", Competencia: "2026-01", AgentID: "agent",
		AgentVersion: "1", SchemaVersion: "1", SnapshotMode: manifest.SnapshotModeFull,
		ObjectSHA256: spool.SHA256, RowCount: 1, SizeBytes: spool.SizeBytes,
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	outPath := filepath.Join(t.TempDir(), "outbox.db")
	out, err := queue.Open(outPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close() })
	f := rawDrainFixture{store: store, out: out, raw: raw, ref: ref,
		storePath: storePath, outPath: outPath, spoolDirectory: spoolDirectory}
	prior, priorRef := raw, ref
	prior.ManifestID, prior.SnapshotID, priorRef.JobID = "prior", "prior", "prior"
	stageRawHashes(t, store, priorRef, map[string][32]byte{"old": {1}})
	require.NoError(t, store.ConfirmPending(priorRef, prior, strings.Repeat("a", 64)))
	stageRawHashes(t, store, ref, map[string][32]byte{"new": {2}})
	f.env = rawEnvelope(t, ref, raw)
	f.env.SpoolName, f.env.UploadURL = spool.Name, "upload"
	require.NoError(t, f.out.Append(f.env))
	return f
}

func stageRawHashes(t *testing.T, store *delta.Store, ref delta.PendingRef,
	hashes map[string][32]byte,
) {
	t.Helper()
	pending, err := store.BeginPendingRef(ref)
	require.NoError(t, err)
	require.NoError(t, pending.Replace(hashes))
}

func rawEnvelope(t *testing.T, ref delta.PendingRef, raw manifest.Raw) queue.Envelope {
	t.Helper()
	payload, err := manifest.CanonicalJSON(raw)
	require.NoError(t, err)
	hash, err := manifest.SHA256(raw)
	require.NoError(t, err)
	return queue.Envelope{Type: queue.TypeRawManifest, JobID: ref.JobID,
		FencingToken: ref.FencingToken, SourceKey: ref.SourceKey,
		ManifestJSON: payload, ManifestSHA256: hash}
}

func TestSucessoConfirmaHashDoServidorAntesDeApagarEnvelope(t *testing.T) {
	f := newRawDrainFixture(t)
	client := rawClientFunc(func(_ context.Context, env queue.Envelope) (RawManifestResponse, error) {
		require.Equal(t, f.env.ManifestJSON, env.ManifestJSON)
		_, _, head, _, _, err := f.store.ChainHead(f.ref.SourceKey)
		require.NoError(t, err)
		require.Equal(t, strings.Repeat("a", 64), head)
		return RawManifestResponse{StatusCode: 201, ManifestSHA256: strings.Repeat("b", 64)}, nil
	})
	require.NoError(t, f.drainer(client).Drain(context.Background(), f.out))
	_, seq, hash, _, ok, err := f.store.ChainHead(f.ref.SourceKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint32(1), seq)
	require.Equal(t, strings.Repeat("b", 64), hash)
	committed, err := f.store.GetCommitted(f.ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"new": {2}}, committed)
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestRedeE5xxPreservamPendenteCabecaEEnvelope(t *testing.T) {
	cases := []struct {
		name     string
		response RawManifestResponse
		err      error
	}{
		{"rede", RawManifestResponse{}, errors.New("network=down")},
		{"5xx", RawManifestResponse{StatusCode: 503}, nil},
		{"limite", RawManifestResponse{StatusCode: 429}, nil},
		{"4xx", RawManifestResponse{StatusCode: 400}, nil},
		{"409_comum", RawManifestResponse{StatusCode: 409}, nil},
		{"sucesso_sem_hash", RawManifestResponse{StatusCode: 200}, nil},
		{"hash_invalido", RawManifestResponse{StatusCode: 200, ManifestSHA256: "bad"}, nil},
		{"409_inconsistente", RawManifestResponse{StatusCode: 409,
			ManifestSHA256: strings.Repeat("b", 64), ForceFull: true, Reason: "base_missing"}, nil},
		{"sucesso_resync", RawManifestResponse{StatusCode: 200,
			ManifestSHA256: strings.Repeat("b", 64), ForceFull: true}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newRawDrainFixture(t)
			before, err := f.out.Peek(10)
			require.NoError(t, err)
			client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
				return tc.response, tc.err
			})
			require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
			after, err := f.out.Peek(10)
			require.NoError(t, err)
			require.Equal(t, before, after)
			_, _, hash, _, _, err := f.store.ChainHead(f.ref.SourceKey)
			require.NoError(t, err)
			require.Equal(t, strings.Repeat("a", 64), hash)
			require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("b", 64)))
		})
	}
}

func TestResposta409TipadaMarcaSoFonteParaFullEDescartaPendente(t *testing.T) {
	f := newRawDrainFixture(t)
	other := f.ref
	other.SourceKey.Competencia = "202602"
	stageRawHashes(t, f.store, other, map[string][32]byte{"other": {3}})
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		return RawManifestResponse{StatusCode: 409, ManifestSHA256: f.env.ManifestSHA256,
			ForceFull: true, Reason: "base_missing"}, nil
	})
	require.NoError(t, f.drainer(client).Drain(context.Background(), f.out))
	reason, force, err := f.store.ForceFull(f.ref.SourceKey)
	require.NoError(t, err)
	require.True(t, force)
	require.Equal(t, "base_missing", reason)
	_, force, err = f.store.ForceFull(other.SourceKey)
	require.NoError(t, err)
	require.False(t, force)
	require.NoError(t, f.store.ConfirmPending(other, f.raw, strings.Repeat("c", 64)))
	committed, err := f.store.GetCommitted(f.ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"old": {1}}, committed)
	require.Error(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("b", 64)))
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestReplayAposCommitLocalAntesDaExclusaoConverge(t *testing.T) {
	f := newRawDrainFixture(t)
	hash := strings.Repeat("b", 64)
	require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, hash))
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		return RawManifestResponse{StatusCode: 200, ManifestSHA256: hash}, nil
	})
	require.NoError(t, f.drainer(client).Drain(context.Background(), f.out))
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestManifestoCorrompidoPermaneceNaFilaSemEnvio(t *testing.T) {
	f := newRawDrainFixture(t)
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.NoError(t, f.out.Delete(items[0].Key))
	f.raw.ManifestID = "different-job"
	f.env.ManifestJSON, err = json.Marshal(f.raw)
	require.NoError(t, err)
	require.NoError(t, f.out.Append(f.env))
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		t.Fatal("manifesto corrompido enviado")
		return RawManifestResponse{}, nil
	})
	require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
	items, err = f.out.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
}

func TestDrainerLegadoPreservaEnvelopeRawImutavel(t *testing.T) {
	f := newRawDrainFixture(t)
	before, err := f.out.Peek(10)
	require.NoError(t, err)
	d := NewDrainer(f.out, breaker.New(5, time.Minute, "legacy"), &stubJobAPI{})
	d.tick(context.Background())
	after, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func Test409ComHashCertoSemIndicacaoOuMotivoPreservaPendente(t *testing.T) {
	for _, response := range []RawManifestResponse{
		{StatusCode: 409, Reason: "base_missing"},
		{StatusCode: 409, ForceFull: true},
		{StatusCode: 409, ForceFull: true, Reason: "  "},
	} {
		f := newRawDrainFixture(t)
		response.ManifestSHA256 = f.env.ManifestSHA256
		client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
			return response, nil
		})
		require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
		_, force, err := f.store.ForceFull(f.ref.SourceKey)
		require.NoError(t, err)
		require.False(t, force)
		require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("b", 64)))
		items, err := f.out.Peek(10)
		require.NoError(t, err)
		require.Len(t, items, 1)
	}
}

func Test409NaoMarcaOutraCompetenciaQuandoChaveContradizManifesto(t *testing.T) {
	f := newRawDrainFixture(t)
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.NoError(t, f.out.Delete(items[0].Key))
	f.env.SourceKey.Competencia = "202602"
	require.NoError(t, f.out.Append(f.env))
	before, err := f.out.Peek(10)
	require.NoError(t, err)
	calls := 0
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		calls++
		return RawManifestResponse{StatusCode: 409, ManifestSHA256: f.env.ManifestSHA256,
			ForceFull: true, Reason: "base_missing"}, nil
	})
	require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
	require.Zero(t, calls)
	after, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, forced, err := f.store.ForceFull(f.env.SourceKey)
	require.NoError(t, err)
	require.False(t, forced)
	require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("c", 64)))
}

func TestDecodificacaoVinculaFonteIntentECompetenciaNormalizada(t *testing.T) {
	f := newRawDrainFixture(t)
	for _, key := range []delta.SourceKey{
		{Source: "sihd", Intent: "profissionais", Competencia: "202601"},
		{Source: "cnes", Intent: "aih", Competencia: "202601"},
		{Source: "cnes", Intent: "profissionais", Competencia: "2026--01"},
	} {
		env := f.env
		env.SourceKey = key
		_, err := decodeRawEnvelope(env)
		require.Error(t, err)
	}
	f.env.SourceKey.Competencia = "2026-01"
	_, err := decodeRawEnvelope(f.env)
	require.NoError(t, err)
}
