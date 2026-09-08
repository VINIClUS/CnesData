package worker

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/writer"
	"github.com/stretchr/testify/require"
)

type mockJobAPI struct {
	mintCalls      int
	registerCalls  int
	failCalls      int
	heartbeatCalls int
	mintErr        error
	mintJob        *Job
}

func newFixtureSpool(t *testing.T) (string, upload.RawSpool) {
	t.Helper()
	directory := t.TempDir()
	spool, err := upload.PrepareRawSpool(context.Background(), directory, func(dst io.Writer) error {
		return writer.WriteRawFullParquet(dst, []delta.Row{{"CPF": "fixture"}})
	})
	require.NoError(t, err)
	return directory, spool
}

type fixtureRawUploader struct{}

func (fixtureRawUploader) PutRaw(_ context.Context, request upload.RawPutRequest) (int64, error) {
	return io.Copy(io.Discard, request.Body)
}

func (f rawDrainFixture) drainer(client RawManifestClient) *RawDrainer {
	drainer := NewRawDrainer(client, f.store, nil)
	drainer.SpoolDirectory, drainer.Uploader = f.spoolDirectory, fixtureRawUploader{}
	return drainer
}

func TestSpoolAusenteSemReciboNaoRegistraManifesto(t *testing.T) {
	f := newRawDrainFixture(t)
	require.NoError(t, upload.RemoveRawSpool(f.spoolDirectory, f.env.SpoolName))
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		t.Error("manifesto_enviado_sem_spool=true")
		return RawManifestResponse{}, nil
	})
	require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("b", 64)))
}

type cleanupFailureOutbox struct {
	*queue.Outbox
	path string
}

func (o cleanupFailureOutbox) MarkRawTerminal(key []byte) error {
	if err := o.Outbox.MarkRawTerminal(key); err != nil {
		return err
	}
	if err := os.Rename(o.path, o.path+".retained"); err != nil {
		return err
	}
	if err := os.Mkdir(o.path, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(o.path, "busy"), []byte("busy"), 0o600)
}

func TestFalhaNaLimpezaRetomaSoLimpezaAposReinicio(t *testing.T) {
	f := newRawDrainFixture(t)
	path := filepath.Join(f.spoolDirectory, f.env.SpoolName)
	calls := 0
	client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
		calls++
		return RawManifestResponse{StatusCode: 200, ManifestSHA256: f.env.ManifestSHA256}, nil
	})
	out := cleanupFailureOutbox{Outbox: f.out, path: path}
	require.Error(t, f.drainer(client).Drain(context.Background(), out))
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	terminal, err := f.out.RawTerminal(items[0].Key)
	require.NoError(t, err)
	require.True(t, terminal)
	require.FileExists(t, path+".retained")
	require.NoError(t, os.Remove(filepath.Join(path, "busy")))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Rename(path+".retained", path))
	reopenRawFixture(t, &f)
	drainer := f.drainer(client)
	drainer.Uploader = nil
	require.NoError(t, drainer.Drain(context.Background(), f.out))
	require.Equal(t, 1, calls)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
	items, err = f.out.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}

type failingEnvelopeStore struct {
	*queue.Outbox
	onDelete func() error
	onAppend func(queue.Envelope) error
}

func (f failingEnvelopeStore) Delete(keys ...[]byte) error {
	if f.onDelete != nil {
		if err := f.onDelete(); err != nil {
			return err
		}
	}
	return f.Outbox.Delete(keys...)
}

func (f failingEnvelopeStore) Append(env queue.Envelope) error {
	if f.onAppend != nil {
		if err := f.onAppend(env); err != nil {
			return err
		}
	}
	return f.Outbox.Append(env)
}

func TestCrashAposAckSoApagaEnvelopeAposPersistirEstado(t *testing.T) {
	for _, status := range []int{201, 409} {
		t.Run(httpStatusName(status), func(t *testing.T) {
			f := newRawDrainFixture(t)
			client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
				response := RawManifestResponse{StatusCode: status, ManifestSHA256: f.env.ManifestSHA256}
				if status == 409 {
					response.ForceFull, response.Reason = true, "base_missing"
				}
				return response, nil
			})
			out := failingEnvelopeStore{Outbox: f.out, onDelete: func() error {
				_, forced, err := f.store.ForceFull(f.ref.SourceKey)
				require.NoError(t, err)
				require.Equal(t, status == 409, forced)
				_, _, hash, _, _, err := f.store.ChainHead(f.ref.SourceKey)
				require.NoError(t, err)
				if status == 201 {
					require.Equal(t, f.env.ManifestSHA256, hash)
				}
				return errors.New("disk=unavailable")
			}}
			drainer := f.drainer(client)
			require.Error(t, drainer.Drain(context.Background(), out))
			items, err := f.out.Peek(10)
			require.NoError(t, err)
			require.Len(t, items, 1)
			require.NoError(t, drainer.Drain(context.Background(), f.out))
			items, err = f.out.Peek(10)
			require.NoError(t, err)
			require.Empty(t, items)
		})
	}
}

func httpStatusName(status int) string {
	if status == 409 {
		return "resync"
	}
	return "sucesso"
}

func TestCrashAntesDoEnvelopeRetomaPendenteDuravel(t *testing.T) {
	f := newRawDrainFixture(t)
	ref := f.ref
	ref.JobID = "orphan"
	stageRawHashes(t, f.store, ref, map[string][32]byte{"partial": {9}})
	executor := &JobExecutor{DeltaStore: f.store, RawOutbox: f.out}
	cycle := rawCycle{ref: ref, hashes: map[string][32]byte{"complete": {8}}}
	raw := f.raw
	raw.ManifestID, raw.SnapshotID = ref.JobID, ref.JobID
	raw.ObjectKey = "raw/tenant/CNES_LOCAL/2026-01/orphan/data.parquet"
	require.NoError(t, executor.enqueueRaw(cycle, raw))
	require.NoError(t, f.store.ConfirmPending(ref, raw, strings.Repeat("d", 64)))
	got, err := f.store.GetCommitted(ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"complete": {8}}, got)
}

func TestRunRawConvertePanicDaExtracaoEmErroDoJob(t *testing.T) {
	f := newRawDrainFixture(t)
	job := Job{ID: "panic-job", TenantID: f.raw.TenantID, FencingToken: 9,
		UploadURL: "https://object.invalid", Params: extractor.ExtractionParams{
			Intent: extractor.IntentCnesProfissionais, Competencia: "202601"},
		RawRequest: &manifest.BuildRequest{SourceType: f.raw.SourceType,
			FileSubtype: f.raw.FileSubtype, Competencia: f.raw.Competencia,
			AgentID: f.raw.AgentID, AgentVersion: f.raw.AgentVersion,
			SchemaVersion: f.raw.SchemaVersion, SnapshotMode: manifest.SnapshotModeFull,
			CreatedAt: f.raw.CreatedAt}}
	executor := &JobExecutor{DeltaStore: f.store, RawOutbox: f.out,
		RawSpoolDirectory: f.spoolDirectory, RawUploader: fixtureRawUploader{},
		RawExtract: func(context.Context, Job) ([]delta.Row, error) { panic("extract") }}
	_, err := executor.RunRaw(context.Background(), &job)
	require.ErrorContains(t, err, "panic in RunRaw")
}

func TestRetryComEnvelopeNaoSobrescreveFingerprintsPendentes(t *testing.T) {
	f := newRawDrainFixture(t)
	executor := &JobExecutor{DeltaStore: f.store, RawOutbox: f.out}
	cycle := rawCycle{ref: f.ref, hashes: map[string][32]byte{"changed": {9}}}
	cycle.spoolName, cycle.uploadURL = f.env.SpoolName, f.env.UploadURL
	before, err := f.out.Peek(10)
	require.NoError(t, err)
	require.NoError(t, executor.enqueueRaw(cycle, f.raw))
	changed := f.raw
	changed.SizeBytes++
	require.Error(t, executor.enqueueRaw(cycle, changed))
	after, err := f.out.Peek(10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, strings.Repeat("d", 64)))
	got, err := f.store.GetCommitted(f.ref.SourceKey)
	require.NoError(t, err)
	require.Equal(t, map[string][32]byte{"new": {2}}, got)
}

func TestPendenteCompletoExisteAntesDePersistirEnvelope(t *testing.T) {
	f := newRawDrainFixture(t)
	ref := f.ref
	ref.JobID = "next-job"
	raw := f.raw
	raw.ManifestID, raw.SnapshotID = ref.JobID, ref.JobID
	raw.ObjectKey = "raw/tenant/CNES_LOCAL/2026-01/next-job/data.parquet"
	out := failingEnvelopeStore{Outbox: f.out, onAppend: func(queue.Envelope) error {
		require.NoError(t, f.store.ConfirmPending(ref, raw, strings.Repeat("d", 64)))
		got, err := f.store.GetCommitted(ref.SourceKey)
		require.NoError(t, err)
		require.Equal(t, map[string][32]byte{"complete": {8}}, got)
		return errors.New("disk=full")
	}}
	executor := &JobExecutor{DeltaStore: f.store, RawOutbox: out}
	cycle := rawCycle{ref: ref, hashes: map[string][32]byte{"complete": {8}}}
	require.Error(t, executor.enqueueRaw(cycle, raw))
}

func TestRawDrainerTambemEntregaEnvelopesLegados(t *testing.T) {
	api := &stubJobAPI{}
	out := newOutbox(t)
	require.NoError(t, out.Append(queue.Envelope{Type: queue.TypeComplete,
		JobUUID: "legacy", EnqueuedAt: time.Now(), SizeBytes: 123}))
	require.NoError(t, NewRawDrainer(nil, nil, api).Drain(context.Background(), out))
	require.Equal(t, "legacy", api.lastJob.ID)
	require.Equal(t, int64(123), api.lastSize)
}

func reopenRawFixture(t *testing.T, f *rawDrainFixture) {
	t.Helper()
	_ = f.store.Close()
	require.NoError(t, f.out.Close())
	store, err := delta.Open(f.storePath)
	require.NoError(t, err)
	out, err := queue.Open(f.outPath)
	require.NoError(t, err)
	f.store, f.out = store, out
	t.Cleanup(func() { _ = store.Close(); _ = out.Close() })
}

func TestReinicioConvergeEmCadaFronteiraDuravel(t *testing.T) {
	for _, phase := range []string{"pendente", "envelope", "ack", "commit", "delete", "resync"} {
		t.Run(phase, func(t *testing.T) {
			f := newRawDrainFixture(t)
			items, err := f.out.Peek(10)
			require.NoError(t, err)
			simulateRawCrash(t, &f, phase)
			reopenRawFixture(t, &f)
			if phase == "pendente" {
				executor := JobExecutor{DeltaStore: f.store, RawOutbox: f.out}
				require.NoError(t, executor.enqueueRaw(rawCycle{ref: f.ref,
					hashes: map[string][32]byte{"new": {2}}, spoolName: f.env.SpoolName,
					uploadURL: f.env.UploadURL}, f.raw))
			}
			client := rawClientFunc(func(_ context.Context,
				env queue.Envelope,
			) (RawManifestResponse, error) {
				require.Equal(t, items[0].Envelope.ManifestJSON, env.ManifestJSON)
				response := RawManifestResponse{StatusCode: 200, ManifestSHA256: f.env.ManifestSHA256}
				if phase == "resync" {
					response.StatusCode, response.ForceFull, response.Reason = 409, true, "base_missing"
				}
				return response, nil
			})
			require.NoError(t, f.drainer(client).Drain(context.Background(), f.out))
			remaining, err := f.out.Peek(10)
			require.NoError(t, err)
			require.Empty(t, remaining)
			committed, err := f.store.GetCommitted(f.ref.SourceKey)
			require.NoError(t, err)
			if phase == "resync" {
				require.Equal(t, map[string][32]byte{"old": {1}}, committed)
			} else {
				require.Equal(t, map[string][32]byte{"new": {2}}, committed)
			}
		})
	}
}

func simulateRawCrash(t *testing.T, f *rawDrainFixture, phase string) {
	t.Helper()
	items, err := f.out.Peek(10)
	require.NoError(t, err)
	if phase == "resync" {
		require.NoError(t, f.store.RequireFull(f.ref, "base_missing"))
	}
	if phase == "commit" || phase == "delete" {
		require.NoError(t, f.store.ConfirmPending(f.ref, f.raw, f.env.ManifestSHA256))
	}
	if phase == "delete" || phase == "pendente" {
		require.NoError(t, f.out.Delete(items[0].Key))
	}
	if phase == "ack" {
		client := rawClientFunc(func(context.Context, queue.Envelope) (RawManifestResponse, error) {
			require.NoError(t, f.store.Close())
			return RawManifestResponse{StatusCode: 200, ManifestSHA256: f.env.ManifestSHA256}, nil
		})
		require.Error(t, f.drainer(client).Drain(context.Background(), f.out))
	}
}

func (m *mockJobAPI) MintUploadURL(_ context.Context, _ JobSpec) (*Job, error) {
	m.mintCalls++
	return m.mintJob, m.mintErr
}
func (m *mockJobAPI) RegisterJob(_ context.Context, _ Job, _ int64) error {
	m.registerCalls++
	return nil
}
func (m *mockJobAPI) FailJob(_ context.Context, _ Job, _ error) error {
	m.failCalls++
	return nil
}
func (m *mockJobAPI) SendHeartbeat(_ context.Context, _ string) error {
	m.heartbeatCalls++
	return nil
}

func newOutbox(t *testing.T) *queue.Outbox {
	t.Helper()
	ob, err := queue.Open(filepath.Join(t.TempDir(), "ob.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ob.Close() })
	return ob
}

func TestOutboxAdapter_RegisterWritesEnvelopeNotInner(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job := Job{ID: "uuid-1"}
	if err := a.RegisterJob(context.Background(), job, 4096); err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	if mock.registerCalls != 0 {
		t.Errorf("inner.RegisterJob called %d times, want 0", mock.registerCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 1 || items[0].Envelope.Type != queue.TypeComplete ||
		items[0].Envelope.JobUUID != "uuid-1" || items[0].Envelope.SizeBytes != 4096 {
		t.Fatalf("envelope mismatch: %+v", items)
	}
}

func TestOutboxAdapter_FailWritesEnvelopeNotInner(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job := Job{ID: "uuid-2"}
	cause := errors.New("extract_failed")
	if err := a.FailJob(context.Background(), job, cause); err != nil {
		t.Fatalf("FailJob: %v", err)
	}
	if mock.failCalls != 0 {
		t.Errorf("inner.FailJob called %d times, want 0", mock.failCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 1 || items[0].Envelope.Type != queue.TypeFail ||
		items[0].Envelope.Cause != "extract_failed" {
		t.Fatalf("envelope mismatch: %+v", items)
	}
}

func TestOutboxAdapter_MintUploadURLDelegates(t *testing.T) {
	mock := &mockJobAPI{mintJob: &Job{ID: "back"}}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job, err := a.MintUploadURL(context.Background(), JobSpec{})
	if err != nil {
		t.Fatalf("MintUploadURL: %v", err)
	}
	if job == nil || job.ID != "back" {
		t.Fatalf("got %+v, want delegated Job", job)
	}
	if mock.mintCalls != 1 {
		t.Fatalf("inner.MintUploadURL called %d times, want 1", mock.mintCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Fatalf("unexpected envelope from MintUploadURL: %+v", items)
	}
}

func TestOutboxAdapter_RegisterJob_PersistsSha256AndMinioKey(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	job := Job{
		ID:       "11111111-2222-3333-4444-555555555555",
		Sha256:   "a" + strings.Repeat("0", 63),
		MinioKey: "354130/CNES_VINCULO/2026-01-01/abc.parquet.gz",
	}
	if err := a.RegisterJob(context.Background(), job, 4096); err != nil {
		t.Fatalf("RegisterJob: %v", err)
	}
	items, _ := ob.Peek(10)
	if len(items) != 1 {
		t.Fatalf("envelope count = %d want 1", len(items))
	}
	env := items[0].Envelope
	if env.SHA256 != job.Sha256 {
		t.Errorf("SHA256 = %q want %q", env.SHA256, job.Sha256)
	}
	if env.MinioKey != job.MinioKey {
		t.Errorf("MinioKey = %q want %q", env.MinioKey, job.MinioKey)
	}
	if env.SizeBytes != 4096 {
		t.Errorf("SizeBytes = %d want 4096", env.SizeBytes)
	}
}

func TestOutboxAdapter_HeartbeatDelegates(t *testing.T) {
	mock := &mockJobAPI{}
	ob := newOutbox(t)
	a := NewOutboxAdapter(mock, ob)
	if err := a.SendHeartbeat(context.Background(), "uuid-3"); err != nil {
		t.Fatalf("SendHeartbeat: %v", err)
	}
	if mock.heartbeatCalls != 1 {
		t.Fatalf("inner.SendHeartbeat called %d times, want 1", mock.heartbeatCalls)
	}
	items, _ := ob.Peek(10)
	if len(items) != 0 {
		t.Fatalf("unexpected envelope from Heartbeat: %+v", items)
	}
}
