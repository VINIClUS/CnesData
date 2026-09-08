package worker_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

type apiStub struct {
	mintFn     func(ctx context.Context, spec worker.JobSpec) (*worker.Job, error)
	registerFn func(ctx context.Context, job worker.Job, size int64) error
	failFn     func(ctx context.Context, job worker.Job, err error) error
	hbFn       func(ctx context.Context, jobID string) error

	mintCalls     int32
	registerCalls int32
}

func (a *apiStub) MintUploadURL(ctx context.Context, spec worker.JobSpec) (*worker.Job, error) {
	atomic.AddInt32(&a.mintCalls, 1)
	return a.mintFn(ctx, spec)
}
func (a *apiStub) RegisterJob(ctx context.Context, job worker.Job, size int64) error {
	atomic.AddInt32(&a.registerCalls, 1)
	if a.registerFn == nil {
		return nil
	}
	return a.registerFn(ctx, job, size)
}
func (a *apiStub) FailJob(ctx context.Context, job worker.Job, err error) error {
	if a.failFn == nil {
		return nil
	}
	return a.failFn(ctx, job, err)
}
func (a *apiStub) SendHeartbeat(ctx context.Context, jobID string) error {
	if a.hbFn == nil {
		return nil
	}
	return a.hbFn(ctx, jobID)
}

type execStub struct {
	runFn          func(ctx context.Context, job *worker.Job) (int64, error)
	committedCalls int32
}

type rawWaitExecStub struct {
	*execStub
	waitFn func(context.Context, *worker.Job) error
}

type appendFailOutbox struct {
	*queue.Outbox
}

func (o appendFailOutbox) Append(queue.Envelope) error {
	return errors.New("outbox=unavailable")
}

func (e *rawWaitExecStub) WaitRawTerminal(ctx context.Context, job *worker.Job) error {
	return e.waitFn(ctx, job)
}

func (e *execStub) Run(ctx context.Context, job *worker.Job) (int64, error) {
	return e.runFn(ctx, job)
}

func (e *execStub) EmitCommitted(_ worker.Job, _ int64) {
	atomic.AddInt32(&e.committedCalls, 1)
}

type sourceStub struct {
	nextFn func(ctx context.Context) (*worker.JobSpec, error)
}

func (s *sourceStub) Next(ctx context.Context) (*worker.JobSpec, error) {
	return s.nextFn(ctx)
}

func TestConsumerLoop_ExitsOnContextDone(t *testing.T) {
	api := &apiStub{
		mintFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) { return nil, nil },
	}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) { return nil, nil }}
	cons := worker.NewConsumer(api, src, &execStub{}, worker.ConsumerConfig{
		PollInterval:      5 * time.Millisecond,
		InterJobJitterMax: time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))
}

func TestConsumerLoop_RegistersAfterSuccessfulRun(t *testing.T) {
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111", TenantID: "354130",
		Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
	var mintCalls int32
	api := &apiStub{
		mintFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) {
			if atomic.AddInt32(&mintCalls, 1) == 1 {
				return job, nil
			}
			return nil, nil
		},
	}
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222",
		Intent: extractor.IntentCnesEstabelecimentos}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) {
		return spec, nil
	}}
	exec := &execStub{runFn: func(_ context.Context, _ *worker.Job) (int64, error) { return 100, nil }}

	cons := worker.NewConsumer(api, src, exec, worker.ConsumerConfig{
		PollInterval:      time.Millisecond,
		InterJobJitterMax: time.Millisecond,
		HeartbeatInterval: time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))
	require.GreaterOrEqual(t, atomic.LoadInt32(&api.registerCalls), int32(1))
}

func TestConsumerLoop_FailsJobOnError(t *testing.T) {
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111",
		Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
	var failCalls int32
	api := &apiStub{
		mintFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) {
			return job, nil
		},
		failFn: func(_ context.Context, _ worker.Job, _ error) error {
			atomic.AddInt32(&failCalls, 1)
			return nil
		},
	}
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222",
		Intent: extractor.IntentCnesEstabelecimentos}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) {
		return spec, nil
	}}
	exec := &execStub{runFn: func(_ context.Context, _ *worker.Job) (int64, error) {
		return 0, errors.New("boom")
	}}
	cons := worker.NewConsumer(api, src, exec, worker.ConsumerConfig{
		PollInterval:      time.Millisecond,
		InterJobJitterMax: time.Millisecond,
		HeartbeatInterval: time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))
	require.GreaterOrEqual(t, atomic.LoadInt32(&failCalls), int32(1))
}

func TestLoop_SequenceMintRunRegister(t *testing.T) {
	api := &apiStub{
		mintFn: func(_ context.Context, spec worker.JobSpec) (*worker.Job, error) {
			return &worker.Job{
				ID:        spec.JobID,
				UploadURL: "http://fake",
				Params:    extractor.ExtractionParams{Intent: spec.Intent},
			}, nil
		},
	}
	exec := &execStub{runFn: func(_ context.Context, j *worker.Job) (int64, error) {
		j.Sha256 = "abc"
		return 100, nil
	}}
	spec := &worker.JobSpec{
		JobID:  "11111111-2222-3333-4444-555555555555",
		Intent: extractor.IntentCnesEstabelecimentos,
	}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) { return spec, nil }}
	cons := worker.NewConsumer(api, src, exec, worker.ConsumerConfig{
		PollInterval:      time.Millisecond,
		HeartbeatInterval: time.Hour,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))

	mint := atomic.LoadInt32(&api.mintCalls)
	register := atomic.LoadInt32(&api.registerCalls)
	if mint < 1 || register < 1 {
		t.Errorf("mint=%d register=%d, want both >= 1", mint, register)
	}
}

func TestConsumerRawAguardaDrainerSemRegistrarOuFalharJobLegado(t *testing.T) {
	for _, executionErr := range []error{nil, errors.New("upload=failed")} {
		ctx, cancel := context.WithCancel(context.Background())
		api := &apiStub{mintFn: func(context.Context, worker.JobSpec) (*worker.Job, error) {
			return &worker.Job{ID: "raw-job", RawRequest: &manifest.BuildRequest{}}, nil
		}, failFn: func(context.Context, worker.Job, error) error {
			t.Error("falha raw enviada ao fluxo legado")
			return nil
		}}
		executor := &execStub{runFn: func(context.Context, *worker.Job) (int64, error) {
			cancel()
			return 10, executionErr
		}}
		source := &sourceStub{nextFn: func(context.Context) (*worker.JobSpec, error) {
			return &worker.JobSpec{}, nil
		}}
		consumer := worker.NewConsumer(api, source, executor,
			worker.ConsumerConfig{HeartbeatInterval: time.Hour})
		require.NoError(t, consumer.Loop(ctx))
		require.Zero(t, api.registerCalls)
		require.Zero(t, executor.committedCalls)
	}
}

func TestConsumerRawMantemHeartbeatAteEnvelopeTerminal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var heartbeats atomic.Int32
	api := &apiStub{mintFn: func(context.Context, worker.JobSpec) (*worker.Job, error) {
		return &worker.Job{ID: "raw-job", RawRequest: &manifest.BuildRequest{}}, nil
	}, hbFn: func(context.Context, string) error {
		heartbeats.Add(1)
		return nil
	}}
	waiting, terminal := make(chan struct{}), make(chan struct{})
	executor := &rawWaitExecStub{execStub: &execStub{
		runFn: func(context.Context, *worker.Job) (int64, error) { return 10, nil },
	}, waitFn: func(ctx context.Context, _ *worker.Job) error {
		close(waiting)
		select {
		case <-terminal:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	source := &sourceStub{nextFn: func(context.Context) (*worker.JobSpec, error) {
		return &worker.JobSpec{}, nil
	}}
	consumer := worker.NewConsumer(api, source, executor, worker.ConsumerConfig{
		PollInterval: time.Millisecond, HeartbeatInterval: time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- consumer.Loop(ctx) }()
	<-waiting
	require.Eventually(t, func() bool { return heartbeats.Load() > 0 }, time.Second, time.Millisecond)
	close(terminal)
	cancel()
	require.NoError(t, <-done)
}

func TestExecutorRawRemoveSpoolQuandoEnvelopeNaoPersiste(t *testing.T) {
	executor, job := newRawExecutor(t), rawJob()
	executor.RawOutbox = appendFailOutbox{Outbox: executor.RawOutbox.(*queue.Outbox)}

	_, err := executor.RunRaw(context.Background(), &job)
	require.ErrorContains(t, err, "outbox=unavailable")
	entries, readErr := os.ReadDir(executor.RawSpoolDirectory)
	require.NoError(t, readErr)
	require.Empty(t, entries)
}

func TestExecutorRawAguardaEnvelopeTerminal(t *testing.T) {
	executor, job := newRawExecutor(t), rawJob()
	_, err := executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- executor.WaitRawTerminal(context.Background(), &job) }()
	select {
	case err := <-done:
		t.Fatalf("espera terminou antes do recibo: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	items, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	require.NoError(t, executor.RawOutbox.MarkRawTerminal(items[0].Key))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("espera nao terminou apos recibo")
	}
}

func TestMetadataRawContraditoriaFalhaAntesDaExtracao(t *testing.T) {
	cases := []struct {
		name   string
		change func(*worker.Job)
	}{
		{"fonte", func(job *worker.Job) { job.RawRequest.SourceType = manifest.SourceTypeSIHD }},
		{"subtipo", func(job *worker.Job) { job.RawRequest.FileSubtype = "AIH" }},
		{"competencia", func(job *worker.Job) { job.RawRequest.Competencia = "2026-02" }},
		{"formato", func(job *worker.Job) { job.Params.Competencia = "2026--01" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor, job := newRawExecutor(t), rawJob()
			tc.change(&job)
			extractions := 0
			executor.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) {
				extractions++
				return nil, nil
			}
			_, err := executor.RunRaw(context.Background(), &job)
			require.Error(t, err)
			require.Zero(t, extractions)
			items, err := executor.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Empty(t, items)
		})
	}
}

func workforceLink(indicator, sus string, hours int64) delta.Row {
	return delta.Row{"CPF": "synthetic", "CNES": "unit", "CBO": "role",
		"TIPO_VINCULO": indicator, "SUS": sus, "CH_TOTAL": hours}
}

func extractRawDelta(t *testing.T, before, after []delta.Row) []delta.Row {
	t.Helper()
	return readRawOperations(t, extractRawDeltaBytes(t, before, after))
}

func extractRawDeltaBytes(t *testing.T, before, after []delta.Row) []byte {
	t.Helper()
	executor, job := newRawExecutor(t), rawJob()
	executor.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) { return before, nil }
	_, err := executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	confirmRawExecutor(t, executor, job)
	job.ID, job.FencingToken = "delta-job", 8
	job.RawRequest.SnapshotMode = manifest.SnapshotModeDelta
	executor.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) { return after, nil }
	var payload []byte
	executor.RawUploader = rawUploadFunc(func(_ context.Context,
		request upload.RawPutRequest,
	) (int64, error) {
		payload, err = io.ReadAll(request.Body)
		return int64(len(payload)), err
	})
	_, err = executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	return payload
}

func TestDeltaComAtualizacaoEInsercaoIndependeDaOrdemDaExtracao(t *testing.T) {
	before := []delta.Row{workforceLink("010101", "S", 20)}
	a, b := workforceLink("010101", "S", 30), workforceLink("010101", "S", 40)
	first := extractRawDeltaBytes(t, before, []delta.Row{a, b})
	second := extractRawDeltaBytes(t, before, []delta.Row{b, a})
	require.True(t, bytes.Equal(first, second), "delta_bytes_dependem_da_ordem=true")
	rows := readRawOperations(t, first)
	require.Len(t, rows, 2)
	operations := map[any]int{}
	var hours []any
	for _, row := range rows {
		operations[row["_op"]]++
		hours = append(hours, row["CH_TOTAL"])
	}
	require.Equal(t, map[any]int{"I": 1, "U": 1}, operations)
	require.ElementsMatch(t, []any{"30", "40"}, hours)
}

func TestDeltaPreservaVinculosCompletosEMultiplicidade(t *testing.T) {
	a, b := workforceLink("010101", "S", 20), workforceLink("020202", "N", 40)
	cases := []struct {
		name          string
		before, after []delta.Row
		operations    []string
	}{
		{"remove_um_vinculo", []delta.Row{a, b}, []delta.Row{a}, []string{"D"}},
		{"muda_um_vinculo", []delta.Row{a, b},
			[]delta.Row{a, workforceLink("020202", "N", 30)}, []string{"U"}},
		{"reordena_vinculos", []delta.Row{a, b}, []delta.Row{b, a}, nil},
		{"remove_vinculo_diferente_so_sus", []delta.Row{a, workforceLink("010101", "N", 20)},
			[]delta.Row{a}, []string{"D"}},
		{"remove_vinculo_diferente_so_indicador", []delta.Row{a, workforceLink("020202", "S", 20)},
			[]delta.Row{a}, []string{"D"}},
		{"reordena_mesma_identidade", []delta.Row{a, workforceLink("010101", "S", 30)},
			[]delta.Row{workforceLink("010101", "S", 30), a}, nil},
		{"remove_duplicata", []delta.Row{a, a}, []delta.Row{a}, []string{"D"}},
		{"adiciona_duplicatas", []delta.Row{a}, []delta.Row{a, a, a}, []string{"I", "I"}},
		{"muda_uma_duplicata", []delta.Row{a, a},
			[]delta.Row{a, workforceLink("010101", "S", 30)}, []string{"U"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rows := extractRawDelta(t, tc.before, tc.after)
			require.Len(t, rows, len(tc.operations))
			for index, op := range tc.operations {
				require.Equal(t, op, rows[index]["_op"])
				require.NotNil(t, rows[index]["TIPO_VINCULO"])
				require.NotNil(t, rows[index]["SUS"])
			}
		})
	}
}

type rawManifestClientFunc func(context.Context, queue.Envelope) (worker.RawManifestResponse, error)

func (f rawManifestClientFunc) SendRawManifest(ctx context.Context,
	env queue.Envelope,
) (worker.RawManifestResponse, error) {
	return f(ctx, env)
}

func restartRawExecutor(t *testing.T, executor *worker.JobExecutor) {
	t.Helper()
	directory := filepath.Dir(executor.RawSpoolDirectory)
	require.NoError(t, executor.DeltaStore.Close())
	require.NoError(t, executor.RawOutbox.(*queue.Outbox).Close())
	store, err := delta.Open(filepath.Join(directory, "delta.db"))
	require.NoError(t, err)
	out, err := queue.Open(filepath.Join(directory, "outbox.db"))
	require.NoError(t, err)
	executor.DeltaStore, executor.RawOutbox = store, out
	t.Cleanup(func() { _ = store.Close(); _ = out.Close() })
}

func TestSpoolRetomaBytesOriginaisAposCrashNoPut(t *testing.T) {
	for _, phase := range []string{"antes_put", "durante_put", "apos_put"} {
		t.Run(phase, func(t *testing.T) {
			executor, job := newRawExecutor(t), rawJob()
			executor.RawUploader = rawUploadFunc(func(_ context.Context,
				request upload.RawPutRequest,
			) (int64, error) {
				if phase == "apos_put" {
					return io.Copy(io.Discard, request.Body)
				}
				if phase == "durante_put" {
					_, _ = io.CopyN(io.Discard, request.Body, 4)
				}
				return 0, errors.New("crash=put")
			})
			_, executionErr := executor.RunRaw(context.Background(), &job)
			require.Equal(t, phase != "apos_put", executionErr != nil)
			items, err := executor.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Len(t, items, 1)
			require.NotEmpty(t, items[0].Envelope.SpoolName)
			path := filepath.Join(executor.RawSpoolDirectory, items[0].Envelope.SpoolName)
			original, err := os.ReadFile(path)
			require.NoError(t, err)
			restartRawExecutor(t, executor)
			verifySpoolReplay(t, executor, original)
			_, err = os.Stat(path)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

func verifySpoolReplay(t *testing.T, executor *worker.JobExecutor, original []byte) {
	t.Helper()
	uploaded := false
	client := rawManifestClientFunc(func(_ context.Context,
		env queue.Envelope,
	) (worker.RawManifestResponse, error) {
		require.True(t, uploaded)
		return worker.RawManifestResponse{StatusCode: 200, ManifestSHA256: env.ManifestSHA256}, nil
	})
	drainer := worker.NewRawDrainer(client, executor.DeltaStore, nil)
	drainer.SpoolDirectory = executor.RawSpoolDirectory
	drainer.Uploader = rawUploadFunc(func(_ context.Context,
		request upload.RawPutRequest,
	) (int64, error) {
		body, err := io.ReadAll(request.Body)
		require.NoError(t, err)
		require.Equal(t, original, body)
		uploaded = true
		return int64(len(body)), nil
	})
	require.NoError(t, drainer.Drain(context.Background(), executor.RawOutbox))
	items, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
}
