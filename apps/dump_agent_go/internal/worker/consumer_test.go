package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/extractor"
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
	runFn func(ctx context.Context, job *worker.Job) (int64, error)
}

func (e *execStub) Run(ctx context.Context, job *worker.Job) (int64, error) {
	return e.runFn(ctx, job)
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
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111", TenantID: "354130", Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
	var mintCalls int32
	api := &apiStub{
		mintFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) {
			if atomic.AddInt32(&mintCalls, 1) == 1 {
				return job, nil
			}
			return nil, nil
		},
	}
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222", Intent: extractor.IntentCnesEstabelecimentos}
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
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111", Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
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
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222", Intent: extractor.IntentCnesEstabelecimentos}
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
