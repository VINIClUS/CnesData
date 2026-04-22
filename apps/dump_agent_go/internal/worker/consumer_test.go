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
	registerFn func(ctx context.Context, spec worker.JobSpec) (*worker.Job, error)
	completeFn func(ctx context.Context, job worker.Job, size int64) error
	failFn     func(ctx context.Context, job worker.Job, err error) error
	hbFn       func(ctx context.Context, jobID string) error
}

func (a *apiStub) RegisterJob(ctx context.Context, spec worker.JobSpec) (*worker.Job, error) {
	return a.registerFn(ctx, spec)
}
func (a *apiStub) CompleteJob(ctx context.Context, job worker.Job, size int64) error {
	return a.completeFn(ctx, job, size)
}
func (a *apiStub) FailJob(ctx context.Context, job worker.Job, err error) error {
	return a.failFn(ctx, job, err)
}
func (a *apiStub) SendHeartbeat(ctx context.Context, jobID string) error {
	return a.hbFn(ctx, jobID)
}

type execStub struct {
	runFn func(ctx context.Context, job worker.Job) (int64, error)
}

func (e *execStub) Run(ctx context.Context, job worker.Job) (int64, error) {
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
		registerFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) { return nil, nil },
		hbFn:       func(_ context.Context, _ string) error { return nil },
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

func TestConsumerLoop_CompletesSuccessfulJob(t *testing.T) {
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111", TenantID: "354130", Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
	var registerCalls, completeCalls int32
	api := &apiStub{
		registerFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) {
			if atomic.AddInt32(&registerCalls, 1) == 1 {
				return job, nil
			}
			return nil, nil
		},
		completeFn: func(_ context.Context, _ worker.Job, _ int64) error {
			atomic.AddInt32(&completeCalls, 1)
			return nil
		},
		failFn: func(_ context.Context, _ worker.Job, _ error) error { return nil },
		hbFn:   func(_ context.Context, _ string) error { return nil },
	}
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222", Intent: extractor.IntentCnesEstabelecimentos}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) {
		return spec, nil
	}}
	exec := &execStub{runFn: func(_ context.Context, _ worker.Job) (int64, error) { return 100, nil }}

	cons := worker.NewConsumer(api, src, exec, worker.ConsumerConfig{
		PollInterval:      time.Millisecond,
		InterJobJitterMax: time.Millisecond,
		HeartbeatInterval: time.Second,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))
	require.GreaterOrEqual(t, atomic.LoadInt32(&completeCalls), int32(1))
}

func TestConsumerLoop_FailsJobOnError(t *testing.T) {
	job := &worker.Job{ID: "11111111-1111-1111-1111-111111111111", Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos}}
	var failCalls int32
	api := &apiStub{
		registerFn: func(_ context.Context, _ worker.JobSpec) (*worker.Job, error) {
			return job, nil
		},
		failFn: func(_ context.Context, _ worker.Job, _ error) error {
			atomic.AddInt32(&failCalls, 1)
			return nil
		},
		completeFn: func(_ context.Context, _ worker.Job, _ int64) error { return nil },
		hbFn:       func(_ context.Context, _ string) error { return nil },
	}
	spec := &worker.JobSpec{JobID: "22222222-2222-2222-2222-222222222222", Intent: extractor.IntentCnesEstabelecimentos}
	src := &sourceStub{nextFn: func(_ context.Context) (*worker.JobSpec, error) {
		return spec, nil
	}}
	exec := &execStub{runFn: func(_ context.Context, _ worker.Job) (int64, error) {
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
