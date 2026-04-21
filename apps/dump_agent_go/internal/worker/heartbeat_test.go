package worker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestHeartbeatLoop_StopsOnContextCancel(t *testing.T) {
	var calls int32
	client := &hbStub{fn: func(ctx context.Context, id string) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = worker.HeartbeatLoop(ctx, client, "job-1", 10*time.Millisecond)
		close(done)
	}()

	time.Sleep(35 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("heartbeat loop did not exit after cancel")
	}
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(2))
}

func TestHeartbeatLoop_TolerantesFalhas(t *testing.T) {
	var calls int32
	client := &hbStub{fn: func(_ context.Context, _ string) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("net down")
	}}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Millisecond)
	defer cancel()

	err := worker.HeartbeatLoop(ctx, client, "job-1", 10*time.Millisecond)
	require.NoError(t, err, "loop should not propagate per-tick errors")
	require.GreaterOrEqual(t, atomic.LoadInt32(&calls), int32(3))
}

type hbStub struct {
	fn func(ctx context.Context, id string) error
}

func (s *hbStub) SendHeartbeat(ctx context.Context, jobID string) error {
	return s.fn(ctx, jobID)
}
