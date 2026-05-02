package worker

import (
	"context"

	"github.com/cnesdata/dumpagent/internal/queue"
)

// OutboxAdapter persists CompleteJob/FailJob calls to a bbolt outbox
// (fire-and-forget for the caller) and delegates RegisterJob/SendHeartbeat
// directly to the inner JobAPIClient. Drain goroutine reads the outbox.
type OutboxAdapter struct {
	inner JobAPIClient
	out   *queue.Outbox
}

// NewOutboxAdapter wraps inner with persistent outbox semantics.
func NewOutboxAdapter(inner JobAPIClient, out *queue.Outbox) *OutboxAdapter {
	return &OutboxAdapter{inner: inner, out: out}
}

// RegisterJob delegates directly: caller needs the returned Job.
func (a *OutboxAdapter) RegisterJob(ctx context.Context, spec JobSpec) (*Job, error) {
	return a.inner.RegisterJob(ctx, spec)
}

// CompleteJob persists envelope; drain dispatches asynchronously.
func (a *OutboxAdapter) CompleteJob(_ context.Context, job Job, sizeBytes int64) error {
	return a.out.Append(queue.Envelope{
		Type:      queue.TypeComplete,
		JobUUID:   job.ID,
		SizeBytes: sizeBytes,
	})
}

// FailJob persists envelope; drain dispatches asynchronously.
func (a *OutboxAdapter) FailJob(_ context.Context, job Job, cause error) error {
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	return a.out.Append(queue.Envelope{
		Type:    queue.TypeFail,
		JobUUID: job.ID,
		Cause:   msg,
	})
}

// SendHeartbeat delegates: high-frequency call, no persistence value.
func (a *OutboxAdapter) SendHeartbeat(ctx context.Context, jobID string) error {
	return a.inner.SendHeartbeat(ctx, jobID)
}
