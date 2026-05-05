package worker

import (
	"context"

	"github.com/cnesdata/dumpagent/internal/queue"
)

// OutboxAdapter persists RegisterJob/FailJob (terminal outcomes) to a
// bbolt outbox (fire-and-forget for the caller) and delegates
// MintUploadURL/SendHeartbeat directly to the inner JobAPIClient. Drain
// goroutine reads the outbox.
type OutboxAdapter struct {
	inner JobAPIClient
	out   *queue.Outbox
}

// NewOutboxAdapter wraps inner with persistent outbox semantics.
func NewOutboxAdapter(inner JobAPIClient, out *queue.Outbox) *OutboxAdapter {
	return &OutboxAdapter{inner: inner, out: out}
}

// MintUploadURL delegates directly: caller needs the returned Job
// (presigned upload URL is single-use + time-limited).
func (a *OutboxAdapter) MintUploadURL(ctx context.Context, spec JobSpec) (*Job, error) {
	return a.inner.MintUploadURL(ctx, spec)
}

// RegisterJob persists envelope; drain dispatches asynchronously.
// FU1: post-upload register threads sha256/minio_key so drain replay
// after agent restart can reconstruct the full Job payload.
func (a *OutboxAdapter) RegisterJob(_ context.Context, job Job, sizeBytes int64) error {
	return a.out.Append(queue.Envelope{
		Type:      queue.TypeComplete,
		JobUUID:   job.ID,
		SizeBytes: sizeBytes,
		SHA256:    job.Sha256,
		MinioKey:  job.MinioKey,
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
