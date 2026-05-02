package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cnesdata/dumpagent/internal/breaker"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
)

const (
	drainTickInterval  = 30 * time.Second
	drainBatchSize     = 20
	drainEvictAge      = 90 * 24 * time.Hour
	drainEvictMaxCount = 10000
	dispatchTimeout    = 30 * time.Second
)

// Drainer ships persisted envelopes to the central_api in FIFO order,
// gated by a circuit breaker. End-of-tick eviction enforces TTL + cap.
type Drainer struct {
	out      *queue.Outbox
	breaker  *breaker.CircuitBreaker
	inner    JobAPIClient
	interval time.Duration
}

// NewDrainer constructs a Drainer with default cadence (30s).
func NewDrainer(out *queue.Outbox, br *breaker.CircuitBreaker, inner JobAPIClient) *Drainer {
	return &Drainer{
		out:      out,
		breaker:  br,
		inner:    inner,
		interval: drainTickInterval,
	}
}

// Run loops until ctx is cancelled.
func (d *Drainer) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			d.tick(ctx)
		}
	}
}

// tick peeks a batch + dispatches each + evicts.
func (d *Drainer) tick(ctx context.Context) {
	items, err := d.out.Peek(drainBatchSize)
	if err != nil {
		slog.Warn("outbox_peek_failed", "err", err.Error())
		return
	}
	for _, item := range items {
		if ctx.Err() != nil {
			return
		}
		if !d.dispatchOne(ctx, item) {
			break // breaker open or rate-limited; abort tick
		}
	}
	deleted, err := d.out.Evict(drainEvictAge, drainEvictMaxCount)
	if err != nil {
		slog.Warn("outbox_evict_failed", "err", err.Error())
		return
	}
	if deleted > 0 {
		slog.Warn("envelope_evicted",
			"event_id", obs.EventQueueEvicted,
			"count", deleted)
	}
}

// dispatchOne returns true when the loop may continue.
func (d *Drainer) dispatchOne(ctx context.Context, item queue.Item) bool {
	callCtx, cancel := context.WithTimeout(ctx, dispatchTimeout)
	defer cancel()

	var resp *http.Response
	var dispErr error

	callErr := d.breaker.Call(callCtx, func(c context.Context) error {
		resp, dispErr = d.callInner(c, item.Envelope)
		cls, _ := queue.Classify(resp, dispErr)
		if cls == queue.ClassTransient {
			if dispErr != nil {
				return dispErr
			}
			return fmt.Errorf("transient http %d", resp.StatusCode)
		}
		return nil // breaker doesn't count Success/Terminal/RateLimit as fault
	})

	if errors.Is(callErr, breaker.ErrOpen) {
		return false
	}

	cls, sleep := queue.Classify(resp, dispErr)
	switch cls {
	case queue.ClassSuccess:
		_ = d.out.Delete(item.Key)
		slog.Info("envelope_drained",
			"event_id", obs.EventQueueDrained,
			"type", string(item.Envelope.Type),
			"job_uuid", item.Envelope.JobUUID)
		return true
	case queue.ClassTerminalDrop:
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		slog.Warn("envelope_terminal_drop",
			"event_id", obs.EventQueueTerminalDrop,
			"job_uuid", item.Envelope.JobUUID,
			"type", string(item.Envelope.Type),
			"http_status", statusCode)
		_ = d.out.Delete(item.Key)
		return true
	case queue.ClassRateLimit:
		slog.Warn("drain_rate_limited",
			"event_id", obs.EventQueueRateLimited,
			"retry_after", sleep.String())
		select {
		case <-ctx.Done():
		case <-time.After(sleep):
		}
		return false
	case queue.ClassTransient:
		item.Envelope.Attempts++
		if dispErr != nil {
			item.Envelope.LastError = dispErr.Error()
		}
		_ = d.out.Delete(item.Key)
		_ = d.out.Append(item.Envelope)
		return false
	}
	return false
}

// callInner translates Envelope into the right JobAPIClient method.
// Returns a synthesized *http.Response when the inner call returns
// *obs.HTTPError so Classify can read the status code.
func (d *Drainer) callInner(ctx context.Context, env queue.Envelope) (*http.Response, error) {
	job := Job{ID: env.JobUUID}
	var apiErr error
	switch env.Type {
	case queue.TypeComplete:
		apiErr = d.inner.CompleteJob(ctx, job, env.SizeBytes)
	case queue.TypeFail:
		apiErr = d.inner.FailJob(ctx, job, errors.New(env.Cause))
	default:
		return nil, fmt.Errorf("unknown envelope type: %s", env.Type)
	}
	if apiErr == nil {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}, nil
	}
	var hErr *obs.HTTPError
	if errors.As(apiErr, &hErr) {
		return &http.Response{StatusCode: hErr.StatusCode, Header: make(http.Header)}, nil
	}
	return nil, apiErr
}
