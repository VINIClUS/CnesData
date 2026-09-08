package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/breaker"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/upload"
)

// RawManifestResponse contém o acknowledgement tipado do servidor.
type RawManifestResponse struct {
	StatusCode     int
	ManifestSHA256 string
	ForceFull      bool
	Reason         string
}

// RawManifestClient envia a identidade imutável do manifesto persistido.
type RawManifestClient interface {
	SendRawManifest(context.Context, queue.Envelope) (RawManifestResponse, error)
}

// RawDrainer confirma estado durável antes de remover manifests da fila.
type RawDrainer struct {
	client         RawManifestClient
	store          *delta.Store
	legacy         JobAPIClient
	SpoolDirectory string
	Uploader       upload.RawUploader
}

// NewRawDrainer constrói o drainer sem depender da composição do serviço.
func NewRawDrainer(client RawManifestClient, store *delta.Store, legacy JobAPIClient) *RawDrainer {
	return &RawDrainer{client: client, store: store, legacy: legacy}
}

// Drain envia um lote de envelopes e conserva qualquer falha para replay.
func (d *RawDrainer) Drain(ctx context.Context, out EnvelopeOutbox) error {
	items, err := out.Peek(drainBatchSize)
	if err != nil {
		return err
	}
	legacy := &Drainer{out: out, breaker: breaker.New(5, time.Minute, "raw_legacy"), inner: d.legacy}
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if item.Envelope.Type != queue.TypeRawManifest {
			if d.legacy == nil || !legacy.dispatchOne(ctx, item) {
				return errors.New("legacy_dispatch=pending")
			}
			continue
		}
		if err := d.deliverRaw(ctx, out, item); err != nil {
			return err
		}
	}
	return nil
}

func (d *RawDrainer) deliverRaw(ctx context.Context, out EnvelopeOutbox, item queue.Item) error {
	raw, err := decodeRawEnvelope(item.Envelope)
	if err != nil {
		return err
	}
	terminal, err := out.RawTerminal(item.Key)
	if err != nil {
		return err
	}
	if !terminal {
		if err := d.uploadAndConfirm(ctx, item.Envelope, raw); err != nil {
			return err
		}
		if err := out.MarkRawTerminal(item.Key); err != nil {
			return err
		}
	}
	if err := upload.RemoveRawSpool(d.SpoolDirectory, item.Envelope.SpoolName); err != nil {
		return err
	}
	return out.Delete(item.Key)
}

func (d *RawDrainer) uploadAndConfirm(ctx context.Context,
	env queue.Envelope, raw manifest.Raw,
) error {
	_, err := upload.PutRawSpool(ctx, d.Uploader, upload.RawSpoolUpload{
		Directory: d.SpoolDirectory, Name: env.SpoolName, URL: env.UploadURL,
		FencingToken: env.FencingToken, Manifest: raw})
	if err != nil {
		return err
	}
	return d.confirm(ctx, env)
}

func (d *RawDrainer) confirm(ctx context.Context, env queue.Envelope) error {
	raw, err := decodeRawEnvelope(env)
	if err != nil {
		return err
	}
	if d.client == nil || d.store == nil {
		return errors.New("raw_drainer=unconfigured")
	}
	callCtx, cancel := context.WithTimeout(ctx, dispatchTimeout)
	defer cancel()
	response, err := d.client.SendRawManifest(callCtx, env)
	if err != nil {
		return err
	}
	ref := pendingRef(env)
	if successfulRawResponse(response) {
		return d.store.ConfirmPending(ref, raw, response.ManifestSHA256)
	}
	if resyncRawResponse(response, env) {
		return d.store.RequireFull(ref, response.Reason)
	}
	return fmt.Errorf("raw_ack=invalid_or_retryable status=%d", response.StatusCode)
}

func successfulRawResponse(response RawManifestResponse) bool {
	return response.StatusCode >= 200 && response.StatusCode < 300 &&
		!response.ForceFull && response.Reason == ""
}

func resyncRawResponse(response RawManifestResponse, env queue.Envelope) bool {
	return response.StatusCode == http.StatusConflict && response.ForceFull &&
		strings.TrimSpace(response.Reason) != "" && response.ManifestSHA256 == env.ManifestSHA256
}

func decodeRawEnvelope(env queue.Envelope) (manifest.Raw, error) {
	var raw manifest.Raw
	if err := json.Unmarshal(env.ManifestJSON, &raw); err != nil {
		return raw, errors.New("raw_manifest=json_invalid")
	}
	canonical, err := manifest.CanonicalJSON(raw)
	if err != nil {
		return raw, err
	}
	hash, err := manifest.SHA256(raw)
	if err != nil {
		return raw, err
	}
	if raw.ManifestID != env.JobID || raw.SnapshotID != env.JobID ||
		env.FencingToken == 0 || hash != env.ManifestSHA256 ||
		!bytes.Equal(canonical, env.ManifestJSON) {
		return raw, errors.New("raw_manifest=identity_invalid")
	}
	return raw, validateRawScope(env.SourceKey, raw)
}

func pendingRef(env queue.Envelope) delta.PendingRef {
	return delta.PendingRef{SourceKey: env.SourceKey, JobID: env.JobID, FencingToken: env.FencingToken}
}

const (
	drainTickInterval   = 30 * time.Second
	drainJitterFraction = 0.20
	drainBatchSize      = 20
	drainEvictAge       = 90 * 24 * time.Hour
	drainEvictMaxCount  = 10000
	dispatchTimeout     = 30 * time.Second
)

// Drainer ships persisted envelopes to the central_api in FIFO order,
// gated by a circuit breaker. End-of-tick eviction enforces TTL + cap.
type Drainer struct {
	out      EnvelopeOutbox
	breaker  *breaker.CircuitBreaker
	inner    JobAPIClient
	interval time.Duration
	rand     func() float64
}

// NewDrainer constructs a Drainer with default cadence (30s).
func NewDrainer(out *queue.Outbox, br *breaker.CircuitBreaker, inner JobAPIClient) *Drainer {
	return &Drainer{
		out:      out,
		breaker:  br,
		inner:    inner,
		interval: drainTickInterval,
		rand:     nil, // nil → JitterAround uses math/rand/v2.Float64
	}
}

// SetRand replaces the rand source. Used by tests for deterministic jitter.
func (d *Drainer) SetRand(r func() float64) {
	d.rand = r
}

// NextInterval returns the next tick wait, jittered by ±drainJitterFraction.
func (d *Drainer) NextInterval() time.Duration {
	return obs.JitterAround(d.interval, drainJitterFraction, d.rand)
}

// Run loops until ctx is cancelled.
func (d *Drainer) Run(ctx context.Context) error {
	timer := time.NewTimer(d.NextInterval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			d.tick(ctx)
			timer.Reset(d.NextInterval())
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
	if item.Envelope.Type == queue.TypeRawManifest {
		return false
	}
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
	return d.applyResponse(ctx, item, resp, dispErr)
}

func (d *Drainer) applyResponse(ctx context.Context, item queue.Item,
	resp *http.Response, dispErr error,
) bool {
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
//
// FU1: TypeComplete envelopes dispatch via RegisterJob (post-upload
// confirmation) and rebuild Job{ID, Sha256, MinioKey} from the persisted
// envelope so replays after agent restart preserve sha256/minio_key.
func (d *Drainer) callInner(ctx context.Context, env queue.Envelope) (*http.Response, error) {
	var apiErr error
	switch env.Type {
	case queue.TypeComplete:
		job := Job{
			ID:       env.JobUUID,
			Sha256:   env.SHA256,
			MinioKey: env.MinioKey,
		}
		apiErr = d.inner.RegisterJob(ctx, job, env.SizeBytes)
	case queue.TypeFail:
		job := Job{ID: env.JobUUID}
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
