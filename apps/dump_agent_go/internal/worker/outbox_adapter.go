package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/writer"
)

// EnvelopeOutbox delimita a persistência de envelopes e suas falhas de disco.
type EnvelopeOutbox interface {
	Peek(int) ([]queue.Item, error)
	Append(queue.Envelope) error
	Delete(...[]byte) error
	Evict(time.Duration, int) (int, error)
	RawTerminal([]byte) (bool, error)
	MarkRawTerminal([]byte) error
}

type rawCycle struct {
	ref       delta.PendingRef
	request   manifest.BuildRequest
	rows      []delta.Row
	set       delta.Set
	hashes    map[string][32]byte
	indexed   map[string]delta.Row
	spoolName string
	uploadURL string
}

const rawTerminalPollInterval = 10 * time.Millisecond

// WaitRawTerminal mantém a lease até o ack remoto estar durável ou o envelope sumir.
func (e *JobExecutor) WaitRawTerminal(ctx context.Context, job *Job) error {
	if job == nil || e.RawOutbox == nil {
		return errors.New("raw_executor=unconfigured")
	}
	ref := delta.PendingRef{JobID: job.ID, FencingToken: job.FencingToken}
	ticker := time.NewTicker(rawTerminalPollInterval)
	defer ticker.Stop()
	for {
		item, exists, err := findRawItem(e.RawOutbox, ref)
		if err != nil || !exists {
			return err
		}
		terminal, err := e.RawOutbox.RawTerminal(item.Key)
		if err != nil {
			return err
		}
		if terminal {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (e *JobExecutor) prepareRaw(ctx context.Context, job *Job) (rawCycle, error) {
	cycle, err := e.rawRequest(job)
	if err != nil {
		return cycle, err
	}
	if err := ctx.Err(); err != nil {
		return cycle, err
	}
	cycle.rows, err = e.RawExtract(ctx, *job)
	if err != nil {
		return cycle, err
	}
	committed, err := e.DeltaStore.GetCommitted(cycle.ref.SourceKey)
	if err != nil {
		return cycle, err
	}
	profile := rawProfile()
	prior := committed
	if cycle.request.SnapshotMode == manifest.SnapshotModeFull {
		prior = nil
	}
	cycle.indexed, err = indexRawRows(cycle.rows, prior)
	if err != nil {
		return cycle, err
	}
	cycle.hashes = make(map[string][32]byte, len(cycle.rows))
	for key, row := range cycle.indexed {
		cycle.hashes[key] = delta.Hash(row, profile.FingerprintColumns)
	}
	cycle.request.RowCount = int64(len(cycle.rows))
	if cycle.request.SnapshotMode == manifest.SnapshotModeDelta {
		if err := cycle.computeChanges(committed); err != nil {
			return cycle, err
		}
		cycle.request.RowCount = int64(cycle.set.TotalCount())
	}
	return cycle, nil
}

func (e *JobExecutor) rawRequest(job *Job) (rawCycle, error) {
	cycle := rawCycle{}
	if err := e.validateRawRequest(job); err != nil {
		return cycle, err
	}
	cycle.ref = delta.PendingRef{SourceKey: deltaKeyFromParams(job.Params),
		JobID: job.ID, FencingToken: job.FencingToken}
	cycle.request = *job.RawRequest
	cycle.request.JobID, cycle.request.TenantID = job.ID, job.TenantID
	key := cycle.ref.SourceKey
	_, forced, err := e.DeltaStore.ForceFull(key)
	if err != nil {
		return cycle, err
	}
	if forced && cycle.request.SnapshotMode == manifest.SnapshotModeDelta {
		return cycle, errors.New("force_full=required")
	}
	snapshot, seq, hash, _, ok, err := e.DeltaStore.ChainHead(key)
	if err != nil {
		return cycle, err
	}
	cycle.request.Previous = nil
	if ok {
		cycle.request.Previous = &manifest.PreviousHead{
			SnapshotID: snapshot, Sequence: seq, ManifestSHA256: hash}
	}
	cycle.request.SizeBytes, cycle.request.ObjectSHA256 = 1, strings.Repeat("0", 64)
	_, err = manifest.Build(cycle.request)
	return cycle, err
}

func (e *JobExecutor) validateRawRequest(job *Job) error {
	if job == nil || job.RawRequest == nil || e.DeltaStore == nil || e.RawOutbox == nil ||
		e.RawUploader == nil || e.RawExtract == nil || job.FencingToken == 0 {
		return errors.New("raw_executor=unconfigured")
	}
	return validateRawIdentity(job)
}

func validateRawIdentity(job *Job) error {
	if job.RawRequest.CreatedAt.IsZero() {
		return errors.New("raw_request=identity_invalid")
	}
	return validateRawScope(deltaKeyFromParams(job.Params), manifest.Raw{
		SourceType: job.RawRequest.SourceType, FileSubtype: job.RawRequest.FileSubtype,
		Competencia: job.RawRequest.Competencia})
}

func validateRawScope(key delta.SourceKey, raw manifest.Raw) error {
	if key.Source != "cnes" || raw.SourceType != manifest.SourceTypeCNESLocal ||
		key.Intent != "profissionais" || raw.FileSubtype != "CNES_VINCULO" {
		return errors.New("raw_source=identity_invalid")
	}
	layout := "200601"
	if len(key.Competencia) == 7 {
		layout = "2006-01"
	}
	month, err := time.Parse(layout, key.Competencia)
	if err != nil || month.Format("2006-01") != raw.Competencia {
		return errors.New("raw_competencia=identity_invalid")
	}
	return nil
}

func (e *JobExecutor) replayRaw(job *Job) (int64, bool, error) {
	if !e.rawReplayConfigured(job) {
		return 0, false, errors.New("raw_executor=unconfigured")
	}
	if err := validateRawIdentity(job); err != nil {
		return 0, false, err
	}
	ref := delta.PendingRef{JobID: job.ID, FencingToken: job.FencingToken}
	env, exists, err := findRawEnvelope(e.RawOutbox, ref)
	if err != nil || !exists {
		return 0, exists, err
	}
	raw, err := decodeRawEnvelope(env)
	if err != nil {
		return 0, true, err
	}
	if err := validateRawScope(deltaKeyFromParams(job.Params), raw); err != nil {
		return 0, true, err
	}
	if raw.TenantID != job.TenantID {
		return 0, true, errors.New("raw_tenant=identity_invalid")
	}
	job.Sha256, job.MinioKey, job.RowCount = raw.ObjectSHA256, raw.ObjectKey, int(raw.RowCount)
	return raw.SizeBytes, true, nil
}

func (e *JobExecutor) rawReplayConfigured(job *Job) bool {
	return job != nil && job.RawRequest != nil && e.RawOutbox != nil && job.FencingToken > 0
}

func rawProfile() delta.Profile {
	return delta.Profile{Source: "cnes", Intent: "profissionais",
		PKExtractor: func(row delta.Row) string {
			key, _ := json.Marshal([5]any{row["CPF"], row["CNES"], row["CBO"],
				row["TIPO_VINCULO"], row["SUS"]})
			return string(key)
		}, FingerprintColumns: []string{
			"CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "CBO", "CNES",
			"TIPO_VINCULO", "SUS", "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS",
			"CH_HOSPITALAR", "FONTE",
		}}
}

func (cycle *rawCycle) computeChanges(committed map[string][32]byte) error {
	for key, row := range cycle.indexed {
		prior, exists := committed[key]
		if !exists {
			cycle.set.Inserts = append(cycle.set.Inserts, row)
		} else if prior != cycle.hashes[key] {
			cycle.set.Updates = append(cycle.set.Updates, row)
		}
	}
	for key := range committed {
		if _, exists := cycle.hashes[key]; exists {
			continue
		}
		var identity rawRowKey
		if err := json.Unmarshal([]byte(key), &identity); err != nil {
			return errors.New("raw_committed_key=invalid")
		}
		var columns [5]any
		if err := json.Unmarshal([]byte(identity.Identity), &columns); err != nil {
			return errors.New("raw_committed_key=invalid")
		}
		cycle.set.Deletes = append(cycle.set.Deletes,
			delta.Row{"CPF": columns[0], "CNES": columns[1], "CBO": columns[2],
				"TIPO_VINCULO": columns[3], "SUS": columns[4]})
	}
	return nil
}

type rawRowKey struct {
	Identity   string
	Occurrence int
}

func indexRawRows(rows []delta.Row, committed map[string][32]byte) (map[string]delta.Row, error) {
	profile := rawProfile()
	prior := map[string][]string{}
	for key := range committed {
		var identity rawRowKey
		if err := json.Unmarshal([]byte(key), &identity); err != nil {
			return nil, errors.New("raw_committed_key=invalid")
		}
		prior[identity.Identity] = append(prior[identity.Identity], key)
	}
	groups := map[string][]delta.Row{}
	for _, row := range rows {
		key := profile.PKExtractor(row)
		groups[key] = append(groups[key], row)
	}
	indexed := map[string]delta.Row{}
	for identity, group := range groups {
		matchRawGroup(group, prior[identity], committed, indexed)
	}
	return indexed, nil
}

func matchRawGroup(rows []delta.Row, prior []string,
	committed map[string][32]byte, indexed map[string]delta.Row,
) {
	slices.Sort(prior)
	byHash := map[[32]byte][]string{}
	for _, key := range prior {
		byHash[committed[key]] = append(byHash[committed[key]], key)
	}
	profile := rawProfile()
	var unmatched []delta.Row
	for _, row := range rows {
		hash := delta.Hash(row, profile.FingerprintColumns)
		keys := byHash[hash]
		if len(keys) == 0 {
			unmatched = append(unmatched, row)
			continue
		}
		indexed[keys[0]], byHash[hash] = row, keys[1:]
	}
	var free []string
	for _, key := range prior {
		if _, matched := indexed[key]; !matched {
			free = append(free, key)
		}
	}
	assignRawOccurrences(unmatched, free, indexed)
}

func assignRawOccurrences(rows []delta.Row, free []string, indexed map[string]delta.Row) {
	profile := rawProfile()
	slices.SortFunc(rows, func(left, right delta.Row) int {
		l := delta.Hash(left, profile.FingerprintColumns)
		r := delta.Hash(right, profile.FingerprintColumns)
		return bytes.Compare(l[:], r[:])
	})
	occurrence := 0
	for i, row := range rows {
		if i < len(free) {
			indexed[free[i]] = row
			continue
		}
		for {
			key, _ := json.Marshal(rawRowKey{profile.PKExtractor(row), occurrence})
			occurrence++
			if _, exists := indexed[string(key)]; !exists {
				indexed[string(key)] = row
				break
			}
		}
	}
}

func (cycle rawCycle) write(dst io.Writer) error {
	if cycle.request.SnapshotMode == manifest.SnapshotModeFull {
		return writer.WriteRawFullParquet(dst, cycle.rows)
	}
	return writer.WriteRawDeltaParquet(dst, cycle.set)
}

func (e *JobExecutor) enqueueRaw(cycle rawCycle, raw manifest.Raw) error {
	payload, err := manifest.CanonicalJSON(raw)
	if err != nil {
		return err
	}
	hash, err := manifest.SHA256(raw)
	if err != nil {
		return err
	}
	env := queue.Envelope{Type: queue.TypeRawManifest, JobID: cycle.ref.JobID,
		FencingToken: cycle.ref.FencingToken, SourceKey: cycle.ref.SourceKey,
		ManifestJSON: payload, ManifestSHA256: hash,
		SpoolName: cycle.spoolName, UploadURL: cycle.uploadURL}
	if _, err := decodeRawEnvelope(env); err != nil {
		return err
	}
	_, exists, err := findRawEnvelope(e.RawOutbox, cycle.ref)
	if err != nil {
		return err
	}
	if exists {
		return e.RawOutbox.Append(env)
	}
	pending, err := e.DeltaStore.BeginPendingRef(cycle.ref)
	if errors.Is(err, delta.ErrPendingExists) {
		pending, err = e.DeltaStore.ResumePendingRef(cycle.ref)
	}
	if err != nil {
		return err
	}
	if err := pending.Replace(cycle.hashes); err != nil {
		return err
	}
	return e.RawOutbox.Append(env)
}

func findRawEnvelope(out EnvelopeOutbox, ref delta.PendingRef) (queue.Envelope, bool, error) {
	item, exists, err := findRawItem(out, ref)
	return item.Envelope, exists, err
}

func findRawItem(out EnvelopeOutbox, ref delta.PendingRef) (queue.Item, bool, error) {
	for limit := drainBatchSize; ; limit *= 2 {
		items, err := out.Peek(limit)
		if err != nil {
			return queue.Item{}, false, err
		}
		for _, item := range items {
			if item.Envelope.Type == queue.TypeRawManifest &&
				item.Envelope.JobID == ref.JobID && item.Envelope.FencingToken == ref.FencingToken {
				return item, true, nil
			}
		}
		if len(items) < limit {
			return queue.Item{}, false, nil
		}
	}
}

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
