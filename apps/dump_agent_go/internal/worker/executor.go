package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"runtime/debug"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/upload"
	"golang.org/x/sync/errgroup"
)

// ErrUnknownIntent indica intent sem pipeline registrada.
var ErrUnknownIntent = errors.New("unknown_intent")

// JobSpec descreve o que o agent quer registrar em /jobs/register.
// IDs são locais ao agent; extraction_id é retornado pelo central.
type JobSpec struct {
	JobID         string
	FonteSistema  string
	TipoExtracao  string
	Competencia   int
	Intent        string
}

// Job payload executado pelo executor.
type Job struct {
	ID        string
	TenantID  string
	UploadURL string
	Params    extractor.ExtractionParams
	Sha256    string
	RowCount  int
}

// JobExecutor executa 1 job end-to-end: DB conn → pipeline → upload.
type JobExecutor struct {
	DB       *sql.DB
	Uploader upload.Uploader
}

// Run executa job. Retorna tamanho total uploadado em bytes.
func (e *JobExecutor) Run(ctx context.Context, job Job) (sizeBytes int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in Run: %v\n%s", r, debug.Stack())
		}
	}()

	pipeline, ok := PipelineFor(job.Params.Intent)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownIntent, job.Params.Intent)
	}

	conn, err := e.DB.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("db_conn: %w", err)
	}
	defer conn.Close()

	pr, pw := io.Pipe()

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		defer pw.Close()
		return pipeline(egCtx, conn, job.Params, pw)
	})

	eg.Go(func() error {
		n, err := e.Uploader.Put(egCtx, job.UploadURL, pr, "application/octet-stream")
		sizeBytes = n
		return err
	})

	return sizeBytes, eg.Wait()
}

// DeltaStageRequest groups inputs for ComputeAndStagePending to keep
// param count <= 4 per CLAUDE.md hard limits.
type DeltaStageRequest struct {
	Store   *delta.Store
	Key     delta.SourceKey
	JobID   string
	Current []delta.Row
}

// ComputeAndStagePending loads the committed snapshot, computes the delta,
// stages new fingerprints in a pending bbolt tx, and returns the DeltaSet
// + PendingTx. Caller must call PendingTx.Commit() after CompleteJob ack
// or PendingTx.Abort() on failure.
//
// ctx is unused today (delta package does not propagate cancellation
// through bbolt operations) but accepted to align with executor patterns
// and to enable future ctx-aware variants.
func ComputeAndStagePending(
	ctx context.Context, req DeltaStageRequest,
) (delta.DeltaSet, *delta.PendingTx, error) {
	prof := delta.ProfileFor(req.Key.Source, req.Key.Intent)
	if prof.Source == "" {
		return delta.DeltaSet{}, nil,
			fmt.Errorf("unknown_profile source=%s intent=%s",
				req.Key.Source, req.Key.Intent)
	}
	committed, err := req.Store.GetCommitted(req.Key)
	if err != nil {
		return delta.DeltaSet{}, nil, fmt.Errorf("get_committed: %w", err)
	}
	currentHashes := make(map[string][32]byte, len(req.Current))
	for _, r := range req.Current {
		pk := prof.PKExtractor(r)
		currentHashes[pk] = delta.Hash(r, prof.FingerprintColumns)
	}
	ds := delta.Compute(committed, currentHashes, req.Current, prof)
	pending, err := req.Store.BeginPending(req.Key, req.JobID)
	if err != nil {
		return delta.DeltaSet{}, nil, err
	}
	for pk, h := range currentHashes {
		if err := pending.Put(pk, h); err != nil {
			pending.Abort()
			return delta.DeltaSet{}, nil, err
		}
	}
	_ = ctx
	return ds, pending, nil
}
