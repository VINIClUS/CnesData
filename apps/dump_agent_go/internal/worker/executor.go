package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"strings"

	"github.com/google/uuid"

	"github.com/cnesdata/dumpagent/internal/audit"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/integrity"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/writer"
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
	MinioKey  string
	Params    extractor.ExtractionParams
	Sha256    string
	RowCount  int
}

// JobExecutor executa 1 job end-to-end: DB conn → pipeline → upload.
// DeltaStore != nil ativa o caminho delta (P3 R1); nil mantém o caminho
// snapshot legado. AuditLogger != nil emite eventos lifecycle
// (Extracted/Uploaded/Committed/Aborted); nil → no-op.
type JobExecutor struct {
	DB          *sql.DB
	Uploader    upload.Uploader
	DeltaStore  *delta.Store
	AuditLogger *audit.Logger
}

// Run executa job. Retorna tamanho total uploadado em bytes. Se DeltaStore
// configurado, despacha para o caminho delta (RunDelta + Commit/Abort);
// caso contrário usa o pipeline snapshot streaming. job.Sha256 é
// preenchido no caminho delta (após o tee de integrity sobre o upload).
func (e *JobExecutor) Run(ctx context.Context, job *Job) (sizeBytes int64, err error) {
	if e.DeltaStore != nil {
		return e.runDeltaWithCommit(ctx, job)
	}
	return e.runSnapshot(ctx, *job)
}

func (e *JobExecutor) runSnapshot(ctx context.Context, job Job) (sizeBytes int64, err error) {
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

// runDeltaWithCommit invoca RunDelta e gerencia o ciclo Commit/Abort do
// PendingTx. Falha em Commit aborta o pending sub-bucket implicitamente
// (bbolt revert na tx) e devolve erro ao caller (Consumer disparará FailJob).
// O evento audit.LifecycleCommitted é emitido pelo Consumer após
// RegisterJob ack (delivery confirmed) via EmitCommitted.
func (e *JobExecutor) runDeltaWithCommit(ctx context.Context, job *Job) (int64, error) {
	size, pending, _, err := e.RunDelta(ctx, job)
	if err != nil {
		return 0, err
	}
	if cerr := pending.Commit(); cerr != nil {
		return 0, fmt.Errorf("delta_commit: %w", cerr)
	}
	return size, nil
}

// EmitCommitted é chamado pelo Consumer após RegisterJob ack para gravar
// o evento lifecycle final no audit log. No-op seguro quando AuditLogger
// nil ou DeltaStore nil (snapshot legacy não emite Committed).
func (e *JobExecutor) EmitCommitted(job Job, size int64) {
	if e.AuditLogger == nil || e.DeltaStore == nil {
		return
	}
	key := deltaKeyFromParams(job.Params)
	e.appendAudit(audit.Event{
		Source: key.Source, Intent: key.Intent,
		Competencia:  key.Competencia,
		ExtractionID: job.ID, JobID: job.ID,
		SHA256:    job.Sha256,
		SizeBytes: size,
		Lifecycle: audit.LifecycleCommitted,
	})
}

// RunDelta executa job em modo delta: extract → materialize → compute →
// stage pending → write delta parquet → upload (com tee SHA-256). Caller
// DEVE invocar PendingTx.Commit() após RegisterJob ack OU
// PendingTx.Abort() em falha. Em caso de erro interno, RunDelta aborta
// o pending e devolve pending=nil. Emite audit Extracted (start) e
// Uploaded/Aborted (após Put). job.Sha256 preenchido em sucesso.
func (e *JobExecutor) RunDelta(
	ctx context.Context, job *Job,
) (sizeBytes int64, pending *delta.PendingTx, ds delta.Set, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in RunDelta: %v\n%s", r, debug.Stack())
		}
	}()
	pipeline, ok := DeltaPipelineFor(job.Params.Intent)
	if !ok {
		return 0, nil, delta.Set{}, fmt.Errorf("%w: %s",
			ErrUnknownIntent, job.Params.Intent)
	}
	conn, err := e.DB.Conn(ctx)
	if err != nil {
		return 0, nil, delta.Set{}, fmt.Errorf("db_conn: %w", err)
	}
	defer conn.Close()
	key := deltaKeyFromParams(job.Params)
	extractionID := uuid.NewString()
	e.appendAudit(audit.Event{
		Source: key.Source, Intent: key.Intent,
		Competencia: key.Competencia,
		ExtractionID: extractionID, JobID: job.ID,
		Lifecycle: audit.LifecycleExtracted,
	})
	rows, err := pipeline(ctx, conn, job.Params)
	if err != nil {
		return 0, nil, delta.Set{}, fmt.Errorf("extract_delta: %w", err)
	}
	ds, pending, err = ComputeAndStagePending(ctx, DeltaStageRequest{
		Store: e.DeltaStore, Key: key, JobID: job.ID, Current: rows,
	})
	if err != nil {
		return 0, nil, delta.Set{}, err
	}
	sizeBytes, err = e.writeAndUploadDelta(ctx, job, ds, uploadCtx{
		Key: key, ExtractionID: extractionID, Pending: pending,
	})
	if err != nil {
		return 0, nil, delta.Set{}, err
	}
	return sizeBytes, pending, ds, nil
}

// uploadCtx agrega params do upload + audit para writeAndUploadDelta
// dentro do limite de 4 parâmetros.
type uploadCtx struct {
	Key          delta.SourceKey
	ExtractionID string
	Pending      *delta.PendingTx
}

func (e *JobExecutor) writeAndUploadDelta(
	ctx context.Context, job *Job, ds delta.Set, uc uploadCtx,
) (int64, error) {
	cols := delta.ProfileFor(uc.Key.Source, uc.Key.Intent).FingerprintColumns
	var buf bytes.Buffer
	if err := writer.WriteDeltaParquet(&buf, ds, cols); err != nil {
		uc.Pending.Abort()
		e.emitAborted(uc, job.ID)
		return 0, fmt.Errorf("write_delta_parquet: %w", err)
	}
	teed, hasher := integrity.SHA256TeeReader(&buf)
	size, err := e.Uploader.Put(ctx, job.UploadURL, teed,
		"application/octet-stream")
	if err != nil {
		uc.Pending.Abort()
		e.emitAborted(uc, job.ID)
		return 0, fmt.Errorf("upload: %w", err)
	}
	job.Sha256 = hasher.SumHex()
	e.appendAudit(audit.Event{
		Source: uc.Key.Source, Intent: uc.Key.Intent,
		Competencia: uc.Key.Competencia,
		ExtractionID: uc.ExtractionID, JobID: job.ID,
		SHA256: job.Sha256, SizeBytes: size,
		Lifecycle: audit.LifecycleUploaded,
	})
	return size, nil
}

func (e *JobExecutor) emitAborted(uc uploadCtx, jobID string) {
	e.appendAudit(audit.Event{
		Source: uc.Key.Source, Intent: uc.Key.Intent,
		Competencia: uc.Key.Competencia,
		ExtractionID: uc.ExtractionID, JobID: jobID,
		Lifecycle: audit.LifecycleAborted,
	})
}

func (e *JobExecutor) appendAudit(ev audit.Event) {
	if e.AuditLogger == nil {
		return
	}
	if err := e.AuditLogger.Append(ev); err != nil {
		slog.Warn("audit_append_failed", "err", err.Error())
	}
}

// deltaKeyFromParams mapeia ExtractionParams.Intent (ex: "profissionais",
// "sihd_producao") para o par (Source, Intent) esperado pelos profiles
// delta. Competencia passa direto.
func deltaKeyFromParams(p extractor.ExtractionParams) delta.SourceKey {
	src, intent := splitIntent(p.Intent)
	return delta.SourceKey{
		Source:      src,
		Intent:      intent,
		Competencia: p.Competencia,
	}
}

// splitIntent reparte o intent do agent (terminado em snake_case) no par
// (source, intent) usado pelos profiles delta. Mantém uma tabela explícita
// para evitar surpresas — adicionar entrada ao introduzir novo intent.
func splitIntent(intent string) (source, base string) {
	switch intent {
	case extractor.IntentCnesProfissionais:
		return "cnes", "profissionais"
	case extractor.IntentCnesEstabelecimentos:
		return "cnes", "estabelecimentos"
	case extractor.IntentCnesEquipes:
		return "cnes", "equipes"
	case extractor.IntentSihdProducao:
		return "sihd", "aih"
	}
	if i := strings.Index(intent, "_"); i > 0 {
		return intent[:i], intent[i+1:]
	}
	return "", intent
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
// stages new fingerprints in a pending bbolt tx, and returns the Set
// + PendingTx. Caller must call PendingTx.Commit() after RegisterJob ack
// or PendingTx.Abort() on failure.
//
// ctx is unused today (delta package does not propagate cancellation
// through bbolt operations) but accepted to align with executor patterns
// and to enable future ctx-aware variants.
func ComputeAndStagePending(
	ctx context.Context, req DeltaStageRequest,
) (delta.Set, *delta.PendingTx, error) {
	prof := delta.ProfileFor(req.Key.Source, req.Key.Intent)
	if prof.Source == "" {
		return delta.Set{}, nil,
			fmt.Errorf("unknown_profile source=%s intent=%s",
				req.Key.Source, req.Key.Intent)
	}
	committed, err := req.Store.GetCommitted(req.Key)
	if err != nil {
		return delta.Set{}, nil, fmt.Errorf("get_committed: %w", err)
	}
	currentHashes := make(map[string][32]byte, len(req.Current))
	for _, r := range req.Current {
		pk := prof.PKExtractor(r)
		currentHashes[pk] = delta.Hash(r, prof.FingerprintColumns)
	}
	ds := delta.Compute(committed, currentHashes, req.Current, prof)
	pending, err := req.Store.BeginPending(req.Key, req.JobID)
	if err != nil {
		return delta.Set{}, nil, err
	}
	for pk, h := range currentHashes {
		if err := pending.Put(pk, h); err != nil {
			pending.Abort()
			return delta.Set{}, nil, err
		}
	}
	_ = ctx
	return ds, pending, nil
}
