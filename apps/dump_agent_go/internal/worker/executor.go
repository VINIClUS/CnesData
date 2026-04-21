package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"runtime/debug"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/upload"
	"golang.org/x/sync/errgroup"
)

// ErrUnknownIntent indica intent sem pipeline registrada.
var ErrUnknownIntent = errors.New("unknown_intent")

// Job payload executado pelo executor.
type Job struct {
	ID        string
	TenantID  string
	UploadURL string
	Params    extractor.ExtractionParams
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
