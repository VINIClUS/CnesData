package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
)

// ShadowExecutor substitui JobExecutor quando DUMP_SHADOW_MODE=true.
// Escreve Parquet+gzip em arquivo local (OutputDir/<job.ID>.parquet.gz)
// em vez de fazer upload para MinIO.
type ShadowExecutor struct {
	DB        *sql.DB
	OutputDir string
}

// Run executa pipeline mas redireciona para arquivo. Retorna tamanho
// do arquivo gerado.
func (s *ShadowExecutor) Run(ctx context.Context, job Job) (sizeBytes int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in ShadowRun: %v\n%s", r, debug.Stack())
		}
	}()

	pipeline, ok := PipelineFor(job.Params.Intent)
	if !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownIntent, job.Params.Intent)
	}

	if err := os.MkdirAll(s.OutputDir, 0o755); err != nil {
		return 0, fmt.Errorf("mkdir_shadow: %w", err)
	}
	path := filepath.Join(s.OutputDir, job.ID+".parquet.gz")
	f, err := os.Create(path)
	if err != nil {
		return 0, fmt.Errorf("create_shadow_file: %w", err)
	}
	defer f.Close()

	conn, err := s.DB.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("db_conn: %w", err)
	}
	defer conn.Close()

	if err := pipeline(ctx, conn, job.Params, f); err != nil {
		return 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

// ErrShadowDirMissing sinaliza config inválida.
var ErrShadowDirMissing = errors.New("shadow_output_dir_missing")
