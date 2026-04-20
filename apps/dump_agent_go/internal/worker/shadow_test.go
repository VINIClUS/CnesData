package worker_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestShadowExecutor_WritesLocalFile(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("0001", "UBS", "05", "354130", "12345"))

	tmp := t.TempDir()
	exe := &worker.ShadowExecutor{DB: db, OutputDir: tmp}
	job := worker.Job{
		ID:     "job-shadow-1",
		Params: extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos, CodMunGest: "354130"},
	}
	size, err := exe.Run(context.Background(), job)
	require.NoError(t, err)
	require.Greater(t, size, int64(0))

	expectedPath := filepath.Join(tmp, "job-shadow-1.parquet.gz")
	info, err := os.Stat(expectedPath)
	require.NoError(t, err)
	require.Equal(t, size, info.Size())
}

func TestShadowExecutor_UnknownIntent(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	exe := &worker.ShadowExecutor{DB: db, OutputDir: t.TempDir()}
	_, err := exe.Run(context.Background(), worker.Job{
		Params: extractor.ExtractionParams{Intent: "unknown"},
	})
	require.ErrorIs(t, err, worker.ErrUnknownIntent)
}
