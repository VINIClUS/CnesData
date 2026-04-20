package worker_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

func TestJobExecutor_Run_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("0001", "UBS", "05", "354130", "12345"))

	var uploaded int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		uploaded = n
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	exe := &worker.JobExecutor{
		DB:       db,
		Uploader: upload.NewHTTP(http.DefaultClient),
	}

	job := worker.Job{
		ID:        "job-1",
		TenantID:  "354130",
		UploadURL: srv.URL,
		Params:    extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos, CodMunGest: "354130"},
	}
	size, err := exe.Run(context.Background(), job)
	require.NoError(t, err)
	require.Greater(t, size, int64(0))
	require.Greater(t, uploaded, int64(0))
}

func TestJobExecutor_Run_UnknownIntent(t *testing.T) {
	db, _, _ := sqlmock.New()
	defer db.Close()

	exe := &worker.JobExecutor{DB: db, Uploader: upload.NewHTTP(http.DefaultClient)}
	_, err := exe.Run(context.Background(), worker.Job{
		Params: extractor.ExtractionParams{Intent: "unknown"},
	})
	require.ErrorIs(t, err, worker.ErrUnknownIntent)
}

func TestJobExecutor_Run_UploadFailure(t *testing.T) {
	db, mock, _ := sqlmock.New()
	defer db.Close()
	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("0001", "UBS", "05", "354130", "12345"))

	exe := &worker.JobExecutor{DB: db, Uploader: &stubUploader{err: errors.New("network down")}}
	_, err := exe.Run(context.Background(), worker.Job{
		Params:    extractor.ExtractionParams{Intent: extractor.IntentCnesEstabelecimentos, CodMunGest: "354130"},
		UploadURL: "ignored",
	})
	require.Error(t, err)
}

type stubUploader struct{ err error }

func (s *stubUploader) Put(_ context.Context, _ string, body io.Reader, _ string) (int64, error) {
	_, _ = io.Copy(io.Discard, body)
	return 0, s.err
}
