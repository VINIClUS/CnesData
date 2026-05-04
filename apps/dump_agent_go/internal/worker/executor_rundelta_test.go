package worker_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

func newDeltaStore(t *testing.T) *delta.Store {
	t.Helper()
	store, err := delta.Open(filepath.Join(t.TempDir(), "delta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func mockEstabelecimentosRow(mock sqlmock.Sqlmock) {
	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("0001", "UBS", "05", "354130", "12345"))
}

func deltaJob(uploadURL, jobID string) worker.Job {
	return worker.Job{
		ID:        jobID,
		TenantID:  "354130",
		UploadURL: uploadURL,
		Params: extractor.ExtractionParams{
			Intent:      extractor.IntentCnesEstabelecimentos,
			CodMunGest:  "354130",
			Competencia: "202605",
		},
	}
}

func TestRunDelta_ColdStartReturnsAllInsertsAndCommits(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mockEstabelecimentosRow(mock)

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
	defer srv.Close()

	store := newDeltaStore(t)
	exe := &worker.JobExecutor{
		DB:         db,
		Uploader:   upload.NewHTTP(http.DefaultClient),
		DeltaStore: store,
	}

	job := deltaJob(srv.URL, "job-cold")
	size, pending, ds, err := exe.RunDelta(context.Background(), &job)
	require.NoError(t, err)
	require.NotNil(t, pending)
	require.Greater(t, size, int64(0))
	require.Len(t, ds.Inserts, 1)
	require.Empty(t, ds.Updates)
	require.Empty(t, ds.Deletes)
	require.NotEmpty(t, job.Sha256, "RunDelta must capture sha256 from upload")

	require.NoError(t, pending.Commit())
	got, err := store.GetCommitted(delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	})
	require.NoError(t, err)
	require.Len(t, got, 1)
}

func TestRunDelta_UploadFailureAbortsPending(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mockEstabelecimentosRow(mock)

	store := newDeltaStore(t)
	exe := &worker.JobExecutor{
		DB:         db,
		Uploader:   &stubFailUploader{err: errors.New("network down")},
		DeltaStore: store,
	}

	job := deltaJob("ignored", "job-fail")
	size, pending, _, err := exe.RunDelta(context.Background(), &job)
	require.Error(t, err)
	require.Nil(t, pending)
	require.Equal(t, int64(0), size)

	got, err := store.GetCommitted(delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	})
	require.NoError(t, err)
	require.Empty(t, got, "abort path must leave committed bucket untouched")
}

func TestRunDelta_UnknownIntent(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	store := newDeltaStore(t)
	exe := &worker.JobExecutor{
		DB:         db,
		Uploader:   upload.NewHTTP(http.DefaultClient),
		DeltaStore: store,
	}
	job := worker.Job{
		ID:     "job-unknown",
		Params: extractor.ExtractionParams{Intent: "unknown"},
	}
	_, _, _, err = exe.RunDelta(context.Background(), &job)
	require.ErrorIs(t, err, worker.ErrUnknownIntent)
}

func TestRun_DeltaPath_DispatchesAndCommits(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mockEstabelecimentosRow(mock)

	srv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			w.WriteHeader(http.StatusOK)
		}))
	defer srv.Close()

	store := newDeltaStore(t)
	exe := &worker.JobExecutor{
		DB:         db,
		Uploader:   upload.NewHTTP(http.DefaultClient),
		DeltaStore: store,
	}

	job := deltaJob(srv.URL, "job-run-delta")
	size, err := exe.Run(context.Background(), &job)
	require.NoError(t, err)
	require.Greater(t, size, int64(0))

	got, err := store.GetCommitted(delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	})
	require.NoError(t, err)
	require.Len(t, got, 1, "Run must commit pending after upload success")
}

func TestRun_DeltaPath_UploadFailReturnsErrAndAborts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	mockEstabelecimentosRow(mock)

	store := newDeltaStore(t)
	exe := &worker.JobExecutor{
		DB:         db,
		Uploader:   &stubFailUploader{err: errors.New("502 bad gateway")},
		DeltaStore: store,
	}

	job := deltaJob("ignored", "job-run-fail")
	_, err = exe.Run(context.Background(), &job)
	require.Error(t, err)

	got, err := store.GetCommitted(delta.SourceKey{
		Source: "cnes", Intent: "estabelecimentos", Competencia: "202605",
	})
	require.NoError(t, err)
	require.Empty(t, got)
}

type stubFailUploader struct{ err error }

func (s *stubFailUploader) Put(
	_ context.Context, _ string, body io.Reader, _ string,
) (int64, error) {
	_, _ = io.Copy(io.Discard, body)
	return 0, s.err
}
