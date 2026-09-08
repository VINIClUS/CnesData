package worker_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	pq "github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func newDeltaStore(t *testing.T) *delta.Store {
	t.Helper()
	store, err := delta.Open(filepath.Join(t.TempDir(), "delta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return store
}

type rawUploadFunc func(context.Context, upload.RawPutRequest) (int64, error)

func (f rawUploadFunc) PutRaw(ctx context.Context, req upload.RawPutRequest) (int64, error) {
	return f(ctx, req)
}

func rawJob() worker.Job {
	return worker.Job{ID: "full-job", TenantID: "tenant", FencingToken: 7,
		UploadURL: "https://object.invalid", Params: extractor.ExtractionParams{
			Intent: extractor.IntentCnesProfissionais, Competencia: "202601",
		}, RawRequest: &manifest.BuildRequest{
			SourceType: manifest.SourceTypeCNESLocal, FileSubtype: "CNES_VINCULO",
			Competencia: "2026-01", AgentID: "agent", AgentVersion: "1", SchemaVersion: "1",
			SnapshotMode: manifest.SnapshotModeFull,
			CreatedAt:    time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}}
}

func newRawExecutor(t *testing.T) *worker.JobExecutor {
	t.Helper()
	directory := t.TempDir()
	out, err := queue.Open(filepath.Join(directory, "outbox.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = out.Close() })
	store, err := delta.Open(filepath.Join(directory, "delta.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	return &worker.JobExecutor{DeltaStore: store, RawOutbox: out,
		RawSpoolDirectory: filepath.Join(directory, "spool"),
		RawExtract: func(context.Context, worker.Job) ([]delta.Row, error) {
			return []delta.Row{{"CPF": "synthetic", "CNES": "unit", "CBO": "role",
				"CNS": "card", "NOME_PROFISSIONAL": "Teste", "NOME_SOCIAL": nil,
				"SEXO": "F", "TIPO_VINCULO": "1", "SUS": "S", "CH_TOTAL": int64(40),
				"CH_AMBULATORIAL": int64(20), "CH_OUTRAS": int64(0),
				"CH_HOSPITALAR": int64(20), "FONTE": "CNES_LOCAL"}}, nil
		}, RawUploader: rawUploadFunc(func(_ context.Context, r upload.RawPutRequest) (int64, error) {
			return io.Copy(io.Discard, r.Body)
		})}
}

func rawPendingRef(job worker.Job) delta.PendingRef {
	return delta.PendingRef{JobID: job.ID, FencingToken: job.FencingToken,
		SourceKey: delta.SourceKey{Source: "cnes", Intent: "profissionais", Competencia: "202601"}}
}

func TestExecutorRawPersistePendenteEEnvelopeAntesDoUpload(t *testing.T) {
	exe, job := newRawExecutor(t), rawJob()
	var payload []byte
	exe.RawUploader = rawUploadFunc(func(_ context.Context,
		request upload.RawPutRequest,
	) (int64, error) {
		items, err := exe.RawOutbox.Peek(10)
		require.NoError(t, err)
		require.Len(t, items, 1)
		_, err = exe.DeltaStore.BeginPendingRef(rawPendingRef(job))
		require.ErrorIs(t, err, delta.ErrPendingExists)
		require.Equal(t, uint64(7), request.FencingToken)
		require.Equal(t, "raw/tenant/CNES_LOCAL/2026-01/full-job/data.parquet", request.ObjectKey)
		payload, err = io.ReadAll(request.Body)
		return int64(len(payload)), err
	})
	size, err := exe.Run(context.Background(), &job)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), size)
	digest := sha256.Sum256(payload)
	require.Equal(t, hex.EncodeToString(digest[:]), job.Sha256)
	file, err := pq.OpenFile(bytes.NewReader(payload), size)
	require.NoError(t, err)
	require.Len(t, file.Schema().Columns(), 14)
	require.Equal(t, int64(1), file.NumRows())
	items, err := exe.RawOutbox.Peek(10)
	require.NoError(t, err)
	var raw manifest.Raw
	require.NoError(t, json.Unmarshal(items[0].Envelope.ManifestJSON, &raw))
	require.Equal(t, job.Sha256, raw.ObjectSHA256)
	require.Equal(t, size, raw.SizeBytes)
	committed, err := exe.DeltaStore.GetCommitted(rawPendingRef(job).SourceKey)
	require.NoError(t, err)
	require.Empty(t, committed)
}

func TestFalhaDeUploadRawPreservaEstadoDuravel(t *testing.T) {
	for _, name := range []string{"rede", "tamanho", "corpo_incompleto"} {
		t.Run(name, func(t *testing.T) {
			exe, job := newRawExecutor(t), rawJob()
			exe.RawUploader = rawUploadFunc(func(_ context.Context, r upload.RawPutRequest) (int64, error) {
				if name == "rede" {
					return 0, errors.New("network=down")
				}
				if name == "corpo_incompleto" {
					return 0, nil
				}
				n, err := io.Copy(io.Discard, r.Body)
				return n + 1, err
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := exe.RunRaw(ctx, &job)
			require.Error(t, err)
			items, err := exe.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Len(t, items, 1)
			_, err = exe.DeltaStore.BeginPendingRef(rawPendingRef(job))
			require.ErrorIs(t, err, delta.ErrPendingExists)
			_, _, _, _, ok, err := exe.DeltaStore.ChainHead(rawPendingRef(job).SourceKey)
			require.NoError(t, err)
			require.False(t, ok)
		})
	}
}

func TestFullForcadoEsperaJobFullSolicitado(t *testing.T) {
	exe, job := newRawExecutor(t), rawJob()
	extract, calls := exe.RawExtract, 0
	exe.RawExtract = func(ctx context.Context, job worker.Job) ([]delta.Row, error) {
		calls++
		return extract(ctx, job)
	}
	ref := rawPendingRef(job)
	require.NoError(t, exe.DeltaStore.RequireFull(ref, "base_missing"))
	job.RawRequest.SnapshotMode = manifest.SnapshotModeDelta
	_, err := exe.RunRaw(context.Background(), &job)
	require.ErrorContains(t, err, "force_full")
	require.Zero(t, calls)
	items, err := exe.RawOutbox.Peek(10)
	require.NoError(t, err)
	require.Empty(t, items)
	job.RawRequest.SnapshotMode = manifest.SnapshotModeFull
	_, err = exe.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	_, force, err := exe.DeltaStore.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.True(t, force)
	items, err = exe.RawOutbox.Peek(10)
	require.NoError(t, err)
	var raw manifest.Raw
	require.NoError(t, json.Unmarshal(items[0].Envelope.ManifestJSON, &raw))
	require.NoError(t, exe.DeltaStore.ConfirmPending(ref, raw, strings.Repeat("a", 64)))
	_, force, err = exe.DeltaStore.ForceFull(ref.SourceKey)
	require.NoError(t, err)
	require.False(t, force)
}

func TestExecutorRawRejeitaConfiguracaoETiposAntesDePersistir(t *testing.T) {
	for _, name := range []string{"extrator_ausente", "tipo_invalido", "delta_sem_base"} {
		t.Run(name, func(t *testing.T) {
			exe, job := newRawExecutor(t), rawJob()
			switch name {
			case "extrator_ausente":
				exe.RawExtract = nil
			case "tipo_invalido":
				exe.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) {
					return []delta.Row{{"CPF": "synthetic", "CH_TOTAL": "40"}}, nil
				}
			case "delta_sem_base":
				job.RawRequest.SnapshotMode = manifest.SnapshotModeDelta
			}
			_, err := exe.RunRaw(context.Background(), &job)
			require.Error(t, err)
			items, err := exe.RawOutbox.Peek(10)
			require.NoError(t, err)
			require.Empty(t, items)
			pending, err := exe.DeltaStore.BeginPendingRef(rawPendingRef(job))
			require.NoError(t, err)
			pending.Abort()
		})
	}
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

func confirmRawExecutor(t *testing.T, executor *worker.JobExecutor, job worker.Job) manifest.Raw {
	t.Helper()
	items, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	require.Len(t, items, 1)
	var raw manifest.Raw
	require.NoError(t, json.Unmarshal(items[0].Envelope.ManifestJSON, &raw))
	require.NoError(t, executor.DeltaStore.ConfirmPending(
		rawPendingRef(job), raw, strings.Repeat("b", 64)))
	require.NoError(t, executor.RawOutbox.Delete(items[0].Key))
	return raw
}

func TestDeltaRawUsaCabecaConfirmadaEPreservaChavesNulasNaExclusao(t *testing.T) {
	executor, job := newRawExecutor(t), rawJob()
	rows := []delta.Row{{"CPF": nil, "CNES": "unit", "CBO": "role"},
		{"CPF": "update", "CNES": "unit", "CBO": "role", "CH_TOTAL": int64(20)},
		{"CPF": "same", "CNES": "unit", "CBO": "role"}}
	executor.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) { return rows, nil }
	_, err := executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	confirmRawExecutor(t, executor, job)
	job.ID, job.FencingToken = "delta-job", 8
	job.RawRequest.SnapshotMode = manifest.SnapshotModeDelta
	rows = []delta.Row{{"CPF": "insert", "CNES": "unit", "CBO": "role"},
		{"CPF": "update", "CNES": "unit", "CBO": "role", "CH_TOTAL": int64(40)}, rows[2]}
	var payload []byte
	executor.RawUploader = rawUploadFunc(func(_ context.Context,
		r upload.RawPutRequest,
	) (int64, error) {
		payload, err = io.ReadAll(r.Body)
		return int64(len(payload)), err
	})
	_, err = executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	items, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	var raw manifest.Raw
	require.NoError(t, json.Unmarshal(items[0].Envelope.ManifestJSON, &raw))
	require.Equal(t, uint32(2), raw.Sequence)
	require.Equal(t, "full-job", *raw.BaseSnapshotID)
	require.Equal(t, strings.Repeat("b", 64), *raw.PreviousManifestSHA256)
	require.Equal(t, int64(3), raw.RowCount)
	decoded := readRawOperations(t, payload)
	require.Equal(t, "insert", decoded[0]["CPF"])
	require.Equal(t, "I", decoded[0]["_op"])
	require.Equal(t, "40", decoded[1]["CH_TOTAL"])
	require.Equal(t, "U", decoded[1]["_op"])
	require.Nil(t, decoded[2]["CPF"])
	require.Equal(t, "D", decoded[2]["_op"])
}

func readRawOperations(t *testing.T, payload []byte) []delta.Row {
	t.Helper()
	reader := pq.NewReader(bytes.NewReader(payload))
	defer reader.Close()
	columns := reader.Schema().Columns()
	rows := make([]pq.Row, 10)
	n, err := reader.ReadRows(rows)
	if err != io.EOF {
		require.NoError(t, err)
	}
	result := make([]delta.Row, n)
	for i, row := range rows[:n] {
		result[i] = delta.Row{}
		row.Range(func(column int, values []pq.Value) bool {
			if !values[0].IsNull() {
				result[i][columns[column][0]] = values[0].String()
			}
			return true
		})
	}
	return result
}

func TestRetryAposFalhaDeUploadIgnoraMudancasNaFonte(t *testing.T) {
	executor, job := newRawExecutor(t), rawJob()
	executor.RawUploader = rawUploadFunc(func(context.Context, upload.RawPutRequest) (int64, error) {
		return 0, errors.New("network=down")
	})
	_, err := executor.RunRaw(context.Background(), &job)
	require.Error(t, err)
	before, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	extractions, uploads := 0, 0
	executor.RawExtract = func(context.Context, worker.Job) ([]delta.Row, error) {
		extractions++
		return []delta.Row{{"CPF": "changed", "CNES": "unit", "CBO": "role"}}, nil
	}
	executor.RawUploader = rawUploadFunc(func(_ context.Context,
		r upload.RawPutRequest,
	) (int64, error) {
		uploads++
		return io.Copy(io.Discard, r.Body)
	})
	_, err = executor.RunRaw(context.Background(), &job)
	require.NoError(t, err)
	require.Zero(t, extractions)
	require.Zero(t, uploads)
	after, err := executor.RawOutbox.Peek(10)
	require.NoError(t, err)
	require.Equal(t, before, after)
	_, _, _, _, exists, err := executor.DeltaStore.ChainHead(rawPendingRef(job).SourceKey)
	require.NoError(t, err)
	require.False(t, exists)
	confirmRawExecutor(t, executor, job)
}
