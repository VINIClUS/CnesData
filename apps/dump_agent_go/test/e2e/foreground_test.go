//go:build e2e

package e2e_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
	"github.com/stretchr/testify/require"
)

const testExtractionUUID = "11111111-1111-1111-1111-111111111111"

func TestForeground_SmokeEndToEnd(t *testing.T) {
	t.Setenv("COMPETENCIA", "2026-01")
	t.Setenv("COD_MUN_IBGE", "354130")

	var uploadedBytes int64
	minioSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n, _ := io.Copy(io.Discard, r.Body)
		atomic.StoreInt64(&uploadedBytes, n)
		w.WriteHeader(http.StatusOK)
	}))
	defer minioSrv.Close()

	var registered, completed, heartbeats int32
	central := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/v1/jobs/register":
			if atomic.AddInt32(&registered, 1) > 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"extraction_id": testExtractionUUID,
				"upload_url":    minioSrv.URL,
			})
		case strings.HasSuffix(r.URL.Path, "/complete"):
			atomic.AddInt32(&completed, 1)
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(r.URL.Path, "/heartbeat"):
			atomic.AddInt32(&heartbeats, 1)
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer central.Close()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	cols := []string{"cnes", "nome_fanta", "tp_unid_id", "codmungest", "cnpj_mant"}
	mock.ExpectQuery("SELECT est.CNES").
		WithArgs("354130").
		WillReturnRows(sqlmock.NewRows(cols).AddRow("0001", "UBS", "05", "354130", "12345"))

	adapter, err := apiclient.NewAdapter(central.URL, "354130", "abc12345", nil)
	require.NoError(t, err)

	src := worker.NewStaticSource(worker.StaticSpec{
		FonteSistema: "CNES_LOCAL",
		TipoExtracao: "estabelecimentos",
		Competencia:  202601,
		Intent:       extractor.IntentCnesEstabelecimentos,
	})

	exe := &worker.JobExecutor{DB: db, Uploader: upload.NewHTTP(nil)}
	cons := worker.NewConsumer(adapter, src, exe, worker.ConsumerConfig{
		PollInterval:      10 * time.Millisecond,
		InterJobJitterMax: time.Millisecond,
		HeartbeatInterval: time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	require.NoError(t, cons.Loop(ctx))

	require.GreaterOrEqual(t, atomic.LoadInt32(&registered), int32(1))
	require.Equal(t, int32(1), atomic.LoadInt32(&completed))
	require.Greater(t, atomic.LoadInt64(&uploadedBytes), int64(0))
}
