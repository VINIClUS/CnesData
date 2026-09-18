package apiclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/worker"
)

func BenchmarkRegisterJob(b *testing.B) {
	body := []byte(`{"job_id":"11111111-1111-1111-1111-111111111111",` +
		`"status":"REGISTERED"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	adapter, err := apiclient.NewAdapter(srv.URL, "354130", "machine-1", nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	job := worker.Job{
		ID:     "11111111-1111-1111-1111-111111111111",
		Sha256: "deadbeef",
		Params: extractor.ExtractionParams{
			Intent:      extractor.IntentCnesEstabelecimentos,
			Competencia: "202601",
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := adapter.RegisterJob(ctx, job, 4096); err != nil {
			b.Fatal(err)
		}
	}
}
