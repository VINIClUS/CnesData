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
	body := []byte(`{"extraction_id":"00000000-0000-0000-0000-000000000001",` +
		`"upload_url":"https://example.invalid/upload"}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	adapter, err := apiclient.NewAdapter(srv.URL, "354130", "machine-1", nil)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	spec := worker.JobSpec{
		JobID:        "11111111-1111-1111-1111-111111111111",
		FonteSistema: "CNES_LOCAL",
		TipoExtracao: "estabelecimentos",
		Competencia:  202601,
		Intent:       extractor.IntentCnesEstabelecimentos,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := adapter.RegisterJob(ctx, spec); err != nil {
			b.Fatal(err)
		}
	}
}
