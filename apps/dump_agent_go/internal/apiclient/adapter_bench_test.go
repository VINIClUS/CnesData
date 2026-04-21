package apiclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cnesdata/dumpagent/internal/apiclient"
)

func BenchmarkAcquireJob(b *testing.B) {
	body := []byte(`{"job_id":"00000000-0000-0000-0000-000000000001",` +
		`"source_system":"cnes_estabelecimentos","tenant_id":"354130",` +
		`"upload_url":"https://example.invalid/upload",` +
		`"object_key":"354130/cnes_estabelecimentos/00000000-0000-0000-0000-000000000001.parquet.gz"}`)
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
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := adapter.AcquireJob(ctx); err != nil {
			b.Fatal(err)
		}
	}
}
