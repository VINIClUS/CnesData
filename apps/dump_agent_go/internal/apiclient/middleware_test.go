package apiclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/stretchr/testify/require"
)

func TestWithTenantID_SetsHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Tenant-Id")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	editor := apiclient.WithTenantID("354130")
	require.NoError(t, editor(context.Background(), req))

	_, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, "354130", got)
}

func TestWithMachineID_SetsHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	editor := apiclient.WithMachineID("abc12345")
	require.NoError(t, editor(context.Background(), req))
	require.Equal(t, "abc12345", req.Header.Get("X-Machine-Id"))
}
