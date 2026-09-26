package rawclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClaimUsaTokenLocalERetornaAgenteDoServidor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "raw-secret", r.Header.Get("X-Raw-Token"))
		require.Equal(t, "machine-1", r.Header.Get("X-Raw-Agent-Id"))
		_, _ = w.Write([]byte(`{"job_id":"job-1","agent_id":"registered-agent",` +
			`"source_type":"SIHD","file_subtype":"SIHD_INTERNACAO",` +
			`"competencia":"2026-09","requested_snapshot_mode":"FULL",` +
			`"fencing_token":2,"lease_until":"2026-09-26T21:00:00Z",` +
			`"raw_upload_path":"/api/v1/edge/jobs/job-1/raw-object"}`))
	}))
	defer srv.Close()
	client := New(srv.URL, srv.Client(), "raw-secret", "machine-1")

	claim, err := client.Next(context.Background())

	require.NoError(t, err)
	require.Equal(t, "registered-agent", claim.AgentID)
	require.Equal(t, uint64(2), claim.FencingToken)
}
