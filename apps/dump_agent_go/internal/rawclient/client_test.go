package rawclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cnesdata/dumpagent/internal/queue"
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

func TestClaimDistingueFilaVaziaErroHTTPERespostaInvalida(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"empty", http.StatusNoContent, "", ""},
		{"error", http.StatusServiceUnavailable, "", "raw_claim_status=503"},
		{"invalid", http.StatusOK, "{", "unexpected EOF"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			claim, err := New(srv.URL, srv.Client(), "", "").Next(context.Background())
			require.Nil(t, claim)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestHeartbeatEnviaFenceERejeitaFalha(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/edge/jobs/job-1/heartbeat", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		var body map[string]uint64
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, uint64(7), body["fencing_token"])
		w.WriteHeader(status)
	}))
	defer srv.Close()
	client := New(srv.URL, srv.Client(), "", "")

	require.NoError(t, client.Heartbeat(context.Background(), "job-1", 7))
	status = http.StatusConflict
	require.ErrorContains(t, client.Heartbeat(context.Background(), "job-1", 7),
		"raw_heartbeat_status=409")
}

func TestManifestRecebeHashEInstrucaoDeRessincronizacao(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/edge/raw-manifests", r.URL.Path)
		var body map[string]json.RawMessage
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.JSONEq(t, `"job-1"`, string(body["job_id"]))
		require.JSONEq(t, `7`, string(body["fencing_token"]))
		require.JSONEq(t, `{"files":[]}`, string(body["manifest"]))
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"manifest_sha256":"hash","full_resync_required":true,` +
			`"reason":"fallback","detail":"lease_lost"}`))
	}))
	defer srv.Close()
	client := New(srv.URL, srv.Client(), "", "")

	response, err := client.SendRawManifest(context.Background(), queue.Envelope{
		JobID: "job-1", FencingToken: 7, ManifestJSON: []byte(`{"files":[]}`),
	})

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, response.StatusCode)
	require.Equal(t, "hash", response.ManifestSHA256)
	require.True(t, response.ForceFull)
	require.Equal(t, "lease_lost", response.Reason)
}

func TestUploadURLRejeitaDestinoForaDaAPI(t *testing.T) {
	client := New("https://api.example", nil, "", "")
	for _, path := range []string{"https://attacker.example/x", "/external/object", "%"} {
		_, err := client.UploadURL(path)
		require.ErrorContains(t, err, "raw_upload_path=invalid")
	}
	url, err := client.UploadURL("/api/v1/edge/jobs/job-1/raw-object")
	require.NoError(t, err)
	require.Equal(t, "https://api.example/api/v1/edge/jobs/job-1/raw-object", url)
}
