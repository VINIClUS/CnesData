package apiclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/worker"
)

func TestNewAdapter_RejectsMissingIDs(t *testing.T) {
	_, err := apiclient.NewAdapter("http://x", "", "m", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "tenant_id_required")

	_, err = apiclient.NewAdapter("http://x", "t", "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "machine_id_required")
}

func TestNewAdapter_SetsFields(t *testing.T) {
	a, err := apiclient.NewAdapter("http://localhost:1", "tenant-1", "machine-1", nil)
	require.NoError(t, err)
	require.Equal(t, "tenant-1", a.TenantID)
	require.Equal(t, "machine-1", a.MachineID)
	require.NotEmpty(t, a.AgentVersion)
	require.NotNil(t, a.Inner)
}

// recordingTransport captures all RoundTrip calls for inspection.
type recordingTransport struct {
	visited []string
}

func (rt *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.visited = append(rt.visited, req.URL.Path)
	return &http.Response{
		StatusCode: 500,
		Body:       io.NopCloser(strings.NewReader(`{"detail":"test-shortcircuit"}`)),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func TestNewAdapter_UsesCustomHTTPClient(t *testing.T) {
	rt := &recordingTransport{}
	httpClient := &http.Client{Transport: rt}

	a, err := apiclient.NewAdapter("http://test.invalid", "tenant-1", "machine-1", httpClient)
	require.NoError(t, err)
	require.NotNil(t, a)

	jobUUID := uuid.NewString()
	_ = a.SendHeartbeat(context.Background(), jobUUID)

	require.NotEmpty(t, rt.visited, "expected RoundTrip to be invoked; injected client unused")
	require.Contains(t, rt.visited[0], "/heartbeat")
}

func newTestAdapter(t *testing.T, handler http.HandlerFunc) *apiclient.Adapter {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	a, err := apiclient.NewAdapter(srv.URL, "tenant-1", "machine-1", nil)
	require.NoError(t, err)
	return a
}

func TestRegisterJob_PostUploadWithSha(t *testing.T) {
	var got apiclient.RegisterRequest
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(
			`{"job_id":"11111111-2222-3333-4444-555555555555","status":"REGISTERED"}`,
		))
	})
	job := worker.Job{
		ID:       "11111111-2222-3333-4444-555555555555",
		Sha256:   "a" + strings.Repeat("0", 63),
		MinioKey: "354130/CNES_VINCULO/2026-01-01/abc.parquet.gz",
		Params: extractor.ExtractionParams{
			Intent:      "cnes_profissionais",
			Competencia: "202601",
		},
	}
	err := a.RegisterJob(context.Background(), job, 4096)
	require.NoError(t, err)
	require.NotNil(t, got.Sha256)
	require.Equal(t, "a"+strings.Repeat("0", 63), *got.Sha256)
}

func TestRegisterJob_5xxReturnsHTTPError(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	err := a.RegisterJob(
		context.Background(),
		worker.Job{ID: "11111111-2222-3333-4444-555555555555"},
		100,
	)
	require.Error(t, err)
	var httpErr *obs.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.StatusCode)
}

func TestRegisterJob_InvalidJobID(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	err := a.RegisterJob(context.Background(), worker.Job{ID: "not-a-uuid"}, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_job_uuid")
}

func TestFailJob_NilCauseUsesDefault(t *testing.T) {
	var gotBody []byte
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.FailJob(context.Background(), worker.Job{
		ID: "55555555-5555-5555-5555-555555555555",
	}, nil)
	require.NoError(t, err)
	require.Contains(t, string(gotBody), "unknown_error")
}

func TestFailJob_ErrorMessagePropagated(t *testing.T) {
	var gotBody []byte
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.FailJob(context.Background(), worker.Job{
		ID: "66666666-6666-6666-6666-666666666666",
	}, errors.New("db_timeout"))
	require.NoError(t, err)
	require.Contains(t, string(gotBody), "db_timeout")
}

func TestRegisterBPASIAJob_InvalidUUID(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	err := a.RegisterBPASIAJob(context.Background(), "not-a-uuid", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_job_uuid")
}

func TestRegisterBPASIAJob_Success(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	err := a.RegisterBPASIAJob(context.Background(),
		"77777777-7777-7777-7777-777777777777",
		[]worker.ManifestEntry{
			{MinioKey: "k1", FatoSubtype: "BPA_C", SizeBytes: 100, Sha256: "s1"},
			{MinioKey: "k2", FatoSubtype: "BPA_I", SizeBytes: 200, Sha256: "s2"},
		},
	)
	require.NoError(t, err)
}

func TestRegisterBPASIAJob_SendsAgentMetadataInBody(t *testing.T) {
	var captured apiclient.JobRegisterRequest
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&captured))
		w.WriteHeader(http.StatusOK)
	})
	err := a.RegisterBPASIAJob(context.Background(),
		"66666666-6666-6666-6666-666666666666",
		[]worker.ManifestEntry{
			{MinioKey: "k", FatoSubtype: "BPA_C", SizeBytes: 100, Sha256: "s"},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, captured.AgentVersion)
	require.NotNil(t, captured.MachineId)
	require.Equal(t, a.AgentVersion, *captured.AgentVersion)
	require.Equal(t, "machine-1", *captured.MachineId)
}

func TestSendHeartbeat_InvalidUUID(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.SendHeartbeat(context.Background(), "bad")
	require.Error(t, err)
}

func TestSendHeartbeat_Success(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.SendHeartbeat(context.Background(), "88888888-8888-8888-8888-888888888888")
	require.NoError(t, err)
}

func TestSendHeartbeat_5xx(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	err := a.SendHeartbeat(context.Background(), "99999999-9999-9999-9999-999999999999")
	require.Error(t, err)
	var httpErr *obs.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
}

func TestMintUploadURL_Created(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/jobs/upload-url" {
			t.Fatalf("path = %s want /api/v1/jobs/upload-url", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{
			"extraction_id": "11111111-2222-3333-4444-555555555555",
			"upload_url": "https://minio/sig",
			"minio_key": "354130/CNES_VINCULO/2026-01-01/abc.parquet.gz"
		}`))
	})
	job, err := a.MintUploadURL(context.Background(), worker.JobSpec{
		JobID:        "11111111-2222-3333-4444-555555555555",
		FonteSistema: "CNES_LOCAL",
		TipoExtracao: "profissionais",
		Competencia:  202601,
		Intent:       "cnes_profissionais",
	})
	require.NoError(t, err)
	require.Equal(t, "https://minio/sig", job.UploadURL)
	require.Equal(t, "354130/CNES_VINCULO/2026-01-01/abc.parquet.gz", job.MinioKey)
}

func TestMintUploadURL_4xx(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	_, err := a.MintUploadURL(context.Background(), worker.JobSpec{
		JobID:        "11111111-2222-3333-4444-555555555555",
		FonteSistema: "CNES_LOCAL",
		TipoExtracao: "profissionais",
		Competencia:  202601,
		Intent:       "cnes_profissionais",
	})
	require.Error(t, err)
	var httpErr *obs.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusConflict, httpErr.StatusCode)
}

var _ worker.RawManifestClient = (*apiclient.Adapter)(nil)

const rawManifestJSON = `{"manifest_version":1,"manifest_id":"job-1","tenant_id":"354130"}`

func rawEnvelope() queue.Envelope {
	return queue.Envelope{
		Type:           queue.TypeRawManifest,
		JobID:          "job-1",
		FencingToken:   7,
		ManifestJSON:   []byte(rawManifestJSON),
		ManifestSHA256: strings.Repeat("a", 64),
	}
}

func rawManifestHandler(
	t *testing.T, status int, payload string, captured *map[string]json.RawMessage,
) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/v1/edge/raw-manifests", r.URL.Path)
		require.Equal(t, http.MethodPost, r.Method)
		require.NoError(t, json.NewDecoder(r.Body).Decode(captured))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(payload))
	}
}

func TestSendRawManifest_EnviaIdentidadeDuravelDoEnvelope(t *testing.T) {
	var got map[string]json.RawMessage
	body := `{"accepted":true,"manifest_id":"job-1","manifest_sha256":"` +
		strings.Repeat("a", 64) + `","full_resync_required":false,"reason":null}`
	a := newTestAdapter(t, rawManifestHandler(t, http.StatusOK, body, &got))

	ack, err := a.SendRawManifest(context.Background(), rawEnvelope())

	require.NoError(t, err)
	require.Equal(t, http.StatusOK, ack.StatusCode)
	require.Equal(t, strings.Repeat("a", 64), ack.ManifestSHA256)
	require.False(t, ack.ForceFull)
	require.Empty(t, ack.Reason)
	require.JSONEq(t, `"job-1"`, string(got["job_id"]))
	require.JSONEq(t, `7`, string(got["fencing_token"]))
	require.Equal(t, rawManifestJSON, string(got["manifest"]))
}

func TestSendRawManifest_PropagaResyncTipado409(t *testing.T) {
	var got map[string]json.RawMessage
	body := `{"accepted":false,"manifest_id":"job-1","manifest_sha256":"` +
		strings.Repeat("a", 64) + `","full_resync_required":true,"reason":"BASE_UNKNOWN"}`
	a := newTestAdapter(t, rawManifestHandler(t, http.StatusConflict, body, &got))

	ack, err := a.SendRawManifest(context.Background(), rawEnvelope())

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, ack.StatusCode)
	require.True(t, ack.ForceFull)
	require.Equal(t, "BASE_UNKNOWN", ack.Reason)
	require.Equal(t, strings.Repeat("a", 64), ack.ManifestSHA256)
}

func TestSendRawManifest_ConflitoDetalhadoNaoViraResync(t *testing.T) {
	var got map[string]json.RawMessage
	a := newTestAdapter(
		t,
		rawManifestHandler(t, http.StatusConflict, `{"detail":"job_fence_rejected"}`, &got),
	)

	ack, err := a.SendRawManifest(context.Background(), rawEnvelope())

	require.NoError(t, err)
	require.Equal(t, http.StatusConflict, ack.StatusCode)
	require.False(t, ack.ForceFull)
	require.Empty(t, ack.Reason)
	require.Empty(t, ack.ManifestSHA256)
}

func TestSendRawManifest_RetentaComOsMesmosBytesPersistidos(t *testing.T) {
	var got map[string]json.RawMessage
	seen := make([]string, 0, 2)
	a := newTestAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		require.NoError(t, json.NewDecoder(r.Body).Decode(&got))
		seen = append(seen, string(got["manifest"])+"|"+string(got["fencing_token"]))
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	env := rawEnvelope()

	_, first := a.SendRawManifest(context.Background(), env)
	_, second := a.SendRawManifest(context.Background(), env)

	require.NoError(t, first)
	require.NoError(t, second)
	require.Len(t, seen, 2)
	require.Equal(t, seen[0], seen[1])
	require.Equal(t, rawManifestJSON+"|7", seen[0])
}

func TestSendRawManifest_ErroDeTransporteRetornaErro(t *testing.T) {
	a, err := apiclient.NewAdapter("http://127.0.0.1:1", "tenant-1", "machine-1", nil)
	require.NoError(t, err)

	ack, sendErr := a.SendRawManifest(context.Background(), rawEnvelope())

	require.Error(t, sendErr)
	require.Zero(t, ack.StatusCode)
}
