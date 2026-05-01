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
	"github.com/cnesdata/dumpagent/internal/obs"
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

func TestRegisterJob_Created(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"extraction_id": "11111111-1111-1111-1111-111111111111",
			"upload_url":    "https://minio.example/put",
		})
	})
	job, err := a.RegisterJob(context.Background(), worker.JobSpec{
		JobID:        "22222222-2222-2222-2222-222222222222",
		Competencia:  202601,
		FonteSistema: "CNES",
		TipoExtracao: "FULL",
		Intent:       "cnes_profissionais",
	})
	require.NoError(t, err)
	require.Equal(t, "11111111-1111-1111-1111-111111111111", job.ID)
	require.Equal(t, "https://minio.example/put", job.UploadURL)
	require.Equal(t, "tenant-1", job.TenantID)
	require.Equal(t, "202601", job.Params.Competencia)
}

func TestRegisterJob_InvalidJobID(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
	})
	_, err := a.RegisterJob(context.Background(), worker.JobSpec{
		JobID:        "not-a-uuid",
		Competencia:  202601,
		FonteSistema: "CNES",
		TipoExtracao: "FULL",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_job_uuid")
}

func TestRegisterJob_5xxReturnsHTTPError(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("kaboom"))
	})
	_, err := a.RegisterJob(context.Background(), worker.JobSpec{
		JobID:        "33333333-3333-3333-3333-333333333333",
		Competencia:  202601,
		FonteSistema: "CNES",
		TipoExtracao: "FULL",
	})
	require.Error(t, err)
	var httpErr *obs.HTTPError
	require.True(t, errors.As(err, &httpErr))
	require.Equal(t, http.StatusInternalServerError, httpErr.StatusCode)
}

func TestCompleteJob_InvalidID(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.CompleteJob(context.Background(), worker.Job{ID: "garbage"}, 100)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid_job_uuid")
}

func TestCompleteJob_Success(t *testing.T) {
	a := newTestAdapter(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	err := a.CompleteJob(context.Background(), worker.Job{
		ID: "44444444-4444-4444-4444-444444444444", Sha256: "abc", RowCount: 10,
	}, 100)
	require.NoError(t, err)
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
