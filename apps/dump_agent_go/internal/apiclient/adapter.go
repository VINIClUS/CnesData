package apiclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/worker"
)

// Adapter implementa worker.JobAPIClient sobre ClientWithResponses gerado.
type Adapter struct {
	Inner     *ClientWithResponses
	TenantID  string
	MachineID string
}

// acquireJobPayload mirror do AcquireJobResponse (Python) — JSON200 do spec é {}.
type acquireJobPayload struct {
	JobID        string `json:"job_id"`
	SourceSystem string `json:"source_system"`
	TenantID     string `json:"tenant_id"`
	UploadURL    string `json:"upload_url"`
	ObjectKey    string `json:"object_key"`
}

// NewAdapter cria Adapter com editors X-Tenant-Id / X-Machine-Id.
func NewAdapter(baseURL, tenantID, machineID string, httpClient *http.Client) (*Adapter, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id_required")
	}
	if machineID == "" {
		return nil, fmt.Errorf("machine_id_required")
	}
	editors := []RequestEditorFn{WithTenantID(tenantID), WithMachineID(machineID)}
	opts := []ClientOption{WithRequestEditorFn(combineEditors(editors))}
	if httpClient != nil {
		opts = append([]ClientOption{WithHTTPClient(httpClient)}, opts...)
	}
	inner, err := NewClientWithResponses(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	return &Adapter{Inner: inner, TenantID: tenantID, MachineID: machineID}, nil
}

func combineEditors(eds []RequestEditorFn) RequestEditorFn {
	return func(ctx context.Context, req *http.Request) error {
		for _, ed := range eds {
			if err := ed(ctx, req); err != nil {
				return err
			}
		}
		return nil
	}
}

// AcquireJob polling. Retorna nil,nil quando fila vazia (204).
func (a *Adapter) AcquireJob(ctx context.Context) (*worker.Job, error) {
	resp, err := a.Inner.AcquireJobApiV1JobsAcquirePostWithResponse(
		ctx, AcquireJobApiV1JobsAcquirePostJSONRequestBody{MachineId: a.MachineID},
	)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode() != http.StatusOK {
		return nil, &obs.HTTPError{StatusCode: resp.StatusCode(), Body: string(resp.Body)}
	}
	var payload acquireJobPayload
	if err := json.Unmarshal(resp.Body, &payload); err != nil {
		return nil, fmt.Errorf("acquire_decode: %w", err)
	}
	return &worker.Job{
		ID:        payload.JobID,
		TenantID:  payload.TenantID,
		UploadURL: payload.UploadURL,
		Params: extractor.ExtractionParams{
			Intent:      payload.SourceSystem,
			Competencia: os.Getenv("COMPETENCIA"),
			CodMunGest:  envOr("COD_MUN_IBGE", payload.TenantID),
		},
	}, nil
}

// CompleteJob sinaliza sucesso ao central.
func (a *Adapter) CompleteJob(ctx context.Context, job worker.Job, sizeBytes int64) error {
	jobID, err := parseJobUUID(job.ID)
	if err != nil {
		return err
	}
	objectKey := fmt.Sprintf("%s/%s/%s.parquet.gz", job.TenantID, job.Params.Intent, job.ID)
	resp, err := a.Inner.CompleteUploadRouteApiV1JobsJobIdCompleteUploadPostWithResponse(
		ctx, jobID, CompleteUploadRouteApiV1JobsJobIdCompleteUploadPostJSONRequestBody{
			MachineId: a.MachineID,
			ObjectKey: objectKey,
			SizeBytes: int(sizeBytes),
		},
	)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

// FailJob no-op HTTP: central_api não expõe /fail; lease reaper cuida.
// Apenas registra severidade baseada na Classify.
func (a *Adapter) FailJob(_ context.Context, job worker.Job, cause error) error {
	kind := obs.Classify(cause)
	_ = job
	_ = kind
	return nil
}

// SendHeartbeat estende lease.
func (a *Adapter) SendHeartbeat(ctx context.Context, jobID string) error {
	id, err := parseJobUUID(jobID)
	if err != nil {
		return err
	}
	resp, err := a.Inner.HeartbeatApiV1JobsJobIdHeartbeatPostWithResponse(
		ctx, id, HeartbeatApiV1JobsJobIdHeartbeatPostJSONRequestBody{MachineId: a.MachineID},
	)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

func parseJobUUID(s string) (openapi_types.UUID, error) {
	var id openapi_types.UUID
	if err := id.UnmarshalText([]byte(s)); err != nil {
		return openapi_types.UUID{}, fmt.Errorf("invalid_job_uuid id=%s: %w", s, err)
	}
	return id, nil
}

func statusError(code int, body []byte) error {
	if code >= 200 && code < 300 {
		return nil
	}
	return &obs.HTTPError{StatusCode: code, Body: string(body)}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
