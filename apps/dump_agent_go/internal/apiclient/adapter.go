package apiclient

import (
	"context"
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
	Inner        *ClientWithResponses
	TenantID     string
	MachineID    string
	AgentVersion string
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
	return &Adapter{
		Inner: inner, TenantID: tenantID, MachineID: machineID,
		AgentVersion: envOr("AGENT_VERSION", "dev"),
	}, nil
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

// RegisterJob cria extraction via /jobs/register e devolve Job com extraction_id + upload_url.
func (a *Adapter) RegisterJob(ctx context.Context, spec worker.JobSpec) (*worker.Job, error) {
	jobUUID, err := parseJobUUID(spec.JobID)
	if err != nil {
		return nil, err
	}
	body := RegisterExtractionApiV1JobsRegisterPostJSONRequestBody{
		AgentVersion: a.AgentVersion,
		Competencia:  spec.Competencia,
		FonteSistema: RegisterRequestFonteSistema(spec.FonteSistema),
		JobId:        jobUUID,
		MachineId:    a.MachineID,
		TenantId:     a.TenantID,
		TipoExtracao: spec.TipoExtracao,
	}
	resp, err := a.Inner.RegisterExtractionApiV1JobsRegisterPostWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusCreated || resp.JSON201 == nil {
		return nil, &obs.HTTPError{StatusCode: resp.StatusCode(), Body: string(resp.Body)}
	}
	extID := resp.JSON201.ExtractionId.String()
	return &worker.Job{
		ID:        extID,
		TenantID:  a.TenantID,
		UploadURL: resp.JSON201.UploadUrl,
		Params: extractor.ExtractionParams{
			Intent:      spec.Intent,
			Competencia: competenciaString(spec.Competencia),
			CodMunGest:  envOr("COD_MUN_IBGE", a.TenantID),
		},
	}, nil
}

// CompleteJob sinaliza sucesso ao central com sha256 + row_count.
func (a *Adapter) CompleteJob(ctx context.Context, job worker.Job, sizeBytes int64) error {
	id, err := parseJobUUID(job.ID)
	if err != nil {
		return err
	}
	_ = sizeBytes
	resp, err := a.Inner.CompleteExtractionApiV1JobsExtractionIdCompletePostWithResponse(
		ctx, id, CompleteExtractionApiV1JobsExtractionIdCompletePostJSONRequestBody{
			Sha256:   job.Sha256,
			RowCount: job.RowCount,
		},
	)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

// FailJob marca extraction como FAILED via /jobs/{id}/fail.
func (a *Adapter) FailJob(ctx context.Context, job worker.Job, cause error) error {
	id, err := parseJobUUID(job.ID)
	if err != nil {
		return err
	}
	msg := "unknown_error"
	if cause != nil {
		msg = cause.Error()
	}
	resp, err := a.Inner.FailExtractionApiV1JobsExtractionIdFailPostWithResponse(
		ctx, id, FailExtractionApiV1JobsExtractionIdFailPostJSONRequestBody{Error: msg},
	)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

// RegisterBPASIAJob chama POST /api/v1/jobs/register com N-file manifest.
// jobID é UUID string do job_id landing.extractions pre-enqueued.
// files vêm serializados do pipeline worker (upload já completou).
func (a *Adapter) RegisterBPASIAJob(
	ctx context.Context, jobID string, files []worker.ManifestEntry,
) error {
	var id openapi_types.UUID
	if err := id.UnmarshalText([]byte(jobID)); err != nil {
		return fmt.Errorf("invalid_job_uuid id=%s: %w", jobID, err)
	}
	body := JobRegisterRequest{
		JobId:        id,
		Files:        toFileManifests(files),
		AgentVersion: &a.AgentVersion,
		MachineId:    &a.MachineID,
	}
	resp, err := a.Inner.PostJobsRegisterWithResponse(ctx, body)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

func toFileManifests(in []worker.ManifestEntry) []FileManifest {
	out := make([]FileManifest, 0, len(in))
	for _, e := range in {
		out = append(out, FileManifest{
			MinioKey:    e.MinioKey,
			FatoSubtype: FileManifestFatoSubtype(e.FatoSubtype),
			SizeBytes:   e.SizeBytes,
			Sha256:      e.Sha256,
		})
	}
	return out
}

// SendHeartbeat estende lease via /jobs/{id}/heartbeat.
// processor_id é query param — agent reutiliza MachineID.
func (a *Adapter) SendHeartbeat(ctx context.Context, jobID string) error {
	id, err := parseJobUUID(jobID)
	if err != nil {
		return err
	}
	params := &HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostParams{
		ProcessorId: a.MachineID,
	}
	resp, err := a.Inner.HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostWithResponse(
		ctx, id, params,
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

func competenciaString(c int) string {
	if c <= 0 {
		return ""
	}
	return fmt.Sprintf("%06d", c)
}
