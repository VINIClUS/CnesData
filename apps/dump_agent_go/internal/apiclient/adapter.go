package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	openapi_types "github.com/oapi-codegen/runtime/types"

	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/worker"
)

// Adapter implementa worker.JobAPIClient sobre ClientWithResponses gerado.
type Adapter struct {
	Inner        *ClientWithResponses
	TenantID     string
	MachineID    string
	AgentVersion string
}

// AdapterConfig agrupa os parâmetros de construção do Adapter. Existe para
// caber AgentVersion (main.Version, via -X ldflags) sem estourar o limite
// de 4 parâmetros de NewAdapter.
type AdapterConfig struct {
	BaseURL      string
	TenantID     string
	MachineID    string
	AgentVersion string
	HTTPClient   *http.Client
}

// NewAdapter cria Adapter com editors X-Tenant-Id / X-Machine-Id.
// AgentVersion resolve, em ordem: env AGENT_VERSION > cfg.AgentVersion > "dev"
// — a env var continua vencendo para permitir override manual em campo.
func NewAdapter(cfg AdapterConfig) (*Adapter, error) {
	if cfg.TenantID == "" {
		return nil, fmt.Errorf("tenant_id_required")
	}
	if cfg.MachineID == "" {
		return nil, fmt.Errorf("machine_id_required")
	}
	editors := []RequestEditorFn{WithTenantID(cfg.TenantID), WithMachineID(cfg.MachineID)}
	opts := []ClientOption{WithRequestEditorFn(combineEditors(editors))}
	if cfg.HTTPClient != nil {
		opts = append([]ClientOption{WithHTTPClient(cfg.HTTPClient)}, opts...)
	}
	inner, err := NewClientWithResponses(cfg.BaseURL, opts...)
	if err != nil {
		return nil, err
	}
	configuredVersion := cfg.AgentVersion
	if configuredVersion == "" {
		configuredVersion = "dev"
	}
	return &Adapter{
		Inner: inner, TenantID: cfg.TenantID, MachineID: cfg.MachineID,
		AgentVersion: envOr("AGENT_VERSION", configuredVersion),
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

// RegisterJob confirma upload completo via POST /api/v1/jobs/register.
// Threadea sha256 (computado pós-upload via SHA256TeeReader) e sizeBytes
// para que o FileManifest do manifesto N-file seja válido.
func (a *Adapter) RegisterJob(ctx context.Context, job worker.Job, sizeBytes int64) error {
	jobUUID, err := parseJobUUID(job.ID)
	if err != nil {
		return err
	}
	files := toFileManifests([]worker.ManifestEntry{{
		MinioKey:    job.MinioKey,
		FatoSubtype: job.FatoSubtype,
		SizeBytes:   sizeBytes,
		Sha256:      job.Sha256,
	}})
	sha := job.Sha256
	body := JobRegisterRequest{
		JobId:        jobUUID,
		Files:        files,
		AgentVersion: &a.AgentVersion,
		MachineId:    &a.MachineID,
		Sha256:       &sha,
	}
	resp, err := a.Inner.RegisterJobApiV1JobsRegisterPostWithResponse(ctx, body)
	if err != nil {
		return err
	}
	return statusError(resp.StatusCode(), resp.Body)
}

// MintUploadURL chama POST /api/v1/jobs/upload-url para criar
// landing.extractions PENDING + obter presigned PUT URL.
func (a *Adapter) MintUploadURL(ctx context.Context, spec worker.JobSpec) (*worker.Job, error) {
	jobUUID, err := parseJobUUID(spec.JobID)
	if err != nil {
		return nil, err
	}
	body := MintUploadUrlApiV1JobsUploadUrlPostJSONRequestBody{
		AgentVersion: &a.AgentVersion,
		Competencia:  competenciaToDate(spec.Competencia),
		Intent:       spec.Intent,
		JobId:        jobUUID,
		MachineId:    &a.MachineID,
		SourceType:   UploadUrlRequestSourceType(spec.FonteSistema),
		TenantId:     a.TenantID,
		TipoExtracao: spec.TipoExtracao,
	}
	resp, err := a.Inner.MintUploadUrlApiV1JobsUploadUrlPostWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusCreated || resp.JSON201 == nil {
		return nil, &obs.HTTPError{StatusCode: resp.StatusCode(), Body: string(resp.Body)}
	}
	extID := resp.JSON201.ExtractionId.String()
	return &worker.Job{
		ID:          extID,
		TenantID:    a.TenantID,
		UploadURL:   resp.JSON201.UploadUrl,
		MinioKey:    resp.JSON201.MinioKey,
		FatoSubtype: string(resp.JSON201.FatoSubtype),
		Params: extractor.ExtractionParams{
			Intent:      spec.Intent,
			Competencia: competenciaString(spec.Competencia),
			CodMunGest:  envOr("COD_MUN_IBGE", a.TenantID),
		},
	}, nil
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
	resp, err := a.Inner.RegisterJobApiV1JobsRegisterPostWithResponse(ctx, body)
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

func competenciaToDate(yyyymm int) openapi_types.Date {
	year := yyyymm / 100
	month := yyyymm % 100
	return openapi_types.Date{Time: time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)}
}

// rawSubmission espelha RawManifestSubmission preservando os bytes canônicos
// persistidos no envelope: reserializar o manifesto poderia divergir do digest
// que o servidor recalcula.
type rawSubmission struct {
	JobId        string          `json:"job_id"`
	FencingToken uint64          `json:"fencing_token"`
	Manifest     json.RawMessage `json:"manifest"`
}

// SendRawManifest implementa worker.RawManifestClient sobre a rota Edge gerada.
// Usa exclusivamente a identidade durável do envelope — nunca rederiva job,
// fence ou manifesto a partir do estado mutável do processo durante um retry.
func (a *Adapter) SendRawManifest(
	ctx context.Context, env queue.Envelope,
) (worker.RawManifestResponse, error) {
	body, err := json.Marshal(rawSubmission{
		JobId:        env.JobID,
		FencingToken: env.FencingToken,
		Manifest:     json.RawMessage(env.ManifestJSON),
	})
	if err != nil {
		return worker.RawManifestResponse{}, err
	}
	resp, err := a.Inner.RegisterRawManifestApiV1EdgeRawManifestsPostWithBodyWithResponse(
		ctx, "application/json", bytes.NewReader(body),
	)
	if err != nil {
		return worker.RawManifestResponse{}, err
	}
	return rawManifestAck(resp), nil
}

func rawManifestAck(
	resp *RegisterRawManifestApiV1EdgeRawManifestsPostResponse,
) worker.RawManifestResponse {
	ack := worker.RawManifestResponse{StatusCode: resp.StatusCode()}
	if resp.JSON200 != nil {
		ack.ManifestSHA256 = resp.JSON200.ManifestSha256
		ack.ForceFull = resp.JSON200.FullResyncRequired
		ack.Reason = derefString(resp.JSON200.Reason)
		return ack
	}
	if resp.JSON409 == nil {
		return ack
	}
	conflict, err := resp.JSON409.AsRawManifestResponse()
	if err != nil || conflict.ManifestSha256 == "" {
		return ack
	}
	ack.ManifestSHA256 = conflict.ManifestSha256
	ack.ForceFull = conflict.FullResyncRequired
	ack.Reason = derefString(conflict.Reason)
	return ack
}

func derefString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
