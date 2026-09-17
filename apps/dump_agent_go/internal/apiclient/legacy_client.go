package apiclient

// Operações legadas do modo de migração que não podem ser geradas a partir de
// docs/contracts/openapi.json: /api/v1/jobs/register aceita apenas um corpo por
// caminho (o N-file gerado) e /api/v1/jobs/{extraction_id}/{fail,heartbeat} não
// possuem rota no servidor atual. Mantidas aqui, fora de generated.go, para que
// `go generate` permaneça reproduzível e o gate de drift continue real.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/oapi-codegen/runtime"
	openapi_types "github.com/oapi-codegen/runtime/types"
)

// ErrLegacyClientUnavailable sinaliza um ClientWithResponses sem *Client concreto.
var ErrLegacyClientUnavailable = errors.New("legacy_client_unavailable")

// RegisterRequest é o manifesto single-file legado de /api/v1/jobs/register.
type RegisterRequest struct {
	AgentVersion string                      `json:"agent_version"`
	Competencia  int                         `json:"competencia"`
	FonteSistema RegisterRequestFonteSistema `json:"fonte_sistema"`
	JobId        openapi_types.UUID          `json:"job_id"`
	MachineId    string                      `json:"machine_id"`
	Sha256       *string                     `json:"sha256,omitempty"`
	TenantId     string                      `json:"tenant_id"`
	TipoExtracao string                      `json:"tipo_extracao"`
}

// RegisterRequestFonteSistema defines model for RegisterRequest.FonteSistema.
type RegisterRequestFonteSistema string

// FailPayload é o corpo legado de /api/v1/jobs/{extraction_id}/fail.
type FailPayload struct {
	Error string `json:"error"`
}

// RegisterExtractionApiV1JobsRegisterPostJSONRequestBody defines body for RegisterExtractionApiV1JobsRegisterPost.
type RegisterExtractionApiV1JobsRegisterPostJSONRequestBody = RegisterRequest

// FailExtractionApiV1JobsExtractionIdFailPostJSONRequestBody defines body for FailExtractionApiV1JobsExtractionIdFailPost.
type FailExtractionApiV1JobsExtractionIdFailPostJSONRequestBody = FailPayload

// HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostParams defines parameters for HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPost.
type HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostParams struct {
	ProcessorId string `form:"processor_id" json:"processor_id"`
}

// LegacyResponse expõe a mesma superfície das respostas geradas.
type LegacyResponse struct {
	Body         []byte
	HTTPResponse *http.Response
}

// StatusCode devolve o código HTTP ou 0 quando não houve resposta.
func (r LegacyResponse) StatusCode() int {
	if r.HTTPResponse == nil {
		return 0
	}
	return r.HTTPResponse.StatusCode
}

// RegisterExtractionApiV1JobsRegisterPostWithResponse envia o manifesto single-file legado.
func (c *ClientWithResponses) RegisterExtractionApiV1JobsRegisterPostWithResponse(
	ctx context.Context,
	body RegisterExtractionApiV1JobsRegisterPostJSONRequestBody,
	reqEditors ...RequestEditorFn,
) (*LegacyResponse, error) {
	return c.legacyJSON(ctx, http.MethodPost, "/api/v1/jobs/register", nil, body, reqEditors)
}

// FailExtractionApiV1JobsExtractionIdFailPostWithResponse marca a extração como falha.
func (c *ClientWithResponses) FailExtractionApiV1JobsExtractionIdFailPostWithResponse(
	ctx context.Context,
	extractionId openapi_types.UUID,
	body FailExtractionApiV1JobsExtractionIdFailPostJSONRequestBody,
	reqEditors ...RequestEditorFn,
) (*LegacyResponse, error) {
	path, err := legacyExtractionPath(extractionId, "fail")
	if err != nil {
		return nil, err
	}
	return c.legacyJSON(ctx, http.MethodPost, path, nil, body, reqEditors)
}

// HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostWithResponse renova o lease legado.
func (c *ClientWithResponses) HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostWithResponse(
	ctx context.Context,
	extractionId openapi_types.UUID,
	params *HeartbeatExtractionApiV1JobsExtractionIdHeartbeatPostParams,
	reqEditors ...RequestEditorFn,
) (*LegacyResponse, error) {
	path, err := legacyExtractionPath(extractionId, "heartbeat")
	if err != nil {
		return nil, err
	}
	query := url.Values{}
	if params != nil {
		query.Set("processor_id", params.ProcessorId)
	}
	return c.legacyJSON(ctx, http.MethodPost, path, query, nil, reqEditors)
}

func legacyExtractionPath(extractionId openapi_types.UUID, action string) (string, error) {
	segment, err := runtime.StyleParamWithLocation(
		"simple", false, "extraction_id", runtime.ParamLocationPath, extractionId,
	)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("/api/v1/jobs/%s/%s", segment, action), nil
}

func (c *ClientWithResponses) legacyJSON(
	ctx context.Context,
	method string,
	path string,
	query url.Values,
	body any,
	reqEditors []RequestEditorFn,
) (*LegacyResponse, error) {
	client, ok := c.ClientInterface.(*Client)
	if !ok {
		return nil, ErrLegacyClientUnavailable
	}
	request, err := legacyRequest(client.Server, method, path, query, body)
	if err != nil {
		return nil, err
	}
	request = request.WithContext(ctx)
	if err := client.applyEditors(ctx, request, reqEditors); err != nil {
		return nil, err
	}
	response, err := client.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &LegacyResponse{Body: payload, HTTPResponse: response}, nil
}

func legacyRequest(
	server string, method string, path string, query url.Values, body any,
) (*http.Request, error) {
	serverURL, err := url.Parse(server)
	if err != nil {
		return nil, err
	}
	operationURL, err := serverURL.Parse("." + path)
	if err != nil {
		return nil, err
	}
	if len(query) > 0 {
		operationURL.RawQuery = query.Encode()
	}
	if body == nil {
		return http.NewRequest(method, operationURL.String(), nil)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequest(method, operationURL.String(), bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	request.Header.Add("Content-Type", "application/json")
	return request, nil
}
