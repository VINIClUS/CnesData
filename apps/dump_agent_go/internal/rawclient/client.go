package rawclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/worker"
)

type Claim struct {
	JobID         string    `json:"job_id"`
	AgentID       string    `json:"agent_id"`
	SourceType    string    `json:"source_type"`
	FileSubtype   string    `json:"file_subtype"`
	Competencia   string    `json:"competencia"`
	SnapshotMode  string    `json:"requested_snapshot_mode"`
	FencingToken  uint64    `json:"fencing_token"`
	Attempt       int       `json:"attempt"`
	LeaseUntil    time.Time `json:"lease_until"`
	RawUploadPath string    `json:"raw_upload_path"`
}

type Client struct {
	baseURL string
	http    *http.Client
}

type tokenTransport struct {
	base  http.RoundTripper
	token string
	agent string
}

func (t tokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	copy := req.Clone(req.Context())
	if t.token != "" {
		copy.Header.Set("X-Raw-Token", t.token)
		copy.Header.Set("X-Raw-Agent-Id", t.agent)
	}
	return t.base.RoundTrip(copy)
}

func New(baseURL string, httpClient *http.Client, localToken, localAgent string) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	copy := *httpClient
	transport := copy.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	copy.Transport = tokenTransport{transport, localToken, localAgent}
	return &Client{baseURL: strings.TrimRight(baseURL, "/"), http: &copy}
}

func (c *Client) HTTPClient() *http.Client { return c.http }

func (c *Client) UploadURL(path string) (string, error) {
	base, err := url.Parse(c.baseURL)
	if err != nil {
		return "", err
	}
	relative, err := url.Parse(path)
	if err != nil || relative.IsAbs() || !strings.HasPrefix(path, "/api/v1/edge/") {
		return "", fmt.Errorf("raw_upload_path=invalid")
	}
	return base.ResolveReference(relative).String(), nil
}

func (c *Client) request(
	ctx context.Context, method, path string, body io.Reader,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	return c.http.Do(req)
}

func (c *Client) Next(ctx context.Context) (*Claim, error) {
	response, err := c.request(ctx, http.MethodGet, "/api/v1/edge/jobs/next", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("raw_claim_status=%d", response.StatusCode)
	}
	var claim Claim
	if err := json.NewDecoder(response.Body).Decode(&claim); err != nil {
		return nil, err
	}
	return &claim, nil
}

func (c *Client) Heartbeat(ctx context.Context, jobID string, fence uint64) error {
	body, _ := json.Marshal(map[string]uint64{"fencing_token": fence})
	response, err := c.request(ctx, http.MethodPost,
		"/api/v1/edge/jobs/"+url.PathEscape(jobID)+"/heartbeat", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("raw_heartbeat_status=%d", response.StatusCode)
	}
	return nil
}

func (c *Client) SendRawManifest(
	ctx context.Context, env queue.Envelope,
) (worker.RawManifestResponse, error) {
	body, err := json.Marshal(struct {
		JobID        string          `json:"job_id"`
		FencingToken uint64          `json:"fencing_token"`
		Manifest     json.RawMessage `json:"manifest"`
	}{env.JobID, env.FencingToken, env.ManifestJSON})
	if err != nil {
		return worker.RawManifestResponse{}, err
	}
	response, err := c.request(ctx, http.MethodPost,
		"/api/v1/edge/raw-manifests", bytes.NewReader(body))
	if err != nil {
		return worker.RawManifestResponse{}, err
	}
	defer response.Body.Close()
	var payload struct {
		ManifestSHA256 string `json:"manifest_sha256"`
		ForceFull      bool   `json:"full_resync_required"`
		Reason         string `json:"reason"`
		Detail         string `json:"detail"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return worker.RawManifestResponse{}, err
	}
	reason := payload.Reason
	if payload.Detail != "" {
		reason = payload.Detail
	}
	return worker.RawManifestResponse{
		StatusCode: response.StatusCode, ManifestSHA256: payload.ManifestSHA256,
		ForceFull: payload.ForceFull, Reason: reason,
	}, nil
}
