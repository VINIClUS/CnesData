// Package auth implements RFC 8628 OAuth Device Flow client for the agent.
//
// The agent calls Client.Authorize() to start the flow (gets device_code +
// user_code), prints the user_code + verification URI to the tech, then
// loops on (*DeviceFlow).Poll() until a Token resolves or a terminal error
// fires.
//
// Server-side trust: caller must use HTTPS and verify the central_api root
// CA cert. This package does not enforce TLS settings.
package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Sentinel errors mapped from RFC 8628 §3.5 server responses.
var (
	ErrExpiredToken     = errors.New("oauth: device code expired")
	ErrAccessDenied     = errors.New("oauth: user denied authorization")
	ErrInvalidGrant     = errors.New("oauth: invalid grant")
	ErrUnsupportedGrant = errors.New("oauth: unsupported grant type")
	ErrInvalidClient    = errors.New("oauth: invalid client")
)

const (
	deviceCodeGrant = "urn:ietf:params:oauth:grant-type:device_code"
	maxInterval     = 60 * time.Second
)

// Client is a stateless OAuth Device Flow client.
//
// Construct via NewClient. Safe for concurrent use across multiple
// independent device flows; not safe to call Poll concurrently on the
// same DeviceFlow.
type Client struct {
	baseURL    string
	httpClient *http.Client
	clock      func() time.Time
	sleep      func(time.Duration)
}

// DeviceFlow holds state for one device flow lifecycle.
//
// NOT goroutine-safe — single caller assumption.
type DeviceFlow struct {
	client                  *Client
	deviceCode              string
	UserCode                string
	VerificationURI         string
	VerificationURIComplete string
	ExpiresAt               time.Time
	interval                time.Duration
}

// Token is the result of a successful device flow.
type Token struct {
	AccessToken  string
	TokenType    string
	ExpiresIn    time.Duration
	RefreshToken string
}

// NewClient builds an OAuth Device Flow client.
//
// baseURL: central_api root (e.g. "https://central.example").
// httpClient: pass nil to use http.DefaultClient.
func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
		clock:      time.Now,
		sleep:      time.Sleep,
	}
}

// Authorize calls POST /oauth/device_authorization. Returns a populated
// DeviceFlow on success.
func (c *Client) Authorize(ctx context.Context, scope string) (*DeviceFlow, error) {
	body, err := json.Marshal(map[string]string{
		"client_id": "agent",
		"scope":     scope,
	})
	if err != nil {
		return nil, fmt.Errorf("oauth: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/oauth/device_authorization", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("oauth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("oauth: network: %w", err)
	}
	defer resp.Body.Close()
	rawBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("oauth: device_authorization failed status=%d body=%s",
			resp.StatusCode, rawBody)
	}
	var parsed struct {
		DeviceCode              string `json:"device_code"`
		UserCode                string `json:"user_code"`
		VerificationURI         string `json:"verification_uri"`
		VerificationURIComplete string `json:"verification_uri_complete"`
		ExpiresIn               int    `json:"expires_in"`
		Interval                int    `json:"interval"`
	}
	if err := json.Unmarshal(rawBody, &parsed); err != nil {
		return nil, fmt.Errorf("oauth: malformed response body=%s: %w", rawBody, err)
	}
	return &DeviceFlow{
		client:                  c,
		deviceCode:              parsed.DeviceCode,
		UserCode:                parsed.UserCode,
		VerificationURI:         parsed.VerificationURI,
		VerificationURIComplete: parsed.VerificationURIComplete,
		ExpiresAt:               c.clock().Add(time.Duration(parsed.ExpiresIn) * time.Second),
		interval:                time.Duration(parsed.Interval) * time.Second,
	}, nil
}

// Poll loops until the device flow resolves. Returns Token on success or
// terminal error: ErrExpiredToken, ErrAccessDenied, ErrInvalidGrant,
// ErrUnsupportedGrant, ErrInvalidClient.
//
// Honors RFC 8628 §3.5 polling rules:
//   - sleep `interval` between polls
//   - on slow_down: server-supplied interval (or 2x local), capped at 60s
//   - on authorization_pending: continue
//   - on 200: return Token
//   - on terminal error: return wrapped sentinel
//
// Respects context cancellation between polls.
// NOT goroutine-safe.
func (d *DeviceFlow) Poll(ctx context.Context) (*Token, error) {
	for {
		if d.client.clock().After(d.ExpiresAt) {
			return nil, ErrExpiredToken
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		d.client.sleep(d.interval)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		tok, retryAfter, err := d.pollOnce(ctx)
		if err != nil {
			return nil, err
		}
		if tok != nil {
			return tok, nil
		}
		if retryAfter > 0 {
			d.interval = retryAfter
			if d.interval > maxInterval {
				d.interval = maxInterval
			}
		}
	}
}

// pollOnce returns (token, retryAfter, err):
//   - token != nil    → success, caller returns
//   - retryAfter > 0  → slow_down with new interval to apply
//   - retryAfter == 0 + err == nil → authorization_pending, caller loops
//   - err != nil      → terminal error
func (d *DeviceFlow) pollOnce(ctx context.Context) (*Token, time.Duration, error) {
	rawBody, status, err := d.doTokenRequest(ctx)
	if err != nil {
		return nil, 0, err
	}
	if status == http.StatusOK {
		return parseTokenResponse(rawBody)
	}
	if status != http.StatusBadRequest {
		return nil, 0, fmt.Errorf("oauth: server error status=%d body=%s", status, rawBody)
	}
	return d.classifyErrorResponse(rawBody)
}

// doTokenRequest executes POST /oauth/token and returns (body, statusCode, err).
func (d *DeviceFlow) doTokenRequest(ctx context.Context) ([]byte, int, error) {
	form := url.Values{}
	form.Set("grant_type", deviceCodeGrant)
	form.Set("device_code", d.deviceCode)
	form.Set("client_id", "agent")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		d.client.baseURL+"/oauth/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, 0, fmt.Errorf("oauth: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := d.client.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("oauth: network: %w", err)
	}
	defer resp.Body.Close()
	rawBody, _ := io.ReadAll(resp.Body)
	return rawBody, resp.StatusCode, nil
}

// parseTokenResponse decodes a 200 OK body into a Token.
func parseTokenResponse(rawBody []byte) (*Token, time.Duration, error) {
	var t struct {
		AccessToken  string `json:"access_token"`
		TokenType    string `json:"token_type"`
		ExpiresIn    int    `json:"expires_in"`
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.Unmarshal(rawBody, &t); err != nil {
		return nil, 0, fmt.Errorf("oauth: malformed token response body=%s: %w", rawBody, err)
	}
	if t.AccessToken == "" {
		return nil, 0, fmt.Errorf("oauth: token response missing access_token body=%s", rawBody)
	}
	return &Token{
		AccessToken:  t.AccessToken,
		TokenType:    t.TokenType,
		ExpiresIn:    time.Duration(t.ExpiresIn) * time.Second,
		RefreshToken: t.RefreshToken,
	}, 0, nil
}

// classifyErrorResponse decodes a 400 error body and maps it to sentinel errors.
func (d *DeviceFlow) classifyErrorResponse(rawBody []byte) (*Token, time.Duration, error) {
	var eb struct {
		Error    string `json:"error"`
		Interval int    `json:"interval"`
	}
	if err := json.Unmarshal(rawBody, &eb); err != nil {
		return nil, 0, fmt.Errorf("oauth: malformed error response body=%s: %w", rawBody, err)
	}
	switch eb.Error {
	case "authorization_pending":
		return nil, 0, nil
	case "slow_down":
		next := time.Duration(eb.Interval) * time.Second
		if next == 0 {
			next = d.interval * 2
		}
		return nil, next, nil
	case "expired_token":
		return nil, 0, fmt.Errorf("%w: body=%s", ErrExpiredToken, rawBody)
	case "access_denied":
		return nil, 0, fmt.Errorf("%w: body=%s", ErrAccessDenied, rawBody)
	case "invalid_grant":
		return nil, 0, fmt.Errorf("%w: body=%s", ErrInvalidGrant, rawBody)
	case "unsupported_grant_type":
		return nil, 0, fmt.Errorf("%w: body=%s", ErrUnsupportedGrant, rawBody)
	case "invalid_client":
		return nil, 0, fmt.Errorf("%w: body=%s", ErrInvalidClient, rawBody)
	default:
		return nil, 0, fmt.Errorf("oauth: unknown error code=%s body=%s", eb.Error, rawBody)
	}
}
