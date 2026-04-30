// Package auth — rotate: background cert rotation loop.
//
// Spawned at dumpagent run startup. Wakes every ~6h (with jitter), checks
// cert TTL, calls POST /provision/cert/rotate over mTLS when within
// rotation window (remaining < total/3), persists new cert + key,
// triggers transport.Client.Reload() for in-process hot-swap.
//
// Loop terminates on:
//   - context cancel (returns nil — graceful)
//   - 4xx server response (returns ErrRotateTerminal — operator must
//     run `dumpagent register --force` to recover)
package auth

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"
)

var (
	ErrRotateNotDue           = errors.New("rotate: cert not within rotation window")
	ErrRotateTerminal         = errors.New("rotate: terminal server error")
	ErrRotateRetriesExhausted = errors.New("rotate: retries exhausted")
)

// RotateClient is the subset of *transport.Client used by Rotator.
// Production: *transport.Client. Tests inject fakes.
type RotateClient interface {
	HTTPClient() *http.Client
	Reload() error
}

// Rotator owns cert rotation lifecycle. Single-instance assumption.
type Rotator struct {
	client    RotateClient
	authDir   string
	baseURL   string
	machineID string
	interval  time.Duration
	fraction  float64
	backoff   []time.Duration
	clock     func() time.Time
	rand      func() float64
	logger    *slog.Logger
}

// NewRotator builds a Rotator with sane defaults: 6h interval, 1/3 TTL
// fraction, [1s, 2s, 4s] backoff, time.Now clock, math/rand v2 jitter,
// slog.Default() logger.
func NewRotator(client RotateClient, authDir, baseURL, machineID string) *Rotator {
	return &Rotator{
		client:    client,
		authDir:   authDir,
		baseURL:   baseURL,
		machineID: machineID,
		interval:  6 * time.Hour,
		fraction:  1.0 / 3.0,
		backoff:   []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second},
		clock:     time.Now,
		rand:      rand.Float64,
		logger:    slog.Default(),
	}
}

// rotateResp is the JSON payload returned by /provision/cert/rotate.
// No refresh_token field — server preserves the existing one.
type rotateResp struct {
	CertPEM    string `json:"cert_pem"`
	CAChainPEM string `json:"ca_chain_pem"`
	ExpiresAt  string `json:"expires_at"`
}

func parseCurrentCert(authDir string) (*x509.Certificate, error) {
	pemBytes, err := LoadCert(authDir)
	if err != nil {
		return nil, fmt.Errorf("load cert: %w", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("decode cert: not PEM CERTIFICATE")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse cert: %w", err)
	}
	return cert, nil
}

// isWithinWindow returns true when the cert's remaining lifetime is below
// the configured fraction of its total lifetime.
//
// remaining := NotAfter - now
// total     := NotAfter - NotBefore
// rotate when remaining < total*fraction
func isWithinWindow(cert *x509.Certificate, fraction float64, now time.Time) bool {
	remaining := cert.NotAfter.Sub(now)
	total := cert.NotAfter.Sub(cert.NotBefore)
	threshold := time.Duration(float64(total) * fraction)
	return remaining < threshold
}

// Run is the loop entry. Implemented in Task 4.
func (r *Rotator) Run(ctx context.Context) error {
	_ = ctx
	return nil
}

// RotateOnce is one cycle. Implemented in Tasks 2-3.
func (r *Rotator) RotateOnce(ctx context.Context) error {
	_ = ctx
	return ErrRotateNotDue
}

func postRotate(
	ctx context.Context, httpClient *http.Client, baseURL, csrPEM string,
	backoff []time.Duration,
) (*rotateResp, error) {
	body, err := json.Marshal(map[string]string{"csr_pem": csrPEM})
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %v", ErrRotateRetriesExhausted, err)
	}
	url := strings.TrimRight(baseURL, "/") + "/provision/cert/rotate"
	var lastErr error
	for attempt := 0; attempt < len(backoff); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", ErrRotateRetriesExhausted, ctx.Err())
			case <-time.After(backoff[attempt-1]):
			}
		}
		raw, parsed, status, err := doRotateRequest(ctx, httpClient, url, body)
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusOK {
			return parsed, nil
		}
		if status >= 400 && status < 500 {
			return nil, fmt.Errorf("%w: status=%d body=%s", ErrRotateTerminal, status, raw)
		}
		lastErr = fmt.Errorf("status=%d body=%s", status, raw)
	}
	return nil, fmt.Errorf("%w: %v", ErrRotateRetriesExhausted, lastErr)
}

func doRotateRequest(
	ctx context.Context, httpClient *http.Client, url string, body []byte,
) (rawBody []byte, parsed *rotateResp, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("network: %w", err)
	}
	defer resp.Body.Close()
	rawBody, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return rawBody, nil, resp.StatusCode, nil
	}
	var p rotateResp
	if err := json.Unmarshal(rawBody, &p); err != nil {
		return rawBody, nil, resp.StatusCode, fmt.Errorf("parse: %w", err)
	}
	return rawBody, &p, resp.StatusCode, nil
}

// persistRotated writes new key + new cert atomically. Refresh token is
// preserved (server doesn't return one in rotate response).
func persistRotated(authDir string, resp *rotateResp, pkcs8DER []byte) error {
	if err := SaveKey(authDir, pkcs8DER); err != nil {
		return fmt.Errorf("save key: %w", err)
	}
	if err := SaveCert(authDir, []byte(resp.CertPEM)); err != nil {
		return fmt.Errorf("save cert: %w", err)
	}
	return nil
}
