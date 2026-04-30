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

// Compile-time guards for symbols wired in Tasks 2-4 (parseCurrentCert,
// isWithinWindow, postRotate, persistRotated). Removed as those wire up.
var (
	_ = pem.Decode
	_ = io.Copy
	_ = x509.ParseCertificate
	_ = json.Marshal
	_ = http.MethodPost
	_ = strings.TrimRight
	_ = fmt.Errorf
	_ = rotateResp{}
)
