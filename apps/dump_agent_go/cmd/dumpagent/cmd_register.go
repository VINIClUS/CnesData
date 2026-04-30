// Package main — cmd_register: `dumpagent register` subcommand.
//
// One-shot enrollment flow:
//   parse flags → load CA pin → bootstrap HTTPS → device flow →
//   keypair + CSR → POST /provision/cert (3x retry) →
//   persist cert/key/refresh → mTLS smoke probe → exit 0
//
// Exit codes:
//
//	0  success
//	1  local I/O / boot fatal
//	2  usage / pre-condition (flag missing, cert exists no --force)
//	3  network / device-flow non-fatal
//	4  /provision/cert failed after retries
//	5  persistence or --ca-pin file unreadable
//	6  device-code expired (rerun)
//	7  access denied (operator clicked deny)
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
	"github.com/cnesdata/dumpagent/internal/platform"
)

type registerFlags struct {
	TenantID  string
	BaseURL   string
	CAPinPath string
	Scope     string
	Force     bool
	NoSmoke   bool
}

var (
	errUsage           = errors.New("register: usage error")
	errProvisionFailed = errors.New("register: provision_cert failed")
	errPersistFailed   = errors.New("register: persist failed")
)

// backoffSequence drives /provision/cert retry. Tests in the same package
// shrink this to milliseconds via fastBackoff.
var backoffSequence = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
}

func cmdRegister(args []string) int {
	flags, err := parseRegisterFlags(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	return runRegisterFlow(flags)
}

// runRegisterFlow is the side-effecting body of cmdRegister; cmdRegister
// stays small enough to satisfy the 50-LOC project hard limit.
func runRegisterFlow(flags registerFlags) int {
	caPEM, err := loadCAPin(flags.CAPinPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return errExitCode(err)
	}
	authDir, err := auth.AuthDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		return 1
	}
	if !flags.Force && certExists(authDir) {
		fmt.Fprintln(os.Stderr, "already_registered (use --force)")
		return 2
	}
	bootstrapHTTP, err := newBootstrapClient(caPEM)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 5
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	tok, code, err := runDeviceFlow(ctx, bootstrapHTTP, flags)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		return code
	}
	return finishProvisioning(ctx, bootstrapHTTP, authDir, caPEM, tok, flags)
}

// runDeviceFlow performs Authorize + Poll, printing the verification block.
// Returns the token on success, or (nil, exitCode, err) on terminal error.
func runDeviceFlow(
	ctx context.Context, httpClient *http.Client, flags registerFlags,
) (*auth.Token, int, error) {
	oauth := auth.NewClient(flags.BaseURL, httpClient)
	flow, err := oauth.Authorize(ctx, flags.Scope)
	if err != nil {
		return nil, errExitCode(err), fmt.Errorf("device authorize: %w", err)
	}
	printVerification(os.Stdout, flow)
	tok, err := flow.Poll(ctx)
	if err != nil {
		return nil, errExitCode(err), fmt.Errorf("device poll: %w", err)
	}
	return tok, 0, nil
}

// finishProvisioning derives machineID, generates CSR, posts /provision/cert
// (with retry), persists artifacts, and runs the optional smoke probe.
func finishProvisioning(
	ctx context.Context, httpClient *http.Client, authDir string, caPEM []byte,
	tok *auth.Token, flags registerFlags,
) int {
	appData, err := platform.AppDataDir()
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: app_data_dir:", err)
		return 1
	}
	machineID, err := platform.ResolveMachineID(appData)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: machine_id:", err)
		return 1
	}
	key, csrPEM, err := auth.GenerateKeyAndCSR(machineID)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: csr:", err)
		return 1
	}
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: pkcs8:", err)
		return 1
	}
	fingerprint := "fp:" + machineID
	resp, err := postProvisionCert(ctx, httpClient, flags.BaseURL,
		tok.AccessToken, string(csrPEM), fingerprint)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		return errExitCode(err)
	}
	if err := persistAll(authDir, resp, pkcs8); err != nil {
		fmt.Fprintln(os.Stderr, "register:", err)
		return errExitCode(err)
	}
	if !flags.NoSmoke {
		smokeMTLS(authDir, flags.BaseURL, flags.TenantID, caPEM)
	}
	fmt.Printf("registered cert_path=%s expires_at=%s\n",
		filepath.Join(authDir, "cert.pem"), resp.ExpiresAt)
	return 0
}

func parseRegisterFlags(args []string) (registerFlags, error) {
	fs := flag.NewFlagSet("register", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	tenantID := fs.String("tenant-id", "", "tenant UUID for X-Tenant-Id (required)")
	baseURL := fs.String("base-url", "", "central_api root URL (required)")
	caPin := fs.String("ca-pin", "", "override embedded CA pin (file with PEM cert)")
	scope := fs.String("scope", "agent", "OAuth scope")
	force := fs.Bool("force", false, "overwrite existing cert+key+refresh")
	noSmoke := fs.Bool("no-smoke", false, "skip post-register mTLS health probe")
	if err := fs.Parse(args); err != nil {
		return registerFlags{}, fmt.Errorf("%w: %v", errUsage, err)
	}
	if *tenantID == "" {
		return registerFlags{}, fmt.Errorf("%w: --tenant-id required", errUsage)
	}
	if *baseURL == "" {
		return registerFlags{}, fmt.Errorf("%w: --base-url required", errUsage)
	}
	return registerFlags{
		TenantID:  *tenantID,
		BaseURL:   *baseURL,
		CAPinPath: *caPin,
		Scope:     *scope,
		Force:     *force,
		NoSmoke:   *noSmoke,
	}, nil
}

// loadCAPin returns PEM bytes from --ca-pin path if non-empty, else the
// embedded auth.CAPinPEM. Wraps file-read errors with errPersistFailed
// (exit 5).
func loadCAPin(path string) ([]byte, error) {
	if path == "" {
		return auth.CAPinPEM, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("%w: read --ca-pin: %v", errPersistFailed, err)
	}
	return b, nil
}

// newBootstrapClient builds an HTTPS-only http.Client that pins server certs
// to caPEM. No client cert (mTLS) — used for /oauth/* and /provision/cert
// during enrollment, before the agent has its own leaf cert.
func newBootstrapClient(caPEM []byte) (*http.Client, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("register: ca pin pem invalid")
	}
	tlsCfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS13,
	}
	return &http.Client{
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
		Timeout:   60 * time.Second,
	}, nil
}

// certExists reports whether <dir>/cert.pem exists (any size, regular file).
func certExists(dir string) bool {
	info, err := os.Stat(dir + "/cert.pem")
	if err != nil {
		return false
	}
	return info.Mode().IsRegular()
}

// printVerification dumps the operator-facing fields to w. Format is
// fixed-column for grep-friendly logs.
func printVerification(w io.Writer, flow *auth.DeviceFlow) {
	fmt.Fprintf(w, "user_code:        %s\n", flow.UserCode)
	fmt.Fprintf(w, "verification_uri: %s\n", flow.VerificationURI)
	fmt.Fprintf(w, "complete_uri:     %s\n", flow.VerificationURIComplete)
	fmt.Fprintf(w, "expires_at:       %s\n", flow.ExpiresAt.UTC().Format(time.RFC3339))
}

// errExitCode maps device-flow + register sentinels to exit codes.
func errExitCode(err error) int {
	switch {
	case errors.Is(err, auth.ErrAccessDenied):
		return 7
	case errors.Is(err, auth.ErrExpiredToken):
		return 6
	case errors.Is(err, auth.ErrInvalidGrant),
		errors.Is(err, auth.ErrUnsupportedGrant),
		errors.Is(err, auth.ErrInvalidClient):
		return 3
	case errors.Is(err, errProvisionFailed):
		return 4
	case errors.Is(err, errPersistFailed):
		return 5
	case errors.Is(err, errUsage):
		return 2
	default:
		return 3
	}
}

type provisionResp struct {
	CertPEM      string `json:"cert_pem"`
	CAChainPEM   string `json:"ca_chain_pem"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    string `json:"expires_at"`
}

// postProvisionCert POSTs to /provision/cert with bearer auth + JSON body.
// Retries on net err and 5xx per backoffSequence; aborts on 4xx.
// Wraps final error with errProvisionFailed (exit 4).
func postProvisionCert(
	ctx context.Context, httpClient *http.Client, baseURL, accessToken,
	csrPEM, fingerprint string,
) (*provisionResp, error) {
	body, err := json.Marshal(map[string]string{
		"csr_pem":             csrPEM,
		"machine_fingerprint": fingerprint,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %v", errProvisionFailed, err)
	}
	url := strings.TrimRight(baseURL, "/") + "/provision/cert"
	var lastErr error
	for attempt := 0; attempt < len(backoffSequence); attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("%w: %v", errProvisionFailed, ctx.Err())
			case <-time.After(backoffSequence[attempt-1]):
			}
		}
		raw, parsed, status, err := doProvisionRequest(ctx, httpClient, url, accessToken, body)
		if err != nil {
			lastErr = err
			continue
		}
		if status == http.StatusOK {
			return parsed, nil
		}
		if status >= 400 && status < 500 {
			return nil, fmt.Errorf("%w: status=%d body=%s", errProvisionFailed, status, raw)
		}
		lastErr = fmt.Errorf("status=%d body=%s", status, raw)
	}
	return nil, fmt.Errorf("%w: after %d attempts: %v",
		errProvisionFailed, len(backoffSequence), lastErr)
}

func doProvisionRequest(
	ctx context.Context, httpClient *http.Client, url, accessToken string, body []byte,
) (rawBody []byte, parsed *provisionResp, status int, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, nil, 0, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("network: %w", err)
	}
	defer resp.Body.Close()
	rawBody, _ = io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return rawBody, nil, resp.StatusCode, nil
	}
	var p provisionResp
	if err := json.Unmarshal(rawBody, &p); err != nil {
		return rawBody, nil, resp.StatusCode, fmt.Errorf("parse: %w", err)
	}
	return rawBody, &p, resp.StatusCode, nil
}

// persistAll writes key → cert → refresh in that order. Returns
// errPersistFailed-wrapped error on any step.
func persistAll(authDir string, resp *provisionResp, pkcs8DER []byte) error {
	if err := auth.SaveKey(authDir, pkcs8DER); err != nil {
		return fmt.Errorf("%w: SaveKey: %v", errPersistFailed, err)
	}
	if err := auth.SaveCert(authDir, []byte(resp.CertPEM)); err != nil {
		return fmt.Errorf("%w: SaveCert: %v", errPersistFailed, err)
	}
	if err := auth.SaveRefreshToken(authDir, resp.RefreshToken); err != nil {
		return fmt.Errorf("%w: SaveRefreshToken: %v", errPersistFailed, err)
	}
	return nil
}

// smokeMTLS placeholder; Task 8 wires real mTLS health probe.
func smokeMTLS(authDir, baseURL, tenantID string, caPEM []byte) {
	_ = authDir
	_ = baseURL
	_ = tenantID
	_ = caPEM
}
