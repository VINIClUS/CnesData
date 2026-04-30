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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
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
// shrink this to milliseconds.
//
//nolint:unused // wired in Phase 6 step 6 (provision_cert retry loop)
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

	oauth := auth.NewClient(flags.BaseURL, bootstrapHTTP)
	flow, err := oauth.Authorize(ctx, flags.Scope)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: device authorize:", err)
		return errExitCode(err)
	}
	printVerification(os.Stdout, flow)

	tok, err := flow.Poll(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "register: device poll:", err)
		return errExitCode(err)
	}
	_ = tok // wired in next task (CSR + /provision/cert)
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
