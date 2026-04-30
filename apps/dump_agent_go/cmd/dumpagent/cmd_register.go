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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
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
	errUsage = errors.New("register: usage error")
	//nolint:unused // wired in Phase 6 step 6 (provision_cert flow)
	errProvisionFailed = errors.New("register: provision_cert failed")
	//nolint:unused // wired in Phase 6 step 7 (persistence flow)
	errPersistFailed = errors.New("register: persist failed")
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
	_ = flags
	// Subsequent tasks populate this body.
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
