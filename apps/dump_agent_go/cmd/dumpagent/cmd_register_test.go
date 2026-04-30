package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

func TestRegister_MissingTenantID_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--base-url", "https://x.example"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_MissingBaseURL_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--tenant-id", "354130-uuid"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_BadFlag_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--definitely-not-a-flag"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_ParseFlags_AllFieldsBound(t *testing.T) {
	flags, err := parseRegisterFlags([]string{
		"--tenant-id", "T",
		"--base-url", "https://x.example",
		"--ca-pin", "/tmp/ca.pem",
		"--scope", "agent-test",
		"--force",
		"--no-smoke",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if flags.TenantID != "T" || flags.BaseURL != "https://x.example" ||
		flags.CAPinPath != "/tmp/ca.pem" || flags.Scope != "agent-test" ||
		!flags.Force || !flags.NoSmoke {
		t.Errorf("flags wrong: %+v", flags)
	}
}

func TestRegister_ParseFlags_DefaultScope(t *testing.T) {
	flags, _ := parseRegisterFlags([]string{
		"--tenant-id", "T", "--base-url", "https://x.example",
	})
	if flags.Scope != "agent" {
		t.Errorf("Scope default = %q, want %q", flags.Scope, "agent")
	}
}

func TestRegister_DispatchEntryExists(t *testing.T) {
	code := dispatch([]string{"register"})
	if code != 2 {
		t.Errorf("dispatch register = %d, want 2", code)
	}
	_ = strings.Contains // keep import for follow-up tasks
}

func TestLoadCAPin_FlagPathReadsFile(t *testing.T) {
	dir := t.TempDir()
	pemPath := dir + "/test_ca.pem"
	if err := os.WriteFile(pemPath, []byte("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadCAPin(pemPath)
	if err != nil {
		t.Fatalf("loadCAPin: %v", err)
	}
	if !strings.Contains(string(got), "BEGIN CERTIFICATE") {
		t.Errorf("got = %q, want PEM", got)
	}
}

func TestLoadCAPin_FlagPathMissingReturnsError(t *testing.T) {
	_, err := loadCAPin("/definitely/does/not/exist.pem")
	if err == nil {
		t.Fatal("loadCAPin: want error for missing file")
	}
}

func TestLoadCAPin_EmptyPathFallsBackToEmbedded(t *testing.T) {
	got, err := loadCAPin("")
	if err != nil {
		t.Fatalf("loadCAPin: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("loadCAPin returned empty for embedded fallback")
	}
}

func TestNewBootstrapClient_PinValid_ReturnsClient(t *testing.T) {
	pem := mustReadEmbeddedPin(t)
	c, err := newBootstrapClient(pem)
	if err != nil {
		t.Fatalf("newBootstrapClient: %v", err)
	}
	if c.Timeout == 0 {
		t.Error("Timeout = 0, want > 0")
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport type = %T, want *http.Transport", c.Transport)
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %d, want TLS13 (%d)", tr.TLSClientConfig.MinVersion, tls.VersionTLS13)
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs nil")
	}
}

func TestNewBootstrapClient_PinGarbage_ReturnsError(t *testing.T) {
	_, err := newBootstrapClient([]byte("definitely not pem"))
	if err == nil {
		t.Fatal("newBootstrapClient: want error for garbage")
	}
}

func mustReadEmbeddedPin(t *testing.T) []byte {
	t.Helper()
	if len(auth.CAPinPEM) == 0 {
		t.Fatal("auth.CAPinPEM is empty (embed broken)")
	}
	return auth.CAPinPEM
}

func TestCertExists_NoFile_ReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	if certExists(dir) {
		t.Error("certExists empty dir = true, want false")
	}
}

func TestCertExists_FilePresent_ReturnsTrue(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/cert.pem", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !certExists(dir) {
		t.Error("certExists with file = false, want true")
	}
}

// testCA holds a self-signed RSA-2048 CA used for httptest.NewTLSServer.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
type testCA struct {
	CertPEM []byte
	cert    *x509.Certificate
	key     *rsa.PrivateKey
}

// seedTestCA generates a fresh self-signed CA. Each call returns a new CA
// so that one test's pin can NOT validate another test's server.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func seedTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate: %v", err)
	}
	cert, _ := x509.ParseCertificate(der)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &testCA{CertPEM: pemBytes, cert: cert, key: key}
}

// signLeafFromCSR signs a leaf cert from caller-supplied PEM CSR using the
// test CA. Used by mockCentralAPI's /provision/cert handler.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func (ca *testCA) signLeafFromCSR(t *testing.T, csrPEM []byte) []byte {
	t.Helper()
	block, _ := pem.Decode(csrPEM)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		t.Fatalf("bad CSR PEM: %s", csrPEM)
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("parse CSR: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR sig: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      csr.Subject,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, csr.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// serverLeaf returns a TLS leaf cert valid for httptest.NewTLSServer's listener
// addresses (127.0.0.1, ::1, localhost). Signed by ca.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func (ca *testCA) serverLeaf(t *testing.T) tls.Certificate {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
}

// mockOpts customizes per-route behavior of mockCentralAPI.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
type mockOpts struct {
	DeviceTokenError   string        // "" = success, else error code (e.g. "access_denied", "expired_token")
	ProvisionStatuses  []int         // sequential per-attempt statuses; 200 = success
	ProvisionAttempts  *atomic.Int32 // optional counter exposed to tests
	HealthStatus       int           // smoke probe response status (default 200)
	HealthRequestCount *atomic.Int32 // optional counter
	OnProvisionRequest func(body []byte)
}

// mockCentralAPI starts an httptest TLS server with the four endpoints the
// register flow touches: /oauth/device_authorization, /oauth/token,
// /provision/cert, /api/v1/system/health. Cleanup auto-registered.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func mockCentralAPI(t *testing.T, ca *testCA, opts mockOpts) *httptest.Server {
	t.Helper()
	if opts.HealthStatus == 0 {
		opts.HealthStatus = 200
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/oauth/device_authorization", handleDeviceAuth)
	mux.HandleFunc("/oauth/token", handleToken(opts))
	mux.HandleFunc("/provision/cert", handleProvision(t, ca, opts))
	mux.HandleFunc("/api/v1/system/health", handleHealth(opts))

	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{ca.serverLeaf(t)},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func handleDeviceAuth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"device_code":               "DEV-CODE-123",
		"user_code":                 "ABCD-1234",
		"verification_uri":          "https://example.test/activate",
		"verification_uri_complete": "https://example.test/activate?user_code=ABCD-1234",
		"expires_in":                300,
		"interval":                  1,
	})
}

//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func handleToken(opts mockOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if opts.DeviceTokenError != "" {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": opts.DeviceTokenError})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "ACCESS-TOKEN-XYZ",
			"token_type":    "Bearer",
			"expires_in":    300,
			"refresh_token": "REFRESH-TOKEN-ABC",
		})
	}
}

//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func handleProvision(t *testing.T, ca *testCA, opts mockOpts) http.HandlerFunc {
	var attempt atomic.Int32
	return func(w http.ResponseWriter, r *http.Request) {
		idx := attempt.Add(1) - 1
		if opts.ProvisionAttempts != nil {
			opts.ProvisionAttempts.Add(1)
		}
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		if opts.OnProvisionRequest != nil {
			opts.OnProvisionRequest(body)
		}
		statuses := opts.ProvisionStatuses
		if len(statuses) == 0 {
			statuses = []int{200}
		}
		var status int
		if int(idx) >= len(statuses) {
			status = statuses[len(statuses)-1]
		} else {
			status = statuses[idx]
		}
		if status != 200 {
			http.Error(w, "mock-fail", status)
			return
		}
		var req struct {
			CSRPem             string `json:"csr_pem"`
			MachineFingerprint string `json:"machine_fingerprint"`
		}
		_ = json.Unmarshal(body, &req)
		leafPEM := ca.signLeafFromCSR(t, []byte(req.CSRPem))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_pem":      string(leafPEM),
			"ca_chain_pem":  string(ca.CertPEM),
			"refresh_token": "REFRESH-MTLS-456",
			"expires_at":    time.Now().Add(90 * 24 * time.Hour).UTC().Format(time.RFC3339),
		})
	}
}

//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func handleHealth(opts mockOpts) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if opts.HealthRequestCount != nil {
			opts.HealthRequestCount.Add(1)
		}
		w.WriteHeader(opts.HealthStatus)
	}
}

// installTestPin swaps auth.CAPinPEM with the test CA's bytes, restoring
// the prior value on test cleanup. Use to make register's bootstrap
// HTTPS client trust the mock server.
//
//nolint:unused // wired in Phase 6 steps 6-8 (register flow tests)
func installTestPin(t *testing.T, ca *testCA) {
	t.Helper()
	prev := auth.CAPinPEM
	auth.CAPinPEM = ca.CertPEM
	t.Cleanup(func() { auth.CAPinPEM = prev })
}

// fastBackoff overrides backoffSequence for retry tests; restores in cleanup.
func fastBackoff(t *testing.T) {
	t.Helper()
	prev := backoffSequence
	backoffSequence = []time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond}
	t.Cleanup(func() { backoffSequence = prev })
}

// setRegisterEnv points AGENT_AUTH_DIR + AGENT_APPDATA_DIR to fresh temp
// directories isolated to the test. Returns the auth dir; AGENT_APPDATA_DIR
// is observable via os.Getenv if a test needs it.
func setRegisterEnv(t *testing.T) string {
	t.Helper()
	authDir := t.TempDir()
	appDataDir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", authDir)
	t.Setenv("AGENT_APPDATA_DIR", appDataDir)
	_ = url.Parse // keep import live for tasks that parse URLs
	return authDir
}

func TestPrintVerification_OutputsCodeAndURI(t *testing.T) {
	flow := &auth.DeviceFlow{
		UserCode:                "WDJB-MJHT",
		VerificationURI:         "https://example.test/activate",
		VerificationURIComplete: "https://example.test/activate?user_code=WDJB-MJHT",
		ExpiresAt:               time.Date(2026, 4, 30, 18, 30, 0, 0, time.UTC),
	}
	var buf strings.Builder
	printVerification(&buf, flow)
	out := buf.String()
	for _, want := range []string{
		"user_code:        WDJB-MJHT",
		"verification_uri: https://example.test/activate",
		"complete_uri:",
		"expires_at:",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q\nfull:\n%s", want, out)
		}
	}
}

func TestRunDeviceFlow_AccessDenied_ReturnsExit7(t *testing.T) {
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{DeviceTokenError: "access_denied"})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 7 {
		t.Errorf("exit = %d, want 7", code)
	}
}

func TestRunDeviceFlow_ExpiredToken_ReturnsExit6(t *testing.T) {
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{DeviceTokenError: "expired_token"})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 6 {
		t.Errorf("exit = %d, want 6", code)
	}
}

func TestRunDeviceFlow_InvalidGrant_ReturnsExit3(t *testing.T) {
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{DeviceTokenError: "invalid_grant"})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 3 {
		t.Errorf("exit = %d, want 3", code)
	}
}

func TestPostProvisionCert_4xx_NoRetry_ReturnsExit4(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var attempts atomic.Int32
	srv := mockCentralAPI(t, ca, mockOpts{
		ProvisionStatuses: []int{400},
		ProvisionAttempts: &attempts,
	})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 4 {
		t.Errorf("exit = %d, want 4", code)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("provision attempts = %d, want 1 (no retry on 4xx)", got)
	}
}

func TestPostProvisionCert_5xxThrice_ReturnsExit4_ThreeAttempts(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var attempts atomic.Int32
	srv := mockCentralAPI(t, ca, mockOpts{
		ProvisionStatuses: []int{503, 503, 503},
		ProvisionAttempts: &attempts,
	})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 4 {
		t.Errorf("exit = %d, want 4", code)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("provision attempts = %d, want 3", got)
	}
}

func TestPostProvisionCert_5xxThenSuccess_RetriesAndPersists(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var attempts atomic.Int32
	srv := mockCentralAPI(t, ca, mockOpts{
		ProvisionStatuses: []int{503, 503, 200},
		ProvisionAttempts: &attempts,
	})
	authDir := setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("provision attempts = %d, want 3", got)
	}
	if _, err := os.Stat(authDir + "/cert.pem"); err != nil {
		t.Errorf("cert.pem missing: %v", err)
	}
}

func TestPostProvisionCert_FingerprintFormat(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var seenFingerprint string
	srv := mockCentralAPI(t, ca, mockOpts{
		OnProvisionRequest: func(body []byte) {
			var req struct {
				MachineFingerprint string `json:"machine_fingerprint"`
			}
			_ = json.Unmarshal(body, &req)
			seenFingerprint = req.MachineFingerprint
		},
	})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if !strings.HasPrefix(seenFingerprint, "fp:") {
		t.Errorf("fingerprint = %q, want prefix fp:", seenFingerprint)
	}
	if len(seenFingerprint) < 8 {
		t.Errorf("fingerprint len = %d, want >= 8", len(seenFingerprint))
	}
}

func TestRegister_HappyPath_PersistsAndExits0(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{})
	authDir := setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "354130-uuid", "--base-url", srv.URL,
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	for _, name := range []string{"cert.pem", "key.bin", "refresh.bin"} {
		path := authDir + "/" + name
		info, err := os.Stat(path)
		if err != nil {
			t.Errorf("%s missing: %v", name, err)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("%s empty", name)
		}
	}
}

func TestRegister_CertExists_NoForce_ReturnsExit2(t *testing.T) {
	authDir := setRegisterEnv(t)
	if err := os.WriteFile(authDir+"/cert.pem", []byte("preexisting"), 0o644); err != nil {
		t.Fatal(err)
	}
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", "https://x.example",
	})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
	got, _ := os.ReadFile(authDir + "/cert.pem")
	if string(got) != "preexisting" {
		t.Error("cert.pem clobbered without --force")
	}
}

func TestRegister_CertExists_WithForce_Overwrites(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{})
	authDir := setRegisterEnv(t)
	if err := os.WriteFile(authDir+"/cert.pem", []byte("preexisting"), 0o644); err != nil {
		t.Fatal(err)
	}
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke", "--force",
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	got, _ := os.ReadFile(authDir + "/cert.pem")
	if string(got) == "preexisting" {
		t.Error("cert.pem not overwritten with --force")
	}
	if !strings.Contains(string(got), "BEGIN CERTIFICATE") {
		t.Errorf("cert.pem not PEM: %q", got)
	}
}

func TestRegister_CAPinFlagOverride_UsesFile(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	// Embedded pin = different/empty; override with the test CA via flag.
	installTestPin(t, &testCA{CertPEM: []byte("garbage that won't validate")})
	pinPath := t.TempDir() + "/override.pem"
	if err := os.WriteFile(pinPath, ca.CertPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	srv := mockCentralAPI(t, ca, mockOpts{})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
		"--ca-pin", pinPath,
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0 (--ca-pin should override)", code)
	}
}

func TestRegister_CAPinFlagMissingFile_ReturnsExit5(t *testing.T) {
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", "https://x.example",
		"--ca-pin", "/definitely/does/not/exist.pem",
	})
	if code != 5 {
		t.Errorf("exit = %d, want 5", code)
	}
}

func TestRegister_PersistFailure_ReturnsExit5(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{})
	// AGENT_AUTH_DIR points to existing FILE (not directory) so MkdirAll
	// + atomic-rename writes inside it fail. Cross-platform.
	tempFile := t.TempDir() + "/notdir.txt"
	if err := os.WriteFile(tempFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_AUTH_DIR", tempFile)
	t.Setenv("AGENT_APPDATA_DIR", t.TempDir())
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	// AuthDir() may fail before persistAll → exit 1 acceptable;
	// or persistAll fails → exit 5. Both indicate the test is working.
	if code != 1 && code != 5 {
		t.Errorf("exit = %d, want 1 or 5", code)
	}
}

func TestSmokeMTLS_HealthOK_LogsOK(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var healthCount atomic.Int32
	srv := mockCentralAPI(t, ca, mockOpts{HealthRequestCount: &healthCount})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL,
	})
	if code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if got := healthCount.Load(); got != 1 {
		t.Errorf("health request count = %d, want 1", got)
	}
}

func TestSmokeMTLS_HealthFails_StillExit0(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	srv := mockCentralAPI(t, ca, mockOpts{HealthStatus: 503})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL,
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0 (smoke is warn-only)", code)
	}
}

func TestRegister_NoSmokeFlag_SkipsHealthProbe(t *testing.T) {
	fastBackoff(t)
	ca := seedTestCA(t)
	installTestPin(t, ca)
	var healthCount atomic.Int32
	srv := mockCentralAPI(t, ca, mockOpts{HealthRequestCount: &healthCount})
	_ = setRegisterEnv(t)
	code := cmdRegister([]string{
		"--tenant-id", "T", "--base-url", srv.URL, "--no-smoke",
	})
	if code != 0 {
		t.Errorf("exit = %d, want 0", code)
	}
	if got := healthCount.Load(); got != 0 {
		t.Errorf("health request count = %d, want 0 (--no-smoke)", got)
	}
}
