package auth_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

// fakeRotateClient implements auth.RotateClient for tests.
type fakeRotateClient struct {
	httpClient *http.Client
}

func (f *fakeRotateClient) HTTPClient() *http.Client { return f.httpClient }
func (f *fakeRotateClient) Reload() error            { return nil }

func TestNewRotator_DefaultsApplied(t *testing.T) {
	c := &fakeRotateClient{httpClient: http.DefaultClient}
	r := auth.NewRotator(c, t.TempDir(), "https://x.example", "abc12345")
	if r == nil {
		t.Fatal("NewRotator returned nil")
	}
}

func TestSentinels_AreDistinct(t *testing.T) {
	if errors.Is(auth.ErrRotateNotDue, auth.ErrRotateTerminal) {
		t.Error("ErrRotateNotDue should not match ErrRotateTerminal")
	}
	if errors.Is(auth.ErrRotateTerminal, auth.ErrRotateRetriesExhausted) {
		t.Error("ErrRotateTerminal should not match ErrRotateRetriesExhausted")
	}
	if errors.Is(auth.ErrRotateNotDue, auth.ErrRotateRetriesExhausted) {
		t.Error("ErrRotateNotDue should not match ErrRotateRetriesExhausted")
	}
	_ = time.Now // keep import alive for follow-up tasks
}

// rotateCA is a self-signed CA used to sign leaf certs in tests.
type rotateCA struct {
	cert *x509.Certificate
	key  *rsa.PrivateKey
}

func seedRotateCA(t *testing.T) *rotateCA {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa keygen: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "rotate-test-ca"},
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
	return &rotateCA{cert: cert, key: key}
}

// signLeaf returns PEM bytes for a leaf cert with the supplied
// NotBefore/NotAfter window.
func (ca *rotateCA) signLeaf(t *testing.T, notBefore, notAfter time.Time) []byte {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-agent"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// seedAuthDirWithCert writes cert.pem + key.bin + refresh.bin to a fresh
// temp dir. Cert validity = (now-1d, now + ttlRemaining). Returns dir.
func seedAuthDirWithCert(t *testing.T, ca *rotateCA, ttlRemaining time.Duration) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)
	notBefore := time.Now().Add(-24 * time.Hour)
	notAfter := time.Now().Add(ttlRemaining)
	certPEM := ca.signLeaf(t, notBefore, notAfter)
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), certPEM, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "key.bin"), []byte("placeholder"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "refresh.bin"), []byte("ph-refresh"), 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestParseCurrentCert_ValidPEM_ReturnsCert(t *testing.T) {
	ca := seedRotateCA(t)
	dir := seedAuthDirWithCert(t, ca, 30*24*time.Hour)
	cert, err := auth.ParseCurrentCertForTest(dir)
	if err != nil {
		t.Fatalf("parseCurrentCert: %v", err)
	}
	if cert == nil {
		t.Fatal("parseCurrentCert returned nil cert")
	}
	if cert.Subject.CommonName != "test-agent" {
		t.Errorf("CN = %q, want test-agent", cert.Subject.CommonName)
	}
}

func TestParseCurrentCert_NoCertFile_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)
	_, err := auth.ParseCurrentCertForTest(dir)
	if err == nil {
		t.Fatal("parseCurrentCert: want error for missing cert.pem")
	}
}

func TestParseCurrentCert_GarbagePEM_ReturnsError(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), []byte("not pem"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := auth.ParseCurrentCertForTest(dir)
	if err == nil {
		t.Fatal("parseCurrentCert: want error for garbage PEM")
	}
}

func TestIsWithinWindow_FreshCert_ReturnsFalse(t *testing.T) {
	now := time.Now()
	cert := &x509.Certificate{
		NotBefore: now.Add(-1 * time.Hour),
		NotAfter:  now.Add(89 * 24 * time.Hour), // ~99% remaining
	}
	if auth.IsWithinWindowForTest(cert, 1.0/3.0, now) {
		t.Error("99% remaining should NOT be within 1/3 window")
	}
}

func TestIsWithinWindow_ExpiringSoon_ReturnsTrue(t *testing.T) {
	now := time.Now()
	cert := &x509.Certificate{
		NotBefore: now.Add(-80 * 24 * time.Hour),
		NotAfter:  now.Add(10 * 24 * time.Hour), // ~11% remaining
	}
	if !auth.IsWithinWindowForTest(cert, 1.0/3.0, now) {
		t.Error("11% remaining SHOULD be within 1/3 window")
	}
}

func TestIsWithinWindow_AtBoundary_ReturnsTrue(t *testing.T) {
	now := time.Now()
	cert := &x509.Certificate{
		NotBefore: now.Add(-90 * 24 * time.Hour),
		NotAfter:  now.Add(30 * 24 * time.Hour), // 25% remaining
	}
	if !auth.IsWithinWindowForTest(cert, 1.0/3.0, now) {
		t.Error("25% remaining (< 33%) should be within window")
	}
}

// mockRotateOpts customizes mockRotateServer behavior.
type mockRotateOpts struct {
	Statuses     []int         // sequential per-attempt statuses; 200 = success
	Attempts     *atomic.Int32 // optional caller-visible counter
	OnRequest    func(body []byte)
	NewCertPEM   []byte // optional cert payload to return on 200
	NewExpiresAt string // optional RFC3339 string
}

// mockRotateServer starts an httptest TLS server with /provision/cert/rotate.
func mockRotateServer(t *testing.T, ca *rotateCA, opts mockRotateOpts) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	var attempt atomic.Int32
	mux.HandleFunc("/provision/cert/rotate", func(w http.ResponseWriter, r *http.Request) {
		idx := attempt.Add(1) - 1
		if opts.Attempts != nil {
			opts.Attempts.Add(1)
		}
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		if opts.OnRequest != nil {
			opts.OnRequest(body)
		}
		statuses := opts.Statuses
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
		certPEM := opts.NewCertPEM
		if len(certPEM) == 0 {
			certPEM = ca.signLeaf(t,
				time.Now().Add(-time.Minute),
				time.Now().Add(90*24*time.Hour),
			)
		}
		expires := opts.NewExpiresAt
		if expires == "" {
			expires = time.Now().Add(90 * 24 * time.Hour).UTC().Format(time.RFC3339)
		}
		caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"cert_pem":     string(certPEM),
			"ca_chain_pem": string(caPEM),
			"expires_at":   expires,
		})
	})

	srv := httptest.NewUnstartedServer(mux)
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("server leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(99),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("server leaf: %v", err)
	}
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{leafDER},
			PrivateKey:  leafKey,
		}},
		MinVersion: tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// trustingClient returns an http.Client that trusts the given CA cert.
func trustingClient(t *testing.T, ca *rotateCA) *http.Client {
	t.Helper()
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS13,
			},
		},
		Timeout: 5 * time.Second,
	}
}

func TestPostRotate_Success_ReturnsParsedResp(t *testing.T) {
	ca := seedRotateCA(t)
	srv := mockRotateServer(t, ca, mockRotateOpts{})
	httpClient := trustingClient(t, ca)
	resp, err := auth.PostRotateForTest(
		t.Context(), httpClient, srv.URL, "fake-csr",
		[]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("postRotate: %v", err)
	}
	certPEM, expires := auth.ExtractRotateRespForTest(resp)
	if !strings.Contains(certPEM, "BEGIN CERTIFICATE") {
		t.Errorf("CertPEM not PEM: %q", certPEM)
	}
	if expires == "" {
		t.Error("ExpiresAt empty")
	}
}

func TestPostRotate_4xx_ReturnsErrRotateTerminal(t *testing.T) {
	ca := seedRotateCA(t)
	var attempts atomic.Int32
	srv := mockRotateServer(t, ca, mockRotateOpts{
		Statuses: []int{401},
		Attempts: &attempts,
	})
	httpClient := trustingClient(t, ca)
	_, err := auth.PostRotateForTest(
		t.Context(), httpClient, srv.URL, "csr",
		[]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond},
	)
	if !errors.Is(err, auth.ErrRotateTerminal) {
		t.Errorf("err = %v, want ErrRotateTerminal", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 4xx)", got)
	}
}

func TestPostRotate_5xxThrice_ReturnsErrRetriesExhausted(t *testing.T) {
	ca := seedRotateCA(t)
	var attempts atomic.Int32
	srv := mockRotateServer(t, ca, mockRotateOpts{
		Statuses: []int{503, 503, 503},
		Attempts: &attempts,
	})
	httpClient := trustingClient(t, ca)
	_, err := auth.PostRotateForTest(
		t.Context(), httpClient, srv.URL, "csr",
		[]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond},
	)
	if !errors.Is(err, auth.ErrRotateRetriesExhausted) {
		t.Errorf("err = %v, want ErrRotateRetriesExhausted", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
}

func TestPostRotate_5xxThenSuccess_RetriesAndReturns(t *testing.T) {
	ca := seedRotateCA(t)
	var attempts atomic.Int32
	srv := mockRotateServer(t, ca, mockRotateOpts{
		Statuses: []int{503, 503, 200},
		Attempts: &attempts,
	})
	httpClient := trustingClient(t, ca)
	resp, err := auth.PostRotateForTest(
		t.Context(), httpClient, srv.URL, "csr",
		[]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond},
	)
	if err != nil {
		t.Fatalf("postRotate: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	certPEM, _ := auth.ExtractRotateRespForTest(resp)
	if certPEM == "" {
		t.Error("resp empty after success")
	}
}

func TestPersistRotated_KeyThenCert_RefreshUntouched(t *testing.T) {
	ca := seedRotateCA(t)
	dir := seedAuthDirWithCert(t, ca, 30*24*time.Hour)
	originalRefresh, err := os.ReadFile(filepath.Join(dir, "refresh.bin"))
	if err != nil {
		t.Fatal(err)
	}
	newCertPEM := ca.signLeaf(t, time.Now(), time.Now().Add(90*24*time.Hour))
	resp := auth.MakeRotateRespForTest(string(newCertPEM), "ca-chain", "2026-07-30T00:00:00Z")
	pkcs8 := []byte("fake-pkcs8-bytes-for-test")
	if err := auth.PersistRotatedForTest(dir, resp, pkcs8); err != nil {
		t.Fatalf("persistRotated: %v", err)
	}
	gotCert, _ := os.ReadFile(filepath.Join(dir, "cert.pem"))
	if string(gotCert) != string(newCertPEM) {
		t.Error("cert.pem not overwritten with new PEM")
	}
	gotRefresh, _ := os.ReadFile(filepath.Join(dir, "refresh.bin"))
	if string(gotRefresh) != string(originalRefresh) {
		t.Error("refresh.bin clobbered (should be untouched)")
	}
}

// recordingFakeClient counts Reload() calls.
type recordingFakeClient struct {
	httpClient  *http.Client
	reloadCount atomic.Int32
}

func (f *recordingFakeClient) HTTPClient() *http.Client { return f.httpClient }
func (f *recordingFakeClient) Reload() error {
	f.reloadCount.Add(1)
	return nil
}

func TestRotateOnce_NotDue_ReturnsErrRotateNotDue(t *testing.T) {
	ca := seedRotateCA(t)
	dir := seedAuthDirWithCert(t, ca, 80*24*time.Hour) // ~99% remaining
	var attempts atomic.Int32
	srv := mockRotateServer(t, ca, mockRotateOpts{Attempts: &attempts})
	r := auth.NewRotator(
		&fakeRotateClient{httpClient: trustingClient(t, ca)},
		dir, srv.URL, "abc12345",
	)
	r.SetBackoffForTest([]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond})
	err := r.RotateOnce(t.Context())
	if !errors.Is(err, auth.ErrRotateNotDue) {
		t.Errorf("err = %v, want ErrRotateNotDue", err)
	}
	if got := attempts.Load(); got != 0 {
		t.Errorf("server saw %d POSTs, want 0", got)
	}
}

func TestRotateOnce_WithinWindow_RotatesPersistsReloads(t *testing.T) {
	ca := seedRotateCA(t)
	// notBefore=-24h, notAfter=+5d → total=6d. Advance clock to +4d so
	// remaining=1d (~17%) is below 1/3 threshold (~2d).
	dir := seedAuthDirWithCert(t, ca, 5*24*time.Hour)
	var attempts atomic.Int32
	srv := mockRotateServer(t, ca, mockRotateOpts{Attempts: &attempts})
	rcv := &recordingFakeClient{httpClient: trustingClient(t, ca)}
	r := auth.NewRotator(rcv, dir, srv.URL, "abc12345")
	r.SetBackoffForTest([]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond})
	r.SetClockForTest(func() time.Time { return time.Now().Add(4 * 24 * time.Hour) })
	err := r.RotateOnce(t.Context())
	if err != nil {
		t.Fatalf("RotateOnce: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("server POSTs = %d, want 1", got)
	}
	if rcv.reloadCount.Load() != 1 {
		t.Errorf("reload count = %d, want 1", rcv.reloadCount.Load())
	}
	gotCert, _ := os.ReadFile(filepath.Join(dir, "cert.pem"))
	if !strings.Contains(string(gotCert), "BEGIN CERTIFICATE") {
		t.Error("cert.pem not overwritten with new PEM")
	}
}

func TestRun_LoopExitsOnContextCancel(t *testing.T) {
	ca := seedRotateCA(t)
	dir := seedAuthDirWithCert(t, ca, 80*24*time.Hour)
	srv := mockRotateServer(t, ca, mockRotateOpts{})
	r := auth.NewRotator(
		&fakeRotateClient{httpClient: trustingClient(t, ca)},
		dir, srv.URL, "abc12345",
	)
	r.SetIntervalForTest(10 * time.Millisecond)
	r.SetBackoffForTest([]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond})
	r.SetRandForTest(func() float64 { return 0.5 })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil on ctx cancel", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit within 500ms after ctx cancel")
	}
}

func TestRun_LoopExitsOnTerminalError(t *testing.T) {
	ca := seedRotateCA(t)
	dir := seedAuthDirWithCert(t, ca, 5*24*time.Hour)
	srv := mockRotateServer(t, ca, mockRotateOpts{Statuses: []int{401}})
	r := auth.NewRotator(
		&fakeRotateClient{httpClient: trustingClient(t, ca)},
		dir, srv.URL, "abc12345",
	)
	r.SetIntervalForTest(10 * time.Millisecond)
	r.SetBackoffForTest([]time.Duration{1 * time.Millisecond, 1 * time.Millisecond, 1 * time.Millisecond})
	r.SetRandForTest(func() float64 { return 0.5 })
	// Advance clock past most of cert lifetime so RotateOnce enters the
	// rotation flow and hits the mock server's 401.
	r.SetClockForTest(func() time.Time { return time.Now().Add(4 * 24 * time.Hour) })
	err := r.Run(t.Context())
	if !errors.Is(err, auth.ErrRotateTerminal) {
		t.Errorf("err = %v, want ErrRotateTerminal", err)
	}
}

func TestNextSleep_JitterStaysInTenPercent(t *testing.T) {
	r := auth.NewRotator(
		&fakeRotateClient{httpClient: http.DefaultClient},
		t.TempDir(), "https://x.example", "abc12345",
	)
	interval := 10 * time.Second
	r.SetIntervalForTest(interval)
	minD := time.Duration(float64(interval) * 0.9)
	maxD := time.Duration(float64(interval) * 1.1)
	for i := 0; i < 200; i++ {
		i := i // capture loop variable for closure
		r.SetRandForTest(func() float64 { return float64(i) / 200.0 })
		got := auth.NextSleepForTest(r)
		if got < minD || got > maxD {
			t.Errorf("nextSleep = %v, want in [%v, %v]", got, minD, maxD)
		}
	}
}
