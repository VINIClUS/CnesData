package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
	"github.com/cnesdata/dumpagent/internal/obs"
)

// mtlsCA is a self-signed CA used to sign leaf certs in mtls-init tests.
type mtlsCA struct {
	cert   *x509.Certificate
	key    *rsa.PrivateKey
	pinPEM []byte
}

func seedMTLSCA(t *testing.T) *mtlsCA {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("ca rsa keygen: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "phase8-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("ca CreateCertificate: %v", err)
	}
	cert, _ := x509.ParseCertificate(der)
	pinPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return &mtlsCA{cert: cert, key: caKey, pinPEM: pinPEM}
}

// seedMTLSAuthDir writes a valid cert.pem + PKCS8 key.bin pair into a
// fresh temp dir. Returns the dir path. Cert is signed by ca and valid
// for 24h.
func seedMTLSAuthDir(t *testing.T, ca *mtlsCA) string {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)

	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf keygen: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &leafKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("sign leaf: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := auth.SaveCert(dir, certPEM); err != nil {
		t.Fatalf("save cert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("marshal pkcs8: %v", err)
	}
	if err := auth.SaveKey(dir, keyDER); err != nil {
		t.Fatalf("save key: %v", err)
	}
	if err := auth.SaveRefreshToken(dir, "ph"); err != nil {
		t.Fatalf("save refresh: %v", err)
	}
	return dir
}

// withCAPinOverride persists pinPEM to dir/ca_pin.pem, mirroring what
// `register --ca-pin` writes via persistAll. initMTLSClient reads it back
// through auth.LoadCAPin, same as it would in production.
func withCAPinOverride(t *testing.T, dir string, pinPEM []byte) {
	t.Helper()
	if err := auth.SaveCAPin(dir, pinPEM); err != nil {
		t.Fatalf("SaveCAPin: %v", err)
	}
}

func TestInitMTLS_CertValid_ReturnsClient(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	withCAPinOverride(t, dir, ca.pinPEM)

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("expected nil err, got %v", err)
	}
	if mtls == nil {
		t.Fatal("expected non-nil mtls client")
	}
	if mtls.HTTPClient() == nil {
		t.Fatal("expected non-nil HTTPClient")
	}
}

func TestInitMTLS_CertMissing_FailClosed(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)
	t.Setenv("AGENT_ALLOW_INSECURE", "")
	withCAPinOverride(t, dir, ca.pinPEM)

	mtls, err := initMTLSClient(dir)
	if err == nil {
		t.Fatal("expected err for missing cert, got nil")
	}
	if mtls != nil {
		t.Fatal("expected nil mtls when cert missing")
	}
}

func TestInitMTLS_CertMissing_FallbackHonored(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", dir)
	t.Setenv("AGENT_ALLOW_INSECURE", "true")
	withCAPinOverride(t, dir, ca.pinPEM)

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("expected nil err with fallback flag, got %v", err)
	}
	if mtls != nil {
		t.Fatal("expected nil mtls in fallback mode")
	}
}

func TestInitMTLS_KeyParseFails_FailClosed(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	t.Setenv("AGENT_ALLOW_INSECURE", "")
	withCAPinOverride(t, dir, ca.pinPEM)

	if err := os.WriteFile(filepath.Join(dir, "key.bin"), []byte("not-pkcs8"), 0o600); err != nil {
		t.Fatal(err)
	}

	mtls, err := initMTLSClient(dir)
	if err == nil {
		t.Fatal("expected err for malformed key, got nil")
	}
	if mtls != nil {
		t.Fatal("expected nil mtls when key parse fails")
	}
}

func TestInitMTLS_KeyParseFails_FallbackHonored(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	t.Setenv("AGENT_ALLOW_INSECURE", "true")
	withCAPinOverride(t, dir, ca.pinPEM)

	if err := os.WriteFile(filepath.Join(dir, "key.bin"), []byte("not-pkcs8"), 0o600); err != nil {
		t.Fatal(err)
	}

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("expected nil err with fallback flag, got %v", err)
	}
	if mtls != nil {
		t.Fatal("expected nil mtls in fallback mode")
	}
}

func TestInitMTLS_NoPersistedPin_FallsBackToSystemTrustStore(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("initMTLSClient: %v", err)
	}
	tr, ok := mtls.HTTPClient().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport type = %T, want *http.Transport", mtls.HTTPClient().Transport)
	}
	if tr.TLSClientConfig.RootCAs != nil {
		t.Error("RootCAs != nil, want nil (system trust store, no ca_pin.pem persisted)")
	}
}

func TestInitMTLS_CorruptedPersistedPin_FailsClosed(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	t.Setenv("AGENT_ALLOW_INSECURE", "")
	if err := auth.SaveCAPin(dir, []byte("not a pem")); err != nil {
		t.Fatalf("SaveCAPin: %v", err)
	}

	mtls, err := initMTLSClient(dir)
	if err == nil {
		t.Fatal("expected error for corrupted persisted pin, got nil")
	}
	if mtls != nil {
		t.Fatal("expected nil mtls for corrupted persisted pin")
	}
}

func TestInitMTLS_EmptyPersistedPinFile_FailsClosed(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	t.Setenv("AGENT_ALLOW_INSECURE", "")
	if err := auth.SaveCAPin(dir, []byte{}); err != nil {
		t.Fatalf("SaveCAPin: %v", err)
	}

	mtls, err := initMTLSClient(dir)
	if err == nil {
		t.Fatal("expected error for present-but-empty ca_pin.pem, got nil")
	}
	if mtls != nil {
		t.Fatal("expected nil mtls for present-but-empty ca_pin.pem")
	}
}

func TestInitMTLS_CorruptedPersistedPin_FallbackHonored(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	t.Setenv("AGENT_ALLOW_INSECURE", "true")
	if err := auth.SaveCAPin(dir, []byte("not a pem")); err != nil {
		t.Fatalf("SaveCAPin: %v", err)
	}

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("expected nil err with fallback flag, got %v", err)
	}
	if mtls != nil {
		t.Fatal("expected nil mtls in fallback mode")
	}
}

// startPlainTLSServer starts an httptest.Server presenting a leaf cert
// signed by ca — no client-cert requirement. Used to prove a persisted
// CA pin lets run's mTLS client actually verify a matching server.
func startPlainTLSServer(t *testing.T, ca *mtlsCA, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("srv key: %v", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, ca.cert, &srvKey.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("srv create: %v", err)
	}
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}},
		MinVersion:   tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestInitMTLS_PersistedPin_ClientVerifiesMatchingServer(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	withCAPinOverride(t, dir, ca.pinPEM)

	srv := startPlainTLSServer(t, ca, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mtls, err := initMTLSClient(dir)
	if err != nil {
		t.Fatalf("initMTLSClient: %v", err)
	}
	resp, err := mtls.HTTPClient().Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
}

func TestHTTPClientFor_NilMTLS_ReturnsNil(t *testing.T) {
	got := httpClientFor(nil)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestHTTPClientFor_NonNil_ReturnsHTTPClient(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	withCAPinOverride(t, dir, ca.pinPEM)
	mtls, err := initMTLSClient(dir)
	if err != nil || mtls == nil {
		t.Fatalf("setup: %v", err)
	}

	got := httpClientFor(mtls)
	if got == nil {
		t.Fatal("expected non-nil http.Client")
	}
	if got != mtls.HTTPClient() {
		t.Fatal("expected got == mtls.HTTPClient() (same pointer)")
	}
}

func TestRunForegroundLogger_MultiHandlerComposition(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "test.log")
	h, closer := buildLoggerHandler(logPath, slog.LevelInfo)
	defer closer()
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
	if _, ok := h.(*obs.MultiHandler); !ok {
		t.Fatalf("expected *obs.MultiHandler, got %T", h)
	}
}
