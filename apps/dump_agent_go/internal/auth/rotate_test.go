package auth_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
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
