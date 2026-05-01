package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

// mtlsCA is a self-signed CA used to sign leaf certs in mtls-init tests.
type mtlsCA struct {
	cert    *x509.Certificate
	key     *rsa.PrivateKey
	pinPEM  []byte
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

// withCAPinOverride swaps auth.CAPinPEM for the duration of t and
// restores it on cleanup.
func withCAPinOverride(t *testing.T, pinPEM []byte) {
	t.Helper()
	orig := auth.CAPinPEM
	auth.CAPinPEM = pinPEM
	t.Cleanup(func() { auth.CAPinPEM = orig })
}

func TestInitMTLS_CertValid_ReturnsClient(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	withCAPinOverride(t, ca.pinPEM)

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
	withCAPinOverride(t, ca.pinPEM)

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
	withCAPinOverride(t, ca.pinPEM)

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
	withCAPinOverride(t, ca.pinPEM)

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
	withCAPinOverride(t, ca.pinPEM)

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

func TestHTTPClientFor_NilMTLS_ReturnsNil(t *testing.T) {
	got := httpClientFor(nil)
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}
}

func TestHTTPClientFor_NonNil_ReturnsHTTPClient(t *testing.T) {
	ca := seedMTLSCA(t)
	dir := seedMTLSAuthDir(t, ca)
	withCAPinOverride(t, ca.pinPEM)
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
