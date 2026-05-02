package diagnose

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTestCert(t *testing.T, dir string, notBefore, notAfter time.Time) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-agent"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCheckCert_PASS_FreshCert(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeTestCert(t, dir, now.Add(-1*time.Hour), now.Add(90*24*time.Hour))
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q fields=%v)", c.Severity, c.Message, c.Fields)
	}
}

func TestCheckCert_WARN_NearExpiry(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeTestCert(t, dir, now.Add(-1*time.Hour), now.Add(3*24*time.Hour))
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN", c.Severity)
	}
}

func TestCheckCert_FAIL_Expired(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeTestCert(t, dir, now.Add(-30*24*time.Hour), now.Add(-1*time.Hour))
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestCheckCert_FAIL_Missing(t *testing.T) {
	dir := t.TempDir()
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestCheckCert_FAIL_BadPEM(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestCheckCert_WARN_NotYetValid(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeTestCert(t, dir, now.Add(24*time.Hour), now.Add(91*24*time.Hour))
	c := checkCert(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN", c.Severity)
	}
}

func TestCheckAuthDir_PASS_AllReadable(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"cert.pem", "key.bin", "refresh.bin"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	c := checkAuthDir(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q)", c.Severity, c.Message)
	}
}

func TestCheckAuthDir_FAIL_KeyMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "cert.pem"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := checkAuthDir(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestCheckAuthDir_WARN_LinuxModeWide(t *testing.T) {
	if !isPOSIXFilesystem() {
		t.Skip("Linux/POSIX-only mode bits test")
	}
	dir := t.TempDir()
	for _, name := range []string{"cert.pem", "refresh.bin"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "key.bin"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	c := checkAuthDir(t.Context(), Config{AuthDir: dir})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN", c.Severity)
	}
}

// isPOSIXFilesystem reports whether the test FS reports POSIX mode bits.
func isPOSIXFilesystem() bool {
	tmp, err := os.CreateTemp("", "perm-probe-*")
	if err != nil {
		return false
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if err := tmp.Chmod(0o644); err != nil {
		return false
	}
	st, err := tmp.Stat()
	if err != nil {
		return false
	}
	return st.Mode().Perm() == 0o644
}
