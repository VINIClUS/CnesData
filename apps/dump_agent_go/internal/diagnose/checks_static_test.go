package diagnose

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.etcd.io/bbolt"
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

func TestCheckOutbox_PASS_FileMissing(t *testing.T) {
	dir := t.TempDir()
	c := checkOutbox(t.Context(), Config{AppData: dir})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (uninitialized)", c.Severity)
	}
}

func TestCheckOutbox_PASS_Empty(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "queue")
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(queueDir, "outbox.db")
	db, err := bbolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists([]byte("outbox"))
		return e
	})
	_ = db.Close()
	c := checkOutbox(t.Context(), Config{AppData: dir})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (count=0)", c.Severity)
	}
}

func TestCheckOutbox_FAIL_Corrupt(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "queue")
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(queueDir, "outbox.db")
	if err := os.WriteFile(dbPath, []byte("not a bbolt file"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := checkOutbox(t.Context(), Config{AppData: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestCheckOutbox_WARN_OldEnvelope(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "queue")
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(queueDir, "outbox.db")
	db, err := bbolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Update(func(tx *bbolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte("outbox"))
		oldNs := time.Now().Add(-31 * 24 * time.Hour).UnixNano()
		key := make([]byte, 12)
		binary.BigEndian.PutUint64(key[0:8], uint64(oldNs))
		return b.Put(key, []byte(`{}`))
	})
	_ = db.Close()
	c := checkOutbox(t.Context(), Config{AppData: dir})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN (old)", c.Severity)
	}
}

func TestCheckLogDir_PASS_Writable(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DUMP_LOGS_DIR", dir)
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q)", c.Severity, c.Message)
	}
}

func TestCheckLogDir_FAIL_NonWritable(t *testing.T) {
	if !isPOSIXFilesystem() {
		t.Skip("POSIX-only readonly-dir test")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0o700) // restore so t.TempDir cleanup works
	t.Setenv("DUMP_LOGS_DIR", dir)
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
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

func TestIsLinuxModeTooWide_EdgeCases(t *testing.T) {
	cases := []struct {
		name string
		mode string
		want bool
	}{
		{"empty", "", false},
		{"too short", "060", false},
		{"too long", "00600", false},
		{"missing leading zero", "1600", false},
		{"0600 strict", "0600", false},
		{"0644 group readable", "0644", true},
		{"0660 group rw", "0660", true},
		{"0601 other exec", "0601", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if !isPOSIXFilesystem() {
				t.Skip("Linux/POSIX-only mode bits test")
			}
			if got := isLinuxModeTooWide(c.mode); got != c.want {
				t.Errorf("isLinuxModeTooWide(%q) = %v want %v", c.mode, got, c.want)
			}
		})
	}
}

func TestCheckOutbox_FAIL_Locked(t *testing.T) {
	dir := t.TempDir()
	queueDir := filepath.Join(dir, "queue")
	if err := os.MkdirAll(queueDir, 0o700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(queueDir, "outbox.db")
	db, err := bbolt.Open(dbPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	c := checkOutbox(t.Context(), Config{AppData: dir})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
	if c.Message != "outbox_locked" && c.Message != "outbox_corrupt" {
		t.Errorf("got msg %q want outbox_locked|outbox_corrupt", c.Message)
	}
}

func TestCheckLogDir_FAIL_ResolveError(t *testing.T) {
	parent := t.TempDir()
	blockingFile := filepath.Join(parent, "not-a-dir")
	if err := os.WriteFile(blockingFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("DUMP_LOGS_DIR", filepath.Join(blockingFile, "subdir"))
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL (msg=%q)", c.Severity, c.Message)
	}
	if c.Message != "log_dir_resolve_error" {
		t.Errorf("got msg %q want log_dir_resolve_error", c.Message)
	}
}

func TestCheckLogDir_WARN_DiskLow(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DUMP_LOGS_DIR", dir)
	orig := diskFreeMBFunc
	diskFreeMBFunc = func(string) int64 { return 50 }
	defer func() { diskFreeMBFunc = orig }()
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN (msg=%q)", c.Severity, c.Message)
	}
	if c.Message != "disk_low" {
		t.Errorf("got msg %q want disk_low", c.Message)
	}
}

func TestCheckLogDir_FAIL_DiskAlmostFull(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DUMP_LOGS_DIR", dir)
	orig := diskFreeMBFunc
	diskFreeMBFunc = func(string) int64 { return 5 }
	defer func() { diskFreeMBFunc = orig }()
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL (msg=%q)", c.Severity, c.Message)
	}
	if c.Message != "disk_almost_full" {
		t.Errorf("got msg %q want disk_almost_full", c.Message)
	}
}

func TestCheckLogDir_PASS_DiskUnknown(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DUMP_LOGS_DIR", dir)
	orig := diskFreeMBFunc
	diskFreeMBFunc = func(string) int64 { return -1 }
	defer func() { diskFreeMBFunc = orig }()
	c := checkLogDir(t.Context(), Config{})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q)", c.Severity, c.Message)
	}
}
