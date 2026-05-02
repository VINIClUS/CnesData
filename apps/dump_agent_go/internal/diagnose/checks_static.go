package diagnose

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/cnesdata/dumpagent/internal/platform"
	"go.etcd.io/bbolt"
)

const certNearExpiryDays = 7

func init() {
	staticChecks = []CheckFunc{checkCert, checkAuthDir, checkOutbox, checkLogDir}
}

func checkCert(ctx context.Context, cfg Config) Check {
	path := filepath.Join(cfg.AuthDir, "cert.pem")
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Check{Name: "cert", Severity: SeverityFail, Message: "cert_missing",
				Fields: map[string]any{"path": path}}
		}
		return Check{Name: "cert", Severity: SeverityFail, Message: "cert_read_error",
			Fields: map[string]any{"err": err.Error()}}
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		return Check{Name: "cert", Severity: SeverityFail, Message: "cert_parse_error"}
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return Check{Name: "cert", Severity: SeverityFail, Message: "cert_parse_error",
			Fields: map[string]any{"err": err.Error()}}
	}
	now := time.Now()
	fp := sha256.Sum256(cert.Raw)
	fpHex := hex.EncodeToString(fp[:])[:16]
	fields := map[string]any{
		"expires_at":         cert.NotAfter.UTC().Format(time.RFC3339),
		"days_remaining":     int(time.Until(cert.NotAfter).Hours() / 24),
		"subject_cn":         cert.Subject.CommonName,
		"fingerprint_sha256": fpHex,
	}
	if now.Before(cert.NotBefore) {
		return Check{Name: "cert", Severity: SeverityWarn, Message: "cert_not_yet_valid", Fields: fields}
	}
	if now.After(cert.NotAfter) {
		return Check{Name: "cert", Severity: SeverityFail, Message: "cert_expired", Fields: fields}
	}
	if time.Until(cert.NotAfter) < certNearExpiryDays*24*time.Hour {
		return Check{Name: "cert", Severity: SeverityWarn, Message: "cert_near_expiry", Fields: fields}
	}
	return Check{Name: "cert", Severity: SeverityPass, Message: "valid",
		Fields: fields}
}

// checkAuthDir verifies cert.pem + key.bin + refresh.bin are readable.
// On Linux, also asserts mode 0600 for key.bin (WARN if wider).
func checkAuthDir(ctx context.Context, cfg Config) Check {
	files := []string{"cert.pem", "key.bin", "refresh.bin"}
	readable := map[string]bool{}
	modes := map[string]string{}
	for _, name := range files {
		p := filepath.Join(cfg.AuthDir, name)
		f, err := os.Open(p)
		if err != nil {
			return Check{Name: "auth_dir", Severity: SeverityFail,
				Message: "file_unreadable",
				Fields:  map[string]any{"file": name, "err": err.Error()}}
		}
		readable[name] = true
		_ = f.Close()
		if st, err := os.Stat(p); err == nil {
			modes[name] = fmt.Sprintf("%04o", st.Mode().Perm())
		}
	}
	fields := map[string]any{
		"auth_dir":         cfg.AuthDir,
		"cert_pem_mode":    modes["cert.pem"],
		"key_bin_mode":     modes["key.bin"],
		"refresh_bin_mode": modes["refresh.bin"],
	}
	if isLinuxModeTooWide(modes["key.bin"]) {
		return Check{Name: "auth_dir", Severity: SeverityWarn,
			Message: "key_perms_too_open", Fields: fields}
	}
	return Check{Name: "auth_dir", Severity: SeverityPass,
		Message: "readable", Fields: fields}
}

// isLinuxModeTooWide reports whether mode like "0644" is wider than 0600.
// On Windows this returns false because Stat() perms are not POSIX-meaningful.
func isLinuxModeTooWide(mode string) bool {
	if runtime.GOOS == "windows" {
		return false
	}
	if mode == "" || len(mode) != 4 || mode[0] != '0' {
		return false
	}
	// Mode bits: owner / group / other. "0600" is fine; anything with non-zero
	// group or other bits is too wide for a private key.
	return mode[2] != '0' || mode[3] != '0'
}

const (
	outboxCapWarn         = 8000
	outboxOldHoursWarn    = 30 * 24
	outboxBucketName      = "outbox"
	outboxOpenLockTimeout = 1 * time.Second
)

func checkOutbox(ctx context.Context, cfg Config) Check {
	path := filepath.Join(cfg.AppData, "queue", "outbox.db")
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return Check{Name: "outbox", Severity: SeverityPass,
			Message: "uninitialized",
			Fields:  map[string]any{"path": path}}
	}
	db, err := bbolt.Open(path, 0o600, &bbolt.Options{
		ReadOnly: true,
		Timeout:  outboxOpenLockTimeout,
	})
	if err != nil {
		msg := "outbox_corrupt"
		if errors.Is(err, bbolt.ErrTimeout) {
			msg = "outbox_locked"
		}
		return Check{Name: "outbox", Severity: SeverityFail, Message: msg,
			Fields: map[string]any{"path": path, "err": err.Error()}}
	}
	defer db.Close()

	count, oldestNs, err := scanOutbox(db)
	if err != nil {
		return Check{Name: "outbox", Severity: SeverityFail, Message: "outbox_corrupt",
			Fields: map[string]any{"err": err.Error()}}
	}
	return outboxResult(path, count, oldestNs)
}

// scanOutbox iterates the outbox bucket counting items + capturing oldest ns timestamp.
func scanOutbox(db *bbolt.DB) (count int, oldestNs int64, err error) {
	err = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(outboxBucketName))
		if b == nil {
			return errors.New("bucket missing")
		}
		c := b.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if count == 0 && len(k) >= 8 {
				oldestNs = int64(binary.BigEndian.Uint64(k[0:8]))
			}
			count++
		}
		return nil
	})
	return count, oldestNs, err
}

// outboxResult builds the final Check from scan results + path size.
func outboxResult(path string, count int, oldestNs int64) Check {
	st, _ := os.Stat(path)
	var sizeBytes int64
	if st != nil {
		sizeBytes = st.Size()
	}
	oldestHours := 0
	if oldestNs > 0 {
		oldestHours = int(time.Since(time.Unix(0, oldestNs)).Hours())
	}
	capPct := count * 100 / 10000
	fields := map[string]any{
		"path":             path,
		"size_bytes":       sizeBytes,
		"count":            count,
		"oldest_age_hours": oldestHours,
		"cap_pct":          capPct,
	}
	if count >= outboxCapWarn {
		return Check{Name: "outbox", Severity: SeverityWarn,
			Message: "approaching_cap", Fields: fields}
	}
	if oldestHours >= outboxOldHoursWarn {
		return Check{Name: "outbox", Severity: SeverityWarn,
			Message: "old_envelopes", Fields: fields}
	}
	return Check{Name: "outbox", Severity: SeverityPass,
		Message: "healthy", Fields: fields}
}

const (
	logDirFreeMBWarnFloor = 100 // < 100MB → WARN
	logDirFreeMBFailFloor = 10  // < 10MB → FAIL
)

func checkLogDir(ctx context.Context, cfg Config) Check {
	path, err := platform.LogsDir()
	if err != nil {
		return Check{Name: "log_dir", Severity: SeverityFail,
			Message: "log_dir_resolve_error",
			Fields:  map[string]any{"err": err.Error()}}
	}
	tmp, err := os.CreateTemp(path, "diag-probe-*")
	if err != nil {
		return Check{Name: "log_dir", Severity: SeverityFail,
			Message: "log_dir_not_writable",
			Fields:  map[string]any{"path": path, "err": err.Error()}}
	}
	_, _ = tmp.Write([]byte{0})
	_ = tmp.Close()
	_ = os.Remove(tmp.Name())

	freeMB := diskFreeMB(path) // -1 if unknown
	fields := map[string]any{"path": path, "free_mb": freeMB, "writable": true}
	if freeMB >= 0 && freeMB < logDirFreeMBFailFloor {
		return Check{Name: "log_dir", Severity: SeverityFail,
			Message: "disk_almost_full", Fields: fields}
	}
	if freeMB >= 0 && freeMB < logDirFreeMBWarnFloor {
		return Check{Name: "log_dir", Severity: SeverityWarn,
			Message: "disk_low", Fields: fields}
	}
	return Check{Name: "log_dir", Severity: SeverityPass,
		Message: "healthy", Fields: fields}
}
