package diagnose

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

const certNearExpiryDays = 7

func init() {
	staticChecks = []CheckFunc{checkCert, checkAuthDir}
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
