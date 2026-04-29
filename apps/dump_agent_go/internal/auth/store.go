// Package auth — store: persistent on-disk storage for cert + key + refresh.
//
// File layout under AuthDir():
//
//	cert.pem      plaintext PEM, mode 0644 (public)
//	key.bin       DPAPI envelope (Windows) OR PKCS8 DER + 0600 (Unix)
//	refresh.bin   DPAPI envelope (Windows) OR raw token + 0600 (Unix)
//
// `.bin` extension is OS-agnostic; content varies by OS. On Windows,
// wrapBytes uses DPAPI per-user (no LOCAL_MACHINE flag). User profile
// rebuild → ErrUnwrapFailed → caller triggers re-registration.
package auth

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cnesdata/dumpagent/internal/platform"
)

// Sentinels.
var (
	ErrNotFound     = errors.New("auth: file not found")
	ErrUnwrapFailed = errors.New("auth: unwrap failed (DPAPI key mismatch?)")
	ErrCorrupted    = errors.New("auth: file corrupted")
)

const (
	certFileName    = "cert.pem"
	keyFileName     = "key.bin"
	refreshFileName = "refresh.bin"
)

// AuthDir resolves the on-disk directory for agent auth artifacts.
// Honors AGENT_AUTH_DIR env var; otherwise platform.AppDataDir() + "/auth".
// Creates the directory with mode 0700 on first call.
func AuthDir() (string, error) { //nolint:revive // stutter is intentional: package is auth, exported type is AuthDir
	if override := os.Getenv("AGENT_AUTH_DIR"); override != "" {
		if err := os.MkdirAll(override, 0o700); err != nil {
			return "", fmt.Errorf("auth: mkdir override: %w", err)
		}
		return override, nil
	}
	base, err := platform.AppDataDir()
	if err != nil {
		return "", fmt.Errorf("auth: app data dir: %w", err)
	}
	dir := filepath.Join(base, "auth")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("auth: mkdir auth: %w", err)
	}
	return dir, nil
}

// SaveCert writes pemBytes to <dir>/cert.pem (mode 0644). PEM is public.
func SaveCert(dir string, pemBytes []byte) error {
	return writeAtomic(filepath.Join(dir, certFileName), pemBytes, 0o644)
}

// LoadCert reads <dir>/cert.pem. Returns ErrNotFound if missing.
func LoadCert(dir string) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, certFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("auth: read cert: %w", err)
	}
	return data, nil
}

// SaveKey wraps pkcs8DER (DPAPI on Windows; identity on Unix), writes 0600.
func SaveKey(dir string, pkcs8DER []byte) error {
	wrapped, err := wrapBytes(pkcs8DER)
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, keyFileName), wrapped, 0o600)
}

// LoadKey reads + unwraps. Returns ErrNotFound | ErrUnwrapFailed | DER bytes.
func LoadKey(dir string) ([]byte, error) {
	enc, err := os.ReadFile(filepath.Join(dir, keyFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("auth: read key: %w", err)
	}
	plain, err := unwrapBytes(enc)
	if err != nil {
		return nil, err
	}
	return plain, nil
}

// SaveRefreshToken wraps + writes the token string. 0600.
func SaveRefreshToken(dir string, token string) error {
	wrapped, err := wrapBytes([]byte(token))
	if err != nil {
		return err
	}
	return writeAtomic(filepath.Join(dir, refreshFileName), wrapped, 0o600)
}

// LoadRefreshToken reads + unwraps. Returns the token string.
func LoadRefreshToken(dir string) (string, error) {
	enc, err := os.ReadFile(filepath.Join(dir, refreshFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("auth: read refresh: %w", err)
	}
	plain, err := unwrapBytes(enc)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

// writeAtomic writes via temp file + rename to avoid torn writes.
func writeAtomic(path string, data []byte, mode os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, mode); err != nil {
		return fmt.Errorf("auth: write tmp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("auth: rename: %w", err)
	}
	return nil
}
