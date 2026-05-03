// Package secrets stores per-FB-source passwords:
//   - Windows: DPAPI-wrapped (CRYPTPROTECT_LOCAL_MACHINE).
//   - Linux/Mac (dev/CI): plaintext file with mode 0600.
//
// Either way, file path is <dir>/<source>.dpapi.
package secrets

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ErrNotSet signals the secret file does not exist for a source.
var ErrNotSet = errors.New("secret_not_set")

// Store wraps a directory of <source>.dpapi files.
type Store struct {
	dir string
}

// NewStore returns a Store rooted at dir. Save creates dir if missing.
func NewStore(dir string) *Store {
	return &Store{dir: dir}
}

// ValidateSource rejects bad source names. Only cnes/sihd/bpa accepted;
// sia is DBF-only (no FB credentials).
func ValidateSource(name string) error {
	switch name {
	case "cnes", "sihd", "bpa":
		return nil
	default:
		return fmt.Errorf("invalid_source name=%q", name)
	}
}

// Save writes the password for the given source. Atomic via .tmp +
// rename. DPAPI on Windows; plaintext+0600 on Linux.
func (s *Store) Save(source, password string) error {
	if err := ValidateSource(source); err != nil {
		return err
	}
	if password == "" {
		return errors.New("password_empty")
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("mkdir_secrets_dir: %w", err)
	}
	wrapped, err := wrapForStorage([]byte(password))
	if err != nil {
		return fmt.Errorf("wrap_secret: %w", err)
	}
	final := s.pathFor(source)
	tmp := final + ".tmp"
	if err := os.WriteFile(tmp, wrapped, 0o600); err != nil {
		return fmt.Errorf("write_tmp: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename: %w", err)
	}
	return nil
}

// Load returns the password for the source. ErrNotSet if file missing.
func (s *Store) Load(source string) (string, error) {
	if err := ValidateSource(source); err != nil {
		return "", err
	}
	data, err := os.ReadFile(s.pathFor(source))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ErrNotSet
		}
		return "", fmt.Errorf("read_secret: %w", err)
	}
	plain, err := unwrapFromStorage(data)
	if err != nil {
		return "", fmt.Errorf("unwrap_secret: %w", err)
	}
	return strings.TrimRight(string(plain), "\r\n"), nil
}

func (s *Store) pathFor(source string) string {
	return filepath.Join(s.dir, source+".dpapi")
}
