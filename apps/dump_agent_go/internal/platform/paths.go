// Package platform abstrai Win32/POSIX specifics (lock, signals, paths, etc).
package platform

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// AppDataDir retorna dir root para estado persistente do agente.
// Honra AGENT_APPDATA_DIR se setado.
// Windows: %LOCALAPPDATA%/CnesAgent
// Linux:   $XDG_STATE_HOME/cnes-agent (ou ~/.local/state/cnes-agent)
func AppDataDir() (string, error) {
	if override := os.Getenv("AGENT_APPDATA_DIR"); override != "" {
		if err := os.MkdirAll(override, 0o755); err != nil {
			return "", fmt.Errorf("app_data_dir_override: %w", err)
		}
		return override, nil
	}
	var base string
	if runtime.GOOS == "windows" {
		base = os.Getenv("LOCALAPPDATA")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("app_data_dir: %w", err)
			}
			base = filepath.Join(home, "AppData", "Local")
		}
		base = filepath.Join(base, "CnesAgent")
	} else {
		base = os.Getenv("XDG_STATE_HOME")
		if base == "" {
			home, err := os.UserHomeDir()
			if err != nil {
				return "", fmt.Errorf("app_data_dir: %w", err)
			}
			base = filepath.Join(home, ".local", "state")
		}
		base = filepath.Join(base, "cnes-agent")
	}
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", fmt.Errorf("app_data_dir_mkdir: %w", err)
	}
	return base, nil
}

// LogsDir retorna dir para arquivos de log. Honra DUMP_LOGS_DIR se setado.
func LogsDir() (string, error) {
	if override := os.Getenv("DUMP_LOGS_DIR"); override != "" {
		if err := os.MkdirAll(override, 0o755); err != nil {
			return "", err
		}
		return override, nil
	}
	base, err := AppDataDir()
	if err != nil {
		return "", err
	}
	path := filepath.Join(base, "logs")
	if err := os.MkdirAll(path, 0o755); err != nil {
		return "", err
	}
	return path, nil
}

// ResolveMachineID retorna ID estável de 8 chars hex. Persistido em dir/machine_id.
// Env MACHINE_ID tem precedência.
func ResolveMachineID(dir string) (string, error) {
	if env := strings.TrimSpace(os.Getenv("MACHINE_ID")); env != "" {
		return env, nil
	}
	storePath := filepath.Join(dir, "machine_id")
	data, err := os.ReadFile(storePath)
	if err == nil {
		if id := strings.TrimSpace(string(data)); id != "" {
			return id, nil
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("read_machine_id: %w", err)
	}

	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("gen_machine_id: %w", err)
	}
	id := hex.EncodeToString(b[:])
	if err := os.WriteFile(storePath, []byte(id), 0o644); err != nil {
		return "", fmt.Errorf("write_machine_id: %w", err)
	}
	return id, nil
}
