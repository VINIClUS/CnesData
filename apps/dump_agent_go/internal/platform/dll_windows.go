//go:build windows

package platform

import (
	"fmt"
	"os"
	"path/filepath"
)

func fbClientPathImpl() (string, error) {
	if p := os.Getenv("FIREBIRD_DLL"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
		return "", fmt.Errorf("fbclient_not_found path=%s: %w", p, ErrFBClientNotFound)
	}
	exe, err := os.Executable()
	if err == nil {
		sibling := filepath.Join(filepath.Dir(exe), "fbclient.dll")
		if _, err := os.Stat(sibling); err == nil {
			return sibling, nil
		}
	}
	return "", fmt.Errorf("windows: %w (set FIREBIRD_DLL or place fbclient.dll next to exe)", ErrFBClientNotFound)
}
