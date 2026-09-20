package service

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// secretKeyMarkers flags env var names that must never be written to the
// service's registry Environment value. HKLM\SYSTEM\CurrentControlSet\
// Services grants ReadKey to Authenticated Users on the default ACL, so
// anything placed there is readable by any logged-in user on the machine.
// Credentials belong exclusively in the DPAPI-backed secrets store
// (`dumpagent set-secret`) — see H4 in docs/edge-agent-audit-2026-09-20.md.
var secretKeyMarkers = []string{"PASSWORD", "SECRET", "TOKEN", "_PW", "APIKEY", "API_KEY"}

// parseEnvFile reads simple KEY=VALUE lines from a flat config file (blank
// lines and #-comments ignored, no quoting/escaping). Matches the format
// documented in docs/runbooks/dumpagent-install-windows.md.
func parseEnvFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open_config: %w", err)
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || !strings.Contains(line, "=") {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read_config: %w", err)
	}
	return lines, nil
}

// splitSecretLines separates KEY=VALUE lines into entries safe for the
// registry Environment value and the keys rejected as secret-like.
func splitSecretLines(lines []string) (safe []string, rejectedKeys []string) {
	for _, line := range lines {
		key, _, _ := strings.Cut(line, "=")
		if isSecretKey(key) {
			rejectedKeys = append(rejectedKeys, key)
			continue
		}
		safe = append(safe, line)
	}
	return safe, rejectedKeys
}

func isSecretKey(key string) bool {
	upper := strings.ToUpper(key)
	for _, marker := range secretKeyMarkers {
		if strings.Contains(upper, marker) {
			return true
		}
	}
	return false
}
