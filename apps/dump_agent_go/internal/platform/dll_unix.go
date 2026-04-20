//go:build !windows

package platform

import "os"

func fbClientPathImpl() (string, error) {
	if p := os.Getenv("FIREBIRD_LIB"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	for _, cand := range []string{
		"/usr/lib/x86_64-linux-gnu/libfbclient.so.2",
		"/usr/lib/libfbclient.so.2",
		"/usr/local/lib/libfbclient.so.2",
	} {
		if _, err := os.Stat(cand); err == nil {
			return cand, nil
		}
	}
	return "", nil
}
