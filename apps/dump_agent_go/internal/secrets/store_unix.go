//go:build !windows

package secrets

// wrapForStorage on non-Windows is identity (caller writes plaintext at 0600).
func wrapForStorage(b []byte) ([]byte, error) {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func unwrapFromStorage(b []byte) ([]byte, error) {
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}
