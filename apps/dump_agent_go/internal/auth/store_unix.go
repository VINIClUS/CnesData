//go:build !windows

package auth

// wrapBytes is identity on Unix; encryption-at-rest delegated to filesystem
// permissions (mode 0600 set in Save* functions).
func wrapBytes(plain []byte) ([]byte, error) {
	cp := make([]byte, len(plain))
	copy(cp, plain)
	return cp, nil
}

// unwrapBytes is identity on Unix.
func unwrapBytes(enc []byte) ([]byte, error) {
	cp := make([]byte, len(enc))
	copy(cp, enc)
	return cp, nil
}
