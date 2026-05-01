//go:build windows

package auth

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// newBlob converts a byte slice into a DataBlob for the DPAPI calls.
func newBlob(data []byte) *windows.DataBlob {
	if len(data) == 0 {
		return &windows.DataBlob{}
	}
	return &windows.DataBlob{
		Size: uint32(len(data)), //nolint:gosec // len is always non-negative
		Data: &data[0],
	}
}

// blobToSlice converts a DataBlob output back to a Go byte slice.
func blobToSlice(blob *windows.DataBlob) []byte {
	if blob.Size == 0 || blob.Data == nil {
		return []byte{}
	}
	return unsafe.Slice(blob.Data, blob.Size)
}

// wrapBytes encrypts plain using DPAPI (per-user, no LOCAL_MACHINE flag).
func wrapBytes(plain []byte) ([]byte, error) {
	var out windows.DataBlob
	if err := windows.CryptProtectData(newBlob(plain), nil, nil, 0, nil, 0, &out); err != nil {
		return nil, fmt.Errorf("auth: DPAPI protect: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	result := blobToSlice(&out)
	cp := make([]byte, len(result))
	copy(cp, result)
	return cp, nil
}

// unwrapBytes decrypts a DPAPI blob. Returns ErrUnwrapFailed if the blob is
// invalid or was encrypted under a different user profile.
func unwrapBytes(enc []byte) ([]byte, error) {
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(newBlob(enc), nil, nil, 0, nil, 0, &out); err != nil {
		return nil, fmt.Errorf("%w: DPAPI unprotect: %v", ErrUnwrapFailed, err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	result := blobToSlice(&out)
	cp := make([]byte, len(result))
	copy(cp, result)
	return cp, nil
}
