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

const dpapiFlags = windows.CRYPTPROTECT_LOCAL_MACHINE

// wrapBytes encrypts plain for the service and the interactive administrator.
func wrapBytes(plain []byte) ([]byte, error) {
	var out windows.DataBlob
	if err := windows.CryptProtectData(
		newBlob(plain), nil, nil, 0, nil, dpapiFlags, &out,
	); err != nil {
		return nil, fmt.Errorf("auth: DPAPI protect: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	result := blobToSlice(&out)
	cp := make([]byte, len(result))
	copy(cp, result)
	return cp, nil
}

// unwrapBytes decrypts a machine-scoped DPAPI blob.
func unwrapBytes(enc []byte) ([]byte, error) {
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(
		newBlob(enc), nil, nil, 0, nil, dpapiFlags, &out,
	); err != nil {
		return nil, fmt.Errorf("%w: DPAPI unprotect: %v", ErrUnwrapFailed, err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	result := blobToSlice(&out)
	cp := make([]byte, len(result))
	copy(cp, result)
	return cp, nil
}
