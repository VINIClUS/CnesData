//go:build windows

package secrets

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

// wrapForStorage encrypts via DPAPI scoped to the local machine. The
// wrapped blob is unreadable on other hosts but readable across users
// on the same host (Windows service decrypts password set via admin GUI).
func wrapForStorage(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("dpapi_empty_input")
	}
	var out windows.DataBlob
	if err := windows.CryptProtectData(newBlob(b), nil, nil, 0, nil,
		windows.CRYPTPROTECT_LOCAL_MACHINE, &out); err != nil {
		return nil, fmt.Errorf("dpapi_protect: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	return cloneBlob(&out), nil
}

func unwrapFromStorage(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("dpapi_empty_blob")
	}
	var out windows.DataBlob
	if err := windows.CryptUnprotectData(newBlob(b), nil, nil, 0, nil,
		windows.CRYPTPROTECT_LOCAL_MACHINE, &out); err != nil {
		return nil, fmt.Errorf("dpapi_unprotect: %w", err)
	}
	defer windows.LocalFree(windows.Handle(unsafe.Pointer(out.Data))) //nolint:errcheck
	return cloneBlob(&out), nil
}

func cloneBlob(b *windows.DataBlob) []byte {
	if b.Size == 0 || b.Data == nil {
		return nil
	}
	src := unsafe.Slice(b.Data, int(b.Size))
	out := make([]byte, b.Size)
	copy(out, src)
	return out
}
