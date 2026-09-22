//go:build windows

package auth

import (
	"bytes"
	"testing"

	"golang.org/x/sys/windows"
)

func TestDPAPI_UsaEscopoDaMaquina(t *testing.T) {
	if dpapiFlags != windows.CRYPTPROTECT_LOCAL_MACHINE {
		t.Fatalf("dpapiFlags = %#x, want machine scope", dpapiFlags)
	}
}

func TestWrapBytes_OutputIsNotIdentity(t *testing.T) {
	plain := []byte("secret-pkcs8-bytes-here")
	enc, err := wrapBytes(plain)
	if err != nil {
		t.Fatalf("wrapBytes: %v", err)
	}
	if bytes.Equal(plain, enc) {
		t.Error("DPAPI envelope should differ from input")
	}
	if len(enc) <= len(plain) {
		t.Errorf("DPAPI envelope expected to grow input, got %d <= %d", len(enc), len(plain))
	}
}

func TestUnwrapBytes_RoundTripWithRealDPAPI(t *testing.T) {
	plain := []byte("round-trip-test-payload")
	enc, err := wrapBytes(plain)
	if err != nil {
		t.Fatalf("wrapBytes: %v", err)
	}
	got, err := unwrapBytes(enc)
	if err != nil {
		t.Fatalf("unwrapBytes: %v", err)
	}
	if !bytes.Equal(got, plain) {
		t.Errorf("round-trip mismatch want=%q got=%q", plain, got)
	}
}
