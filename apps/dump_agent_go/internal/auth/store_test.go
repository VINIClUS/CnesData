package auth

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAuthDir_DefaultsToAppDataDir(t *testing.T) {
	t.Setenv("AGENT_AUTH_DIR", "")
	dir, err := AuthDir()
	if err != nil {
		t.Fatalf("AuthDir: %v", err)
	}
	if !strings.HasSuffix(filepath.ToSlash(dir), "/auth") {
		t.Errorf("expected dir suffix /auth, got %s", dir)
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Errorf("expected dir to exist, stat err=%v", err)
	}
}

func TestAuthDir_HonorsAGENT_AUTH_DIR(t *testing.T) {
	override := t.TempDir()
	t.Setenv("AGENT_AUTH_DIR", override)
	dir, err := AuthDir()
	if err != nil {
		t.Fatalf("AuthDir: %v", err)
	}
	if dir != override {
		t.Errorf("want %s got %s", override, dir)
	}
}

func TestAuthDir_CreatesMissingDir(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "nested", "auth")
	t.Setenv("AGENT_AUTH_DIR", target)
	if _, err := AuthDir(); err != nil {
		t.Fatalf("AuthDir: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil || !info.IsDir() {
		t.Errorf("expected dir created, err=%v", err)
	}
}

func TestSaveCert_LoadCert_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := []byte("-----BEGIN CERTIFICATE-----\nABC\n-----END CERTIFICATE-----\n")
	if err := SaveCert(dir, want); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	got, err := LoadCert(dir)
	if err != nil {
		t.Fatalf("LoadCert: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip mismatch want=%q got=%q", want, got)
	}
}

func TestLoadCert_NotFound_ReturnsErrNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadCert(dir)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound got %v", err)
	}
}

func TestSaveKey_LoadKey_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := []byte("PKCS8 DER bytes go here")
	if err := SaveKey(dir, want); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
	got, err := LoadKey(dir)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("round-trip mismatch want=%q got=%q", want, got)
	}
}

func TestLoadKey_NotFound_ReturnsErrNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadKey(dir)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound got %v", err)
	}
}

func TestSaveRefreshToken_LoadRefreshToken_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := "rt-43-chars-padding-padding-padding-padding"
	if err := SaveRefreshToken(dir, want); err != nil {
		t.Fatalf("SaveRefreshToken: %v", err)
	}
	got, err := LoadRefreshToken(dir)
	if err != nil {
		t.Fatalf("LoadRefreshToken: %v", err)
	}
	if got != want {
		t.Errorf("round-trip mismatch want=%q got=%q", want, got)
	}
}

func TestLoadRefreshToken_NotFound_ReturnsErrNotFound(t *testing.T) {
	dir := t.TempDir()
	_, err := LoadRefreshToken(dir)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound got %v", err)
	}
}

func TestSaveKey_WriteIsAtomic_NoTmpLeftover(t *testing.T) {
	dir := t.TempDir()
	if err := SaveKey(dir, []byte("data")); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
	tmp := filepath.Join(dir, "key.bin.tmp")
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("tmp file should not exist after Save, stat err=%v", err)
	}
}

func TestSaveKey_OverwriteExisting_Succeeds(t *testing.T) {
	dir := t.TempDir()
	if err := SaveKey(dir, []byte("first")); err != nil {
		t.Fatalf("first SaveKey: %v", err)
	}
	if err := SaveKey(dir, []byte("second")); err != nil {
		t.Fatalf("second SaveKey: %v", err)
	}
	got, err := LoadKey(dir)
	if err != nil {
		t.Fatalf("LoadKey: %v", err)
	}
	if string(got) != "second" {
		t.Errorf("want second got %s", got)
	}
}

func TestLoadKey_CorruptedDPAPI_ReturnsErrUnwrapFailed(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("DPAPI-only; on Unix wrap is identity so random bytes round-trip cleanly")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "key.bin"), []byte("not a real dpapi blob"), 0o600); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := LoadKey(dir)
	if !errors.Is(err, ErrUnwrapFailed) {
		t.Errorf("want ErrUnwrapFailed got %v", err)
	}
}
