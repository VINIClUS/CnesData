package main

import (
	"crypto/tls"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/cnesdata/dumpagent/internal/auth"
)

func TestRegister_MissingTenantID_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--base-url", "https://x.example"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_MissingBaseURL_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--tenant-id", "354130-uuid"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_BadFlag_ReturnsExit2(t *testing.T) {
	code := cmdRegister([]string{"--definitely-not-a-flag"})
	if code != 2 {
		t.Errorf("exit = %d, want 2", code)
	}
}

func TestRegister_ParseFlags_AllFieldsBound(t *testing.T) {
	flags, err := parseRegisterFlags([]string{
		"--tenant-id", "T",
		"--base-url", "https://x.example",
		"--ca-pin", "/tmp/ca.pem",
		"--scope", "agent-test",
		"--force",
		"--no-smoke",
	})
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if flags.TenantID != "T" || flags.BaseURL != "https://x.example" ||
		flags.CAPinPath != "/tmp/ca.pem" || flags.Scope != "agent-test" ||
		!flags.Force || !flags.NoSmoke {
		t.Errorf("flags wrong: %+v", flags)
	}
}

func TestRegister_ParseFlags_DefaultScope(t *testing.T) {
	flags, _ := parseRegisterFlags([]string{
		"--tenant-id", "T", "--base-url", "https://x.example",
	})
	if flags.Scope != "agent" {
		t.Errorf("Scope default = %q, want %q", flags.Scope, "agent")
	}
}

func TestRegister_DispatchEntryExists(t *testing.T) {
	code := dispatch([]string{"register"})
	if code != 2 {
		t.Errorf("dispatch register = %d, want 2", code)
	}
	_ = strings.Contains // keep import for follow-up tasks
}

func TestLoadCAPin_FlagPathReadsFile(t *testing.T) {
	dir := t.TempDir()
	pemPath := dir + "/test_ca.pem"
	if err := os.WriteFile(pemPath, []byte("-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := loadCAPin(pemPath)
	if err != nil {
		t.Fatalf("loadCAPin: %v", err)
	}
	if !strings.Contains(string(got), "BEGIN CERTIFICATE") {
		t.Errorf("got = %q, want PEM", got)
	}
}

func TestLoadCAPin_FlagPathMissingReturnsError(t *testing.T) {
	_, err := loadCAPin("/definitely/does/not/exist.pem")
	if err == nil {
		t.Fatal("loadCAPin: want error for missing file")
	}
}

func TestLoadCAPin_EmptyPathFallsBackToEmbedded(t *testing.T) {
	got, err := loadCAPin("")
	if err != nil {
		t.Fatalf("loadCAPin: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("loadCAPin returned empty for embedded fallback")
	}
}

func TestNewBootstrapClient_PinValid_ReturnsClient(t *testing.T) {
	pem := mustReadEmbeddedPin(t)
	c, err := newBootstrapClient(pem)
	if err != nil {
		t.Fatalf("newBootstrapClient: %v", err)
	}
	if c.Timeout == 0 {
		t.Error("Timeout = 0, want > 0")
	}
	tr, ok := c.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("Transport type = %T, want *http.Transport", c.Transport)
	}
	if tr.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Errorf("MinVersion = %d, want TLS13 (%d)", tr.TLSClientConfig.MinVersion, tls.VersionTLS13)
	}
	if tr.TLSClientConfig.RootCAs == nil {
		t.Error("RootCAs nil")
	}
}

func TestNewBootstrapClient_PinGarbage_ReturnsError(t *testing.T) {
	_, err := newBootstrapClient([]byte("definitely not pem"))
	if err == nil {
		t.Fatal("newBootstrapClient: want error for garbage")
	}
}

func mustReadEmbeddedPin(t *testing.T) []byte {
	t.Helper()
	if len(auth.CAPinPEM) == 0 {
		t.Fatal("auth.CAPinPEM is empty (embed broken)")
	}
	return auth.CAPinPEM
}

func TestCertExists_NoFile_ReturnsFalse(t *testing.T) {
	dir := t.TempDir()
	if certExists(dir) {
		t.Error("certExists empty dir = true, want false")
	}
}

func TestCertExists_FilePresent_ReturnsTrue(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(dir+"/cert.pem", []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !certExists(dir) {
		t.Error("certExists with file = false, want true")
	}
}
