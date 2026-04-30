package main

import (
	"strings"
	"testing"
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
