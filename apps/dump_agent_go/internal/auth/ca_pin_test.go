package auth_test

import (
	"testing"

	"github.com/cnesdata/dumpagent/internal/auth"
)

func TestCAPinPEM_NilByDefault(t *testing.T) {
	if len(auth.CAPinPEM) != 0 {
		t.Fatalf("auth.CAPinPEM = %d bytes, want nil (system trust store default)", len(auth.CAPinPEM))
	}
}
