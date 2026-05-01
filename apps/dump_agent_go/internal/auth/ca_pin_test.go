package auth_test

import (
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/cnesdata/dumpagent/internal/auth"
)

func TestCAPinPEM_NotEmpty(t *testing.T) {
	if len(auth.CAPinPEM) == 0 {
		t.Fatal("embedded root_ca.pem is empty")
	}
}

func TestCAPinPEM_ParsesAsCertificate(t *testing.T) {
	block, _ := pem.Decode(auth.CAPinPEM)
	if block == nil {
		t.Fatal("pem.Decode returned nil block")
	}
	if block.Type != "CERTIFICATE" {
		t.Fatalf("block.Type = %q, want CERTIFICATE", block.Type)
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}
}

func TestCAPinPEM_IsCA(t *testing.T) {
	block, _ := pem.Decode(auth.CAPinPEM)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !cert.IsCA {
		t.Error("embedded cert IsCA = false, want true")
	}
}
