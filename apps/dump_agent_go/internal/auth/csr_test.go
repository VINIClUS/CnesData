package auth

import (
	"crypto/elliptic"
	"crypto/x509"
	"encoding/pem"
	"strings"
	"testing"
)

func TestGenerateKeyAndCSR_ReturnsECP256Key(t *testing.T) {
	key, _, err := GenerateKeyAndCSR("test-machine-id")
	if err != nil {
		t.Fatalf("GenerateKeyAndCSR: %v", err)
	}
	if key == nil {
		t.Fatal("nil key")
	}
	if key.Curve != elliptic.P256() {
		t.Errorf("want P256 curve got %v", key.Curve)
	}
}

func TestGenerateKeyAndCSR_SubjectCN_MatchesMachineID(t *testing.T) {
	machineID := "abc12345"
	_, csrPEM, err := GenerateKeyAndCSR(machineID)
	if err != nil {
		t.Fatalf("GenerateKeyAndCSR: %v", err)
	}
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		t.Fatal("pem.Decode returned nil")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificateRequest: %v", err)
	}
	if csr.Subject.CommonName != machineID {
		t.Errorf("want CN=%s got %s", machineID, csr.Subject.CommonName)
	}
}

func TestGenerateKeyAndCSR_SignatureValid(t *testing.T) {
	_, csrPEM, err := GenerateKeyAndCSR("test")
	if err != nil {
		t.Fatalf("GenerateKeyAndCSR: %v", err)
	}
	block, _ := pem.Decode(csrPEM)
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificateRequest: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Errorf("CheckSignature: %v", err)
	}
}

func TestGenerateKeyAndCSR_PEMBlockType(t *testing.T) {
	_, csrPEM, err := GenerateKeyAndCSR("test")
	if err != nil {
		t.Fatalf("GenerateKeyAndCSR: %v", err)
	}
	if !strings.Contains(string(csrPEM), "BEGIN CERTIFICATE REQUEST") {
		t.Errorf("expected CERTIFICATE REQUEST block, got: %s", csrPEM)
	}
}

func TestGenerateKeyAndCSR_EmptyMachineID_ReturnsError(t *testing.T) {
	_, _, err := GenerateKeyAndCSR("")
	if err == nil {
		t.Fatal("expected error for empty machineID")
	}
	if !strings.Contains(err.Error(), "machineID") {
		t.Errorf("error should mention machineID, got: %v", err)
	}
}
