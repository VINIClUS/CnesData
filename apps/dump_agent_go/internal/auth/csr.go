package auth

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
)

// GenerateKeyAndCSR creates a fresh EC P-256 keypair + CSR.
//
// Subject CN = machineID; no extensions. Server-side CertAuthority adds
// tenant_id + agent_id custom OIDs to the leaf cert (Phase 1).
//
// Caller persists the key via store.SaveKey(dir, pkcs8DER):
//
//	key, csrPEM, err := auth.GenerateKeyAndCSR(machineID)
//	pkcs8DER, _ := x509.MarshalPKCS8PrivateKey(key)
//	store.SaveKey(dir, pkcs8DER)
//	// send csrPEM to /provision/cert
func GenerateKeyAndCSR(machineID string) (*ecdsa.PrivateKey, []byte, error) {
	if machineID == "" {
		return nil, nil, fmt.Errorf("csr: machineID required")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("csr: keygen: %w", err)
	}
	tmpl := &x509.CertificateRequest{
		Subject:            pkix.Name{CommonName: machineID},
		SignatureAlgorithm: x509.ECDSAWithSHA256,
	}
	derBytes, err := x509.CreateCertificateRequest(rand.Reader, tmpl, key)
	if err != nil {
		return nil, nil, fmt.Errorf("csr: create: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: derBytes,
	})
	return key, csrPEM, nil
}
