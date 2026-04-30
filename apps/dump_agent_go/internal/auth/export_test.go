package auth

import (
	"context"
	"crypto/x509"
	"fmt"
	"net/http"
	"time"
)

// SetClockSleep is a test-only helper to inject deterministic clock + sleep.
// Same package as Client; available only during `go test`.
func (c *Client) SetClockSleep(now func() time.Time, sleep func(time.Duration)) {
	c.clock = now
	c.sleep = sleep
}

// Phase 6: tests swap the embedded pin per-case.
func SetCAPinPEM(b []byte) { CAPinPEM = b }

// Phase 7 step 2 exports.
func ParseCurrentCertForTest(dir string) (*x509.Certificate, error) {
	return parseCurrentCert(dir)
}

func IsWithinWindowForTest(cert *x509.Certificate, fraction float64, now time.Time) bool {
	return isWithinWindow(cert, fraction, now)
}

// Phase 7 step 3 exports.
func PostRotateForTest(
	ctx context.Context, httpClient *http.Client, baseURL, csrPEM string,
	backoff []time.Duration,
) (any, error) {
	return postRotate(ctx, httpClient, baseURL, csrPEM, backoff)
}

func PersistRotatedForTest(authDir string, resp any, pkcs8DER []byte) error {
	r, ok := resp.(*rotateResp)
	if !ok {
		return fmt.Errorf("PersistRotatedForTest: bad resp type %T", resp)
	}
	return persistRotated(authDir, r, pkcs8DER)
}

func MakeRotateRespForTest(certPEM, caChainPEM, expiresAt string) any {
	return &rotateResp{
		CertPEM:    certPEM,
		CAChainPEM: caChainPEM,
		ExpiresAt:  expiresAt,
	}
}

func ExtractRotateRespForTest(resp any) (certPEM, expiresAt string) {
	r, ok := resp.(*rotateResp)
	if !ok {
		return "", ""
	}
	return r.CertPEM, r.ExpiresAt
}
