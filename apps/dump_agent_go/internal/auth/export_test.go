package auth

import (
	"crypto/x509"
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
