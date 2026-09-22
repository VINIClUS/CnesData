// Package transport — mtls: mTLS-enabled http.Client for agent → central_api.
//
// Loads cert + key from Phase 4 storage (internal/auth), verifies server's
// cert against a pinned root CA (or, if none given, the platform trust
// store), supports lock-free hot-reload of the client cert via
// atomic.Pointer[tls.Certificate] exposed through
// tls.Config.GetClientCertificate.
package transport

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

// DefaultClientTimeout caps full request lifetime (handshake + body) to
// prevent goroutines from hanging on flaky municipal WAN links.
const DefaultClientTimeout = 60 * time.Second

var (
	ErrCAPinInvalid   = errors.New("transport: ca pin pem invalid")
	ErrCertLoad       = errors.New("transport: cert load failed")
	ErrKeyLoad        = errors.New("transport: key load failed")
	ErrKeyParseFailed = errors.New("transport: pkcs8 key parse failed")
)

type Client struct {
	authDir    string
	httpClient *http.Client
	cert       atomic.Pointer[tls.Certificate]
}

// NewMTLSClient builds the client used for mTLS calls to central_api. A nil
// or empty caPinPEM leaves tls.Config.RootCAs nil, which falls back to the
// platform trust store (nil, not an empty pool — an empty pool trusts
// nothing).
func NewMTLSClient(authDir string, caPinPEM []byte) (*Client, error) {
	var caPool *x509.CertPool
	if len(caPinPEM) > 0 {
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caPinPEM) {
			return nil, ErrCAPinInvalid
		}
	}

	c := &Client{authDir: authDir}
	initialCert, err := loadCertificate(authDir)
	if err != nil {
		return nil, err
	}
	c.cert.Store(initialCert)

	tlsCfg := &tls.Config{
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS13,
		GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return c.cert.Load(), nil
		},
	}
	transport := &http.Transport{TLSClientConfig: tlsCfg}
	c.httpClient = &http.Client{
		Transport: transport,
		Timeout:   DefaultClientTimeout,
	}
	return c, nil
}

func (c *Client) HTTPClient() *http.Client {
	return c.httpClient
}

func (c *Client) Reload() error {
	newCert, err := loadCertificate(c.authDir)
	if err != nil {
		return err
	}
	c.cert.Store(newCert)
	return nil
}

func loadCertificate(authDir string) (*tls.Certificate, error) {
	certPEM, err := auth.LoadCert(authDir)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrCertLoad, err)
	}
	keyDER, err := auth.LoadKey(authDir)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrKeyLoad, err)
	}
	key, err := x509.ParsePKCS8PrivateKey(keyDER)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyParseFailed, err)
	}
	block, _ := pem.Decode(certPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, ErrCertLoad
	}
	return &tls.Certificate{
		Certificate: [][]byte{block.Bytes},
		PrivateKey:  key,
	}, nil
}
