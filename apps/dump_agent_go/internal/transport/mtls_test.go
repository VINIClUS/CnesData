package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnesdata/dumpagent/internal/auth"
)

func makeTestCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("ca create: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ca parse: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return cert, key, pemBytes
}

func makeLeafCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, cn string) ([]byte, []byte) {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("leaf key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &leafKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("leaf create: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatalf("leaf key marshal: %v", err)
	}
	return leafPEM, keyDER
}

func seedAuthDir(t *testing.T, dir string, leafPEM, keyDER []byte) {
	t.Helper()
	if err := auth.SaveCert(dir, leafPEM); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	if err := auth.SaveKey(dir, keyDER); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
}

func TestNewMTLSClient_NilCAPin_ReturnsErrCAPinInvalid(t *testing.T) {
	authDir := t.TempDir()
	_, err := NewMTLSClient(authDir, nil)
	if !errors.Is(err, ErrCAPinInvalid) {
		t.Errorf("want ErrCAPinInvalid got %v", err)
	}
}

func TestNewMTLSClient_GarbageCAPin_ReturnsErrCAPinInvalid(t *testing.T) {
	authDir := t.TempDir()
	_, err := NewMTLSClient(authDir, []byte("not a pem"))
	if !errors.Is(err, ErrCAPinInvalid) {
		t.Errorf("want ErrCAPinInvalid got %v", err)
	}
}

func TestNewMTLSClient_AuthDirMissing_ReturnsErrCertLoad(t *testing.T) {
	_, _, caPEM := makeTestCA(t)
	authDir := t.TempDir()
	_, err := NewMTLSClient(authDir, caPEM)
	if !errors.Is(err, ErrCertLoad) {
		t.Errorf("want ErrCertLoad got %v", err)
	}
	if !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("want auth.ErrNotFound chain got %v", err)
	}
}

func TestNewMTLSClient_KeyMissing_ReturnsErrKeyLoad(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafPEM, _ := makeLeafCert(t, caCert, caKey, "agent-001")
	authDir := t.TempDir()
	if err := auth.SaveCert(authDir, leafPEM); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	_, err := NewMTLSClient(authDir, caPEM)
	if !errors.Is(err, ErrKeyLoad) {
		t.Errorf("want ErrKeyLoad got %v", err)
	}
	if !errors.Is(err, auth.ErrNotFound) {
		t.Errorf("want auth.ErrNotFound chain got %v", err)
	}
}

func TestNewMTLSClient_MalformedPKCS8_ReturnsErrKeyParseFailed(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafPEM, _ := makeLeafCert(t, caCert, caKey, "agent-001")
	authDir := t.TempDir()
	if err := auth.SaveCert(authDir, leafPEM); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	if err := auth.SaveKey(authDir, []byte("not pkcs8 bytes at all")); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
	_, err := NewMTLSClient(authDir, caPEM)
	if !errors.Is(err, ErrKeyParseFailed) {
		t.Errorf("want ErrKeyParseFailed got %v", err)
	}
}

// startMTLSServer starts an httptest.Server with TLS configured to require
// client certs signed by caCert. Server's own cert is signed by caCert too.
func startMTLSServer(
	t *testing.T,
	caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	handler http.HandlerFunc,
) *httptest.Server {
	t.Helper()
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("srv key: %v", err)
	}
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(90 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("srv create: %v", err)
	}
	clientPool := x509.NewCertPool()
	clientPool.AddCert(caCert)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{{
			Certificate: [][]byte{srvDER},
			PrivateKey:  srvKey,
		}},
		ClientCAs:  clientPool,
		ClientAuth: tls.RequireAndVerifyClientCert,
		MinVersion: tls.VersionTLS13,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func TestNewMTLSClient_HappyPath_ReturnsClient(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafPEM, keyDER := makeLeafCert(t, caCert, caKey, "agent-happy")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafPEM, keyDER)

	c, err := NewMTLSClient(authDir, caPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}
	if c == nil {
		t.Fatal("nil client")
	}
	if c.HTTPClient() == nil {
		t.Error("nil http.Client")
	}
}

func TestHTTPClient_HandshakeSucceedsAgainstMTLSServer(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafPEM, keyDER := makeLeafCert(t, caCert, caKey, "agent-001")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafPEM, keyDER)

	srv := startMTLSServer(t, caCert, caKey, func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			t.Error("server got no peer certs")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		cn := r.TLS.PeerCertificates[0].Subject.CommonName
		if cn != "agent-001" {
			t.Errorf("want CN=agent-001 got %s", cn)
		}
		w.WriteHeader(http.StatusOK)
	})

	c, err := NewMTLSClient(authDir, caPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}
	resp, err := c.HTTPClient().Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d", resp.StatusCode)
	}
}

func TestHTTPClient_HandshakeFails_WhenServerCertNotPinned(t *testing.T) {
	caACert, caAKey, caAPEM := makeTestCA(t)
	leafPEM, keyDER := makeLeafCert(t, caACert, caAKey, "agent-001")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafPEM, keyDER)

	caBCert, caBKey, _ := makeTestCA(t)
	srv := startMTLSServer(t, caBCert, caBKey, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	c, err := NewMTLSClient(authDir, caAPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}
	_, err = c.HTTPClient().Get(srv.URL + "/x")
	if err == nil {
		t.Fatal("expected handshake error got nil")
	}
	if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "x509") {
		t.Errorf("expected cert verification error, got: %v", err)
	}
}

func TestReload_AfterCertRotation_NewCertSentInHandshake(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafA, keyA := makeLeafCert(t, caCert, caKey, "agent-old")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafA, keyA)

	var lastCN atomic.Value
	lastCN.Store("")
	srv := startMTLSServer(t, caCert, caKey, func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			lastCN.Store(r.TLS.PeerCertificates[0].Subject.CommonName)
		}
		w.WriteHeader(http.StatusOK)
	})

	c, err := NewMTLSClient(authDir, caPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}
	httpClient := c.HTTPClient()
	resp, err := httpClient.Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("Get1: %v", err)
	}
	resp.Body.Close()
	if got := lastCN.Load().(string); got != "agent-old" {
		t.Errorf("first GET want agent-old got %s", got)
	}

	leafB, keyB := makeLeafCert(t, caCert, caKey, "agent-new")
	if err := auth.SaveCert(authDir, leafB); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	if err := auth.SaveKey(authDir, keyB); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
	if err := c.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	httpClient.Transport.(*http.Transport).CloseIdleConnections()

	resp2, err := httpClient.Get(srv.URL + "/y")
	if err != nil {
		t.Fatalf("Get2: %v", err)
	}
	resp2.Body.Close()
	if got := lastCN.Load().(string); got != "agent-new" {
		t.Errorf("after Reload want agent-new got %s", got)
	}
}

func TestReload_FilesMissing_ReturnsErrAndKeepsOldCert(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafA, keyA := makeLeafCert(t, caCert, caKey, "agent-keep")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafA, keyA)

	c, err := NewMTLSClient(authDir, caPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}

	if err := os.Remove(filepath.Join(authDir, "cert.pem")); err != nil {
		t.Fatalf("rm cert: %v", err)
	}
	if err := c.Reload(); err == nil {
		t.Fatal("expected Reload error")
	} else if !errors.Is(err, ErrCertLoad) {
		t.Errorf("want ErrCertLoad got %v", err)
	}

	srv := startMTLSServer(t, caCert, caKey, func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn := r.TLS.PeerCertificates[0].Subject.CommonName
			if cn != "agent-keep" {
				t.Errorf("want agent-keep got %s", cn)
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	resp, err := c.HTTPClient().Get(srv.URL + "/x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	resp.Body.Close()
}

func spamRequests(stop <-chan struct{}, client *http.Client, url string) {
	for {
		select {
		case <-stop:
			return
		default:
			if resp, err := client.Get(url); err == nil {
				resp.Body.Close()
			}
		}
	}
}

func TestReload_ConcurrentWithRequests_NoRace(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	leafA, keyA := makeLeafCert(t, caCert, caKey, "agent-init")
	authDir := t.TempDir()
	seedAuthDir(t, authDir, leafA, keyA)

	srv := startMTLSServer(t, caCert, caKey, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	c, err := NewMTLSClient(authDir, caPEM)
	if err != nil {
		t.Fatalf("NewMTLSClient: %v", err)
	}
	httpClient := c.HTTPClient()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			spamRequests(stop, httpClient, srv.URL+"/x")
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			leaf, key := makeLeafCert(t, caCert, caKey, "agent-rotated")
			if err := auth.SaveCert(authDir, leaf); err != nil {
				t.Errorf("SaveCert: %v", err)
				return
			}
			if err := auth.SaveKey(authDir, key); err != nil {
				t.Errorf("SaveKey: %v", err)
				return
			}
			if err := c.Reload(); err != nil {
				t.Errorf("Reload: %v", err)
				return
			}
		}
		close(stop)
	}()

	wg.Wait()
}


func TestNewMTLSClient_WrongPEMBlockType_ReturnsErrCertLoad(t *testing.T) {
	caCert, caKey, caPEM := makeTestCA(t)
	_, keyDER := makeLeafCert(t, caCert, caKey, "agent-001")
	authDir := t.TempDir()
	wrongPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if err := auth.SaveCert(authDir, wrongPEM); err != nil {
		t.Fatalf("SaveCert: %v", err)
	}
	if err := auth.SaveKey(authDir, keyDER); err != nil {
		t.Fatalf("SaveKey: %v", err)
	}
	_, err := NewMTLSClient(authDir, caPEM)
	if !errors.Is(err, ErrCertLoad) {
		t.Errorf("want ErrCertLoad got %v", err)
	}
}
