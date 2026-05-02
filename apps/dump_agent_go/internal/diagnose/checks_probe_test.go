package diagnose

import (
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestProbeCentralAPI_PASS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()
	c := probeCentralAPI(t.Context(), Config{BaseURL: srv.URL})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q fields=%v)", c.Severity, c.Message, c.Fields)
	}
}

func TestProbeCentralAPI_FAIL_5xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := probeCentralAPI(t.Context(), Config{BaseURL: srv.URL})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestProbeCentralAPI_FAIL_NetUnreachable(t *testing.T) {
	c := probeCentralAPI(t.Context(), Config{BaseURL: "http://127.0.0.1:1"})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestProbeMinIO_PASS_DialOK(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c := probeMinIO(t.Context(), Config{MinIOEP: ln.Addr().String()})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q)", c.Severity, c.Message)
	}
}

func TestProbeMinIO_FAIL_DialRefused(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	c := probeMinIO(t.Context(), Config{MinIOEP: addr})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestProbeFirebird_FAIL_BadDSN(t *testing.T) {
	c := probeFirebird(t.Context(), Config{FBDsn: ""})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestProbeFirebird_PASS_RealFB(t *testing.T) {
	dsn := getEnvOrSkip(t, "FB_TEST_DSN")
	c := probeFirebird(t.Context(), Config{FBDsn: dsn})
	if c.Severity != SeverityPass {
		t.Errorf("got severity %q want PASS (msg=%q)", c.Severity, c.Message)
	}
}

func getEnvOrSkip(t *testing.T, key string) string {
	t.Helper()
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		t.Skipf("%s not set; skipping FB live probe", key)
	}
	return v
}
