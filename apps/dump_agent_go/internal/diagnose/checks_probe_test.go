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

func TestProbeCentralAPI_FAIL_BaseURLMissing(t *testing.T) {
	c := probeCentralAPI(t.Context(), Config{BaseURL: ""})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
	if c.Message != "base_url_missing" {
		t.Errorf("got msg %q want base_url_missing", c.Message)
	}
}

func TestProbeCentralAPI_WARN_UnexpectedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"unknown"}`))
	}))
	defer srv.Close()
	c := probeCentralAPI(t.Context(), Config{BaseURL: srv.URL})
	if c.Severity != SeverityWarn {
		t.Errorf("got severity %q want WARN (msg=%q)", c.Severity, c.Message)
	}
	if c.Message != "unexpected_body" {
		t.Errorf("got msg %q want unexpected_body", c.Message)
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

func TestProbeMinIO_FAIL_EndpointMissing(t *testing.T) {
	c := probeMinIO(t.Context(), Config{MinIOEP: ""})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
	if c.Message != "endpoint_missing" {
		t.Errorf("got msg %q want endpoint_missing", c.Message)
	}
}

func TestProbeFirebird_FAIL_BadDSN(t *testing.T) {
	c := probeFirebird(t.Context(), Config{FBDsn: ""})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", c.Severity)
	}
}

func TestProbeFirebird_FAIL_DriverOrConnectError(t *testing.T) {
	c := probeFirebird(t.Context(), Config{FBDsn: "user:pass@127.0.0.1:1/nonexistent.gdb"})
	if c.Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL (msg=%q fields=%v)", c.Severity, c.Message, c.Fields)
	}
	if c.Message != "open_error" && c.Message != "query_error" {
		t.Errorf("got msg %q want open_error|query_error", c.Message)
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

func TestRedactDSN_WithAt(t *testing.T) {
	got := redactDSN("user:pass@host:3050/db.gdb")
	want := "***@host:3050/db.gdb"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestRedactDSN_NoAt(t *testing.T) {
	in := "host:3050/db.gdb"
	got := redactDSN(in)
	if got != in {
		t.Errorf("got %q want %q (unchanged)", got, in)
	}
}
