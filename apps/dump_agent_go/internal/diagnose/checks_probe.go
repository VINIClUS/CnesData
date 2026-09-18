package diagnose

import (
	"context"
	"database/sql"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

const probeNetTimeout = 5 * time.Second

func init() {
	probeChecks = []CheckFunc{probeCentralAPI, probeFirebird, probeMinIO}
}

func probeCentralAPI(ctx context.Context, cfg Config) Check {
	if cfg.BaseURL == "" {
		return Check{Name: "central_api", Severity: SeverityFail,
			Message: "base_url_missing"}
	}
	url := strings.TrimRight(cfg.BaseURL, "/") + "/api/v1/system/health"
	probeCtx, cancel := context.WithTimeout(ctx, probeNetTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, url, nil)
	if err != nil {
		return Check{Name: "central_api", Severity: SeverityFail,
			Message: "request_build_error",
			Fields:  map[string]any{"err": err.Error()}}
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	latencyMs := time.Since(start).Milliseconds()
	if err != nil {
		return Check{Name: "central_api", Severity: SeverityFail,
			Message: "network_unreachable",
			Fields:  map[string]any{"base_url": cfg.BaseURL, "err": err.Error(), "latency_ms": latencyMs}}
	}
	defer resp.Body.Close()
	return parseHealthResponse(resp, cfg.BaseURL, latencyMs)
}

// parseHealthResponse interprets a /system/health HTTP response.
func parseHealthResponse(resp *http.Response, baseURL string, latencyMs int64) Check {
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	fields := map[string]any{
		"base_url":    baseURL,
		"status_code": resp.StatusCode,
		"latency_ms":  latencyMs,
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Check{Name: "central_api", Severity: SeverityFail,
			Message: "non_2xx_status", Fields: fields}
	}
	body := string(bodyBytes)
	if !strings.Contains(body, "ok") && !strings.Contains(body, "healthy") {
		fields["body_excerpt"] = body
		return Check{Name: "central_api", Severity: SeverityWarn,
			Message: "unexpected_body", Fields: fields}
	}
	return Check{Name: "central_api", Severity: SeverityPass,
		Message: "reachable", Fields: fields}
}

func probeFirebird(ctx context.Context, cfg Config) Check {
	if cfg.FBDsn == "" {
		return Check{Name: "firebird", Severity: SeverityFail,
			Message: "dsn_missing"}
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeNetTimeout)
	defer cancel()
	db, err := sql.Open("firebirdsql", cfg.FBDsn)
	if err != nil {
		return Check{Name: "firebird", Severity: SeverityFail,
			Message: "open_error",
			Fields:  map[string]any{"err": err.Error()}}
	}
	defer db.Close()
	start := time.Now()
	row := db.QueryRowContext(probeCtx, "SELECT 1 FROM RDB$DATABASE")
	var v int
	if err := row.Scan(&v); err != nil {
		return Check{Name: "firebird", Severity: SeverityFail,
			Message: "query_error",
			Fields:  map[string]any{"err": err.Error(), "latency_ms": time.Since(start).Milliseconds()}}
	}
	return Check{Name: "firebird", Severity: SeverityPass,
		Message: "reachable",
		Fields: map[string]any{
			"dsn_redacted": redactDSN(cfg.FBDsn),
			"latency_ms":   time.Since(start).Milliseconds(),
		}}
}

func probeMinIO(ctx context.Context, cfg Config) Check {
	if cfg.MinIOEP == "" {
		return Check{Name: "minio", Severity: SeverityFail,
			Message: "endpoint_missing"}
	}
	start := time.Now()
	conn, err := net.DialTimeout("tcp", cfg.MinIOEP, probeNetTimeout)
	latencyMs := time.Since(start).Milliseconds()
	if err != nil {
		return Check{Name: "minio", Severity: SeverityFail,
			Message: "dial_failed",
			Fields:  map[string]any{"endpoint": cfg.MinIOEP, "err": err.Error(), "latency_ms": latencyMs}}
	}
	_ = conn.Close()
	return Check{Name: "minio", Severity: SeverityPass,
		Message: "reachable",
		Fields:  map[string]any{"endpoint": cfg.MinIOEP, "latency_ms": latencyMs}}
}

// redactDSN strips user/password from a DSN like "user:pass@host:port/path".
func redactDSN(dsn string) string {
	at := strings.Index(dsn, "@")
	if at == -1 {
		return dsn
	}
	return "***@" + dsn[at+1:]
}
