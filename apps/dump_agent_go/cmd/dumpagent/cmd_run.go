package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/auth"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/discover"
	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/cnesdata/dumpagent/internal/secrets"
	"github.com/cnesdata/dumpagent/internal/service"
	"github.com/cnesdata/dumpagent/internal/transport"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
)

// RunFlags parâmetros CLI extras passados via `dumpagent run`.
// BPA/SIA only; CNES/SIHD seguem via env vars tradicionais.
type RunFlags struct {
	BPAGDBPath   string
	SIADir       string
	FBClientPath string
}

func defaultRunFlags() RunFlags {
	return RunFlags{
		BPAGDBPath:   os.Getenv("BPA_GDB_PATH"),
		SIADir:       os.Getenv("SIA_DIR"),
		FBClientPath: os.Getenv("FBCLIENT_PATH"),
	}
}

func parseRunFlags(args []string) RunFlags {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	bpaGdb := fs.String("bpa-gdb", os.Getenv("BPA_GDB_PATH"), "BPAMAG.GDB absolute path")
	siaDir := fs.String("sia-dir", os.Getenv("SIA_DIR"), "SIA DBF directory")
	fbClient := fs.String("fbclient-path", os.Getenv("FBCLIENT_PATH"), "fbclient.dll path (Windows x86)")
	fs.Bool("verbose", false, "enable DEBUG logging")
	fs.Bool("v", false, "enable DEBUG logging (short)")
	_ = fs.Parse(args)
	return RunFlags{BPAGDBPath: *bpaGdb, SIADir: *siaDir, FBClientPath: *fbClient}
}

// setupBootLogger resolves logs dir + builds the rotating/eventlog handler
// and installs it as slog default. Returns (closer, true) on success;
// (nil, false) on logs_dir_init failure (caller should exit 1).
func setupBootLogger(verbose bool) (func(), bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	logsDir, err := platform.LogsDir()
	if err != nil {
		slog.Error("logs_dir_init", "err", err.Error())
		return nil, false
	}
	handler, closer := buildLoggerHandler(
		filepath.Join(logsDir, "dumpagent.log"), level)
	slog.SetDefault(slog.New(handler))
	return closer, true
}

// loadDiscoverYAML loads %PROGRAMDATA%\dumpagent\config.yaml unless the
// AGENT_DISABLE_DISCOVER bypass env is set. Missing file is not an error
// (returns empty Config); only parse errors propagate.
func loadDiscoverYAML(path string) (discover.Config, error) {
	if os.Getenv("AGENT_DISABLE_DISCOVER") == "true" {
		return discover.Config{}, nil
	}
	cfg, err := discover.LoadYAML(path)
	if err != nil {
		if errors.Is(err, discover.ErrNoYAML) {
			return discover.Config{}, nil
		}
		return discover.Config{}, err
	}
	return cfg, nil
}

func logOverrides(logger *slog.Logger, overrides []OverrideRecord) {
	for _, o := range overrides {
		logger.Warn("config_override",
			"layer", o.Layer.String(),
			"source", o.Source,
			"field", o.Field)
	}
}

func logPasswordSource(logger *slog.Logger, source string, ps PasswordSource) {
	if ps == PasswordSourceMasterkey {
		logger.Warn("password_default_active",
			"source", source,
			"hint", "run dumpagent set-secret "+source)
		return
	}
	logger.Info("password_source",
		"source", source,
		"value", ps.String())
}

// runFlagsToCLIFlags adapts RunFlags to the per-source FBDSNFlags.
// Today RunFlags carries BPA + SIA only; CNES + SIHD ride env-only at
// the legacy code path.
func runFlagsToCLIFlags(rf RunFlags) RunCLIFlags {
	return RunCLIFlags{
		BPA: FBDSNFlags{DatabasePath: rf.BPAGDBPath},
		SIA: rf.SIADir,
	}
}

// resolveBootConfig loads YAML, applies CLI/env/YAML resolution chain,
// resolves CNES password, and emits override/source WARN logs. Returns
// (PathConfig, cnesPassword, true) on success; (zero, "", false) on any
// fatal step (caller should exit 1).
func resolveBootConfig(appData string, flags RunFlags) (PathConfig, string, bool) {
	yamlPath := filepath.Join(appData, "config.yaml")
	cfg, err := loadDiscoverYAML(yamlPath)
	if err != nil {
		slog.Error("yaml_invalid", "path", yamlPath, "err", err.Error())
		return PathConfig{}, "", false
	}
	resolved := ResolvePathConfig(cfg, os.Getenv, runFlagsToCLIFlags(flags))
	logOverrides(slog.Default(), resolved.Overrides)
	store := secrets.NewStore(filepath.Join(appData, "secrets"))
	pw, src, err := ResolvePassword("cnes", resolved.CNES.User,
		os.Getenv, store.Load)
	if err != nil {
		slog.Error("password_resolve", "source", "cnes", "err", err.Error())
		return PathConfig{}, "", false
	}
	logPasswordSource(slog.Default(), "cnes", src)
	return resolved, pw, true
}

func runForeground(ctx context.Context, verbose bool, flags RunFlags) int {
	closer, ok := setupBootLogger(verbose)
	if !ok {
		return 1
	}
	defer closer()

	slog.Info("boot", "version", Version, "mode", "run")

	appData, err := platform.AppDataDir()
	if err != nil {
		slog.Error("app_data_dir", "err", err.Error())
		return 1
	}

	machineID, err := platform.ResolveMachineID(appData)
	if err != nil {
		slog.Error("machine_id", "err", err.Error())
		return 1
	}
	slog.Info("machine_id_resolved", "machine_id", machineID)

	authDir, err := auth.AuthDir()
	if err != nil {
		slog.Error("auth_dir_init", "err", err.Error())
		return 1
	}

	mtlsClient, err := initMTLSClient(authDir)
	if err != nil {
		slog.Error("mtls_init_fatal",
			"err", err.Error(),
			"hint", "run 'dumpagent register' or set AGENT_ALLOW_INSECURE=true")
		return 1
	}

	startRotatorIfPossible(ctx, mtlsClient, authDir, machineID)

	slog.Info("run_flags",
		"bpa_gdb", flags.BPAGDBPath,
		"sia_dir", flags.SIADir,
		"fbclient_path", flags.FBClientPath,
	)

	if err := preFlightClockCheck(ctx); err != nil {
		slog.Error("clock_fatal", "err", err.Error())
		_ = os.WriteFile(filepath.Join(appData, "CLOCK_FATAL.txt"),
			[]byte(err.Error()+"\nRun: w32tm /resync or configure NTP\n"), 0o644)
		return 1
	}

	lock, err := platform.AcquireSingleInstanceLock(appData, "dumpagent")
	if err != nil {
		slog.Error("lock_failed", "err", err.Error())
		return 1
	}
	defer func() { _ = lock.Release() }()

	jitter := time.Duration(rand.Int63n(int64(maxJitter()))) * time.Nanosecond
	slog.Info("startup_jitter", "duration", jitter.String())
	if !sleepCancellable(ctx, jitter) {
		return 0
	}

	resolvedPaths, cnesPw, ok := resolveBootConfig(appData, flags)
	if !ok {
		return 1
	}

	db, err := openFirebird(resolvedPaths.CNES, cnesPw)
	if err != nil {
		slog.Error("firebird_open", "err", err.Error())
		return 1
	}
	defer db.Close()

	innerAPIClient, err := buildAPIClient(machineID, httpClientFor(mtlsClient))
	if err != nil {
		slog.Error("api_client_init", "err", err.Error())
		return 1
	}

	outbox, apiClient, ok := openOutboxAndStartDrain(ctx, appData, innerAPIClient)
	if !ok {
		return 1
	}
	defer func() { _ = outbox.Close() }()

	_ = buildDispatchConfig(flags, innerAPIClient)

	source, err := buildJobSource()
	if err != nil {
		slog.Error("source_init", "err", err.Error())
		return 1
	}

	deltaStore, deltaCloser := wireDeltaStore(appData)
	defer deltaCloser()

	exe := buildExecutor(appData, db, deltaStore)
	cons := worker.NewConsumer(apiClient, source, exe, worker.ConsumerConfig{
		PollInterval:      5 * time.Second,
		InterJobJitterMax: 5 * time.Second,
		HeartbeatInterval: 5 * time.Minute,
	})

	if err := cons.Loop(ctx); err != nil {
		slog.Error("loop_error", "err", err.Error())
		return 1
	}
	slog.Info("shutdown_clean")
	return 0
}

func startRotatorIfPossible(
	ctx context.Context, mtls *transport.Client, authDir, machineID string,
) {
	if mtls == nil {
		slog.Info("rotator_skipped", "reason", "no_mtls")
		return
	}
	baseURL := envOr("CENTRAL_API_URL", "http://localhost:8000")
	rotator := auth.NewRotator(mtls, authDir, baseURL, machineID)
	_ = obs.SafeGo(func() error {
		if rerr := rotator.Run(ctx); rerr != nil {
			slog.Error("rotator_terminal", "err", rerr.Error())
		}
		return nil
	}, "rotator")
	slog.Info("rotator_started", "machine_id", machineID)
}

// initMTLSClient constructs the shared mTLS http.Client used by both
// the rotator (Phase 7) and apiclient (Phase 8). Fail-closed by
// default: cert load failures abort boot. Set AGENT_ALLOW_INSECURE=true
// to fall back to plain HTTP during fleet rollout. Logs the outcome
// once at boot; callers do NOT re-log.
func initMTLSClient(authDir string) (*transport.Client, error) {
	mtls, err := transport.NewMTLSClient(authDir, auth.CAPinPEM)
	if err == nil {
		slog.Info("mtls_init_ok")
		return mtls, nil
	}
	if os.Getenv("AGENT_ALLOW_INSECURE") == "true" {
		slog.Warn("mtls_fallback_active",
			"reason", err.Error(),
			"AGENT_ALLOW_INSECURE", "true")
		return nil, nil
	}
	return nil, err
}

// httpClientFor returns mtls.HTTPClient() or nil. nil-handling lets
// apiclient.NewAdapter fall back to http.DefaultClient (plain HTTP)
// when AGENT_ALLOW_INSECURE=true permitted insecure boot.
func httpClientFor(mtls *transport.Client) *http.Client {
	if mtls == nil {
		return nil
	}
	return mtls.HTTPClient()
}

func preFlightClockCheck(ctx context.Context) error {
	skew, err := platform.CheckClockSkew(ctx, nil, 5*time.Second)
	if err != nil {
		slog.Warn("ntp_unreachable", "err", err.Error())
		return nil
	}
	level := platform.ClassifySkew(skew)
	slog.Info("ntp_skew", "skew", skew.String(), "level", level.String())
	if level == platform.SkewFatal {
		return &clockFatalErr{skew: skew}
	}
	return nil
}

type clockFatalErr struct{ skew time.Duration }

func (c *clockFatalErr) Error() string {
	return "clock_fatal skew=" + c.skew.String() + " (>60min). Fix NTP sync."
}

func sleepCancellable(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func maxJitter() time.Duration {
	raw := os.Getenv("DUMP_MAX_JITTER_SECONDS")
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		return 30 * time.Minute
	}
	return time.Duration(secs) * time.Second
}

func openFirebird(p discover.FBDSN, password string) (*sql.DB, error) {
	cfg := fbdriver.ConnConfig{
		Host:     p.Host,
		Port:     p.Port,
		Path:     p.DatabasePath,
		User:     p.User,
		Password: password,
		Charset:  p.Charset,
	}
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	db, err := sql.Open("firebirdsql", fbdriver.BuildDSN(cfg))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	return db, nil
}

func fbPort() int {
	p, err := strconv.Atoi(os.Getenv("DB_PORT"))
	if err != nil || p <= 0 {
		return 3050
	}
	return p
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func buildAPIClient(machineID string, httpClient *http.Client) (*apiclient.Adapter, error) {
	baseURL := envOr("CENTRAL_API_URL", "http://localhost:8000")
	tenantID := os.Getenv("TENANT_ID")
	if tenantID == "" {
		return nil, &stubErr{msg: "env_required var=TENANT_ID"}
	}
	return apiclient.NewAdapter(baseURL, tenantID, machineID, httpClient)
}

func buildJobSource() (worker.JobSpecSource, error) {
	fonte := envOr("FONTE_SISTEMA", "CNES_LOCAL")
	tipo := envOr("TIPO_EXTRACAO", "estabelecimentos")
	intent := envOr("INTENT", "estabelecimentos")
	compRaw := os.Getenv("COMPETENCIA_YYYYMM")
	if compRaw == "" {
		return nil, &stubErr{msg: "env_required var=COMPETENCIA_YYYYMM"}
	}
	comp, err := strconv.Atoi(compRaw)
	if err != nil {
		return nil, fmt.Errorf("invalid_competencia value=%q: %w", compRaw, err)
	}
	return worker.NewStaticSource(worker.StaticSpec{
		FonteSistema: fonte,
		TipoExtracao: tipo,
		Competencia:  comp,
		Intent:       intent,
	}), nil
}

type stubErr struct{ msg string }

func (s *stubErr) Error() string { return s.msg }

// buildLoggerHandler composes the rotating-file handler with the Windows
// Event Log handler under a MultiHandler. EventLogHandler is a no-op
// on non-Windows. Both Close functions are tied to the returned closer.
func buildLoggerHandler(logPath string, level slog.Level) (slog.Handler, func()) {
	rotating, rotatingCloser := obs.NewRotatingHandler(logPath, level)
	eventlog, _ := obs.NewEventLogHandler(service.EventSourceName)
	multi := obs.NewMultiHandler(rotating, eventlog)
	return multi, func() {
		rotatingCloser()
		_ = eventlog.Close()
	}
}

// buildExecutor returns a ShadowExecutor when DUMP_SHADOW_MODE=true,
// otherwise a live JobExecutor. deltaStore != nil routes the executor
// through the delta path (P3 R1); nil keeps legacy snapshot behavior.
func buildExecutor(
	appData string, db *sql.DB, deltaStore *delta.Store,
) worker.JobExecutorIface {
	if os.Getenv("DUMP_SHADOW_MODE") == "true" {
		shadowDir := envOr("DUMP_SHADOW_DIR", filepath.Join(appData, "shadow"))
		slog.Info("shadow_mode_enabled", "output_dir", shadowDir)
		return &worker.ShadowExecutor{DB: db, OutputDir: shadowDir}
	}
	return &worker.JobExecutor{
		DB:         db,
		Uploader:   upload.NewHTTP(nil),
		DeltaStore: deltaStore,
	}
}

// wireDeltaStore opens the delta state DB and returns the store + a closer.
// Delta mode is the only execution path; failure to open is fatal at boot.
func wireDeltaStore(appData string) (*delta.Store, func()) {
	store := openDeltaStore(appData)
	if store == nil {
		return nil, func() {}
	}
	slog.Info("delta_mode_enabled")
	return store, func() { _ = store.Close() }
}

// openDeltaStore opens the delta state DB at <appData>/state/delta.db and
// runs a 24h GC of stale pending sub-buckets. Returns nil on open error
// (caller boots without delta wiring; cycle-level errors surface separately).
func openDeltaStore(appData string) *delta.Store {
	path := filepath.Join(appData, "state", "delta.db")
	store, err := delta.Open(path)
	if err != nil {
		slog.Error("delta_store_open_failed",
			"path", path, "err", err.Error())
		return nil
	}
	count, gcErr := store.GarbageCollectStalePending(24 * time.Hour)
	if gcErr != nil {
		slog.Warn("delta_pending_gc_failed", "err", gcErr.Error())
	} else if count > 0 {
		slog.Info("delta_pending_gc", "count", count)
	}
	return store
}

