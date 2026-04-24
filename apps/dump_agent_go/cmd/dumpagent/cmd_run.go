package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/fbdriver"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/platform"
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

func runForeground(ctx context.Context, verbose bool, flags RunFlags) int {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	logsDir, err := platform.LogsDir()
	if err != nil {
		slog.Error("logs_dir_init", "err", err.Error())
		return 1
	}
	handler, closer := obs.NewRotatingHandler(filepath.Join(logsDir, "dumpagent.log"), level)
	defer closer()
	slog.SetDefault(slog.New(handler))

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

	db, err := openFirebird()
	if err != nil {
		slog.Error("firebird_open", "err", err.Error())
		return 1
	}
	defer db.Close()

	apiClient, err := buildAPIClient(machineID)
	if err != nil {
		slog.Error("api_client_init", "err", err.Error())
		return 1
	}

	_ = buildDispatchConfig(flags, apiClient)

	source, err := buildJobSource()
	if err != nil {
		slog.Error("source_init", "err", err.Error())
		return 1
	}

	var exe worker.JobExecutorIface
	if os.Getenv("DUMP_SHADOW_MODE") == "true" {
		shadowDir := envOr("DUMP_SHADOW_DIR", filepath.Join(appData, "shadow"))
		slog.Info("shadow_mode_enabled", "output_dir", shadowDir)
		exe = &worker.ShadowExecutor{DB: db, OutputDir: shadowDir}
	} else {
		exe = &worker.JobExecutor{DB: db, Uploader: upload.NewHTTP(nil)}
	}
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

func openFirebird() (*sql.DB, error) {
	cfg := fbdriver.ConnConfig{
		Host:     os.Getenv("DB_HOST"),
		Port:     fbPort(),
		Path:     os.Getenv("DB_PATH"),
		User:     envOr("DB_USER", "SYSDBA"),
		Password: os.Getenv("DB_PASSWORD"),
		Charset:  envOr("DB_CHARSET", "WIN1252"),
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

func buildAPIClient(machineID string) (*apiclient.Adapter, error) {
	baseURL := envOr("CENTRAL_API_URL", "http://localhost:8000")
	tenantID := os.Getenv("TENANT_ID")
	if tenantID == "" {
		return nil, &stubErr{msg: "env_required var=TENANT_ID"}
	}
	return apiclient.NewAdapter(baseURL, tenantID, machineID, nil)
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

// buildDispatchConfig constrói worker.DispatchConfig para BPA_MAG/SIA_LOCAL
// a partir dos flags da CLI + adapter (para RegisterFunc).
// O dispatcher BPA/SIA é acionado em fluxo distinto do JobExecutor clássico
// (CNES/SIHD seguem por intentPipelines).
func buildDispatchConfig(flags RunFlags, adapter *apiclient.Adapter) worker.DispatchConfig {
	var register worker.RegisterFunc
	if adapter != nil {
		register = adapter.RegisterBPASIAJob
	}
	return worker.DispatchConfig{
		BPA: worker.BPAPipelineConfig{
			GDBPath:      flags.BPAGDBPath,
			FBHost:       envOr("DB_HOST", "localhost"),
			FBPort:       fbPort(),
			FBUser:       envOr("DB_USER", "SYSDBA"),
			FBPassword:   os.Getenv("DB_PASSWORD"),
			FBClientPath: flags.FBClientPath,
			Uploader:     upload.NewHTTP(nil),
			Register:     register,
		},
		SIA: worker.SIAPipelineConfig{
			SIADir:   flags.SIADir,
			Uploader: upload.NewHTTP(nil),
			Register: register,
		},
	}
}
