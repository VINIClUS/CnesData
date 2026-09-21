package main

import (
	"context"
	"database/sql"
	"log/slog"

	"github.com/cnesdata/dumpagent/internal/apiclient"
	"github.com/cnesdata/dumpagent/internal/auth"
	"github.com/cnesdata/dumpagent/internal/platform"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/transport"
	"github.com/cnesdata/dumpagent/internal/worker"
)

type runBootConfig struct {
	appData   string
	machineID string
	mtls      *transport.Client
	flags     RunFlags
}

type runResources struct {
	db             *sql.DB
	outbox         *queue.Outbox
	innerAPIClient *apiclient.Adapter
	apiClient      worker.JobAPIClient
}

func initializeRun(ctx context.Context, flags RunFlags) (runBootConfig, bool) {
	slog.Info("boot", "version", Version, "mode", "run")
	appData, err := platform.AppDataDir()
	if err != nil {
		slog.Error("app_data_dir", "err", err.Error())
		return runBootConfig{}, false
	}
	machineID, err := platform.ResolveMachineID(appData)
	if err != nil {
		slog.Error("machine_id", "err", err.Error())
		return runBootConfig{}, false
	}
	slog.Info("machine_id_resolved", "machine_id", machineID)
	authDir, err := auth.AuthDir()
	if err != nil {
		slog.Error("auth_dir_init", "err", err.Error())
		return runBootConfig{}, false
	}
	mtls, err := initMTLSClient(authDir)
	if err != nil {
		slog.Error("mtls_init_fatal", "err", err.Error(),
			"hint", "run 'dumpagent register' or set AGENT_ALLOW_INSECURE=true")
		return runBootConfig{}, false
	}
	startRotatorIfPossible(ctx, mtls, authDir, machineID)
	slog.Info("run_flags", "bpa_gdb", flags.BPAGDBPath, "sia_dir", flags.SIADir,
		"fbclient_path", flags.FBClientPath)
	return runBootConfig{
		appData: appData, machineID: machineID, mtls: mtls, flags: flags,
	}, true
}

func openRunResources(ctx context.Context, boot runBootConfig) (*runResources, bool) {
	resolvedPaths, cnesPw, ok := resolveBootConfig(boot.appData, boot.flags)
	if !ok {
		return nil, false
	}
	db, err := openFirebird(resolvedPaths.CNES, cnesPw)
	if err != nil {
		slog.Error("firebird_open", "err", err.Error())
		return nil, false
	}
	inner, err := buildAPIClient(boot.machineID, httpClientFor(boot.mtls))
	if err != nil {
		_ = db.Close()
		slog.Error("api_client_init", "err", err.Error())
		return nil, false
	}
	outbox, apiClient, ok := openOutboxAndStartDrain(ctx, boot.appData, inner)
	if !ok {
		_ = db.Close()
		return nil, false
	}
	return &runResources{db: db, outbox: outbox, innerAPIClient: inner, apiClient: apiClient}, true
}

func (r *runResources) close() {
	_ = r.outbox.Close()
	_ = r.db.Close()
}
