package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"time"

	"github.com/cnesdata/dumpagent/internal/worker"
)

type workerRunConfig struct {
	appData   string
	db        *sql.DB
	machineID string
}

func runWorker(ctx context.Context, cfg workerRunConfig, apiClient worker.JobAPIClient) int {
	source, err := buildJobSource()
	if err != nil {
		slog.Error("source_init", "err", err.Error())
		return 1
	}

	deltaStore, deltaCloser := wireDeltaStore(cfg.appData)
	defer deltaCloser()
	auditLogger := wireAuditLogger(cfg.appData, cfg.machineID, os.Getenv("TENANT_ID"))

	exe, err := buildExecutor(cfg.appData, cfg.db, deltaStore, auditLogger)
	if err != nil {
		slog.Error("executor_init", "err", err.Error())
		return 1
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
