package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
	"github.com/cnesdata/dumpagent/internal/manifest"
	"github.com/cnesdata/dumpagent/internal/obs"
	"github.com/cnesdata/dumpagent/internal/queue"
	"github.com/cnesdata/dumpagent/internal/rawclient"
	"github.com/cnesdata/dumpagent/internal/secrets"
	"github.com/cnesdata/dumpagent/internal/upload"
	"github.com/cnesdata/dumpagent/internal/worker"
)

type rawRun struct {
	client  *rawclient.Client
	outbox  *queue.Outbox
	exe     *worker.JobExecutor
	drainer *worker.RawDrainer
	tenant  string
}

type rawDatabases struct {
	cnes *sql.DB
	sihd *sql.DB
	bpa  *sql.DB
	sia  string
}

func rawJobFromClaim(
	client *rawclient.Client, tenant string, claim rawclient.Claim,
) (*worker.Job, error) {
	if !manifest.ValidRawPair(manifest.SourceType(claim.SourceType), claim.FileSubtype) ||
		claim.FencingToken == 0 || claim.AgentID == "" {
		return nil, fmt.Errorf("raw_claim=invalid")
	}
	url, err := client.UploadURL(claim.RawUploadPath)
	if err != nil {
		return nil, err
	}
	intent := strings.ToLower(claim.FileSubtype)
	if claim.SourceType == string(manifest.SourceTypeCNESLocal) {
		intent = extractor.IntentCnesProfissionais
	}
	return &worker.Job{
		ID: claim.JobID, TenantID: tenant, UploadURL: url,
		FencingToken: claim.FencingToken, Attempt: claim.Attempt,
		Params: extractor.ExtractionParams{
			Intent: intent, Competencia: strings.ReplaceAll(claim.Competencia, "-", ""),
			CodMunGest: os.Getenv("COD_MUN_IBGE"),
		},
		RawRequest: &manifest.BuildRequest{
			SourceType:  manifest.SourceType(claim.SourceType),
			FileSubtype: claim.FileSubtype, Competencia: claim.Competencia,
			AgentID: claim.AgentID, AgentVersion: Version, SchemaVersion: "1",
			SnapshotMode: manifest.SnapshotMode(claim.SnapshotMode), CreatedAt: time.Now().UTC(),
		},
	}, nil
}

func runRawForeground(ctx context.Context, boot runBootConfig) int {
	paths, cnesPassword, ok := resolveBootConfig(boot.appData, boot.flags)
	if !ok {
		return 1
	}
	cnesDB, err := openFirebird(paths.CNES, cnesPassword)
	if err != nil {
		slog.Error("raw_cnes_open", "err", err.Error())
		return 1
	}
	defer cnesDB.Close()
	sihdDB, bpaDB := openOptionalRawDBs(boot.appData, paths)
	if sihdDB != nil {
		defer sihdDB.Close()
	}
	if bpaDB != nil {
		defer bpaDB.Close()
	}
	run, closeRun, err := prepareRawRun(boot, rawDatabases{cnesDB, sihdDB, bpaDB, paths.SIADir})
	if err != nil {
		slog.Error("raw_run_init", "err", err.Error())
		return 1
	}
	defer closeRun()
	if err := run.drainer.Drain(ctx, run.outbox); err != nil {
		slog.Warn("raw_replay_pending", "err", err.Error())
	}
	return run.loop(ctx)
}

func openOptionalRawDBs(appData string, paths PathConfig) (*sql.DB, *sql.DB) {
	store := secrets.NewStore(filepath.Join(appData, "secrets"))
	var sihd, bpa *sql.DB
	if paths.SIHD.DatabasePath != "" {
		pw, _, err := ResolvePassword("sihd", paths.SIHD.User, os.Getenv, store.Load)
		if err == nil {
			sihd, err = openFirebird(paths.SIHD, pw)
		}
		if err != nil {
			slog.Error("raw_sihd_open", "err", err.Error())
		}
	}
	if paths.BPA.DatabasePath != "" {
		pw, _, err := ResolvePassword("bpa", paths.BPA.User, os.Getenv, store.Load)
		if err == nil {
			bpa, err = openFirebird(paths.BPA, pw)
		}
		if err != nil {
			slog.Error("raw_bpa_open", "err", err.Error())
		}
	}
	return sihd, bpa
}

func prepareRawRun(
	boot runBootConfig, db rawDatabases,
) (rawRun, func(), error) {
	tenant := os.Getenv("TENANT_ID")
	if tenant == "" {
		return rawRun{}, nil, fmt.Errorf("env_required var=TENANT_ID")
	}
	client := rawclient.New(envOr("CENTRAL_API_URL", "http://localhost:8000"),
		httpClientFor(boot.mtls), os.Getenv("RAW_LOCAL_TOKEN"), boot.machineID)
	outbox, err := queue.Open(filepath.Join(boot.appData, "queue", "outbox.db"))
	if err != nil {
		return rawRun{}, nil, err
	}
	store := openDeltaStore(boot.appData)
	if store == nil {
		_ = outbox.Close()
		return rawRun{}, nil, fmt.Errorf("raw_delta_store=unavailable")
	}
	spoolDir := filepath.Join(boot.appData, "queue", "raw-spool")
	legacy, err := buildAPIClient(boot.machineID, httpClientFor(boot.mtls))
	if err != nil {
		_ = store.Close()
		_ = outbox.Close()
		return rawRun{}, nil, err
	}
	exe := &worker.JobExecutor{
		DB: db.cnes, DeltaStore: store, RawOutbox: outbox,
		RawSpoolDirectory: spoolDir, RawUploader: upload.NewHTTP(client.HTTPClient()),
		RawPayload: worker.NewRawPayloadExtractor(worker.RawSourcesConfig{
			SIHD: db.sihd, BPA: db.bpa, SIADir: db.sia,
		}),
		RawExtract: rawCnesExtractor(db.cnes),
	}
	drainer := worker.NewRawDrainer(client, store, legacy)
	drainer.SpoolDirectory = spoolDir
	drainer.Uploader = upload.NewHTTP(client.HTTPClient())
	return rawRun{client, outbox, exe, drainer, tenant}, func() {
		_ = store.Close()
		_ = outbox.Close()
	}, nil
}

func rawCnesExtractor(db *sql.DB) func(context.Context, worker.Job) ([]delta.Row, error) {
	return func(ctx context.Context, job worker.Job) ([]delta.Row, error) {
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		rows := make(chan extractor.CnesProfissionalRow, 128)
		result := obs.SafeGo(func() error {
			defer close(rows)
			return extractor.ExtractCnesProfissionais(ctx, conn, job.Params, rows)
		}, "raw_cnes_extract")
		var output []delta.Row
		for row := range rows {
			output = append(output, row.ToRow())
		}
		return output, <-result
	}
}

func (r rawRun) loop(ctx context.Context) int {
	for ctx.Err() == nil {
		jobs, stops := r.claimBatch(ctx)
		if len(jobs) > 0 {
			r.processBatch(ctx, jobs)
		}
		for _, stop := range stops {
			stop()
		}
		if err := r.drainer.Drain(ctx, r.outbox); err != nil {
			slog.Warn("raw_drain_pending", "err", err.Error())
		}
		if !sleepCancellable(ctx, 5*time.Second) {
			break
		}
	}
	return 0
}

func (r rawRun) claimBatch(ctx context.Context) ([]*worker.Job, []func()) {
	var jobs []*worker.Job
	var stops []func()
	for len(jobs) < 10 {
		claim, err := r.client.Next(ctx)
		if err != nil {
			slog.Warn("raw_claim_failed", "err", err.Error())
			break
		}
		if claim == nil {
			break
		}
		job, err := rawJobFromClaim(r.client, r.tenant, *claim)
		if err != nil {
			slog.Error("raw_claim_invalid", "err", err.Error())
			continue
		}
		jobs = append(jobs, job)
		stops = append(stops, r.startHeartbeat(ctx, job))
	}
	return jobs, stops
}

func (r rawRun) startHeartbeat(ctx context.Context, job *worker.Job) func() {
	heartbeatCtx, cancel := context.WithCancel(ctx)
	done := obs.SafeGo(func() error {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return nil
			case <-ticker.C:
				if err := r.client.Heartbeat(heartbeatCtx, job.ID, job.FencingToken); err != nil {
					slog.Warn("raw_heartbeat_failed", "job_id", job.ID, "err", err.Error())
				}
			}
		}
	}, "raw_heartbeat")
	return func() { cancel(); <-done }
}

func (r rawRun) processBatch(ctx context.Context, jobs []*worker.Job) {
	sort.Slice(jobs, func(i, j int) bool {
		left, right := jobs[i].RawRequest, jobs[j].RawRequest
		if left.SourceType != right.SourceType {
			return left.SourceType < right.SourceType
		}
		if left.Competencia != right.Competencia {
			return left.Competencia < right.Competencia
		}
		return left.FileSubtype < right.FileSubtype
	})
	for _, job := range jobs {
		if _, err := r.exe.RunRawSource(ctx, []*worker.Job{job}); err != nil {
			slog.Error("raw_source_failed", "source", job.RawRequest.SourceType,
				"err", err.Error())
		}
	}
}
