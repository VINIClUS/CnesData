package worker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cnesdata/dumpagent/internal/audit"
	"github.com/cnesdata/dumpagent/internal/delta"
	"github.com/cnesdata/dumpagent/internal/extractor"
)

func TestAppendAudit_NilLoggerNoOp(t *testing.T) {
	e := &JobExecutor{AuditLogger: nil}
	e.appendAudit(audit.Event{Lifecycle: audit.LifecycleExtracted})
}

func TestAppendAudit_LoggerWritesEvent(t *testing.T) {
	dir := t.TempDir()
	logger := audit.New(dir, "m", "t",
		[]byte("0123456789abcdef0123456789abcdef"))
	logger.SetTimeNow(func() time.Time {
		return time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	})
	e := &JobExecutor{AuditLogger: logger}
	e.appendAudit(audit.Event{
		Source: "cnes", Intent: "profissionais",
		Competencia: "202605", ExtractionID: "ext-1",
		JobID: "job-1", Lifecycle: audit.LifecycleExtracted,
	})
	files, err := filepath.Glob(filepath.Join(dir, "events-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)
}

func TestEmitCommitted_NilAuditLoggerNoOp(t *testing.T) {
	e := &JobExecutor{AuditLogger: nil, DeltaStore: &delta.Store{}}
	job := Job{ID: "job-1"}
	e.EmitCommitted(job, 0)
}

func TestEmitCommitted_NilDeltaStoreNoOp(t *testing.T) {
	dir := t.TempDir()
	logger := audit.New(dir, "m", "t",
		[]byte("0123456789abcdef0123456789abcdef"))
	e := &JobExecutor{AuditLogger: logger, DeltaStore: nil}
	job := Job{ID: "job-1", Params: extractor.ExtractionParams{
		Intent: extractor.IntentCnesEstabelecimentos, Competencia: "202605",
	}}
	e.EmitCommitted(job, 1024)
	files, _ := filepath.Glob(filepath.Join(dir, "events-*.jsonl"))
	require.Empty(t, files, "snapshot path must not emit Committed audit")
}

func TestEmitCommitted_WritesCommittedEvent(t *testing.T) {
	dir := t.TempDir()
	logger := audit.New(dir, "machine-1", "tenant-1",
		[]byte("0123456789abcdef0123456789abcdef"))
	logger.SetTimeNow(func() time.Time {
		return time.Date(2026, 5, 4, 10, 0, 0, 0, time.UTC)
	})
	e := &JobExecutor{AuditLogger: logger, DeltaStore: &delta.Store{}}
	job := Job{
		ID:     "11111111-2222-3333-4444-555555555555",
		Sha256: "deadbeef",
		Params: extractor.ExtractionParams{
			Intent:      extractor.IntentCnesEstabelecimentos,
			Competencia: "202605",
		},
	}
	e.EmitCommitted(job, 1024)

	files, err := filepath.Glob(filepath.Join(dir, "events-*.jsonl"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	content, err := os.ReadFile(files[0])
	require.NoError(t, err)
	body := string(content)
	require.Contains(t, body, "\"lifecycle\":\"committed\"")
	require.Contains(t, body, "\"sha256\":\"deadbeef\"")
	require.Contains(t, body, "\"source\":\"cnes\"")
	require.Contains(t, body, "\"intent\":\"estabelecimentos\"")
	require.Contains(t, body, "\"competencia\":\"202605\"")
	require.Contains(t, body, "\"job_id\":\""+job.ID+"\"")
	require.Equal(t, 1, strings.Count(body, "\n"), "exactly one event line")
}
