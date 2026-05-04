package worker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cnesdata/dumpagent/internal/audit"
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
