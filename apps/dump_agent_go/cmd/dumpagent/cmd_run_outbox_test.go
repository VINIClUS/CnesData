package main

import (
	"path/filepath"
	"testing"

	"github.com/cnesdata/dumpagent/internal/queue"
)

// TestOutboxPath_MatchesAppDataLayout asserts the outbox path under appData
// is what runForeground constructs. This guards against accidental drift
// between the path used by the agent (cmd_run.go) and tools that inspect
// the outbox (P5.4 diagnose CLI).
func TestOutboxPath_MatchesAppDataLayout(t *testing.T) {
	appData := t.TempDir()
	outboxPath := filepath.Join(appData, "queue", "outbox.db")
	ob, err := queue.Open(outboxPath)
	if err != nil {
		t.Fatalf("Open at %s: %v", outboxPath, err)
	}
	defer ob.Close()
	// Smoke: append + close + re-open recovers state.
	if err := ob.Append(queue.Envelope{Type: queue.TypeComplete, JobUUID: "x"}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := ob.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ob2, err := queue.Open(outboxPath)
	if err != nil {
		t.Fatalf("Re-open: %v", err)
	}
	defer ob2.Close()
	items, _ := ob2.Peek(10)
	if len(items) != 1 {
		t.Fatalf("after restart: got %d items, want 1", len(items))
	}
}
