//go:build !windows

package obs

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestEventLogHandler_UnixIsNoOp(t *testing.T) {
	h, err := NewEventLogHandler("DumpAgent")
	if err != nil {
		t.Fatalf("unix stub must not error, got %v", err)
	}
	if h == nil {
		t.Fatal("unix stub must return non-nil handler")
	}
	if h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("unix stub Enabled must be false")
	}
	rec := slog.NewRecord(time.Time{}, slog.LevelError, "x", 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("unix stub Handle must not error, got %v", err)
	}
	if err := h.Close(); err != nil {
		t.Fatalf("unix stub Close must not error, got %v", err)
	}
}
