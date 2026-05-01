//go:build windows

package obs

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type capturedWrite struct {
	level string
	eid   uint32
	msg   string
}

type fakeEventlogWriter struct {
	writes []capturedWrite
}

func (f *fakeEventlogWriter) Info(eid uint32, msg string) error {
	f.writes = append(f.writes, capturedWrite{"info", eid, msg})
	return nil
}
func (f *fakeEventlogWriter) Warning(eid uint32, msg string) error {
	f.writes = append(f.writes, capturedWrite{"warning", eid, msg})
	return nil
}
func (f *fakeEventlogWriter) Error(eid uint32, msg string) error {
	f.writes = append(f.writes, capturedWrite{"error", eid, msg})
	return nil
}
func (f *fakeEventlogWriter) Close() error { return nil }

func newTestHandler(w eventlogWriter) *winEventLogHandler {
	return &winEventLogHandler{w: w, source: "DumpAgentTest"}
}

func TestWinEventLog_DropsBelowWarn(t *testing.T) {
	w := &fakeEventlogWriter{}
	h := newTestHandler(w)
	if h.Enabled(context.Background(), slog.LevelInfo) {
		t.Fatal("Enabled must be false at INFO")
	}
	if h.Enabled(context.Background(), slog.LevelDebug) {
		t.Fatal("Enabled must be false at DEBUG")
	}
}

func TestWinEventLog_RoutesWarnToWarning(t *testing.T) {
	w := &fakeEventlogWriter{}
	h := newTestHandler(w)
	rec := slog.NewRecord(time.Time{}, slog.LevelWarn, "upload_failed", 0)
	rec.AddAttrs(slog.Any("event_id", EventUploadFailed),
		slog.String("job_uuid", "abc"))
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle err: %v", err)
	}
	if len(w.writes) != 1 {
		t.Fatalf("expected 1 write, got %d", len(w.writes))
	}
	got := w.writes[0]
	if got.level != "warning" {
		t.Errorf("level got %s want warning", got.level)
	}
	if got.eid != uint32(EventUploadFailed) {
		t.Errorf("eid got %d want %d", got.eid, EventUploadFailed)
	}
	if !strings.Contains(got.msg, "msg=upload_failed") {
		t.Errorf("msg missing payload: %q", got.msg)
	}
	if !strings.Contains(got.msg, "job_uuid=abc") {
		t.Errorf("msg missing job_uuid: %q", got.msg)
	}
}

func TestWinEventLog_RoutesErrorToError(t *testing.T) {
	w := &fakeEventlogWriter{}
	h := newTestHandler(w)
	rec := slog.NewRecord(time.Time{}, slog.LevelError, "extract_failed", 0)
	rec.AddAttrs(slog.Any("event_id", EventExtractFailed))
	_ = h.Handle(context.Background(), rec)
	if w.writes[0].level != "error" {
		t.Fatalf("level got %s want error", w.writes[0].level)
	}
}

func TestWinEventLog_DefaultsToEventUnknown(t *testing.T) {
	w := &fakeEventlogWriter{}
	h := newTestHandler(w)
	rec := slog.NewRecord(time.Time{}, slog.LevelWarn, "no_id", 0)
	_ = h.Handle(context.Background(), rec)
	if w.writes[0].eid != uint32(EventUnknown) {
		t.Fatalf("expected EventUnknown=%d, got %d", EventUnknown, w.writes[0].eid)
	}
}

func TestWinEventLog_DisabledHandlerIsNoop(t *testing.T) {
	h := &winEventLogHandler{disabled: true}
	if h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("disabled must be false")
	}
	rec := slog.NewRecord(time.Time{}, slog.LevelError, "x", 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("disabled Handle: %v", err)
	}
}

func TestWinEventLog_RespectsAGENT_EVENTLOG_DISABLED(t *testing.T) {
	t.Setenv("AGENT_EVENTLOG_DISABLED", "true")
	h, err := NewEventLogHandler("DumpAgentTest")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if h.Enabled(context.Background(), slog.LevelError) {
		t.Fatal("env-disabled handler must be Enabled=false")
	}
}
