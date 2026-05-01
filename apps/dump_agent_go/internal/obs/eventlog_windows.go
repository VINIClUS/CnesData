//go:build windows

package obs

import (
	"context"
	"log/slog"
	"os"

	"golang.org/x/sys/windows/svc/eventlog"
)

// eventlogWriter is the surface satisfied by *eventlog.Log and the
// fake used in tests. Restricts the imported package to what the
// handler actually uses.
type eventlogWriter interface {
	Info(eid uint32, msg string) error
	Warning(eid uint32, msg string) error
	Error(eid uint32, msg string) error
	Close() error
}

// Compile-time interface assertion: ensure winEventLogHandler implements
// EventLogHandler so future signature drift fails at build, not runtime.
var _ EventLogHandler = (*winEventLogHandler)(nil)

type winEventLogHandler struct {
	w        eventlogWriter
	source   string
	disabled bool
	attrs    []slog.Attr
}

// NewEventLogHandler opens the Application log under source. If
// AGENT_EVENTLOG_DISABLED=true or eventlog.Open fails (eventlog service
// down, source unregistered), returns a non-nil handler with disabled=true
// so MultiHandler can skip silently. The error return is reserved for
// future use; v1 always returns nil.
func NewEventLogHandler(source string) (EventLogHandler, error) {
	if os.Getenv("AGENT_EVENTLOG_DISABLED") == "true" {
		return &winEventLogHandler{source: source, disabled: true}, nil
	}
	w, err := eventlog.Open(source)
	if err != nil {
		slog.Warn("eventlog_open_failed",
			"source", source,
			"err", err.Error(),
			"hint", "run 'agent.exe install' to register source")
		return &winEventLogHandler{source: source, disabled: true}, nil
	}
	return &winEventLogHandler{w: w, source: source}, nil
}

func (h *winEventLogHandler) Enabled(_ context.Context, l slog.Level) bool {
	if h.disabled {
		return false
	}
	return l >= slog.LevelWarn
}

func (h *winEventLogHandler) Handle(_ context.Context, r slog.Record) error {
	if h.disabled || h.w == nil {
		return nil
	}
	for _, a := range h.attrs {
		r.AddAttrs(a)
	}
	eid := extractEventID(r)
	msg := FormatCompact(r)
	switch {
	case r.Level >= slog.LevelError:
		_ = h.w.Error(uint32(eid), msg)
	case r.Level >= slog.LevelWarn:
		_ = h.w.Warning(uint32(eid), msg)
	}
	return nil
}

func (h *winEventLogHandler) WithAttrs(a []slog.Attr) slog.Handler {
	cp := *h
	cp.attrs = append(append([]slog.Attr{}, h.attrs...), a...)
	return &cp
}

func (h *winEventLogHandler) WithGroup(_ string) slog.Handler { return h }

func (h *winEventLogHandler) Close() error {
	if h.w == nil {
		return nil
	}
	return h.w.Close()
}

func extractEventID(r slog.Record) EventID {
	id := EventUnknown
	r.Attrs(func(a slog.Attr) bool {
		if a.Key != "event_id" {
			return true
		}
		if v, ok := a.Value.Any().(EventID); ok {
			id = v
			return false
		}
		return true
	})
	return id
}
