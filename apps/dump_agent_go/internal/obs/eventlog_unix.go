//go:build !windows

package obs

import (
	"context"
	"log/slog"
)

type unixEventLogHandler struct{}

// NewEventLogHandler returns a no-op handler on non-Windows platforms.
func NewEventLogHandler(_ string) (EventLogHandler, error) {
	return &unixEventLogHandler{}, nil
}

func (*unixEventLogHandler) Enabled(_ context.Context, _ slog.Level) bool   { return false }
func (*unixEventLogHandler) Handle(_ context.Context, _ slog.Record) error { return nil }
func (h *unixEventLogHandler) WithAttrs(_ []slog.Attr) slog.Handler         { return h }
func (h *unixEventLogHandler) WithGroup(_ string) slog.Handler              { return h }
func (*unixEventLogHandler) Close() error                                   { return nil }
