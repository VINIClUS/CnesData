package obs

import "log/slog"

// EventLogHandler is a slog.Handler with an explicit Close.
// Real impl on Windows talks to the Windows Event Log;
// no-op stub on POSIX so cross-platform code can wire unconditionally.
type EventLogHandler interface {
	slog.Handler
	Close() error
}
