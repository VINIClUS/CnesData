package platform

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// NotifyShutdown retorna ctx cancelado em SIGTERM/SIGINT (POSIX) ou
// Ctrl-C/Ctrl-Break (Windows, via os.Interrupt — SetConsoleCtrlHandler
// fica para Plan B quando Service precisa).
func NotifyShutdown(parent context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
}
