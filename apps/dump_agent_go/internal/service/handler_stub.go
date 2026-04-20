//go:build !windows

package service

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// Funções são no-op em POSIX. Na prática dump_agent em Linux sempre roda
// foreground; systemd unit será adicionada em iteração futura.

// RunAsService stub POSIX — retorna erro.
func RunAsService(_ string) int {
	fmt.Fprintln(os.Stderr, "service mode not supported on POSIX")
	return 2
}

// Install stub POSIX.
func Install(_ []string) int {
	fmt.Fprintln(os.Stderr, "install: not supported on POSIX (use systemd unit)")
	return 2
}

// Uninstall stub POSIX.
func Uninstall() int {
	fmt.Fprintln(os.Stderr, "uninstall: not supported on POSIX")
	return 2
}

// SetRunner no-op em POSIX — assinatura compatível com Windows variant.
func SetRunner(_ func(ctx context.Context, verbose bool) int) {}

// ErrUnsupported marca chamada inválida em POSIX.
var ErrUnsupported = errors.New("service_operations_unsupported_on_posix")
