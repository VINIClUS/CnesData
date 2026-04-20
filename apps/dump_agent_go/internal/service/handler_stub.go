//go:build !windows

package service

import (
	"errors"
	"fmt"
	"os"
)

// Funções são no-op em POSIX. Na prática dump_agent em Linux sempre roda
// foreground; systemd unit será adicionada em iteração futura.
// Versões Windows ficam em handler.go + install_windows.go (Plan B Tasks 3-4).

// RunAsService stub POSIX — retorna erro, não deveria ser chamado.
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

// SetRunner placeholder — Windows variante em Plan B Task 3 injeta runForeground.
func SetRunner(_ func()) {}

// ErrUnsupported marca chamada inválida em POSIX.
var ErrUnsupported = errors.New("service_operations_unsupported_on_posix")
