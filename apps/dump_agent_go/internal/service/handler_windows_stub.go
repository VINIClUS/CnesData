//go:build windows

package service

// Stubs substituídos em Plan B Task 3 (handler.go) e Task 4 (install_windows.go).
// Para Task 2 apenas permitir build Windows.

// RunAsService placeholder — Task 3 implementa com svc.Run.
func RunAsService(_ string) int {
	return 0
}

// Install placeholder — Task 4 implementa svc/mgr.
func Install(_ []string) int {
	return 0
}

// Uninstall placeholder — Task 4 implementa.
func Uninstall() int {
	return 0
}

// SetRunner placeholder — Task 3 substitui para injetar runForeground.
func SetRunner(_ func()) {}
