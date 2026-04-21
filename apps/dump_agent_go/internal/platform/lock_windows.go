//go:build windows

package platform

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/windows"
)

type winLock struct {
	handle windows.Handle
}

func (w *winLock) Release() error {
	_ = windows.ReleaseMutex(w.handle)
	return windows.CloseHandle(w.handle)
}

// AcquireSingleInstanceLock cria named mutex Global\CnesAgent_<name>.
// Retorna erro se mutex já existe (ERROR_ALREADY_EXISTS = 183).
func AcquireSingleInstanceLock(_, name string) (SingleInstanceLock, error) {
	mutexName, err := syscall.UTF16PtrFromString("Global\\CnesAgent_" + name)
	if err != nil {
		return nil, fmt.Errorf("utf16: %w", err)
	}
	handle, err := windows.CreateMutex(nil, false, mutexName)
	if err != nil {
		return nil, fmt.Errorf("create_mutex: %w", err)
	}
	if last := windows.GetLastError(); last == syscall.ERROR_ALREADY_EXISTS {
		_ = windows.CloseHandle(handle)
		return nil, fmt.Errorf("already_running lock=%s", name)
	}
	return &winLock{handle: handle}, nil
}
