//go:build windows

package main

import "golang.org/x/sys/windows/svc"

func detectAutoMode() int {
	isService, err := svc.IsWindowsService()
	if err == nil && isService {
		return cmdService(nil)
	}
	return cmdRun(nil)
}
