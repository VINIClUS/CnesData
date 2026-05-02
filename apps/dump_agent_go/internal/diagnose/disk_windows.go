//go:build windows

package diagnose

import (
	"golang.org/x/sys/windows"
)

func diskFreeMB(path string) int64 {
	var freeBytesAvailable, totalBytes, totalFreeBytes uint64
	pPath, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return -1
	}
	if err := windows.GetDiskFreeSpaceEx(pPath, &freeBytesAvailable, &totalBytes, &totalFreeBytes); err != nil {
		return -1
	}
	return int64(freeBytesAvailable / (1024 * 1024))
}
