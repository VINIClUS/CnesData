//go:build !windows

package diagnose

import (
	"golang.org/x/sys/unix"
)

func diskFreeMB(path string) int64 {
	var stat unix.Statfs_t
	if err := unix.Statfs(path, &stat); err != nil {
		return -1
	}
	return int64(stat.Bavail) * int64(stat.Bsize) / (1024 * 1024)
}
