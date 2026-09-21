//go:build !windows

package platform

// RestrictStatePath is a no-op because POSIX mode bits provide the boundary.
func RestrictStatePath(string) error { return nil }

// RestrictStateTree is a no-op because POSIX mode bits provide the boundary.
func RestrictStateTree(string) error { return nil }
