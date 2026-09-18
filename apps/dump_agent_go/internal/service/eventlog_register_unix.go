//go:build !windows

package service

// RegisterEventSource is a no-op on POSIX.
func RegisterEventSource(_ string) error { return nil }

// RemoveEventSource is a no-op on POSIX.
func RemoveEventSource(_ string) error { return nil }
