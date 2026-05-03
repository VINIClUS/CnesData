//go:build !windows

package discover

// RegistryHits returns nil on non-Windows (no registry available).
func RegistryHits(_ Profile) []RegistryHit {
	return nil
}
