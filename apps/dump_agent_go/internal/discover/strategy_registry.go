package discover

// RegistryHit is one path discovered via Windows Registry probe.
// Score is computed by callers via Score() once file existence is checked.
type RegistryHit struct {
	Path string
}
