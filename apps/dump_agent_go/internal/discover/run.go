package discover

import (
	"context"
	"sync"
	"time"
)

// RunConfig wires Run to its filesystem + registry seams.
type RunConfig struct {
	FS          FS
	Drives      DriveEnumerator
	RegistryFn  registryFn
	PerProbeTTL time.Duration
}

// Run kicks off 4 source probes in parallel; each probe is bound by its
// own context.WithTimeout(PerProbeTTL). Always returns 4 results in
// SourceID order (CNES, SIHD, BPA, SIA), even when a probe times out
// or context is cancelled (empty result in that case).
func Run(ctx context.Context, cfg RunConfig) []SourceResult {
	sources := []SourceID{SourceCNES, SourceSIHD, SourceBPA, SourceSIA}
	results := make([]SourceResult, len(sources))
	var wg sync.WaitGroup
	wg.Add(len(sources))
	for i, src := range sources {
		go func(i int, src SourceID) {
			defer wg.Done()
			results[i] = runOneProbe(ctx, cfg, src)
		}(i, src)
	}
	wg.Wait()
	return results
}

func runOneProbe(parent context.Context, cfg RunConfig, src SourceID) SourceResult {
	ctx, cancel := context.WithTimeout(parent, cfg.PerProbeTTL)
	defer cancel()
	return dispatchProbe(ctx, cfg, src)
}

func dispatchProbe(ctx context.Context, cfg RunConfig, src SourceID) SourceResult {
	deps := probeDeps{FS: cfg.FS, Drives: cfg.Drives, Reg: cfg.RegistryFn}
	if src == SourceSIA {
		return probeSIA(ctx, deps)
	}
	return probeFB(ctx, ProfileFor(src), deps)
}
