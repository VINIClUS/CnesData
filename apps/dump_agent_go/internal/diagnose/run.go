package diagnose

import (
	"context"
	"fmt"
	"time"
)

const perCheckTimeout = 5 * time.Second

// CheckFunc executes a single check and returns a Check result.
type CheckFunc func(ctx context.Context, cfg Config) Check

// staticChecks run unconditionally. probeChecks run only when Config.Probe.
// Both are package-level for test injection.
var (
	staticChecks []CheckFunc
	probeChecks  []CheckFunc
)

// Run executes all enabled checks sequentially with per-check timeout + panic recovery.
func Run(ctx context.Context, cfg Config) []Check {
	results := make([]Check, 0, len(staticChecks)+len(probeChecks))
	for _, fn := range staticChecks {
		results = append(results, runOne(ctx, fn, cfg, "static"))
	}
	if cfg.Probe {
		for _, fn := range probeChecks {
			results = append(results, runOne(ctx, fn, cfg, "probe"))
		}
	}
	return results
}

// runOne wraps a CheckFunc with timeout + panic recovery.
func runOne(parent context.Context, fn CheckFunc, cfg Config, kind string) (out Check) {
	ctx, cancel := context.WithTimeout(parent, perCheckTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			out = Check{
				Name:     "panic_" + kind,
				Severity: SeverityFail,
				Message:  "internal error",
				Fields:   map[string]any{"panic": fmt.Sprint(r)},
			}
		}
	}()
	return fn(ctx, cfg)
}
