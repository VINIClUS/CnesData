package diagnose

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRun_NoProbeRunsStaticOnly(t *testing.T) {
	calls := []string{}
	staticChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			calls = append(calls, "s1")
			return Check{Name: "s1", Severity: SeverityPass}
		},
	}
	probeChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			calls = append(calls, "p1")
			return Check{Name: "p1", Severity: SeverityPass}
		},
	}
	checks := Run(t.Context(), Config{Probe: false})
	if len(checks) != 1 {
		t.Errorf("got %d checks want 1", len(checks))
	}
	if len(calls) != 1 || calls[0] != "s1" {
		t.Errorf("got calls %v", calls)
	}
}

func TestRun_WithProbeRunsAll(t *testing.T) {
	staticChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			return Check{Name: "s1", Severity: SeverityPass}
		},
	}
	probeChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			return Check{Name: "p1", Severity: SeverityPass}
		},
	}
	checks := Run(t.Context(), Config{Probe: true})
	if len(checks) != 2 {
		t.Errorf("got %d checks want 2", len(checks))
	}
}

func TestRun_PerCheckTimeout(t *testing.T) {
	staticChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			<-ctx.Done()
			return Check{Name: "hang", Severity: SeverityFail, Message: ctx.Err().Error()}
		},
	}
	probeChecks = nil
	cfg := Config{Probe: false}
	start := time.Now()
	checks := Run(t.Context(), cfg)
	elapsed := time.Since(start)
	if elapsed > 7*time.Second {
		t.Errorf("Run took %v; expected < 7s (per-check timeout 5s)", elapsed)
	}
	if len(checks) != 1 || checks[0].Severity != SeverityFail {
		t.Errorf("got %+v want 1 FAIL", checks)
	}
}

func TestRun_PanicRecoveryReportsFail(t *testing.T) {
	staticChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check {
			panic(errors.New("boom"))
		},
	}
	probeChecks = nil
	checks := Run(t.Context(), Config{})
	if len(checks) != 1 {
		t.Fatalf("got %d checks want 1", len(checks))
	}
	if checks[0].Severity != SeverityFail {
		t.Errorf("got severity %q want FAIL", checks[0].Severity)
	}
	if _, ok := checks[0].Fields["panic"]; !ok {
		t.Errorf("missing panic field: %+v", checks[0])
	}
}

func TestRun_DeterministicOrder(t *testing.T) {
	staticChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check { return Check{Name: "a"} },
		func(ctx context.Context, cfg Config) Check { return Check{Name: "b"} },
	}
	probeChecks = []CheckFunc{
		func(ctx context.Context, cfg Config) Check { return Check{Name: "c"} },
	}
	checks := Run(t.Context(), Config{Probe: true})
	if len(checks) != 3 || checks[0].Name != "a" || checks[1].Name != "b" || checks[2].Name != "c" {
		t.Errorf("order broken: %+v", checks)
	}
}
