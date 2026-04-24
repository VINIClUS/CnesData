# dump_agent_go — Test layout

## Layers

| Layer | Location | How to run |
|---|---|---|
| Unit (whitebox + blackbox) | `internal/<pkg>/*_test.go` | `go test ./internal/...` |
| Fuzz | `internal/<pkg>/*_fuzz_test.go` | `go test -fuzz=Fuzz -fuzztime=30s ./internal/<pkg>/` |
| Bench | `internal/<pkg>/*_bench_test.go` | `go test -bench=. -benchmem ./internal/<pkg>/` |
| Integration | `test/integration/*_test.go` | `go test -tags=integration ./test/integration/` |
| E2E | `test/e2e/*_test.go` | `go test -tags=e2e ./test/e2e/` |

Default `go test ./...` runs unit only (integration + e2e gated by build
tags).

## CI labels (PR-level triggers)

| Label | Jobs activated |
|---|---|
| `run-windows-integration` | `integration-windows` (FB 2.5 CNES/SIHD) + `bpa-integration-windows` (FB 1.5 BPA) |
| `run-integration` | `sia-integration` (Linux, DBF fixtures) |

Nightly schedule (`30 2 * * *` UTC) runs all three jobs regardless of label
state.

Applying a label on an open PR triggers re-run via
`on.pull_request.types: [opened, synchronize, reopened, labeled]` (added
in PR #50).

## Coverage gate

65% minimum on the filtered set, enforced by
`.github/workflows/dump-agent-go.yml` → `lint-test-linux` → Coverage gate
step.

Local reproduction:

```bash
go test -race -count=1 -coverprofile=coverage.out ./...
grep -v -E "internal/apiclient/generated\.go|cmd/|internal/service/|_windows\.go:" \
  coverage.out > coverage.filtered.out
go tool cover -func=coverage.filtered.out | tail -1
```

Filter exclusions:
- `internal/apiclient/generated.go` — oapi-codegen output.
- `cmd/` — all entrypoints (main packages, not covered by unit tests).
- `internal/service/` — Windows service wrapper (platform-specific).
- `*_windows.go` — `//go:build windows`-tagged files; cannot run on Linux CI.

## `export_test.go` pattern

Package-private symbols exposed to black-box `*_test` packages via
`var Export = private` declarations in `<pkg>/export_test.go`. The
`_test.go` suffix scopes the exports to test builds only — they do NOT
leak into the production API. See `internal/worker/export_test.go` for
the canonical example.
