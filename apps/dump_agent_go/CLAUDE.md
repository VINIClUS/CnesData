# dump_agent_go — Edge Agent (Go implementation)

## Executive Summary

Port Go do `dump_agent` Python. Roda no edge (Firebird CNES / SIHD) extraindo
Parquet raw e enviando para MinIO via presigned PUT. Migração em curso: ver
`docs/superpowers/specs/2026-04-20-dump-agent-go-migration-design.md`.

## Role

Edge Agent. Binário estático Windows amd64 (alvo primário) + Linux amd64
(dev/CI). Long-poll `central_api`, 3 goroutines coordenadas por errgroup
(extract/write/upload), panic recovery sandboxed.

## Layout

- `cmd/dumpagent/` — entrypoint
- `internal/obs/` — SafeRun, SafeGo, WithBackoff, classify, slog
- `internal/platform/` — Win32/POSIX boundary (build tags)
- `internal/fbdriver/` — DSN builder sobre `database/sql`
- `internal/extractor/` — CNES (3-query+merge) e SIHD extractors
- `internal/writer/` — Parquet+gzip streaming via io.Pipe
- `internal/upload/` — HTTP PUT streaming
- `internal/apiclient/` — gerado via oapi-codegen
- `internal/worker/` — JobExecutor, Consumer.Loop, heartbeat

## Build

`make build-windows` → `dist/dumpagent.exe` (cross-compile de Linux se
driver pure-Go).

## Test

- `make test` — unit (mocks). `make test-e2e` — smoke com stub API + FB fake.
- Coverage gate: 65% on filtered set (excludes `generated.go`, `cmd/`,
  `internal/service/`, `*_windows.go`). Reproduce via `go test -race -count=1
  -coverprofile=coverage.out ./...` + `grep -v` + `go tool cover -func`.
- CI label vocab:
  - `run-windows-integration` → runs `integration-windows` (FB 2.5 CNES/SIHD)
    + `bpa-integration-windows` (FB 1.5 BPA). Windows-latest runners.
  - `run-integration` → runs `sia-integration` (Linux, DBF fixtures).
  - Nightly schedule at `30 2 * * *` UTC runs all three regardless of label.
- Full layout + filter regex: `apps/dump_agent_go/test/README.md`.

## Gotchas

- Bug Firebird -501: `LFCES021 ↔ LFCES060` 1-query retorna NULLs silenciosos.
  Workaround = 3 queries + merge em Go por `SEQ_EQUIPE[:4]`. **NÃO** simplificar.
- Encoding WIN1252 sujo em bancos CNES legados → UTF-8 sanitize obrigatório
  antes de serializar Parquet (ver `internal/extractor/sanitize.go`).
- Clock drift: NTP pré-flight em boot (ver `internal/platform/ntp_check.go`).
  Skew > 60min → exit(1).
- Panic recovery: TODO spawn de goroutine passa por `SafeGo`/`SafeRun`. Nunca
  `go func()...()` direto em código de produção.

## BPA + SIA extractors (T9/T10, 2026-04-23)

- `internal/extractor/bpa.go` — FB 1.5 via nakagami/firebirdsql. Reads BPA_C_LINHAS + BPA_I_LINHAS. GDB path via `--bpa-gdb` or `BPA_GDB_PATH`. Windows x86 FB 1.5 server required at runtime.
- `internal/extractor/sia.go` — DBF via LindsayBradford/go-dbf with cp1252 sanitize. Reads S_APA, S_BPI, S_BPIHST, S_CDN, CADMUN from directory supplied via `--sia-dir` or `SIA_DIR`.
- N-file manifest: one job per `(source_type, competencia)`, emits N Parquets. See `internal/worker/bpa_sia_pipeline.go`.
- FB 1.5 + nakagami driver compatibility: T1 spike **FAIL at fixture_generation_fail**
  (fdb Python library can't load FB 1.5 `fbclient.dll` — missing `fb_interpret`
  symbol, added in FB 2.0). Wire-protocol validation remains unvalidated. See
  issue #51 for pivot options; parent spec `2026-04-23-spike-report.md` (local)
  §Resolution has the full traceback.
