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

`make test` — unit (mocks). `make test-e2e` — smoke com stub API + FB fake.

## Gotchas

- Bug Firebird -501: `LFCES021 ↔ LFCES060` 1-query retorna NULLs silenciosos.
  Workaround = 3 queries + merge em Go por `SEQ_EQUIPE[:4]`. **NÃO** simplificar.
- Encoding WIN1252 sujo em bancos CNES legados → UTF-8 sanitize obrigatório
  antes de serializar Parquet (ver `internal/extractor/sanitize.go`).
- Clock drift: NTP pré-flight em boot (ver `internal/platform/ntp_check.go`).
  Skew > 60min → exit(1).
- Panic recovery: TODO spawn de goroutine passa por `SafeGo`/`SafeRun`. Nunca
  `go func()...()` direto em código de produção.
