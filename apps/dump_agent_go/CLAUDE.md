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
  - `run-windows-integration` → runs `integration-windows` (FB 2.5 service
    runs both CNES/SIHD and BPA fixtures via isql). Windows-latest runner.
  - `run-integration` → runs `sia-integration` (Linux, DBF fixtures).
  - Nightly schedule at `30 2 * * *` UTC runs both regardless of label.
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
- FB 1.5 + nakagami driver compatibility: T1 spike **PASS via schema-parity in CI**.
  CI runs the nakagami driver against a synthetic FB 2.5 ODS-11 GDB built
  from `BPA_synthetic.sql` (matches production column types + order;
  nullability relaxed on 6 columns vs the deleted `gen_bpa_gdb_fixture.py`
  so seed data can exercise the NULL-tolerant `COALESCE` scan path in
  `bpa.go`). Production schema nullability NOT YET introspected — pending
  manual `RDB$RELATION_FIELDS` query against a real `BPAMAG.GDB`; capture
  in `docs/data-dictionary-bpa.md` when done. Production wire-protocol
  fidelity is asserted by upstream vendor claim + manual smoke via
  `spike_fb15.exe` against real FB 1.5 edge servers. Issue #51 closed by PR-B.

## Phase 6: register subcommand (2026-04-30)

- `cmd/dumpagent/cmd_register.go` — `dumpagent register --tenant-id <T> --base-url <URL>`
  runs Device Flow + CSR + `/provision/cert` + DPAPI persist + mTLS smoke probe.
- Exit codes: 0=ok, 1=local I/O, 2=usage, 3=net, 4=provision, 5=persist, 6=expired, 7=denied.
- `internal/auth/ca_pin.go` embeds `internal/auth/root_ca.pem` as `auth.CAPinPEM`.
  Repo holds a self-signed test placeholder. **Production binaries must
  overlay the real CA before `go build`:**

      cp /secure/ops/cnesdata-prod-ca.pem apps/dump_agent_go/internal/auth/root_ca.pem
      make build-windows
      git checkout -- apps/dump_agent_go/internal/auth/root_ca.pem

  Override at runtime with `--ca-pin /path/to/ca.pem` for staging/dev.
- Re-register: refused with exit 2 unless `--force` (overwrites all
  three files: cert.pem + key.bin + refresh.bin).
- Smoke probe is warn-only: a failed `/api/v1/system/health` does not
  roll back the persisted cert. Pass `--no-smoke` for air-gapped installs.
- Phase 7 = background rotation loop (`internal/auth/rotate.go`); Phase 8
  flips the apiclient default to mTLS.

## Phase 7: cert rotation loop (2026-04-30)

- `internal/auth/rotate.go` — `Rotator` background goroutine spawned in
  `dumpagent run` when cert is present. Wakes every ~6h ± 10% jitter,
  checks `cert.pem` TTL, calls `POST /provision/cert/rotate` over mTLS
  when remaining < total/3 (~30 days for default 90-day cert).
- On 4xx (cert_revoked / agent_revoked / invalid_request) → loop logs
  `rotate_terminal_stop` and exits. Operator must run
  `dumpagent register --force` to re-enroll.
- On 5xx / network err → 3x exponential backoff (1s/2s/4s); on exhaustion
  loop logs `rotate_attempt_failed` and retries on next tick.
- Persist order: `auth.SaveKey` → `auth.SaveCert`. `refresh.bin` is NOT
  touched (server preserves the existing refresh_token).
- After persist, calls `transport.Client.Reload()` for in-process
  hot-swap (atomic.Pointer in mtls.go). New TLS handshakes use the new
  cert without restart.
- `cmd/dumpagent/cmd_run.go` wires via `startRotatorIfPossible`: missing
  cert / mTLS init failure logs warn and continues without rotation
  (agent runs in non-mTLS mode until `dumpagent register`).
- Phase 8 = wire `apiclient.Adapter` to use `mtlsClient.HTTPClient()`.

## Phase 8: apiclient mTLS wiring (2026-04-30)

- `cmd/dumpagent/cmd_run.go` — `runForeground` constructs single
  `*transport.Client` after machine_id resolve, shares between
  Phase 7 rotator and Phase 8 apiclient. Rotator's `Reload()`
  hot-swaps cert for both via `atomic.Pointer[tls.Certificate]`
  (Phase 5 contract).
- `initMTLSClient(authDir)` returns `(mtls, nil)` on success,
  `(nil, err)` fail-closed when `transport.NewMTLSClient` fails.
  `AGENT_ALLOW_INSECURE=true` flips fail-closed → fallback `(nil, nil)`
  (plain HTTP via `http.DefaultClient`). Boot logs `mtls_init_ok` or
  `mtls_fallback_active` once; no per-job re-log.
- `httpClientFor(mtls)` returns `mtls.HTTPClient()` or nil; threaded
  to `buildAPIClient(machineID, httpClient)`. `apiclient.NewAdapter`
  signature unchanged (already accepted `*http.Client`).
- MinIO uploads (presigned PUT) untouched — `upload.NewHTTP(nil)`
  remains plain HTTP, direct to MinIO not central_api.
- 8-phase zero-trust migration COMPLETE. Agent runs mTLS by default;
  unregistered agents must `dumpagent register` first OR set
  `AGENT_ALLOW_INSECURE=true` for fleet rollout escape hatch.
