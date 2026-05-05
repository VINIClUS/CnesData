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

## Phase 5: hardening (2026-05-01/02)

- **5.1 Windows Event Log sink** — WARN+ slog fan-out under source `DumpAgent` via `obs.MultiHandler`; build-tag no-op on Linux/Mac. `agent.exe install/uninstall` registers idempotently. Banded event IDs (`internal/obs/events.go`: 1xxx auth, 2xxx queue, 3xxx extract, 4xxx upload, 5xxx diagnose, 8xxx breaker, 9xxx generic). `obs.FormatCompact` single-line UTF-8-safe truncated at 8KB. Rollback: `AGENT_EVENTLOG_DISABLED=true`. Locale-independent unregister via `errors.Is(syscall.ERROR_FILE_NOT_FOUND)`.
- **5.2 Outbox + Circuit Breaker** — `internal/queue/` bbolt persistent queue at `%PROGRAMDATA%\dumpagent\queue\outbox.db` for `CompleteJob`/`FailJob`; uint64 time-ordered keys, fsync per Append, 10k cap, 90-day TTL. `internal/breaker/` CLOSED/OPEN/HALF_OPEN (threshold 5, reset 60s) gates drain + RegisterJob. `Drainer.Run` 30s tick, peek 20, classify 2xx delete / 4xx drop / 5xx retry / 429 Retry-After. `startDrainWithWatcher` re-spawns under `SafeGo`; outbox open fail = exit 1.
- **5.3 Backoff + Jitter** — `internal/obs/jitter.go` `JitterAround` + `DecorrelatedJitter`. Drain tick [24s,36s], breaker reset [40s,80s], rotate retries decorrelated 1s→30s. `math/rand/v2.Float64` default; `SetRand`/`SetClock` for tests. Filtered cov ≥ 81.5%.
- **5.4 Diagnose CLI** — `dumpagent diagnose [--probe] [--json]` read-only health: static (cert/auth_dir/outbox/log_dir) + probe (central_api mTLS / FB SELECT 1 / MinIO TCP). PASS/WARN/FAIL severity, exit 0 or 1. Outbox check via `bbolt.Open(ReadOnly: true)`; missing = PASS. Disk-free via `golang.org/x/sys`.

## Phase 9 — Path discovery + per-source secrets (2026-05-03)

`dumpagent discover` auto-detects 4 sources (cnes/sihd/bpa FB DSNs + sia DBF dir); writes `%PROGRAMDATA%\dumpagent\config.yaml` with top pick uncommented + alternates as comments. `dumpagent set-secret <cnes|sihd|bpa>` stores DPAPI-wrapped FB password (Linux: 0600 plaintext fallback). `dumpagent run` precedence: CLI > env > YAML > default; password chain env > DPAPI > `masterkey` (SYSDBA only) → WARN `password_default_active`. `AGENT_DISABLE_DISCOVER=true` bypasses YAML for legacy env-only deploys. Strategies: pure-Go Windows registry (FB Project + Datasus vendor + Uninstall keys WOW64) + drive-walk filesystem templates per-profile. New diagnose check `discover_yaml` reports `sources_ready=N/4`.

## Phase 10 — Delta detection (2026-05-03)

Delta is the only execution path (no flag, no legacy snapshot). SHA-256 row-fingerprint delta for cnes/sihd/bpa. State store at `%PROGRAMDATA%\dumpagent\state\delta.db` (bbolt). Per (source, intent, competencia): `committed/<path>` sub-bucket holds last-cycle hashes; `pending/<path>/<job_id>` holds in-flight; atomic tx-swap on CompleteJob ack via P5.2 outbox. Delta Parquet emits `_op ∈ {I,U,D}` column; D rows carry PK-only. SIA stays full-extract. 24h GC of stale pending on Open. `internal/delta/` package (types/profiles/fingerprint/compute/store/writer); `JobExecutor.RunDelta` runs the production path. data_processor `cdc_merger.merge_delta` applies D inline + I/U via apply_iu_fn callback. Cold-start emits all-I + WARN. New diagnose check `delta_store`.

## Phase 11 — Integrity + Audit (P2, 2026-05-04)

`RunDelta` tees Parquet upload through `integrity.SHA256TeeReader`; sha256 hex flows in CompletePayload + landing.extractions.sha256. data_processor `verify_and_route_delta` recomputes on download; mismatch raises IntegrityError. Edge writes 4-state HMAC-signed JSONL audit at `%PROGRAMDATA%\dumpagent\audit\events-YYYY-MM-DD.jsonl`. Lifecycle: extracted → uploaded → committed (or aborted). HMAC-SHA256 over CanonicalJSON-without-hmac (sorted keys via Go stdlib map sort). Key 32B random, DPAPI-wrapped at `secrets/audit_hmac.dpapi` (P1 secrets store reused). CLI `dumpagent audit verify <path>` exit 0/1/2. Boot HMAC fail → audit no-op + WARN; cycle continues.

## Phase 12 — Post-upload register normalization (FU1, 2026-05-04)

Cnes/sihd path normalized to BPA/SIA's post-upload register pattern.
`Adapter.MintUploadURL` calls new `POST /api/v1/jobs/upload-url` to create
landing.extractions PENDING + presigned PUT URL. Edge uploads Parquet,
captures sha256 via `integrity.SHA256TeeReader`, then calls existing
`POST /api/v1/jobs/register` with `RegisterRequest.Sha256` set. `CompleteJob`
removed entirely from edge code (dead /complete route never existed).
`audit.LifecycleCommitted` emitted by Consumer.processJob after RegisterJob
ack via new `JobExecutorIface.EmitCommitted` method. `Job.MinioKey` field
added (returned from upload-url mint). Outbox now queues `RegisterJob`
envelopes (sha256 + minio_key + size_bytes); `MintUploadURL` is direct
(caller needs returned values). No DB migration. landing.extractions.sha256
column populated on every cycle (not NULL like P2).
