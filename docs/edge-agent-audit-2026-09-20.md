# dumpagent edge audit — siha (192.168.1.80) — findings log

Session: 2026-09-20. Durable scratch copy; canonical copy goes into the PR.

## Confirmed by direct source reading (H1-H9) — see plan file for details
See /home/vinicius/.claude/plans/instale-debbug-e-corrija-distributed-trinket.md

## H10 (NEW, confirmed empirically) — INTENT/intent vocabulary mismatch, agent<->central_api

Two independent code paths disagree on the intent string vocabulary and there is no
shared source of truth:

- `apps/central_api/src/central_api/routes/jobs.py::_FATO_SUBTYPE_FOR` keys on
  `(source_type, intent)` where intent is **prefixed**: `"cnes_profissionais"`,
  `"cnes_estabelecimentos"`, `"cnes_equipes"`.
- `apps/dump_agent_go/internal/worker/outbox_adapter.go::validateRawScope` requires
  `key.Intent == "profissionais"` — **bare**, unprefixed — where `key.Intent` comes from
  `deltaKeyFromParams` -> `splitIntent(p.Intent)`.

Agent's own default (`cmd_run.go`, `INTENT` env var) is `"estabelecimentos"` (bare,
unprefixed) — matches NEITHER vocabulary exactly.

Empirical repro on siha: with `INTENT` unset (default `estabelecimentos`), every
`POST /api/v1/jobs/upload-url` from the agent returned HTTP 422 with body
`{"detail": "unsupported_source_intent=CNES_LOCAL/estabelecimentos"}` (central_api log,
6 consecutive 422s). Setting `INTENT=cnes_estabelecimentos` made central_api accept the
mint call (201 Created) — but this has NOT been checked against `validateRawScope`'s bare
"profissionais"/"estabelecimentos" expectation; per source reading, `_FATO_SUBTYPE_FOR`
and `validateRawScope` may have NO single INTENT value that satisfies both. Needs the
enumeration check before deciding the fix (add a translator layer vs unify the vocabulary).

**Impact:** on a production install with defaults, agent uploads for CNES_LOCAL
estabelecimentos/profissionais/equipes are dead on arrival — every mint attempt 422s.

## H11 (NEW, confirmed empirically) — Go client can't parse central_api's plain-string 422 detail

`central_api` raises `HTTPException(status_code=422, detail=f"unsupported_source_intent=...")`
— a bare string — in several handlers (jobs.py `_resolve_fato_subtype` at minimum). FastAPI's
default *validation* 422 shape is `{"detail": [{"loc":..., "msg":..., "type":...}]}`
(list of objects), and the Go client (`internal/apiclient`, generated from OpenAPI) appears to
unconditionally decode error bodies as that shape.

Empirical repro: agent log shows
`{"level":"WARN","msg":"mint_upload_url_failed","err":"json: cannot unmarshal string into Go
struct field HTTPValidationError.detail of type []apiclient.ValidationError"}`
— this is NOT the real error. The real error (visible only via central_api's own log /
direct curl) was `unsupported_source_intent=CNES_LOCAL/estabelecimentos`. The Go client's
JSON-shape assumption throws away the message and reports a useless secondary parse failure
instead, which actively impeded debugging this session and would impede any operator's.

**Fix shape:** central_api should always emit FastAPI's structured `detail` shape (never a
bare string) for any 4xx/5xx an edge agent can hit, OR the Go client should fall back to
raw-body-as-string when JSON-shape decoding fails, instead of swallowing the real message.
Prefer the server-side fix (contract discipline) + client fallback as defense in depth.

## Open / unresolved — needs the Firebird-reachability probe before writing up

- `job.RawRequest` is (per grep of all non-test `.go` files) **never assigned** anywhere in
  agent code — only read (`== nil` checks). So `rawRequest()`/`replayRaw()`
  (`outbox_adapter.go`) should be dead code in the shipped wiring, confirming the earlier
  exploration report. Yet the 6 upload-url mints observed carried the EXACT placeholder
  signature that `rawRequest()` sets (`SizeBytes=1, ObjectSHA256=strings.Repeat("0",64)`,
  minio_key pattern `.../CNES_VINCULO/...`). Root cause of this apparent contradiction is
  unresolved — could be: (a) a second unfound placeholder-producing site in the delta/
  snapshot commit path coincidentally matching the same values, or (b) contamination from
  a prior failed run's queued state replaying (queue/outbox.db and state/delta.db were NOT
  wiped between the two `dumpagent run` invocations in this session — only the log file was
  deleted). (b) is the more likely explanation: exactly 6 mint attempts recur across restarts
  (6x 422 in the pre-fix runs, 6x 201 in the post-fix run) — consistent with a persisted
  cycle count surviving process restart, not a fresh per-boot count.
- **Not yet established whether Firebird is reachable from the agent at all.** Something
  (PID 9880, identity unconfirmed) has been listening on 127.0.0.1:3050 continuously since
  before this session started (present in the very first T0 baseline), independent of the
  registered `FirebirdServerDefaultInstance`/`FirebirdGuardianDefaultInstance`/
  `FirebirdServerFB50` services (all `Stopped`/`Disabled` throughout). `Start-Service
  FirebirdServerDefaultInstance` failed (`StartServiceFailed`, no further detail). Real
  extraction against `C:\CnesDataTest\db\CNES.GDB` has not been proven end-to-end.
- Candidate H12 (unconfirmed): if Firebird is unreachable and the agent still produces
  "successful" placeholder uploads with no ERROR-level log line, that is a severe silent-
  failure defect (edge looks healthy, central sees green, zero real data ever lands).
  Needs the reachability probe to confirm or refute.

## Evidence trail

- central_api log: `/tmp/edge-audit/central_api.log` (6x 422 `unsupported_source_intent`,
  then 6x 201 after `INTENT=cnes_estabelecimentos`).
- Agent log (siha): `%LOCALAPPDATA%\CnesAgent\logs\dumpagent.log` — captured excerpts above.
- `landing.extractions` rows for job_ids `a2afc4e5…`, `176e45ca…`, `f4da9f23…`,
  `a13f3775…`, `edad0de0…`, `21833ca4…` — all `CNES_LOCAL`/`PENDING`, sha256 zeros,
  size_bytes=1, minio_key `354130/CNES_VINCULO/2026-09-01/<job_id>.parquet.gz`.
- T0a/T0b snapshots: `/tmp/edge-audit/t0a.json`, `t0b.json` — zero diff (clean noise floor).
- dumpagent.exe built at `audit-2bf257e`, deployed to `C:\Program Files\CnesAgent\`.
- CNES.GDB copy verified byte-identical (SHA256 match) at `C:\CnesDataTest\db\CNES.GDB`;
  original untouched (mtime unchanged: 2026-09-18T19:08:04.34Z before and after).

## Firebird reachability probe (resolved)

`spike_fb15.exe` (same `nakagami/firebirdsql` driver the agent uses) against
`localhost:3050` / `C:\CnesDataTest\db\CNES.GDB` / SYSDBA:masterkey: **connection and
authentication succeeded**; the only error was `SQL error code = -204, Table unknown,
BPA_CAB` — expected, since `spike_fb15`'s hardcoded probe query targets a BPA-Mag table,
not present in a CNES database. This is a query-target mismatch in the spike tool itself,
not a connectivity problem.

**Conclusion: Firebird is reachable, wire-protocol compatible (FB 1.5), and the GDB copy
opens and authenticates correctly. H12 (silent-failure-to-empty-upload) is REFUTED for the
connectivity dimension** — the driver stack works end to end down to query execution.
Owning process: PID 9880, `C:\Firebird\Firebird_1_5\bin\fbserver.exe`, started 18/09/2026
15:06:47, running standalone (NOT via the `FirebirdServerDefaultInstance` SCM service,
which remained `Stopped` throughout — explains the service/listener status mismatch noted
in the plan).

The size_bytes=1/sha-zeros placeholder mechanism in the 6 observed mints remains
unexplained (see "Open/unresolved" above) — deprioritized in favor of the named
install/uninstall/residue task, which had zero coverage at this point in the session.

## Corrections after review

- **E2E claim, corrected:** the agent → central_api control-plane leg was verified
  end-to-end (real `dumpagent install` → real Windows Service → `Start-Service` →
  `POST /jobs/upload-url` → `201 Created`, confirmed via `landing.extractions` rows and
  central_api's own log). **The object-storage leg was NOT verified**: `mc ls --recursive
  local/cnesdata-landing` was empty throughout, all six extraction rows stayed `PENDING`
  (never `UPLOADED`/`REGISTERED`), and every row carried the `size_bytes=1`/sha-zeros
  placeholder signature, not real data. `/jobs/register` was never called. The placeholder-
  signature mechanism (see "Open/unresolved" above) remains unexplained — this is the
  thread that would explain why upload/register never happened, not a poll-timing artifact.
- **H10, narrowed:** `validateRawScope`'s bare-string check (`"profissionais"`) is only
  reachable via `job.RawRequest != nil`, and grep across all non-test `.go` files found
  zero assignment sites for `RawRequest`. The two vocabularies (`_FATO_SUBTYPE_FOR`'s
  `cnes_`-prefixed intents vs `validateRawScope`'s bare intents) therefore do **not**
  collide at runtime today — one side is dead code. Fix: correct the agent's `INTENT`
  default to `cnes_estabelecimentos` (matches `_FATO_SUBTYPE_FOR`). File the bare-vs-
  prefixed divergence in `validateRawScope` as a **latent** collision for whenever the raw
  path gets wired up, not an active bug.
- **H4 fix shape, corrected:** registry `Environment` (`REG_MULTI_SZ` under the service's
  own `HKLM\SYSTEM\CurrentControlSet\Services\CnesDumpAgent` key) is confirmed **empirically
  functional** (real service booted and ran using it) but is NOT safe for secrets —
  `Get-Acl` on the parent `...\Services` key shows `NT AUTHORITY\Authenticated Users:
  ReadKey`, i.e. any logged-in user on the machine can read it. The runbook's
  `config.env` includes `DB_PASSWORD`. Fix must split: `Environment` carries only
  non-secret config (`TENANT_ID`, `CENTRAL_API_URL`, `COMPETENCIA_YYYYMM`, `CNES_DB_PATH`,
  etc.); passwords stay exclusively in the existing DPAPI-backed `secrets/*.dpapi` store via
  `dumpagent set-secret`, which `password_config.go`'s env > store > default chain already
  supports.
- **H1/H4 dependency:** H4's registry `Environment` value only gets cleaned up if H1 is
  fixed first — uninstall-while-running leaves the whole service key (env included) behind
  until the process is killed or the machine reboots.
- **Rollback runbook also broken by H5**, not just the install runbook:
  `docs/runbooks/dumpagent-rollback.md` steps 6 and 8 both read
  `$env:LOCALAPPDATA\CnesAgent\logs\*` — proven empirically wrong for a service-run agent
  (service's `machine_id` was `abd45add`; interactive admin's was a different `e0e9cc87`,
  in a different directory tree entirely). An operator following step 8 for a post-mortem
  after a real (service-mode) incident copies an empty or stale directory.

## Fixes applied (this branch)

All defects H1-H11 found during the siha audit were fixed, each with a regression test
that fails against the pre-fix code:

| # | Fix | Files |
|---|---|---|
| H1, H2, H7 | Extracted a portable `scmConnector`/`scmService` seam so uninstall orchestration (stop-before-delete, idempotent-on-absent, event-source-removed-only-after-successful-delete) is unit-tested on Linux via a fake, not just reachable on Windows CI | `internal/service/lifecycle.go` (new), `lifecycle_test.go` (new), `install_windows.go` |
| H3 | `dumpagent uninstall` stays state-preserving by default (matches the rollback runbook's own assumption); `--purge` opts into removing the full state root | `cmd/dumpagent/cmd_uninstall.go`, `main.go` |
| H4 | `--config` now actually reaches the service: non-secret keys go into the service's own registry `Environment` (`REG_MULTI_SZ`), injected by the SCM at start. Secret-like keys (`PASSWORD`/`SECRET`/`TOKEN`/`APIKEY`) are rejected — `HKLM\SYSTEM\...\Services` grants `ReadKey` to Authenticated Users, confirmed via `Get-Acl` on siha | `internal/service/envconfig.go` (new), `install_windows.go` |
| H5 | State root moved from `%LOCALAPPDATA%\CnesAgent` (per-user) to `%ProgramData%\CnesAgent` (machine-wide) — interactive admin sessions and the LocalSystem service now resolve to the same path by construction | `internal/platform/paths.go` |
| H6 | Runbook rewritten to use the `CNES_`/`SIHD_`/`BPA_`-prefixed env var names `resolveFB` actually reads; a Go test parses the runbook's `config.env` block and fails if it drifts from that prefix again | `docs/runbooks/dumpagent-install-windows.md`, `cmd/dumpagent/runbook_env_vars_test.go` (new) |
| H8 | `Release()` now unlinks the POSIX lock file after unlocking (Windows uses a named mutex, no file involved — this defect was POSIX-only) | `internal/platform/lock_unix.go` |
| H9 | Added `MINIO_PUBLIC_ENDPOINT` (defaults to `MINIO_ENDPOINT`), used only for the presigned-URL client so an edge agent on a different network than central_api's internal MinIO alias can still complete uploads | `packages/cnes_infra/src/cnes_infra/config.py`, `apps/central_api/src/central_api/deps.py` |
| H10 | Agent's default `INTENT`/`TIPO_EXTRACAO` corrected to `cnes_estabelecimentos`, matching central_api's `_FATO_SUBTYPE_FOR` map (the previous unprefixed default 422'd on every fresh install) | `cmd/dumpagent/cmd_run.go` |
| H11 | All plain-string 422 `detail`s in agent-facing routes replaced with a `validation_error()` helper matching FastAPI's own `HTTPValidationError` schema (list of `{loc,msg,type}`) — the Go client only has a typed decode path for 422, and a bare string crashed it with an opaque secondary error, hiding the real message | `apps/central_api/src/central_api/validation_errors.py` (new), `routes/jobs.py`, `routes/extractions.py`, `routes/raw_jobs.py` |

Verification performed on this branch: `go build`/`go vet` (Linux + `GOOS=windows`
cross-compile), `go test -race ./...` (all packages green), `golangci-lint run` (v1.64.8,
CI-pinned version, clean — required installing a matching `go1.26.2` toolchain locally
since the snap-installed Go 1.27.1 trips an export-data version mismatch in that
golangci-lint release), 84.9% filtered coverage (gate: 65%), `ruff check` clean on all
touched Python, and the full relevant `pytest` suites green.

Not fixed in this branch (documented, out of scope):
- The `size_bytes=1`/sha-zeros placeholder mint-loop mechanism (see "Open/unresolved"
  above) — root cause still unexplained; deprioritized in favor of the named
  install/uninstall/residue task.
- `packages/cnes_infra/src/cnes_infra/storage/object_storage.py`'s `MinioObjectStorage`
  (used by `data_processor`, not the edge-agent-facing route) has the same
  internal-vs-public endpoint conflation as H9, but wasn't empirically exercised by this
  audit and is a different code path — flagged for a future pass, not fixed here.

## Re-run: field verification of the fixed binary on siha (2026-09-20, later same day)

The original audit's residue-zero result came from *manual* cleanup, not from the fixed
`uninstall` — none of the `//go:build windows` code (`scmConnector`, `writeServiceEnvironment`,
the ProgramData move) had actually executed on real Windows. Per the plan's own Verification
step 4, re-ran the install → running → uninstall cycle on siha with a binary cross-compiled
from this branch's HEAD (`4bd91df`, `-X main.Version=audit-rerun-4bd91df`) to close that gap.

Baseline: fresh `snapshot.ps1` T0a/T0b ~5min apart (zero filesystem delta between them),
cross-checked against the *previous* session's T2 — also zero delta across a 41-minute gap,
confirming nothing from the first session left a slow-acting trace.

Evidence, in sequence:

1. **H4 (`--config` reaches the service, secrets rejected):** `install --config ... --start-type
   manual` printed `warn: rejected secret-like keys from --config ...: [CNES_DB_PASSWORD]` and
   exited 0. `Get-ItemProperty HKLM:\...\Services\CnesDumpAgent -Name Environment` showed all
   10 non-secret keys present, `CNES_DB_PASSWORD` absent.
2. **H5 (ProgramData state root):** after `Start-Service`, `C:\ProgramData\CnesAgent` existed;
   both `%LOCALAPPDATA%` variants (interactive admin and `systemprofile`) did not.
3. **H1 (stop-before-delete, Case A — the defect that motivated this fix):** ran `uninstall`
   while the service was `Running`. Exit 0, `uninstalled service=CnesDumpAgent`. A **fresh**
   PowerShell invocation (not the same session) confirmed `Test-Path HKLM:\SYSTEM\
   CurrentControlSet\Services\CnesDumpAgent` → `False` — no reboot needed, no deletion-pending
   residue. `Get-Service CnesDumpAgent` errored (service truly gone). The eventlog source
   registry entry was removed (`False`), while a historical Application-log entry from this
   same run remained readable — correct: source *registration* is cleaned up, log *entries*
   are not, by design. State root `C:\ProgramData\CnesAgent` survived (state-preserving
   default, per H3).
4. **H2 (idempotency, Case B):** ran `uninstall` again immediately. Exit 0,
   `uninstall: service=CnesDumpAgent already absent (ok)`.
5. **H3 (`--purge`):** ran `uninstall --purge` against the already-absent service. Exit 0,
   `purged state dir=C:\ProgramData\CnesAgent`; `Test-Path` on that dir then returned `False`.
6. Removed `C:\Program Files\CnesAgent` and the `C:\CnesDataTest\` GDB copy scratch root.
   Re-confirmed original `CNES.GDB`: `702947328` bytes, `18/09/2026 16:08:04` — byte-for-byte
   and mtime-identical to the pre-audit baseline. Firebird services unchanged
   (`FirebirdServerDefaultInstance`/`FirebirdGuardianDefaultInstance` Stopped/Manual,
   `FirebirdServerFB50` Stopped/Disabled).
7. Final `snapshot.ps1` (T2) diffed against T0a: **zero delta** across every tracked surface
   (filesystem, services, scheduled tasks, local users, firewall rules, all registry subkey
   sets including `Services`, `Uninstall` x2, `EventLog\Application` sources, `Run`/`RunOnce`).

Conclusion: H1, H2, H3, H4 and H5 are now field-verified on real Windows against this
branch's HEAD, not just unit-tested against a fake SCM. H7's ordering (event source removed
only after a successful stop+delete) held on the success path exercised here; the failure
branch it was written for -- a stop that fails, which must NOT delete the service or remove
the event source -- was not (and can't safely be) provoked on a real device, and remains
covered only by `TestUninstall_FalhaAoParar_NaoExcluiENaoRemoveEventSource` against the fake
SCM. No new findings surfaced during the re-run. The e2e upload leg (control-plane mint
vs. actual object landing in the bucket) was intentionally **not** re-exercised on this pass — it's orthogonal to the install/uninstall
defects this re-run targets, and the placeholder-mint mystery documented above remains open
and unrelated to this verification.
