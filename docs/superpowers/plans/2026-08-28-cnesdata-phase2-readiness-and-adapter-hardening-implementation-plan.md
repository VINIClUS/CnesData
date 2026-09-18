# CnesData Phase 2 Readiness and Adapter Hardening Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this
> plan task by task, with a review checkpoint after every logical issue.

**Goal:** Restore a green integration base and deliver conformant SQLite, DynamoDB,
filesystem, S3, local JSONL/Parquet audit, S3 Object Lock audit, and outbox adapters without
exceeding three parallel feature worktrees.

**Architecture:** The accepted Phase 1 domain, request values, ports, and reusable contract
cases remain the source of truth. CND-019 is a serial readiness gate. CND-020 through
CND-023 add backend leaf modules in parallel. CND-024 connects the existing outbox methods
to audit sinks. CND-025 owns shared exports, emulator wiring, the integrated matrix, and the
final Phase 2 gate.

**Tech stack:** Python 3.13, Pydantic domain models, `sqlite3`, boto3, DynamoDB Local 3.3.1,
Moto 5.2.3, LocalStack 2026.08.0, botocore Stubber, Polars, pytest, Hypothesis, Ruff,
Docker Compose, GitHub Actions.

**Design amendment:**
`docs/superpowers/specs/2026-08-28-cnesdata-phase2-readiness-and-adapter-hardening-design.md`

**Existing governing plan:**
`docs/superpowers/plans/2026-08-23-cnesdata-data-plane-local-profile-implementation-plan.md`

## Execution rules

- Do not create a dependent branch until every dependency is merged into a green
  `develop`.
- Use one issue per worktree and the exact branch names in this plan.
- Keep no more than three feature worktrees plus the controller lane.
- Feature worktrees may add only their owned leaf modules and tests. The controller owns
  `pyproject.toml`, `uv.lock`, CI, Compose, shared `__init__.py` exports, generated schemas,
  global configuration, and tracker documentation.
- Use test-driven development: add the smallest failing behavior, run it and confirm the
  expected failure, implement, rerun the focused test, then run the issue gate.
- Do not weaken an accepted Phase 1 contract to make one backend pass. A real port defect
  requires a controller-lane amendment with both backends and the shared contract updated
  together.
- Commit only after the focused tests and lint for touched files pass.
- A worktree passing in isolation is not completion; the issue must merge and its resulting
  `develop` commit must be green before it unlocks a dependent.

## Materialization sequence

After this plan is merged, create only CND-019 and link it from #93 and #97. After CND-019
is merged and the integrated commit is green, create CND-020 through CND-025. Copy each
packet's goal, dependencies, allowed paths, acceptance criteria, and verification commands
into the issue body. Replace logical links with assigned issue numbers in #93 and #97; do
not renumber the CND IDs.

---

### Task 1: CND-019 — Phase 2 readiness gate

**Branch:** `fix/cnd-019-phase2-readiness`  
**Lane:** controller  
**Depends on:** CND-014 integrated  
**Blocks:** CND-020, CND-021, CND-022, CND-023

**Files:**

- Modify: `.github/workflows/ci.yml`
- Create: `scripts/ci_python_gate.sh`
- Modify: `scripts/baseline_matrix.py`
- Modify: `scripts/baseline_matrix_test.py`
- Modify: `tests/perf/macro/test_data_processor_e2e.py`
- Modify: `tests/perf/micro/test_upsert_bench.py`
- Modify: `tests/perf/soak/test_upsert_soak.py`
- Modify: `tests/perf/spike/test_upsert_spike.py`
- Modify: `tests/perf/stress/test_upsert_stress.py`
- Create: `docs/baselines/2026-08-28-phase2-ready.json`
- Do not modify: `pyproject.toml`, `uv.lock` (the required versions are already locked)

**Step 1: Reproduce the readiness failures**

Run:

```bash
uv run ruff check .
uv run pytest scripts/baseline_matrix_test.py -q
uv run pytest -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
```

Expected at the inspected base: local Ruff 0.15.11 may pass while CI's unpinned Ruff 0.16.5
reports 14 `PLR0917` findings; the fast suite aborts while collecting the five performance
modules that import the absent legacy repository.

**Step 2: Add failing baseline-waiver tests**

Extend `scripts/baseline_matrix_test.py` with cases proving:

- the approved suite, SHA, exit code, and exact normalized missing-module signature are
  accepted together;
- the same exit code with `AssertionError`, a different missing module, or an empty output
  is rejected;
- a matching failure on an unapproved SHA or suite is rejected;
- the canonical document has exactly three fields: exception
  `ModuleNotFoundError`, module
  `cnes_infra.storage.repositories.estabelecimento_repo`, and the following sorted
  `affected_paths`: `tests/perf/macro/test_data_processor_e2e.py`,
  `tests/perf/micro/test_upsert_bench.py`, `tests/perf/soak/test_upsert_soak.py`,
  `tests/perf/spike/test_upsert_spike.py`, and
  `tests/perf/stress/test_upsert_stress.py`; serialize it as compact sorted-key JSON and
  encode it as UTF-8;
- the report contains the SHA-256 of that canonical document, not captured stdout/stderr;
- changing one byte in any canonical field changes the hash and rejects the waiver.

Run:

```bash
uv run pytest scripts/baseline_matrix_test.py -q
```

Expected: FAIL because `SuiteResult` and `_waiver_for` do not inspect output.

**Step 3: Harden the fingerprint**

Capture combined stdout/stderr privately during execution. Parse only the exception class,
missing module, and repository-relative affected test paths; sort paths, serialize the
canonical document specified in Step 2, and hash its UTF-8 bytes with SHA-256. Keep commit
SHA, suite name, and exit code checks. `SuiteResult` exposes the digest but not the raw log,
so `asdict` cannot leak the captured output into the report.

Run the test from Step 2 and confirm it passes.

**Step 4: Make baseline collection intentional**

In each of the five listed performance modules, import the legacy
`estabelecimento_repo` subject through `pytest.importorskip` with a reason that states the
repository is absent from the reconciled `develop`. Keep the performance test bodies
unchanged. This turns an irrelevant collection crash into an explicit skip without hiding
other import or execution failures.

Run:

```bash
uv run pytest --collect-only -m "not integration and not postgres and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" -q
```

Expected: exit 0; the five legacy performance modules are skipped rather than collection
errors.

**Step 5: Align the complete CI environment with `uv.lock`**

In `.github/workflows/ci.yml`, replace the unbounded test-tool `pip install` with
version-pinned `astral-sh/setup-uv@v8.0.0`, request uv 0.9.26, and run
`uv sync --locked --all-packages`. Execute Python tools through `uv run`. This makes CI use
the checked-in Ruff 0.15.11, Moto 5.2.3, Testcontainers 4.14.2, and the rest of the locked
graph rather than resolving a new graph on every run. Do not regenerate `uv.lock`.

Do not add a global `PLR0917` ignore. A later Ruff upgrade is a separate reviewed change
that either refactors the exact call sites or adds path-scoped compatibility entries.

Add `workflow_dispatch`. Add `.github/workflows/ci.yml` and
`scripts/ci_python_gate.sh` to both pull-request and push path filters. At the inspected base
the workflow itself appears only under `pull_request.paths`; without the push entry,
merging CND-019 may not create the required integrated-`develop` run. A manual run is a
recovery tool, not a substitute for the required push run on the exact merge SHA.

**Step 6: Version the CI-equivalent command script**

Create `scripts/ci_python_gate.sh` with this content and make it executable:

```bash
#!/usr/bin/env bash
set -euo pipefail

gate_openapi="$(mktemp)"
gate_contracts="$(mktemp -d)"
trap 'rm -f "$gate_openapi"; rm -rf "$gate_contracts"' EXIT

uv run python scripts/gen_openapi.py --output "$gate_openapi"
diff -u docs/contracts/openapi.json "$gate_openapi"
uv run python scripts/gen_contracts.py --output "$gate_contracts/"
diff -ru docs/contracts/schemas/ "$gate_contracts/"
uv run ruff check .
(
  cd packages/cnes_infra
  uv run alembic -c alembic.ini upgrade head
)
uv run pytest packages/cnes_domain packages/cnes_infra \
  -m "not bigquery and not e2e and not stress and not soak and not spike" \
  --cov --cov-config=pyproject.toml --cov-report=term-missing
uv run pytest apps/ \
  -m "not integration and not bigquery and not e2e and not stress and not soak and not spike and not windows_only" \
  --cov --cov-config=.coveragerc --cov-report=term-missing
```

Replace the corresponding CI steps with one call to `bash scripts/ci_python_gate.sh` after
the locked environment is installed. Run `bash -n scripts/ci_python_gate.sh` before using
it.

Run:

```bash
uv run ruff --version
uv run ruff check .
```

Expected: Ruff 0.15.11 and exit 0.

**Step 7: Commit the readiness implementation**

Commit the code and CI changes before recording the baseline so `_commit_sha()` identifies
the exact reviewed implementation rather than the old base with a dirty worktree.

```bash
git add .github/workflows/ci.yml scripts/ci_python_gate.sh scripts/baseline_matrix.py \
  scripts/baseline_matrix_test.py \
  tests/perf/macro/test_data_processor_e2e.py \
  tests/perf/micro/test_upsert_bench.py \
  tests/perf/soak/test_upsert_soak.py \
  tests/perf/spike/test_upsert_spike.py \
  tests/perf/stress/test_upsert_stress.py
git commit -m "fix(ci): establish phase 2 readiness gate"
```

**Step 8: Run and record the readiness matrix**

Run:

```bash
uv run python scripts/baseline_matrix.py --output docs/baselines/2026-08-28-phase2-ready.json
```

Expected: exit 0. Any retained waiver must match the hardened historical fingerprint; no
new waiver may cover a Phase 2 failure.

Then run the repository CI-equivalent commands from `.github/workflows/ci.yml`, including
schema drift, lint, migrations, package coverage, and app coverage:

```bash
docker compose -p cnesdata up -d --wait postgres
DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
PG_TEST_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
COMPETENCIA_ANO=2026 COMPETENCIA_MES=1 DISABLE_PANDERA_IMPORT_WARNING=True \
COD_MUN_IBGE=354130 ID_MUNICIPIO_IBGE7=3541308 \
CNPJ_MANTENEDORA=55293427000117 bash scripts/ci_python_gate.sh
```

Commit the report separately. Its `commit_sha` must equal the parent readiness commit from
Step 7 and the worktree must otherwise be clean.

```bash
git add docs/baselines/2026-08-28-phase2-ready.json
git commit -m "test(baseline): record phase 2 readiness"
```

**Step 9: Open and integrate the PR**

Open a PR to `develop`. The issue is complete only after the PR run and the push run for the
integrated `develop` commit are successful and linked in the issue.

**Acceptance criteria:**

- [ ] CI and local verification use the same `uv.lock` and versioned gate script.
- [ ] CI installs the locked Moto 5.2.3 and Testcontainers 4.14.2 for feature lanes.
- [ ] `push.paths` triggers CI for the CND-019 merge commit.
- [ ] `ruff check .` is green without a global `PLR0917` suppression.
- [ ] The fast suite collects with explicit legacy performance skips.
- [ ] A same-exit-code/different-failure test proves the waiver is rejected.
- [ ] A fresh baseline report is committed.
- [ ] PR CI and integrated-`develop` CI are green and linked.
- [ ] The integrated push run's `head_sha` equals the exact CND-019 merge commit on
  `develop`.

---

### Task 2: CND-020 — SQLite control-plane adapter

**Branch:** `feat/cnd-020-sqlite-control-plane`  
**Lane:** feature  
**Depends on:** CND-019  
**Blocks:** CND-024, CND-025

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_schema.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_claims.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_idempotency.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_publication.py`
- Create: `packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py`
- Create: `packages/cnes_infra/tests/control_plane/test_sqlite_races.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`

**Step 1: Create the contract fixture and prove RED**

Build a temporary-database fixture with a `MutableClock`. Parametrize
`control_plane_cases()` against the SQLite adapter, keeping the case name in the pytest ID.

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py -q
```

Expected: FAIL because `cnes_infra.control_plane.sqlite_adapter` does not exist.

**Step 2: Create schema and transaction shell**

Implement the schema in `sqlite_schema.py`, the public adapter in `sqlite_adapter.py`, and
the focused claim, idempotency, and publication operations in their named modules. Preserve
the original constructor `SQLiteControlPlane(database_path: Path, clock: Callable[[],
datetime])` and `initialize() -> None`. The implementation includes:

- one schema creation path for tenants, memberships, agents, jobs, raw manifest references,
  runs, run dependencies, run units, dispatches, idempotency records, dataset versions,
  dataset pointers, access requests, and outbox events;
- canonical serialization/deserialization helpers for accepted Pydantic entities;
- `PRAGMA foreign_keys=ON`, bounded `busy_timeout`, and `journal_mode=WAL`;
- an operation-scoped connection and `BEGIN IMMEDIATE` write transaction helper;
- uniqueness constraints for aggregate identity, active dispatch, idempotency identity, and
  outbox `event_id`.

Run only the first failing shared case, implement to green, and repeat in this order:
`authorization_jobs`, `raw_chains`, `run_discovery`, `run_units_atomic`, `unit_claim`,
`unit_fences`, `degraded_unit`, `cancellation`, `dispatch`, `dispatch_expiry`,
`idempotency`, `publication`.

**Step 3: Add SQLite boundary tests**

Add focused tests for:

- a failed transition rolling back both the aggregate mutation and outbox insert;
- two writers racing the same claim, lease renewal, and publication compare-and-set;
- monotonically increasing fencing tokens after expiry;
- busy timeout exhaustion returning a deterministic infrastructure error;
- reopen/restart preserving records, leases, pointers, and pending outbox;
- rejection when the configured database path is on a declared network filesystem mode;
- deterministic ordering and limits for every list method.

Use barriers and independent connections; do not share one connection between racing
workers.

**Step 4: Verify**

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py \
  packages/cnes_infra/tests/control_plane/test_sqlite_races.py -q
uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane/sqlite_* \
  packages/cnes_infra/tests/control_plane/test_sqlite_*
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/sqlite_schema.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_claims.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_idempotency.py \
  packages/cnes_infra/src/cnes_infra/control_plane/sqlite_publication.py \
  packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py \
  packages/cnes_infra/tests/control_plane/test_sqlite_races.py
git commit -m "feat(control-plane): add sqlite adapter"
```

**Acceptance criteria:**

- [ ] All shared control-plane cases pass.
- [ ] Aggregate mutation and outbox insertion are atomic.
- [ ] Concurrency, restart, ordering, and rollback cases pass.
- [ ] WAL support is explicitly limited to same-host local filesystems.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 3: CND-021 — DynamoDB control-plane adapter

**Branch:** `feat/cnd-021-dynamodb-control-plane`  
**Lane:** feature  
**Depends on:** CND-019  
**Blocks:** CND-024, CND-025

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_keys.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_codec.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_publication.py`
- Create: `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Create: `packages/cnes_infra/tests/control_plane/test_dynamodb_stale_gsi.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`

**Step 1: Create a Moto fixture and prove RED**

Create the Phase 2 table with all base keys, GSIs, and TTL attribute used by the adapter.
Parametrize `control_plane_cases()` against it.

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py -q
```

Expected: FAIL because `cnes_infra.control_plane.dynamodb_adapter` does not exist.

**Step 2: Implement the single-table mapping**

Put pure key builders in `dynamodb_keys.py`, Decimal-free item codecs in
`dynamodb_codec.py`, the public `DynamoDBControlPlane` in `dynamodb_adapter.py`, and focused
claim/publication transactions in their named modules. Use tenant-prefixed base keys and
explicit entity discriminators. Provide query access for:

- claimable jobs by tenant, agent, state, and availability;
- raw manifests by source identity and sequence;
- waiting runs by collision-free `run_dependency_key`;
- recoverable runs by state and lease time;
- run units and dispatches by run;
- pending outbox events by delivery state and creation order.

Every GSI lookup returns candidates. Strongly reread each base item before a claim,
transition, recovery, or delivery decision and reject a stale state, lease, dispatch, or
fence through a conditional write.

Implement the shared cases incrementally in this order: `authorization_jobs`,
`raw_chains`, `run_discovery`, `run_units_atomic`, `unit_claim`, `unit_fences`,
`degraded_unit`, `cancellation`, `dispatch`, `dispatch_expiry`, `idempotency`, and
`publication`.

**Step 3: Make transaction limits explicit**

Centralize `TransactWriteItems` construction. Reject duplicate item keys before sending a
request. Add boundary tests proving:

- `put_run_units` with 99 units succeeds and emits at most 100 unique actions;
- `put_run_units` with 100 units is rejected before boto3 is called;
- cancellation with 98 units succeeds as run + units + outbox;
- cancellation with 99 units is rejected before boto3 is called;
- no transaction contains two condition/update/put/delete actions for the same key;
- a canceled or failed transaction leaves no partial aggregate or outbox mutation.

Use an injected client spy to inspect the exact action list independently of Moto.

**Step 4: Test GSI and TTL semantics**

Inject stale/duplicate GSI candidates and prove the strongly consistent base reread prevents
an invalid claim or recovery. Keep an expired idempotency item physically present and prove
`expires_at` controls replacement synchronously. Do not wait for emulator TTL deletion.

**Step 5: Verify**

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_stale_gsi.py -q
uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_* \
  packages/cnes_infra/tests/control_plane/test_dynamodb_*
```

Expected: PASS under the lock-resolved Moto version. Real integration against DynamoDB
Local is deferred to CND-025.

**Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_keys.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_codec.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py \
  packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_publication.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_stale_gsi.py
git commit -m "feat(control-plane): add dynamodb adapter"
```

**Acceptance criteria:**

- [ ] All shared control-plane cases pass under Moto.
- [ ] Strong base rereads protect every GSI-driven mutation.
- [ ] TTL is never used as synchronous business truth.
- [ ] The 99/100 and 98/99 transaction boundaries have negative tests.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 4: CND-022 — Filesystem and S3 object stores

**Branch:** `feat/cnd-022-object-stores`  
**Lane:** feature  
**Depends on:** CND-019  
**Blocks:** CND-025  
**Does not block:** CND-024

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/object_store/filesystem.py`
- Create: `packages/cnes_infra/src/cnes_infra/object_store/s3.py`
- Create: `packages/cnes_infra/src/cnes_infra/object_store/_common.py`
- Create: `packages/cnes_infra/tests/object_store/test_filesystem.py`
- Create: `packages/cnes_infra/tests/object_store/test_s3.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/object_store/__init__.py`

`_common.py` is an additive hardening extension to governing Task 12. It centralizes only
private validation/digest behavior, is exclusively owned by CND-022, and adds no public
export or downstream configuration.

**Step 1: Parametrize the shared contract and prove RED**

Run every `object_store_cases()` entry against a temporary filesystem root and a Moto S3
bucket.

```bash
uv run pytest packages/cnes_infra/tests/object_store -q
```

Expected: FAIL because both adapter modules are absent.

**Step 2: Implement safe shared key and digest helpers**

Put shared key validation and streaming digest helpers in `_common.py`. Reject empty,
absolute, backslash, empty-segment, `.`, and `..` keys before backend access. Stream bytes
while computing SHA-256; never publish a mismatched digest.

**Step 3: Implement filesystem publication without overwrite**

For `put` and `promote`:

1. create a unique temporary file in the destination directory;
2. stream content, flush, and `fsync` the file;
3. verify SHA-256;
4. use an atomic same-filesystem no-overwrite link/publish primitive;
5. `fsync` the parent directory;
6. remove the temporary name and `fsync` the directory again.

On `EEXIST`, hash the destination. Return its stat only when content matches; otherwise
raise `Conflict`. Never use `os.replace` for immutable publication.

Use an adapter-owned recoverable temporary-name namespace containing the destination-key
digest and a random writer token. Serialize cleanup with the same destination lock used
for publication and never sweep arbitrary hidden files. Classify recovery explicitly:

- when a valid destination and temporary share an inode after a post-link crash, preserve
  the final name and unlink only the temporary name;
- when no destination exists, remove an abandoned pre-publication temporary only after the
  destination lock proves no writer is active, then retry normally;
- when a different valid destination exists, remove the losing temporary under that lock
  without touching the destination.

Add fault injection after temporary creation, file `fsync`, atomic link, first parent
`fsync`, temporary unlink, and final parent `fsync`. Reopen after every injected crash and
prove a complete destination is preserved or the operation can be retried safely.

Add barrier-controlled races for identical writers and conflicting writers. Assert one
complete destination, correct hash, no partial reads, and no orphan adapter temporary
files after recovery.

**Step 4: Implement destination-conditional S3 writes**

Use `PutObject` with `IfNoneMatch="*"` for final destinations. Store expected SHA-256 in
metadata. Handle:

- 412 by rereading destination metadata/body and deciding idempotent success versus
  conflict;
- 409 `ConditionalRequestConflict` with a bounded reread/retry;
- digest mismatch without creating a destination;
- `promote` through conditional destination upload rather than an unconditional copy.

When retention headers are configured, include `Content-MD5` or an SDK checksum. Use a
botocore Stubber to assert headers, validate the returned checksum, and simulate
409/412, `BadDigest`, and absent/mismatched checksum responses because Moto coverage alone
is not sufficient.

**Step 5: Record the WORM boundary**

Add tests that prove the request is constructed for Object Lock configuration, but mark
retention enforcement as requiring real AWS. Do not add a test name, docstring, or release
note claiming Moto proves WORM.

**Step 6: Verify**

```bash
uv run pytest packages/cnes_infra/tests/object_store -q
uv run ruff check packages/cnes_infra/src/cnes_infra/object_store \
  packages/cnes_infra/tests/object_store
```

Expected: PASS.

**Step 7: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/object_store/_common.py \
  packages/cnes_infra/src/cnes_infra/object_store/filesystem.py \
  packages/cnes_infra/src/cnes_infra/object_store/s3.py \
  packages/cnes_infra/tests/object_store/test_filesystem.py \
  packages/cnes_infra/tests/object_store/test_s3.py
git commit -m "feat(object-store): add filesystem and s3 adapters"
```

**Acceptance criteria:**

- [ ] Both adapters pass all shared object-store cases.
- [ ] Filesystem same-key races cannot overwrite or expose partial bytes.
- [ ] Every filesystem durable-boundary fault is recoverable without deleting unrelated
  files or leaving adapter temporary files.
- [ ] S3 409 and 412 paths are both tested.
- [ ] Object Lock uploads include a required checksum when enabled; response checksum,
  missing-checksum, mismatch, and `BadDigest` paths are tested.
- [ ] Phase 2 makes no real-WORM claim.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 5: CND-023 — Local and S3 Object Lock audit sinks

**Branch:** `feat/cnd-023-audit-sinks`  
**Lane:** first feature lane freed after a W7 merge  
**Depends on:** CND-019  
**Blocks:** CND-024, CND-025

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/audit/local_sink.py`
- Create: `packages/cnes_infra/src/cnes_infra/audit/s3_object_lock_sink.py`
- Create: `packages/cnes_infra/tests/contracts/audit_sink_contract.py`
- Create: `packages/cnes_infra/tests/audit/test_local_sink.py`
- Create: `packages/cnes_infra/tests/audit/test_s3_object_lock_sink.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/audit/__init__.py`

`tests/contracts/audit_sink_contract.py` is an additive hardening extension to governing
Task 13. It gives both approved sinks one backend-neutral case set, is exclusively owned by
CND-023, and changes no public runtime path or constructor.

**Step 1: Define backend-neutral audit cases and prove RED**

The new shared cases require stable canonical serialization, preservation of event identity
and tenant/aggregate metadata, idempotent replay, deterministic keys/order, and propagation
of permanent backend errors. Preserve the approved constructors
`LocalAuditSink(root: Path, parquet_batch_size: int = 1000)` and
`S3ObjectLockAuditSink(client, bucket: str, retention_days: int)`.

```bash
uv run pytest packages/cnes_infra/tests/audit -q
```

Expected: FAIL because the sink modules are absent.

**Step 2: Implement the recoverable local sink**

Append canonical newline-terminated JSON under a process-safe lock. Flush and `fsync` JSONL
before inserting `event_id`, byte offset, length, and digest into a SQLite index transaction.
Use daily paths `audit/<tenant>/<yyyy>/<mm>/<dd>/events.jsonl`. On startup:

- scan from the last indexed offset;
- truncate an incomplete final line;
- reject a complete but invalid record without silently skipping it;
- backfill valid unindexed records;
- make a replayed indexed `event_id` idempotent.

Add fault injection after file write, after file `fsync`, before index commit, and after
index commit. Reopen the sink after every injected crash and prove no complete event is lost
and no malformed/duplicate local record is appended.

Materialize immutable Polars Parquet batches only from indexed complete records. A batch
commit never deletes or truncates valid JSONL; replaying batch materialization produces the
same batch identity and content.

**Step 3: Implement the S3 Object Lock sink**

At initialization, require `GetObjectLockConfiguration` to report Object Lock enabled. Use
the deterministic key `audit/<tenant>/<yyyy>/<mm>/<dd>/<event_id>.json`. Upload one canonical
event with:

- `IfNoneMatch="*"`;
- `ObjectLockMode="COMPLIANCE"`;
- a UTC retain-until date equal to `event.created_at + retention_days`;
- SHA-256 metadata;
- an explicit base64 `ChecksumSHA256` of the body.

Compare the returned checksum with the request digest before returning success. Treat
`BadDigest`, an absent/mismatched checksum, and a disabled Object Lock configuration as
failures. On 412, compare the existing object digest and return only same-content
idempotent success. On 409 `ConditionalRequestConflict`, perform a bounded reread/retry and
never fall back to unconditional overwrite.

Use botocore Stubber for exact request/response and 409/412/`BadDigest` cases. Moto may
exercise basic S3 behavior, but the test name and assertion must say request/capability
conformance rather than WORM enforcement.

**Step 4: Verify**

```bash
uv run pytest packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit -q
uv run ruff check packages/cnes_infra/src/cnes_infra/audit/local_sink.py \
  packages/cnes_infra/src/cnes_infra/audit/s3_object_lock_sink.py \
  packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/audit/local_sink.py \
  packages/cnes_infra/src/cnes_infra/audit/s3_object_lock_sink.py \
  packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit/test_local_sink.py \
  packages/cnes_infra/tests/audit/test_s3_object_lock_sink.py
git commit -m "feat(audit): add local and object lock sinks"
```

**Acceptance criteria:**

- [ ] Both sinks pass the shared audit cases.
- [ ] Local crash-window recovery, partial-tail truncation, and idempotent Parquet batching
  are tested.
- [ ] S3 Object Lock request, checksum response, 409, 412, and `BadDigest` paths are tested.
- [ ] Stable `event_id` and at-least-once semantics are explicit.
- [ ] No emulator result is described as proof of WORM enforcement.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 6: CND-024 — Outbox dispatcher and recovery

**Branch:** `feat/cnd-024-outbox-dispatcher`  
**Lane:** feature  
**Depends on:** CND-020, CND-021, CND-023  
**Blocks:** CND-025  
**Independent of:** CND-022

**Files:**

- Create: `packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py`
- Create: `packages/cnes_domain/tests/test_outbox_dispatcher.py`
- Do not modify infrastructure modules, shared package exports, or profile wiring.

**Step 1: Add port-only recovery tests and prove RED**

Use deterministic in-memory `ControlPlanePort` and `AuditSinkPort` fakes. This issue proves
the domain service independently of backend imports; the real adapter matrix belongs to
CND-025.

```bash
uv run pytest packages/cnes_domain/tests/test_outbox_dispatcher.py -q
```

Expected: FAIL because `cnes_domain.outbox_dispatcher` does not exist.

**Step 2: Implement one bounded dispatch cycle**

Implement `DispatchResult(delivered: int, failed: int)` and
`dispatch_once(control_plane: ControlPlanePort, sink: AuditSinkPort, now: datetime,
limit: int = 100) -> DispatchResult`. One cycle:

1. calls `pending_outbox(limit)`;
2. visits events in returned order;
3. calls `sink.append(event)`;
4. calls `mark_outbox_delivered(event_id, now)` only after append succeeds;
5. catches and counts a sink or mark-delivered error without mutating canonical event data
   and continues to later events;
6. returns delivered and failed counts.

Reject `limit < 1`. Do not add a hidden persistence channel, backend import, or bypass of
the accepted ports.

**Step 3: Prove failure and replay semantics**

Add tests for:

- empty queue and bounded batch ordering;
- sink failure leaving the event pending;
- mark-delivered failure after append causing a later duplicate append with the same
  `event_id`;
- restart/retry eventually marking delivery;
- one tenant's failure not mutating another tenant's event;
- a failure on one event not preventing later events from being attempted.

The expected attempt guarantee is at least once. The local index and deterministic S3 key
make duplicate attempts idempotent, but the dispatcher does not claim exactly once.

**Step 4: Verify**

```bash
uv run pytest packages/cnes_domain/tests/test_outbox_dispatcher.py \
  --cov=cnes_domain.outbox_dispatcher --cov-branch --cov-fail-under=100 -q
uv run ruff check packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py \
  packages/cnes_domain/tests/test_outbox_dispatcher.py
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py \
  packages/cnes_domain/tests/test_outbox_dispatcher.py
git commit -m "feat(domain): dispatch control plane outbox"
```

**Acceptance criteria:**

- [ ] The dispatcher depends only on accepted domain ports and values.
- [ ] Delivery is marked only after successful append.
- [ ] Crash and replay tests prove at-least-once behavior without loss.
- [ ] CND-022 is not introduced as a dependency.
- [ ] No infrastructure, shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 7: CND-025 — Phase 2 adapter conformance gate

**Branch:** `test/cnd-025-adapter-conformance`  
**Lane:** controller; no feature worktree remains active during final integration  
**Depends on:** CND-020, CND-021, CND-022, CND-023, CND-024  
**Blocks:** Phase 3 materialization

**Files:**

- Create: `tests/integration/test_local_adapter_matrix.py`
- Create: `tests/integration/test_aws_adapter_matrix.py`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/object_store/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/audit/__init__.py`
- Modify: `pyproject.toml` (register `local_profile`, `dynamodb_local`, `s3_integration`)
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/python-quality.yml`
- Modify: `docker-compose.yml`
- Create: `scripts/ci_phase2_adapters.sh`
- Do not modify: `uv.lock` (the Python dependencies are already locked)
- Update after merge: #93 and #97 with issue links and CI evidence

**Step 1: Rebase the controller on integrated dependencies**

Confirm `develop` contains the merge commits for CND-020 through CND-024 and is green.
Create the branch only then. Do not merge feature branches into this worktree manually.

**Step 2: Create public exports once**

Export the concrete control planes, object stores, and audit sinks from the three new
subpackage `__init__.py` files and the existing root `cnes_infra/__init__.py`. Keep
`dispatch_once` in `cnes_domain.outbox_dispatcher`; do not re-export it from infrastructure.
Add import-smoke assertions to both integration tests without changing Phase 1 ports.

**Step 3: Pin the integration emulators and one Compose lifecycle**

Add the `aws-test` profile to `docker-compose.yml` with these exact multi-platform image
references:

```text
amazon/dynamodb-local:3.3.1@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab
localstack/localstack:2026.08.0@sha256:21fe0a67fe7993a5b0082a29bfc94fce5a15b6622c87b0d98f9df2882a9afca3
```

Expose DynamoDB Local at `http://localhost:18000` and LocalStack S3 at
`http://localhost:4566`, with region `us-east-1`, test credentials, and bucket
`cnesdata-test`. Create the audit bucket with Object Lock enabled. Record both image
references in test output. Create the DynamoDB table/GSIs from the adapter-owned schema
used by Moto tests. These services are test-only and never dependencies of `PROFILE=local`.

LocalStack verifies adapter wiring and basic capability, not AWS conditional-write
atomicity or retention enforcement. Moto remains the locked fast double; a real-AWS WORM
and concurrency gate remains deferred.

Create executable `scripts/ci_phase2_adapters.sh` as the single local/CI lifecycle for the
two emulators and the matrix:

```bash
#!/usr/bin/env bash
set -euo pipefail

phase2_project="${PHASE2_COMPOSE_PROJECT:-cnesdata}"

phase2_cleanup() {
  phase2_status=$?
  trap - EXIT
  docker compose -p "$phase2_project" --profile aws-test \
    down -v --remove-orphans || true
  exit "$phase2_status"
}
trap 'exit 130' INT
trap 'exit 143' TERM
trap phase2_cleanup EXIT

docker compose -p "$phase2_project" --profile aws-test config --images
docker compose -p "$phase2_project" --profile aws-test up -d --wait \
  dynamodb-local aws-emulator
uv run pytest tests/integration/test_local_adapter_matrix.py \
  tests/integration/test_aws_adapter_matrix.py -q
```

Run `bash -n scripts/ci_phase2_adapters.sh`. `config --images` records the resolved pinned
images in local and workflow logs. The `EXIT` trap must preserve the test exit status and
tear down the same named Compose project on success, failure, or interruption.

**Step 4: Run the integrated matrix**

`test_local_adapter_matrix.py` must cover SQLite, filesystem, local audit JSONL/Parquet,
`dispatch_once`, and restart preservation. `test_aws_adapter_matrix.py` must cover DynamoDB
Local, LocalStack S3 object storage, the S3 Object Lock audit request/capability path, and
outbox replay. Together they cover:

- every shared control-plane case against SQLite and DynamoDB Local;
- DynamoDB transaction-boundary and stale-GSI cases;
- every shared object-store case against filesystem and the S3-compatible test endpoint;
- filesystem races and S3 409/412 request/error injection;
- local audit crash recovery/Parquet replay and S3 audit checksum/idempotency behavior;
- outbox dispatch across both control planes and both audit sinks, including two
  dispatchers racing without event loss;
- process restart with pending outbox, leases, idempotency, and dataset pointers preserved;
- explicit capability assertions showing Object Lock/WORM remains unverified.

Run:

```bash
PHASE2_COMPOSE_PROJECT=cnesdata bash scripts/ci_phase2_adapters.sh
```

Expected: PASS.

**Step 5: Run repository gates**

Use `scripts/ci_phase2_adapters.sh` from both workflow surfaces without duplicating its
commands:

- append a Phase 2 matrix step to `.github/workflows/ci.yml` after the CND-019 gate. This
  workflow already runs for pull requests and pushes to `develop`; add `tests/**`,
  `docker-compose.yml`, and the Phase 2 script to both path filters;
- add a `phase2-adapters` job to `.github/workflows/python-quality.yml` for only `schedule`
  and `workflow_dispatch` events. It uses `astral-sh/setup-uv@v8.0.0` with uv 0.9.26, runs
  `uv sync --locked --all-packages`, and calls the same script. Do not enable the existing
  PR-context-dependent quality jobs on push.

This makes the PR, exact integrated `develop` push, scheduled run, and manual run execute
one versioned matrix with unconditional teardown. Locally, run the generic gate and
baseline before the matrix so its cleanup may safely stop the named Compose project:

```bash
docker compose -p cnesdata up -d --wait postgres
trap 'docker compose -p cnesdata --profile aws-test down -v --remove-orphans' EXIT
DB_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
PG_TEST_URL=postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test \
COMPETENCIA_ANO=2026 COMPETENCIA_MES=1 DISABLE_PANDERA_IMPORT_WARNING=True \
COD_MUN_IBGE=354130 ID_MUNICIPIO_IBGE7=3541308 \
CNPJ_MANTENEDORA=55293427000117 bash scripts/ci_python_gate.sh
uv run python scripts/baseline_matrix.py --output /tmp/phase2-final-baseline.json
PHASE2_COMPOSE_PROJECT=cnesdata bash scripts/ci_phase2_adapters.sh
```

Expected: all commands exit 0. Compare the generated baseline with the CND-019 artifact and
investigate every unexpected regression; do not broaden a waiver.

**Step 6: Commit and open the gate PR**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/__init__.py \
  packages/cnes_infra/src/cnes_infra/object_store/__init__.py \
  packages/cnes_infra/src/cnes_infra/audit/__init__.py \
  packages/cnes_infra/src/cnes_infra/__init__.py \
  tests/integration/test_local_adapter_matrix.py \
  tests/integration/test_aws_adapter_matrix.py \
  scripts/ci_phase2_adapters.sh pyproject.toml \
  .github/workflows/ci.yml .github/workflows/python-quality.yml docker-compose.yml
git commit -m "test(infra): gate phase 2 adapter conformance"
```

The issue is complete only after the PR and integrated `develop` runs are green. The
integrated CI run must contain the Phase 2 matrix step and its `head_sha` must equal the
exact CND-025 merge commit. Record both run URLs in CND-025, then check Phase 2 complete in
#93 and #97.

**Acceptance criteria:**

- [ ] All Phase 2 logical dependencies are present in `develop` before branch creation.
- [ ] Shared exports are created once with no add/add conflicts.
- [ ] DynamoDB Local 3.3.1 and LocalStack 2026.08.0 use pinned multi-platform index
  digests.
- [ ] The full adapter and outbox matrix passes.
- [ ] The same versioned matrix runs on the PR and exact integrated `develop` SHA.
- [ ] One named Compose project is torn down on success and failure.
- [ ] Baseline and CI are green without a broadened waiver.
- [ ] LocalStack results are labeled adapter/capability evidence, not AWS atomicity or WORM
  evidence.
- [ ] No WORM, exactly-once, or cross-host SQLite claim is made.
- [ ] #93 and #97 contain assigned issue links and final green evidence.

## Final dependency and evidence checklist

| Item | May branch when | Merge evidence required |
|---|---|---|
| CND-019 | Documentation amendment merged | Green PR CI and green integrated `develop` CI |
| CND-020 | CND-019 evidence recorded | Focused SQLite suite and green PR CI |
| CND-021 | CND-019 evidence recorded | Focused DynamoDB suite and green PR CI |
| CND-022 | CND-019 evidence recorded | Object-store suite and green PR CI |
| CND-023 | CND-019 evidence recorded and one lane freed | Audit suite and green PR CI |
| CND-024 | CND-020, CND-021, CND-023 merged green | Domain dispatcher suite and green PR CI |
| CND-025 | CND-020 through CND-024 merged green | Full matrix on PR and exact integrated `develop` SHA |

Phase 3 issues remain unmaterialized until CND-025 closes this checklist.
