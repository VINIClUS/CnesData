# CnesData Phase 2 Readiness and Adapter Hardening Implementation Plan

> **For Codex:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this
> plan task by task, with a review checkpoint after every logical issue.

**Goal:** Restore a green integration base and deliver conformant SQLite, DynamoDB,
filesystem, S3, local-audit, CloudWatch, and outbox adapters without exceeding three
parallel feature worktrees.

**Architecture:** The accepted Phase 1 domain, request values, ports, and reusable contract
cases remain the source of truth. CND-019 is a serial readiness gate. CND-020 through
CND-023 add backend leaf modules in parallel. CND-024 connects the existing outbox methods
to audit sinks. CND-025 owns shared exports, emulator wiring, the integrated matrix, and the
final Phase 2 gate.

**Tech stack:** Python 3.13, Pydantic domain models, `sqlite3`, boto3, DynamoDB Local, Moto,
botocore Stubber, pytest, Hypothesis, Ruff, GitHub Actions.

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
- the report contains the normalized failure signature used for the decision.

Run:

```bash
uv run pytest scripts/baseline_matrix_test.py -q
```

Expected: FAIL because `SuiteResult` and `_waiver_for` do not inspect output.

**Step 3: Harden the fingerprint**

Capture combined stdout/stderr in `SuiteResult`, normalize only unstable absolute workspace
prefixes and duration lines, and compare the complete approved exception type, missing
module name, and affected performance-suite set. Keep commit SHA, suite name, and exit code
checks. Persist the normalized signature, not the full test log, in the report.

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

**Step 5: Align Ruff in CI and local execution**

Use the lock-resolved Ruff 0.15.11 in CI instead of the bare `ruff` install that resolved
0.16.5 in runs 33168644394 and 33168995825. Pin `ruff==0.15.11` in
`.github/workflows/ci.yml`. In the same installer command, add the Phase 2 test dependencies
already present in `uv.lock`: `moto[dynamodb,s3]==5.2.3` and
`testcontainers[postgres,minio]==4.14.2`. Do not regenerate the lockfile.

Do not add a global `PLR0917` ignore. A later Ruff upgrade is a separate reviewed change
that either refactors the exact call sites or adds path-scoped compatibility entries.

Run:

```bash
uv run ruff --version
uv run ruff check .
```

Expected: Ruff 0.15.11 and exit 0.

**Step 6: Commit the readiness implementation**

Commit the code and CI changes before recording the baseline so `_commit_sha()` identifies
the exact reviewed implementation rather than the old base with a dirty worktree.

```bash
git add .github/workflows/ci.yml scripts/baseline_matrix.py \
  scripts/baseline_matrix_test.py \
  tests/perf/macro/test_data_processor_e2e.py \
  tests/perf/micro/test_upsert_bench.py \
  tests/perf/soak/test_upsert_soak.py \
  tests/perf/spike/test_upsert_spike.py \
  tests/perf/stress/test_upsert_stress.py
git commit -m "fix(ci): establish phase 2 readiness gate"
```

**Step 7: Run and record the readiness matrix**

Run:

```bash
uv run python scripts/baseline_matrix.py --output docs/baselines/2026-08-28-phase2-ready.json
```

Expected: exit 0. Any retained waiver must match the hardened historical fingerprint; no
new waiver may cover a Phase 2 failure.

Then run the repository CI-equivalent commands from `.github/workflows/ci.yml`, including
schema drift, lint, migrations, package coverage, and app coverage.

Commit the report separately. Its `commit_sha` must equal the parent readiness commit from
Step 6 and the worktree must otherwise be clean.

```bash
git add docs/baselines/2026-08-28-phase2-ready.json
git commit -m "test(baseline): record phase 2 readiness"
```

**Step 8: Open and integrate the PR**

Open a PR to `develop`. The issue is complete only after the PR run and the push run for the
integrated `develop` commit are successful and linked in the issue.

**Acceptance criteria:**

- [ ] CI and local Ruff use the same reviewed version.
- [ ] CI installs Moto 5.2.3 and Testcontainers 4.14.2 for the feature lanes.
- [ ] `ruff check .` is green without a global `PLR0917` suppression.
- [ ] The fast suite collects with explicit legacy performance skips.
- [ ] A same-exit-code/different-failure test proves the waiver is rejected.
- [ ] A fresh baseline report is committed.
- [ ] PR CI and integrated-`develop` CI are green and linked.

---

### Task 2: CND-020 — SQLite control-plane adapter

**Branch:** `feat/cnd-020-sqlite-control-plane`  
**Lane:** feature  
**Depends on:** CND-019  
**Blocks:** CND-024, CND-025

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite.py`
- Create: `packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`

**Step 1: Create the contract fixture and prove RED**

Build a temporary-database fixture with a `MutableClock`. Parametrize
`control_plane_cases()` against the SQLite adapter, keeping the case name in the pytest ID.

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py -q
```

Expected: FAIL because `cnes_infra.control_plane.sqlite` does not exist.

**Step 2: Create schema and transaction shell**

Implement the adapter in `sqlite.py` with:

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
uv run pytest packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py -q
uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane/sqlite.py \
  packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/sqlite.py \
  packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py
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

- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb.py`
- Create: `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`

**Step 1: Create a Moto fixture and prove RED**

Create the Phase 2 table with all base keys, GSIs, and TTL attribute used by the adapter.
Parametrize `control_plane_cases()` against it.

```bash
uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py -q
```

Expected: FAIL because `cnes_infra.control_plane.dynamodb` does not exist.

**Step 2: Implement the single-table mapping**

Use tenant-prefixed base keys and explicit entity discriminators. Provide query access for:

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
uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py -q
uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane/dynamodb.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py
```

Expected: PASS under the lock-resolved Moto version. Real integration against DynamoDB
Local is deferred to CND-025.

**Step 6: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/dynamodb.py \
  packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py
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

Add barrier-controlled races for identical writers and conflicting writers. Assert one
complete destination, correct hash, no partial reads, and no orphan temporary files.

**Step 4: Implement destination-conditional S3 writes**

Use `PutObject` with `IfNoneMatch="*"` for final destinations. Store expected SHA-256 in
metadata. Handle:

- 412 by rereading destination metadata/body and deciding idempotent success versus
  conflict;
- 409 `ConditionalRequestConflict` with a bounded reread/retry;
- digest mismatch without creating a destination;
- `promote` through conditional destination upload rather than an unconditional copy.

When retention headers are configured, include `Content-MD5` or an SDK checksum. Use a
botocore Stubber to assert headers and to simulate 409/412 because Moto coverage alone is
not sufficient.

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
- [ ] S3 409 and 412 paths are both tested.
- [ ] Object Lock uploads include a required checksum when enabled.
- [ ] Phase 2 makes no real-WORM claim.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 5: CND-023 — Local and CloudWatch audit sinks

**Branch:** `feat/cnd-023-audit-sinks`  
**Lane:** first feature lane freed after a W7 merge  
**Depends on:** CND-019  
**Blocks:** CND-024, CND-025

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/audit/local.py`
- Create: `packages/cnes_infra/src/cnes_infra/audit/cloudwatch.py`
- Create: `packages/cnes_infra/tests/contracts/audit_sink_contract.py`
- Create: `packages/cnes_infra/tests/audit/test_local_sink.py`
- Create: `packages/cnes_infra/tests/audit/test_cloudwatch_sink.py`
- Do not create: `packages/cnes_infra/src/cnes_infra/audit/__init__.py`

**Step 1: Define backend-neutral audit cases and prove RED**

The new shared cases require stable canonical serialization, preservation of event identity
and tenant/aggregate metadata, replay behavior, ordering within one sink, and propagation of
permanent backend errors.

```bash
uv run pytest packages/cnes_infra/tests/audit -q
```

Expected: FAIL because the sink modules are absent.

**Step 2: Implement the recoverable local sink**

Append canonical newline-terminated JSON under a process-safe lock. Flush and `fsync` JSONL
before inserting `event_id`, byte offset, length, and digest into a SQLite index transaction.
On startup:

- scan from the last indexed offset;
- truncate an incomplete final line;
- reject a complete but invalid record without silently skipping it;
- backfill valid unindexed records;
- make a replayed indexed `event_id` idempotent.

Add fault injection after file write, after file `fsync`, before index commit, and after
index commit. Reopen the sink after every injected crash and prove no complete event is lost
and no malformed/duplicate local record is appended.

**Step 3: Implement the CloudWatch sink**

Send one `PutLogEvents` event synchronously for each `append` call while retaining stable
`event_id` in the canonical JSON message. Validate CloudWatch message and request limits
before the call. Do not buffer events behind a successful `append`, because the dispatcher
would then mark an event delivered before CloudWatch accepted it. Use bounded retry for
throttling and service failures; do not retry validation failures. Tests use Stubber
responses for success, throttling, ambiguous failure, and permanent rejection.

Document and test at-least-once semantics: an ambiguous retry may create duplicate
CloudWatch events, and consumers deduplicate by `event_id`.

**Step 4: Verify**

```bash
uv run pytest packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit -q
uv run ruff check packages/cnes_infra/src/cnes_infra/audit/local.py \
  packages/cnes_infra/src/cnes_infra/audit/cloudwatch.py \
  packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/audit/local.py \
  packages/cnes_infra/src/cnes_infra/audit/cloudwatch.py \
  packages/cnes_infra/tests/contracts/audit_sink_contract.py \
  packages/cnes_infra/tests/audit
git commit -m "feat(audit): add local and cloudwatch sinks"
```

**Acceptance criteria:**

- [ ] Both sinks pass the shared audit cases.
- [ ] Local crash-window recovery and partial-tail truncation are tested.
- [ ] CloudWatch retryable and permanent errors are distinguished.
- [ ] Stable `event_id` and at-least-once semantics are explicit.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 6: CND-024 — Transactional outbox dispatcher

**Branch:** `feat/cnd-024-outbox-dispatcher`  
**Lane:** feature  
**Depends on:** CND-020, CND-021, CND-023  
**Blocks:** CND-025  
**Independent of:** CND-022

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/audit/outbox.py`
- Create: `packages/cnes_infra/tests/audit/test_outbox_dispatcher.py`
- Do not modify shared package exports or profile wiring.

**Step 1: Add the four-backend matrix and prove RED**

Parametrize dispatcher behavior over SQLite and DynamoDB control planes and local and
CloudWatch audit sinks. Use deterministic fakes for sink faults in addition to real sink
fixtures.

```bash
uv run pytest packages/cnes_infra/tests/audit/test_outbox_dispatcher.py -q
```

Expected: FAIL because `cnes_infra.audit.outbox` does not exist.

**Step 2: Implement one bounded dispatch cycle**

The dispatcher receives `ControlPlanePort`, `AuditSinkPort`, clock, and batch size. One cycle:

1. calls `pending_outbox(limit)`;
2. visits events in returned order;
3. calls `sink.append(event)`;
4. calls `mark_outbox_delivered(event_id, clock.now())` only after append succeeds;
5. stops or records a deterministic failure according to the configured error policy;
6. returns counts for attempted, delivered, and failed events.

Do not add a hidden second persistence channel or bypass the accepted ports.

**Step 3: Prove failure and replay semantics**

Add tests for:

- empty queue and bounded batch ordering;
- sink failure leaving the event pending;
- mark-delivered failure after append causing a later duplicate append with the same
  `event_id`;
- restart/retry eventually marking delivery;
- two dispatcher instances racing without losing an event;
- one tenant's failure not mutating another tenant's event;
- both control-plane adapters and both sinks.

The expected guarantee is at least once. Do not assert exactly once for CloudWatch.

**Step 4: Verify**

```bash
uv run pytest packages/cnes_infra/tests/audit/test_outbox_dispatcher.py -q
uv run ruff check packages/cnes_infra/src/cnes_infra/audit/outbox.py \
  packages/cnes_infra/tests/audit/test_outbox_dispatcher.py
```

Expected: PASS.

**Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/audit/outbox.py \
  packages/cnes_infra/tests/audit/test_outbox_dispatcher.py
git commit -m "feat(audit): dispatch transactional outbox"
```

**Acceptance criteria:**

- [ ] All four control-plane/audit combinations pass.
- [ ] Delivery is marked only after successful append.
- [ ] Crash and replay tests prove at-least-once behavior without loss.
- [ ] CND-022 is not introduced as a dependency.
- [ ] No shared export, manifest, lock, CI, or Compose file is touched.

---

### Task 7: CND-025 — Phase 2 adapter conformance gate

**Branch:** `test/cnd-025-adapter-conformance`  
**Lane:** controller; no feature worktree remains active during final integration  
**Depends on:** CND-020, CND-021, CND-022, CND-023, CND-024  
**Blocks:** Phase 3 materialization

**Files:**

- Create: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/object_store/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/audit/__init__.py`
- Create: `packages/cnes_infra/tests/integration/conftest.py`
- Create: `packages/cnes_infra/tests/integration/test_phase2_adapter_matrix.py`
- Do not modify: `docker-compose.yml`, `.github/workflows/ci.yml`, `pyproject.toml`, `uv.lock`
- Update after merge: #93 and #97 with issue links and CI evidence

**Step 1: Rebase the controller on integrated dependencies**

Confirm `develop` contains the merge commits for CND-020 through CND-024 and is green.
Create the branch only then. Do not merge feature branches into this worktree manually.

**Step 2: Create public exports once**

Export the concrete adapters and dispatcher from the three new `__init__.py` files without
changing the Phase 1 port definitions. Add import-smoke assertions to the integration test.

**Step 3: Pin the integration emulator**

In `packages/cnes_infra/tests/integration/conftest.py`, start this exact verified-publisher
image through the already locked Testcontainers dependency:

```text
amazon/dynamodb-local:3.3.1@sha256:ff89bd48ff32cd8d9be5fee8873b65b8854dc408f1afe881be6eb00247bc0dab
```

Record the image reference in test output. Create the table and GSIs from the same
adapter-owned schema description used by Moto tests. Do not use `latest` and do not modify
Compose for this test-only service.

Moto remains lockfile-pinned for fast tests. Do not add LocalStack solely to simulate S3
Object Lock; its retention behavior is not Phase 2 acceptance evidence.

**Step 4: Run the integrated matrix**

`test_phase2_adapter_matrix.py` must cover:

- every shared control-plane case against SQLite and DynamoDB Local;
- DynamoDB transaction-boundary and stale-GSI cases;
- every shared object-store case against filesystem and Moto S3;
- filesystem races and S3 409/412 request/error injection;
- local audit crash recovery and CloudWatch Stubber retry behavior;
- outbox dispatch across both control planes and both sinks;
- process restart with pending outbox, leases, idempotency, and dataset pointers preserved;
- explicit capability assertions showing Object Lock/WORM remains unverified.

Run:

```bash
uv run pytest packages/cnes_infra/tests/contracts \
  packages/cnes_infra/tests/control_plane \
  packages/cnes_infra/tests/object_store \
  packages/cnes_infra/tests/audit \
  packages/cnes_infra/tests/integration/test_phase2_adapter_matrix.py -q
```

Expected: PASS.

**Step 5: Run repository gates**

Run the exact current CI commands from `.github/workflows/ci.yml`, then:

```bash
uv run ruff check .
uv run python scripts/baseline_matrix.py --output /tmp/phase2-final-baseline.json
```

Expected: all commands exit 0. Compare the generated baseline with the CND-019 artifact and
investigate every unexpected regression; do not broaden a waiver.

**Step 6: Commit and open the gate PR**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/__init__.py \
  packages/cnes_infra/src/cnes_infra/object_store/__init__.py \
  packages/cnes_infra/src/cnes_infra/audit/__init__.py \
  packages/cnes_infra/tests/integration/conftest.py \
  packages/cnes_infra/tests/integration/test_phase2_adapter_matrix.py
git commit -m "test(infra): gate phase 2 adapter conformance"
```

The issue is complete only after the PR and integrated `develop` runs are green. Record the
run URLs in CND-025, then check Phase 2 complete in #93 and #97.

**Acceptance criteria:**

- [ ] All Phase 2 logical dependencies are present in `develop` before branch creation.
- [ ] Shared exports are created once with no add/add conflicts.
- [ ] DynamoDB Local uses the pinned 3.3.1 multi-platform index digest.
- [ ] The full adapter and outbox matrix passes.
- [ ] Baseline and CI are green without a broadened waiver.
- [ ] Capability limits are documented and no WORM/exactly-once/cross-host SQLite claim is
  made.
- [ ] #93 and #97 contain assigned issue links and final green evidence.

## Final dependency and evidence checklist

| Item | May branch when | Merge evidence required |
|---|---|---|
| CND-019 | Documentation amendment merged | Green PR CI and green integrated `develop` CI |
| CND-020 | CND-019 evidence recorded | Focused SQLite suite and green PR CI |
| CND-021 | CND-019 evidence recorded | Focused DynamoDB suite and green PR CI |
| CND-022 | CND-019 evidence recorded | Object-store suite and green PR CI |
| CND-023 | CND-019 evidence recorded and one lane freed | Audit suite and green PR CI |
| CND-024 | CND-020, CND-021, CND-023 merged green | Four-backend dispatcher matrix and green PR CI |
| CND-025 | CND-020 through CND-024 merged green | Full matrix plus green integrated `develop` CI |

Phase 3 issues remain unmaterialized until CND-025 closes this checklist.
