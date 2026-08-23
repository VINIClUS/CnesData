# CnesData Data Plane Foundation and Local Profile Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the local-first CNES vertical slice from immutable raw Parquet through SQLite-backed orchestration, deterministic reconciliation, atomic dataset publication, authorized serving JSON, and the React dashboard without any local runtime dependency on PostgreSQL, MinIO, Keycloak, BigQuery, or AWS.

**Architecture:** Add frozen Pydantic wire contracts in `cnes_contracts`, storage-neutral PEP 544 ports and immutable domain models in `cnes_domain`, and local SQLite/filesystem/audit adapters in `cnes_infra`. The FastAPI API and Polars processor compose those ports only in serial integration tasks; the existing Go Edge Agent evolves in place to emit manifest v1 and honor full-resync responses, while the Bun/React dashboard reads only authorized serving JSON selected by `DatasetPointer`.

**Tech Stack:** Python 3.13, Pydantic 2, PEP 544 Protocols, Polars, FastAPI, stdlib `sqlite3`, Hypothesis/pytest/pytest-cov, Go 1.26.2 with bbolt and parquet-go, React 18, TypeScript strict, Bun 1.3, TanStack Query, Vitest/Playwright, Ruff (`py313`).

**Spec:** `docs/superpowers/specs/2026-08-16-parquet-data-plane-orchestration-design.md`; execution order and ownership: `docs/superpowers/specs/2026-08-23-cnesdata-redesign-execution-design.md`

## Global Constraints

- `local` is single-tenant, one installation per municipality;
- `local` uses SQLite and filesystem storage;
- `aws` is multi-tenant and uses DynamoDB and S3;
- PostgreSQL/RDS, MinIO, Keycloak, and BigQuery are absent after cutover;
- Parquet objects and published dataset versions are immutable;
- dashboard navigation reads materialized serving JSON, not Athena or raw Parquet;
- processing is at-least-once with idempotency, leases, and fencing;
- Edge Agents perform extraction and transport transformations only;
- normalization, reconciliation, and serving materialization are central concerns;
- Stripe is enabled only in the AWS SaaS profile;
- local billing is disabled and has no remote validation;
- critical authorization and publication do not depend on a stale cache, GSI, or TTL;
- SIHD, BPA, and SIA may use the legacy path only during migration;
- the final legacy removal gate requires every retained active source to be migrated.
- Python package coverage: 100% branch where already enforced;
- Python app coverage: 90% line where already enforced;
- Go Edge Agent: race-enabled suite and at least 65% filtered coverage;
- dashboard: lint, typecheck, unit tests, build, and relevant E2E tests;
- function body at most 50 lines;
- cyclomatic complexity at most 10;
- line width at most 100 characters;
- file length at most 500 lines;
- no direct commits to `main`.
- Use base branch `develop`; the current inspected planning baseline is `f1ca71bb4277e9b1354fa11d8997a00871fa6c36` (PR #89, parent `6230b7343172481558d4a76ff35c20bb9c615700`).
- Every feature worktree starts from the latest green `develop` containing all listed dependencies; do not stack a dependent task on an unmerged feature branch.
- Feature tasks must not edit `pyproject.toml`, `uv.lock`, package-wide `__init__.py` exports, application bootstrap/dependency injection, generated OpenAPI/JSON Schema, CI, Compose, or frontend lockfiles. Only the serial integration tasks identified below own those surfaces.
- Keep legacy PostgreSQL/MinIO/BigQuery paths available only behind migration mode; this plan does not delete them and does not implement AWS composition, billing, SIHD/BPA/SIA parity, or final cutover.

---

## Baseline and locked file map

The plan was mapped against the exact GitHub tree at the pinned SHA. Existing files reused in place include `packages/cnes_contracts/src/cnes_contracts/landing.py`, `packages/cnes_domain/src/cnes_domain/ports/object_storage.py`, `packages/cnes_infra/src/cnes_infra/storage/object_storage.py`, `packages/cnes_infra/src/cnes_infra/ingestion/cnes_oficial_web_adapter.py`, `apps/central_api/src/central_api/app.py`, `apps/central_api/src/central_api/deps.py`, `apps/data_processor/src/data_processor/adapters/cnes_local_adapter.py`, `apps/data_processor/src/data_processor/adapters/cnes_nacional_adapter.py`, `apps/dump_agent_go/internal/delta/**`, and `apps/web_dashboard/src/api/**`. Target modules are created beside them; legacy modules are not expanded into the target ports.

### Cross-worktree interface registry

These names and signatures are frozen by Tasks 5–9. Later tasks import them directly from their defining module until a serial integration task adds package exports.

```python
# cnes_domain.control_plane.enums
class JobState(StrEnum):
    PENDING = "PENDING"; LEASED = "LEASED"; SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"; FAILED_FINAL = "FAILED_FINAL"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"; CANCELED = "CANCELED"

class RunState(StrEnum):
    PLANNED = "PLANNED"; WAITING_INPUTS = "WAITING_INPUTS"
    PROCESSING = "PROCESSING"; PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"; PUBLISHED_DEGRADED = "PUBLISHED_DEGRADED"
    FAILED = "FAILED"; CANCEL_REQUESTED = "CANCEL_REQUESTED"
    CANCELED = "CANCELED"

class RunUnitState(StrEnum):
    PENDING = "PENDING"; LEASED = "LEASED"; SUCCEEDED = "SUCCEEDED"
    SUCCEEDED_DEGRADED = "SUCCEEDED_DEGRADED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"; FAILED_FINAL = "FAILED_FINAL"
    CANCELED = "CANCELED"

class RunStage(StrEnum):
    NORMALIZE = "NORMALIZE"; RECONCILE = "RECONCILE"
    MATERIALIZE = "MATERIALIZE"
class DispatchState(StrEnum):
    RESERVED = "RESERVED"; STARTED = "STARTED"; TERMINAL = "TERMINAL"
class DispatchOutcome(StrEnum):
    SUCCEEDED = "SUCCEEDED"; FAILED = "FAILED"; CANCELED = "CANCELED"

class AgentState(StrEnum): ACTIVE = "ACTIVE"; REVOKED = "REVOKED"
class AccessRequestState(StrEnum): PENDING = "PENDING"; APPROVED = "APPROVED"; REJECTED = "REJECTED"

# cnes_domain.control_plane.entities; every model uses
# ConfigDict(frozen=True, extra="forbid") and timezone-aware UTC datetimes.
class Tenant(BaseModel):
    tenant_id: str; municipality_name: str; created_at: datetime
class Membership(BaseModel):
    tenant_id: str; user_id: str; role: str; created_at: datetime
class Agent(BaseModel):
    tenant_id: str; agent_id: str; state: AgentState; version: str
    certificate_fingerprint: str; last_seen_at: datetime | None; created_at: datetime
class Job(BaseModel):
    tenant_id: str; job_id: str; agent_id: str; source_type: str
    file_subtype: str; competencia: str; requested_snapshot_mode: str
    state: JobState; attempt: int; fencing_token: int
    lease_owner: str | None; lease_until: datetime | None
    result_manifest_id: str | None; result_manifest_key: str | None
    error_code: str | None; created_at: datetime
class RunDependency(BaseModel):
    source_type: str; file_subtype: str; required: bool
class Run(BaseModel):
    tenant_id: str; run_id: str; competencia: str; dataset_name: str
    state: RunState; dependencies: tuple[RunDependency, ...]
    missing_sources: tuple[str, ...]; created_at: datetime
class ManifestRef(BaseModel):
    manifest_id: str; manifest_key: str
class RawManifestRecord(BaseModel):
    tenant_id: str; manifest_id: str; manifest_key: str; agent_id: str
    source_type: str; file_subtype: str; competencia: str
    snapshot_mode: str; snapshot_id: str; base_snapshot_id: str | None; sequence: int
    previous_manifest_sha256: str | None; manifest_sha256: str; created_at: datetime
class RunUnit(BaseModel):
    tenant_id: str; run_id: str; unit_id: str
    stage: RunStage; source_type: str | None; file_subtype: str | None
    partition: str; depends_on_unit_ids: tuple[str, ...]
    input_manifests: tuple[ManifestRef, ...]; state: RunUnitState; attempt: int
    fencing_token: int; lease_owner: str | None; lease_until: datetime | None
    dispatch_id: str | None; output_manifests: tuple[ManifestRef, ...]; error_code: str | None
class RunDispatch(BaseModel):
    tenant_id: str; run_id: str; wave_id: str; dispatch_id: str; generation: int
    unit_ids: tuple[str, ...]; state: DispatchState; lease_until: datetime
    execution_ref: str | None = None; terminal_outcome: DispatchOutcome | None = None
class DatasetVersion(BaseModel):
    tenant_id: str; dataset_name: str; version_id: str; run_id: str
    run_manifest_key: str; created_at: datetime
class DatasetPointer(BaseModel):
    tenant_id: str; dataset_name: str; pointer_name: str
    version_id: str; updated_at: datetime
class AccessRequest(BaseModel):
    tenant_id: str; request_id: str; user_id: str
    state: AccessRequestState; decided_by: str | None; decided_at: datetime | None
class IdempotencyRecord(BaseModel):
    tenant_id: str; scope: str; key: str; request_hash: str; status: str
    resource_id: str; created_at: datetime; expires_at: datetime
class OutboxEvent(BaseModel):
    tenant_id: str; event_id: str; event_type: str; aggregate_id: str
    payload: dict[str, JsonValue]; created_at: datetime; delivered_at: datetime | None

# cnes_domain.control_plane.commands
class ClaimJob(BaseModel):
    tenant_id: str; job_id: str; owner: str; now: datetime; lease_seconds: int
class RenewJobLease(BaseModel):
    tenant_id: str; job_id: str; owner: str; fencing_token: int
    now: datetime; lease_seconds: int
class CompleteJob(BaseModel):
    tenant_id: str; job_id: str; owner: str; fencing_token: int
    manifest: RawManifestRecord
class FailJob(BaseModel):
    tenant_id: str; job_id: str; owner: str; fencing_token: int
    error_code: str; retryable: bool
class CancelJob(BaseModel):
    tenant_id: str; job_id: str; requested_by: str
class TransitionRun(BaseModel):
    tenant_id: str; run_id: str; expected_state: RunState
    new_state: RunState; missing_sources: tuple[str, ...] = ()
class PutRunUnits(BaseModel):
    tenant_id: str; run_id: str; expected_run_state: RunState
    units: tuple[RunUnit, ...]
class ClaimRunUnit(BaseModel):
    tenant_id: str; run_id: str; unit_id: str; dispatch_id: str; owner: str
    now: datetime; lease_seconds: int
class CommitRunUnit(BaseModel):
    tenant_id: str; run_id: str; unit_id: str; dispatch_id: str; owner: str
    fencing_token: int; output_manifests: tuple[ManifestRef, ...]
class FailRunUnit(BaseModel):
    tenant_id: str; run_id: str; unit_id: str; dispatch_id: str; owner: str
    fencing_token: int; error_code: str; retryable: bool
class FinalizeRunCancellation(BaseModel):
    tenant_id: str; run_id: str; expected_state: Literal[RunState.CANCEL_REQUESTED]
    canceled_at: datetime
class ReserveRunDispatch(BaseModel):
    tenant_id: str; run_id: str; wave_id: str
    unit_ids: tuple[str, ...]; now: datetime; lease_seconds: int
class BindRunDispatch(BaseModel):
    tenant_id: str; run_id: str; dispatch_id: str; execution_ref: str
    now: datetime; lease_seconds: int
class FinishRunDispatch(BaseModel):
    tenant_id: str; run_id: str; dispatch_id: str
    outcome: DispatchOutcome; finished_at: datetime
class BeginIdempotency(BaseModel):
    tenant_id: str; scope: str; key: str; request_hash: str
    resource_id: str; now: datetime; expires_at: datetime
class IdempotencyOutcome(BaseModel):
    record: IdempotencyRecord; created: bool
class PublicationPermit(BaseModel):
    tenant_id: str; run_id: str; policy_version: int; fencing_token: int
    binding_context: object | None = None
class PublishDataset(BaseModel):
    version: DatasetVersion; pointer_name: str; expected_version_id: str | None
    final_state: RunState; missing_sources: tuple[str, ...]
    publication_permit: PublicationPermit; event: OutboxEvent

# cnes_domain.ports.control_plane
class ControlPlanePort(Protocol):
    def get_tenant(self, tenant_id: str) -> Tenant | None: ...
    def put_tenant(self, tenant: Tenant) -> None: ...
    def get_membership(self, tenant_id: str, user_id: str) -> Membership | None: ...
    def put_membership(self, membership: Membership) -> None: ...
    def get_agent(self, tenant_id: str, agent_id: str) -> Agent | None: ...
    def put_agent(self, agent: Agent) -> None: ...
    def create_job(self, job: Job, event: OutboxEvent) -> Job: ...
    def get_job(self, tenant_id: str, job_id: str) -> Job | None: ...
    def latest_succeeded_job(self, tenant_id: str, agent_id: str,
                             source_type: str, file_subtype: str,
                             competencia: str) -> Job | None: ...
    def list_raw_manifest_chain(self, tenant_id: str, source_type: str,
                                file_subtype: str, competencia: str,
                                limit: int = 31) -> tuple[ManifestRef, ...]: ...
    def list_claimable_jobs(self, tenant_id: str, agent_id: str,
                            limit: int) -> tuple[Job, ...]: ...
    def claim_job(self, command: ClaimJob) -> Job | None: ...
    def renew_job_lease(self, command: RenewJobLease) -> Job: ...
    def complete_job(self, command: CompleteJob, event: OutboxEvent) -> Job: ...
    def fail_job(self, command: FailJob, event: OutboxEvent) -> Job: ...
    def cancel_job(self, command: CancelJob, event: OutboxEvent) -> Job: ...
    def put_run(self, run: Run) -> None: ...
    def get_run(self, tenant_id: str, run_id: str) -> Run | None: ...
    def list_waiting_runs_for_dependency(self, tenant_id: str, source_type: str,
                                         file_subtype: str, competencia: str,
                                         limit: int = 100) -> tuple[Run, ...]: ...
    def list_recoverable_runs(self, now: datetime, limit: int = 100) -> tuple[Run, ...]: ...
    def transition_run(self, command: TransitionRun, event: OutboxEvent) -> Run: ...
    def put_run_units(self, command: PutRunUnits) -> tuple[RunUnit, ...]: ...
    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]: ...
    def claim_run_unit(self, command: ClaimRunUnit) -> RunUnit | None: ...
    def commit_run_unit(self, command: CommitRunUnit, event: OutboxEvent) -> RunUnit: ...
    def fail_run_unit(self, command: FailRunUnit, event: OutboxEvent) -> RunUnit: ...
    def finalize_run_cancellation(self, command: FinalizeRunCancellation,
                                  event: OutboxEvent) -> Run: ...
    def reserve_run_dispatch(self, command: ReserveRunDispatch) -> RunDispatch: ...
    def bind_run_dispatch(self, command: BindRunDispatch) -> RunDispatch: ...
    def finish_run_dispatch(self, command: FinishRunDispatch) -> RunDispatch: ...
    def get_active_run_dispatch(self, tenant_id: str, run_id: str) -> RunDispatch | None: ...
    def begin_idempotency(self, command: BeginIdempotency) -> IdempotencyOutcome: ...
    def publish_dataset(self, command: PublishDataset) -> DatasetPointer: ...
    def get_dataset_pointer(self, tenant_id: str, dataset_name: str) -> DatasetPointer | None: ...
    def get_dataset_version(self, tenant_id: str, dataset_name: str,
                            version_id: str) -> DatasetVersion | None: ...
    def put_access_request(self, request: AccessRequest, event: OutboxEvent) -> None: ...
    def get_access_request(self, tenant_id: str, request_id: str) -> AccessRequest | None: ...
    def decide_access_request(self, request: AccessRequest,
                              event: OutboxEvent) -> AccessRequest: ...
    def pending_outbox(self, limit: int) -> tuple[OutboxEvent, ...]: ...
    def mark_outbox_delivered(self, event_id: str, delivered_at: datetime) -> None: ...

# cnes_domain.ports.object_store
@dataclass(frozen=True, slots=True)
class ObjectStat:
    key: str; size_bytes: int; sha256: str
class ObjectStorePort(Protocol):
    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat: ...
    def open(self, key: str) -> ContextManager[BinaryIO]: ...
    def stat(self, key: str) -> ObjectStat | None: ...
    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat: ...
    def delete(self, key: str) -> None: ...

# cnes_domain.ports.processing and cnes_domain.ports.serving
class StartRunExecution(BaseModel):
    tenant_id: str; run_id: str; wave_id: str
    dispatch_id: str; unit_ids: tuple[str, ...]; max_concurrency: int
class RunUnitMessage(BaseModel):
    tenant_id: str; run_id: str; wave_id: str; dispatch_id: str
    unit_id: str; owner: str; now: datetime; lease_seconds: int
class CancelRunExecution(BaseModel):
    tenant_id: str; run_id: str; execution_ref: str | None
class ProcessorExecutorPort(Protocol):
    def start(self, request: StartRunExecution) -> str: ...
    def cancel(self, request: CancelRunExecution) -> None: ...
    def status(self, execution_ref: str) -> "ExecutionStatus": ...
class ExecutionStatus(StrEnum):
    RUNNING = "RUNNING"; SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"; CANCELED = "CANCELED"
class ExecutionPermit(BaseModel):
    tenant_id: str; run_id: str; max_concurrency: int
    policy_version: int; fencing_token: int; binding_context: object | None = None
ConcurrencyPolicy = Callable[[Run, RunDispatch, int], ExecutionPermit]
ExecutionStarted = Callable[[Run, StartRunExecution, str, ExecutionPermit], None]
@dataclass(frozen=True, slots=True)
class ExecutionCallbacks:
    policy: ConcurrencyPolicy; started: ExecutionStarted
@dataclass(frozen=True, slots=True)
class ExecutionPolicyConfig:
    deployment_limit: int; dispatch_lease_seconds: int; callbacks: ExecutionCallbacks
class ServingRequest(BaseModel):
    user_id: str; tenant_id: str; dataset_name: str
class ServingGrant(BaseModel):
    tenant_id: str; run_id: str; version_id: str; object_keys: tuple[str, ...]
class ServingAccessPort(Protocol):
    def authorize(self, request: ServingRequest) -> ServingGrant: ...
class AuditSinkPort(Protocol):
    def append(self, event: OutboxEvent) -> None: ...

# cnes_contracts.manifests.raw and outputs
class SourceType(StrEnum):
    CNES_LOCAL = "CNES_LOCAL"; CNES_NACIONAL = "CNES_NACIONAL"
    SIHD = "SIHD"; BPA_MAG = "BPA_MAG"; SIA_LOCAL = "SIA_LOCAL"
class SnapshotMode(StrEnum): FULL = "FULL"; DELTA = "DELTA"
class RawManifest(BaseModel):
    manifest_version: Literal[1]; manifest_id: str; tenant_id: str
    source_type: SourceType; file_subtype: str; competencia: str; agent_id: str
    agent_version: str; schema_version: str; snapshot_mode: SnapshotMode
    snapshot_id: str; base_snapshot_id: str | None; sequence: int
    previous_manifest_sha256: str | None; object_sha256: str
    row_count: int; size_bytes: int; object_key: str; created_at: datetime
class OutputManifest(BaseModel):
    manifest_version: Literal[1]; manifest_id: str; tenant_id: str
    layer: Literal["normalized", "reconciliation", "serving"]
    source_type: SourceType | None; competencia: str
    run_id: str; unit_id: str; attempt: int; schema_version: str
    object_key: str; object_sha256: str; row_count: int; created_at: datetime
class RunManifest(BaseModel):
    manifest_version: Literal[1]; tenant_id: str; dataset_name: str
    run_id: str; competencia: str; outputs: tuple[OutputManifest, ...]
    missing_sources: tuple[str, ...]; published_at: datetime
class ServingDocument(BaseModel):
    schema_version: str; document_name: str; tenant_id: str
    run_id: str; generated_at: datetime; payload: dict[str, JsonValue]

# cnes_contracts.manifests.processing
class NormalizeRequest(BaseModel):
    tenant_id: str; run_id: str; unit_id: str; attempt: int
    source_type: SourceType; raw_manifests: tuple[RawManifest, ...]
    target_keys: tuple[str, ...]; normalized_at: datetime
class NormalizeResult(BaseModel):
    manifests: tuple[OutputManifest, ...]
class ReconcileRequest(BaseModel):
    tenant_id: str; competencia: str; run_id: str; unit_id: str; attempt: int
    normalized_manifests: tuple[OutputManifest, ...]
    reconciliation_key: str; divergence_key: str; reconciled_at: datetime
class ReconcileResult(BaseModel):
    reconciliation_manifest: OutputManifest
    divergence_manifest: OutputManifest
    kpis: dict[str, int]
class MaterializeRequest(BaseModel):
    tenant_id: str; competencia: str; run_id: str; unit_id: str; attempt: int
    reconciliation_manifest: OutputManifest
    divergence_manifest: OutputManifest
    missing_sources: tuple[str, ...]; target_keys: tuple[str, ...]; generated_at: datetime
class MaterializeResult(BaseModel):
    manifests: tuple[OutputManifest, ...]
    documents: tuple[ServingDocument, ...]
```

`JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]`
is defined in `cnes_domain.control_plane.entities` for outbox values and with the
same structural alias in `cnes_contracts.manifests.outputs` for wire documents;
the duplication keeps both dependency-independent packages free of a new cycle.
Identifier helpers are frozen in `cnes_domain.control_plane.ids`:

```python
@dataclass(frozen=True, slots=True)
class JobIdentity:
    tenant_id: str; agent_id: str; source_type: str; file_subtype: str
    competencia: str; idempotency_key: str

@dataclass(frozen=True, slots=True)
class RunUnitIdentity:
    run_id: str; stage: RunStage; source_type: str = ""
    file_subtype: str = ""; partition: str = "all"

def job_id(identity: JobIdentity) -> str:
    return sha256("\x1f".join((identity.tenant_id, identity.agent_id,
                              identity.source_type, identity.file_subtype,
                              identity.competencia,
                              identity.idempotency_key)).encode()).hexdigest()[:32]

def unit_id(identity: RunUnitIdentity) -> str:
    return sha256("\x1f".join((identity.run_id, identity.stage.value,
                              identity.source_type, identity.file_subtype,
                              identity.partition)).encode()).hexdigest()[:32]

def run_dependency_key(tenant_id: str, source_type: str, file_subtype: str,
                       competencia: str) -> str:
    return "RUN_DEP#" + "#".join((tenant_id, source_type, file_subtype, competencia))
```

## Phase 0 — serial integration baseline

### Task 1: CND-000 — Reconcile `main` into `develop`

**Files:**
- Create: `tests/negative/test_repository_baseline.py`
- Modify through the merge resolution only: `.env.example`, `README.md`, `docs/architecture.md`, `docs/project-context.md`, `docs/roadmap.md`
- Add through the merge: `docs/development.md`

**Interfaces:**
- Consumes: Git refs `develop@f1ca71bb4277e9b1354fa11d8997a00871fa6c36`, `main@c9dee715d57ddaec453fde8b17989a8addb57e7f`, merge base `9bbab1f345c17e8f464877dec86b0d2737d513c8`. At planning time `develop` is 149 commits ahead and 5 behind `main`.
- Produces: a green reconciliation commit on `develop` containing both histories; no product API changes.

- [ ] **Step 1: Write the failing repository-baseline test**

```python
from pathlib import Path


def test_develop_contem_documentacao_de_desenvolvimento_da_main():
    root = Path(__file__).parents[2]
    assert (root / "docs/development.md").is_file()
    for path in ("README.md", "docs/architecture.md", "docs/project-context.md"):
        text = (root / path).read_text(encoding="utf-8")
        assert "<<<<<<<" not in text and ">>>>>>>" not in text
```

- [ ] **Step 2: Run the test before the merge**

Run: `uv run pytest tests/negative/test_repository_baseline.py -q`

Expected: FAIL because `docs/development.md` is absent on the pinned `develop` tree.

- [ ] **Step 3: Fetch, stage the merge without committing, and resolve the known paths**

Run `git fetch origin main` and then `git merge --no-ff --no-commit origin/main` so the merge uses the fresh remote ref and cannot auto-commit before the RED test is included. Preserve the `develop` monorepo/app map and test commands while incorporating `origin/main`'s developer setup and environment documentation. Do not accept either side wholesale for the five modified Markdown/config files; validate the resulting `.env.example` contains no secret values and retains `COD_MUN_IBGE=354130`, `ID_MUNICIPIO_IBGE7=3541308`, and `CNPJ_MANTENEDORA=55293427000117` as documented examples.

- [ ] **Step 4: Verify the merge**

Run: `uv run pytest tests/negative/test_repository_baseline.py -q && uv run ruff check tests/negative/test_repository_baseline.py`

Expected: PASS and no Ruff findings.

- [ ] **Step 5: Commit**

```bash
git add .env.example README.md docs/architecture.md docs/development.md \
  docs/project-context.md docs/roadmap.md tests/negative/test_repository_baseline.py
git commit -m "chore(repo): reconcile origin/main into develop"
```

### Task 2: CND-001 — Record the green baseline matrix

**Files:**
- Create: `scripts/baseline_matrix.py`
- Create: `scripts/baseline_matrix_test.py`
- Create: `docs/baselines/2026-08-23-develop.json`
- Modify: none

**Interfaces:**
- Consumes: dependency-complete SHA from CND-000 and the existing Python, Go, dashboard, and integration commands.
- Produces: `write_report(path: Path, results: Sequence[SuiteResult], commit_sha: str) -> None`, with JSON keys `commit_sha`, `recorded_at`, and `suites[{name,command,exit_code,duration_seconds}]`.

- [ ] **Step 1: Write the failing serializer test**

```python
def test_grava_matriz_com_sha_e_resultados(tmp_path):
    target = tmp_path / "baseline.json"
    write_report(target, [SuiteResult("python-fast", "pytest -q", 0, 1.25)], "abc123")
    body = json.loads(target.read_text())
    assert body["commit_sha"] == "abc123"
    assert body["suites"][0]["exit_code"] == 0
```

- [ ] **Step 2: Prove the test fails**

Run: `uv run pytest scripts/baseline_matrix_test.py::test_grava_matriz_com_sha_e_resultados -q`

Expected: FAIL with `ModuleNotFoundError` for `scripts.baseline_matrix`.

- [ ] **Step 3: Add the typed runner and capture all suites**

Implement frozen `SuiteResult(name: str, command: str, exit_code: int, duration_seconds: float)`, `run_suite(name: str, command: Sequence[str]) -> SuiteResult`, and `write_report(...)`. Execute and record: fast Python tests, package coverage, app coverage, Go race/coverage, dashboard lint/typecheck/test/build, and Docker-marked integration tests. A non-zero baseline suite blocks CND-002 unless its exact existing failure is recorded with an approved waiver in the same JSON object as `waiver`.

- [ ] **Step 4: Verify and write the report**

Run: `uv run pytest scripts/baseline_matrix_test.py -q && uv run python scripts/baseline_matrix.py --output docs/baselines/2026-08-23-develop.json`

Expected: serializer tests PASS; the command exits zero only when every unwaived suite is green.

- [ ] **Step 5: Commit**

```bash
git add scripts/baseline_matrix.py scripts/baseline_matrix_test.py \
  docs/baselines/2026-08-23-develop.json
git commit -m "test(repo): record redesign baseline matrix"
```

### Task 3: CND-002 — Freeze CNES source, Gold, and dashboard fixtures

**Files:**
- Create: `docs/fixtures/data-plane/cnes-local-v1.parquet`
- Create: `docs/fixtures/data-plane/cnes-nacional-v1.parquet`
- Create: `docs/fixtures/data-plane/cnes-gold-v2.parquet`
- Create: `docs/fixtures/data-plane/cnes-serving-v1.json`
- Create: `docs/fixtures/data-plane/fixture-manifest.json`
- Create: `scripts/verify_data_plane_fixtures.py`
- Create: `scripts/verify_data_plane_fixtures_test.py`
- Modify: none

**Interfaces:**
- Consumes: representative, anonymized rows from current CNES local/national source contracts and frozen PostgreSQL Gold/dashboard outputs.
- Produces: immutable fixture manifest with `fixture_version=1`, `competencia`, per-file `sha256`, `row_count`, `schema_version`, and KPI values; CND-050–054 consume these exact files.

- [ ] **Step 1: Write the failing fixture-verifier test**

```python
def test_rejeita_fixture_com_hash_divergente(tmp_path):
    (tmp_path / "rows.json").write_text("[]", encoding="utf-8")
    manifest = {"fixture_version": 1, "files": {"rows.json": {"sha256": "0" * 64}}}
    with pytest.raises(FixtureError, match="sha256_mismatch"):
        verify_fixture_set(tmp_path, manifest)
```

- [ ] **Step 2: Prove the verifier is absent**

Run: `uv run pytest scripts/verify_data_plane_fixtures_test.py -q`

Expected: FAIL with `ModuleNotFoundError` for `scripts.verify_data_plane_fixtures`.

- [ ] **Step 3: Freeze data and implement exact verification**

Implement `verify_fixture_set(root: Path, manifest: Mapping[str, JsonValue]) -> None` to recompute SHA-256 and Parquet row counts. The fixture policy is locked for this plan: natural key `(CPF or CNS, CNES, CBO, competencia)`; `CNES_LOCAL` supplies the selected value when both sources have a non-null conflict; `CNES_NACIONAL` fills a null local value; every conflict is retained in a divergence row with both values and source manifest IDs. The serving fixture contains only `schema_version`, `tenant_id`, `run_id`, `generated_at`, `competencia`, `kpis`, `divergence_counts`, and `missing_sources`.

- [ ] **Step 4: Verify hashes, schemas, and anonymization**

Run: `uv run pytest scripts/verify_data_plane_fixtures_test.py -q && uv run python scripts/verify_data_plane_fixtures.py docs/fixtures/data-plane`

Expected: PASS; no CPF/CNS/name from production appears in the fixture set.

- [ ] **Step 5: Commit**

```bash
git add docs/fixtures/data-plane scripts/verify_data_plane_fixtures.py \
  scripts/verify_data_plane_fixtures_test.py
git commit -m "test(fixtures): freeze local CNES vertical slice"
```

### Task 4: CND-003 — Add backlog and path-ownership conventions

**Files:**
- Create: `.github/ISSUE_TEMPLATE/cnesdata-implementation.yml`
- Create: `docs/development/worktree-ownership.md`
- Create: `scripts/tests/test_worktree_policy.py`
- Modify: none

**Interfaces:**
- Consumes: execution design §§8–12.
- Produces: issue form fields `logical_id`, `depends_on`, `allowed_paths`, `forbidden_shared_paths`, `interfaces_consumed`, `interfaces_produced`, `verification_commands`; branch regex `^(feat|fix|test|docs)/cnd-[0-9]{3}-[a-z0-9-]+$`.

- [ ] **Step 1: Write the failing policy test**

```python
def test_issue_template_exige_campos_de_dispatch():
    body = yaml.safe_load(Path(".github/ISSUE_TEMPLATE/cnesdata-implementation.yml").read_text())
    ids = {item.get("id") for item in body["body"]}
    assert REQUIRED_IDS <= ids
```

- [ ] **Step 2: Prove the template is missing**

Run: `uv run pytest scripts/tests/test_worktree_policy.py -q`

Expected: FAIL with `FileNotFoundError`.

- [ ] **Step 3: Add the issue form and exact ownership table**

Set `REQUIRED_IDS` to the seven names in **Interfaces**. Document the integration-owned files listed in **Global Constraints**, at most three active feature worktrees plus one integration controller, and the definition-of-ready/done gates verbatim from the execution design.

- [ ] **Step 4: Verify policy syntax**

Run: `uv run pytest scripts/tests/test_worktree_policy.py -q && uv run ruff check scripts/tests/test_worktree_policy.py`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .github/ISSUE_TEMPLATE/cnesdata-implementation.yml \
  docs/development/worktree-ownership.md scripts/tests/test_worktree_policy.py
git commit -m "docs(repo): define CND worktree ownership"
```

## Phase 1 — canonical contracts and ports

### Task 5: CND-010 — Canonical control-plane domain

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/enums.py`
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/entities.py`
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/commands.py`
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/errors.py`
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/ids.py`
- Create: `packages/cnes_domain/src/cnes_domain/control_plane/transitions.py`
- Create: `packages/cnes_domain/tests/control_plane/test_entities.py`
- Create: `packages/cnes_domain/tests/control_plane/test_ids.py`
- Create: `packages/cnes_domain/tests/control_plane/test_transitions.py`
- Modify: none

**Interfaces:**
- Consumes: no new target code; only Python stdlib and Pydantic already declared by `cnes_domain`.
- Produces: every enum, model, command, `JsonValue`, `JobIdentity`, `RunUnitIdentity`, and one-argument ID helper in the registry; `InvalidTransition`, `Conflict`, `NotFound`, `LeaseLost`, and `FenceRejected` exceptions; `transition_job`, `transition_run`, and `transition_run_unit` returning copied immutable models.

- [ ] **Step 1: Write failing immutability and transition tests**

```python
def test_job_e_imutavel_e_rejeita_transicao_invalida(job):
    with pytest.raises(ValidationError):
        job.state = JobState.LEASED
    with pytest.raises(InvalidTransition, match="PENDING->SUCCEEDED"):
        transition_job(job, JobState.SUCCEEDED)


def test_unit_id_e_deterministico():
    identity = RunUnitIdentity("run-1", RunStage.NORMALIZE,
                               "CNES_LOCAL", "CNES_VINCULO", "all")
    assert unit_id(identity) == unit_id(identity)


def test_run_dependency_key_e_canonica():
    assert run_dependency_key("354130", "CNES_LOCAL", "CNES_VINCULO", "2026-07") == (
        "RUN_DEP#354130#CNES_LOCAL#CNES_VINCULO#2026-07")


def test_dispatch_terminal_exige_outcome(dispatch_values):
    with pytest.raises(ValidationError, match="terminal_outcome_required"):
        RunDispatch.model_validate({**dispatch_values, "state": "TERMINAL",
                                    "terminal_outcome": None})


def test_comandos_de_unit_carregam_mesmo_dispatch_id(claim_values):
    claim = ClaimRunUnit.model_validate({**claim_values,
                                         "dispatch_id": "fedcba9876543210"})
    assert claim.dispatch_id == "fedcba9876543210"
```

Also test that `Run` rejects an empty dependency tuple, duplicate
`(source_type,file_subtype)` pairs, blank dataset/source/subtype values, a `#` delimiter inside a
dependency-key component, and a competência outside `YYYY-MM`. Test that
`FinalizeRunCancellation.expected_state` accepts only `CANCEL_REQUESTED`.

- [ ] **Step 2: Prove the modules are absent**

Run: `uv run pytest packages/cnes_domain/tests/control_plane -q`

Expected: collection FAIL with `ModuleNotFoundError: cnes_domain.control_plane`.

- [ ] **Step 3: Add the exact registry models and transition tables**

Use the registry field lists without aliases. `Run.dependencies` is non-empty and unique by
`(source_type,file_subtype)`; `dataset_name`, dependency identifiers, and `competencia` are strict
server-side values, with competência matching `YYYY-MM`. A `NORMALIZE` unit requires non-null
`source_type`/`file_subtype` and non-empty direct `input_manifests`; `RECONCILE` and `MATERIALIZE`
units require null source/subtype, empty direct inputs, non-empty `depends_on_unit_ids`, and resolve
their immutable inputs only from satisfied predecessor refs. Dependency IDs are unique, never
self-referential, and every `manifest_key` is an immutable manifest-sidecar key rather than a
data-object key. `run_dependency_key` is the only codec for dependency lookup keys used by SQLite,
DynamoDB, and billing; it rejects blank components, `#`, and invalid competência before joining the
four parts.
`JobIdentity` and `RunUnitIdentity` validate the same component rules as their entities; `job_id`
and `unit_id` accept exactly one value object so no canonical identifier helper exceeds the
four-parameter quality gate. `RunDispatch` requires positive generation, non-empty unique ordered unit IDs, lowercase 16-hex
wave/dispatch IDs, UTC lease, null outcome while `RESERVED|STARTED`, non-null execution ref while
`STARTED`, and exactly one terminal outcome while `TERMINAL`. Initial RunUnits have null
`dispatch_id`; claim stores a valid ID, and commit/fail commands require it.
`Job.result_manifest_id` and `.result_manifest_key` are either both null or both non-null; a
succeeded Job has both and the key matches its tenant/source/competence snapshot namespace.
`RawManifestRecord` is the dependency-free control-plane index projection of one already validated
wire manifest: its IDs/source/agent/competence match the completing Job, hashes are lowercase
64-hex, FULL is sequence 1 with null base/previous hash, and DELTA has both with sequence at least 2.
Legal terminal transitions are: Job `PENDING|FAILED_RETRYABLE -> LEASED`,
`LEASED -> SUCCEEDED|FAILED_RETRYABLE|FAILED_FINAL|CANCEL_REQUESTED`,
`CANCEL_REQUESTED -> CANCELED`; Run
`PLANNED -> WAITING_INPUTS|PROCESSING|CANCEL_REQUESTED`,
`WAITING_INPUTS -> PROCESSING|FAILED|CANCEL_REQUESTED`,
`PROCESSING -> PUBLISHING|FAILED|CANCEL_REQUESTED`,
`PUBLISHING -> PUBLISHED|PUBLISHED_DEGRADED|FAILED`,
`CANCEL_REQUESTED -> CANCELED`; RunUnit `PENDING|FAILED_RETRYABLE -> LEASED`,
`LEASED -> SUCCEEDED|SUCCEEDED_DEGRADED|FAILED_RETRYABLE|FAILED_FINAL`, and any nonterminal state to
`CANCELED` only after the parent run requests cancellation. `SUCCEEDED_DEGRADED` is legal only for
a final-failed optional `NORMALIZE` dependency, carries no output manifests, records its sanitized
error, atomically adds `source_type/file_subtype` to the parent Run's `missing_sources`, and satisfies
downstream dependency readiness. Required-normalization and downstream final failures remain
`FAILED_FINAL` and fail the Run.

- [ ] **Step 4: Verify domain coverage and Ruff**

Run: `uv run ruff check packages/cnes_domain/src/cnes_domain/control_plane packages/cnes_domain/tests/control_plane && uv run pytest packages/cnes_domain/tests/control_plane --cov=packages/cnes_domain/src/cnes_domain/control_plane --cov-branch --cov-fail-under=100 -q`

Expected: PASS with 100% branch coverage.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/control_plane \
  packages/cnes_domain/tests/control_plane
git commit -m "feat(domain): add canonical control plane"
```

### Task 6: CND-011 — Data-plane manifest contracts

**Files:**
- Create: `packages/cnes_contracts/src/cnes_contracts/manifests/raw.py`
- Create: `packages/cnes_contracts/src/cnes_contracts/manifests/outputs.py`
- Create: `packages/cnes_contracts/src/cnes_contracts/manifests/processing.py`
- Create: `packages/cnes_contracts/src/cnes_contracts/manifests/validation.py`
- Create: `packages/cnes_contracts/tests/manifests/test_raw.py`
- Create: `packages/cnes_contracts/tests/manifests/test_outputs.py`
- Create: `packages/cnes_contracts/tests/manifests/test_processing.py`
- Create: `packages/cnes_contracts/tests/manifests/test_validation.py`
- Modify: none

**Interfaces:**
- Consumes: Pydantic 2 already declared by `cnes_contracts`.
- Produces: `SourceType`, `SnapshotMode`, `RawManifest`, `OutputManifest`, `RunManifest`, `ServingDocument`, and all six request/result models from the registry; `manifest_sha256(model: BaseModel) -> str`; `validate_object_key(manifest: RawManifest | OutputManifest) -> None`.

- [ ] **Step 1: Write failing strict-contract tests**

```python
def test_delta_exige_base_e_hash_anterior(raw_values):
    raw_values.update(snapshot_mode="DELTA", base_snapshot_id=None,
                      previous_manifest_sha256=None, sequence=2)
    with pytest.raises(ValidationError, match="delta_chain_required"):
        RawManifest.model_validate(raw_values)


def test_manifest_hash_independe_da_ordem(raw_manifest):
    assert manifest_sha256(raw_manifest) == hashlib.sha256(
        raw_manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    ).hexdigest()


def test_processing_requests_sao_estritos(normalize_values):
    with pytest.raises(ValidationError):
        NormalizeRequest.model_validate({**normalize_values, "source_type": "UNKNOWN"})


def test_processing_results_suportam_multiplos_artefatos(output_manifest, serving_document):
    normalized = NormalizeResult(manifests=(output_manifest,))
    materialized = MaterializeResult(
        manifests=(output_manifest,), documents=(serving_document,))
    assert normalized.manifests == (output_manifest,)
    assert materialized.documents[0].document_name == "overview"


def test_target_keys_rejeitam_repeticao(normalize_values):
    key = "normalized/354130/CNES_LOCAL/2026-07/run-1/cnes.parquet"
    with pytest.raises(ValidationError, match="target_keys_unique"):
        NormalizeRequest.model_validate({**normalize_values, "target_keys": (key, key)})


def test_manifesto_de_divergencia_aceita_zero_linhas(output_values):
    manifest = OutputManifest.model_validate({
        **output_values, "layer": "reconciliation", "source_type": None,
        "row_count": 0,
    })
    assert manifest.row_count == 0


def test_normalize_aceita_cadeias_independentes_por_subtipo(normalize_values):
    bpa_c = raw_chain("BPA_MAG", "BPA_C", sequences=(1, 2))
    bpa_i = raw_chain("BPA_MAG", "BPA_I", sequences=(1,))
    request = NormalizeRequest.model_validate({
        **normalize_values,
        "source_type": SourceType.BPA_MAG,
        "raw_manifests": (*bpa_c, *bpa_i),
    })
    assert tuple(m.file_subtype for m in request.raw_manifests) == (
        "BPA_C", "BPA_C", "BPA_I",
    )
```

- [ ] **Step 2: Prove the contracts are absent**

Run: `uv run pytest packages/cnes_contracts/tests/manifests -q`

Expected: collection FAIL with `ModuleNotFoundError: cnes_contracts.manifests`.

- [ ] **Step 3: Add strict models and canonical serialization**

Apply `ConfigDict(frozen=True, strict=True, extra="forbid")` to manifests and processing request/results, UTC-aware datetime validation, and lowercase 64-hex hashes. `RawManifest.size_bytes` is positive and its `row_count` is non-negative; `OutputManifest.row_count` is non-negative so a verified empty quality/divergence Parquet remains a valid immutable artifact. FULL requires `sequence=1` and null chain fields; DELTA requires non-null base/hash with `sequence>=2`. `NormalizeRequest` requires non-empty manifests with one tenant, source type, and competência; the request itself supplies the target run/unit/attempt identity. It orders groups by `file_subtype` and each subtype independently forms one valid FULL/DELTA chain; this permits BPA/SIA multi-file sources without treating two `sequence=1` manifests as one broken chain. Source normalizers validate their required subtype set against the selected `SourcePipeline.dependencies`. Both request `target_keys` tuples are non-empty, unique, and each key must match its layer plus the request tenant/run prefix; workers write exactly those keys and never invent sibling names. `NormalizeResult.manifests` is non-empty and its ordered object keys must equal `NormalizeRequest.target_keys`. `MaterializeResult.manifests` and `.documents` are non-empty, have the same length as `MaterializeRequest.target_keys`, and each ordered manifest object key equals the corresponding target key; members of each result agree on tenant/run and, for manifests, competence. Stage tests assert that result identity also matches its request. `ServingDocument.schema_version` matches `^[a-z0-9-]+-v[1-9][0-9]*$`, `document_name` matches `^[a-z0-9][a-z0-9_-]*$`, and multiple documents in one result have unique names. `OutputManifest` requires `source_type` for `normalized` and requires it to be null for `reconciliation`/`serving`; it always carries `competencia`. `ReconcileRequest` requires a non-empty tuple of normalized manifests with matching tenant, competence, and run; source-specific cardinality belongs to the source reconciler, so later SIHD/BPA/SIA tasks reuse this contract. Every request enforces matching tenant/run/unit fields. Canonical hash input is `model_dump_json(exclude_none=False, by_alias=False)` with model field order; object keys must exactly match the layer layouts from spec §6.1.

- [ ] **Step 4: Verify contract coverage**

Run: `uv run ruff check packages/cnes_contracts/src/cnes_contracts/manifests packages/cnes_contracts/tests/manifests && uv run pytest packages/cnes_contracts/tests/manifests --cov=cnes_contracts.manifests --cov-branch --cov-fail-under=100 -q`

Expected: PASS with 100% branch coverage.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_contracts/src/cnes_contracts/manifests \
  packages/cnes_contracts/tests/manifests
git commit -m "feat(contracts): add versioned data plane manifests"
```

### Task 7: CND-012 — Target ports and request values

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/ports/control_plane.py`
- Create: `packages/cnes_domain/src/cnes_domain/ports/object_store.py`
- Create: `packages/cnes_domain/src/cnes_domain/ports/processing.py`
- Create: `packages/cnes_domain/src/cnes_domain/ports/serving.py`
- Create: `packages/cnes_domain/src/cnes_domain/ports/audit.py`
- Create: `packages/cnes_domain/tests/ports/test_target_ports.py`
- Modify: none

**Interfaces:**
- Consumes: accepted CND-010 entities/commands and the CND-011 wire-manifest boundary; ports use
  dependency-free `ManifestRef`/`RawManifestRecord` projections and never import infrastructure.
- Produces: the five PEP 544 Protocols plus `ObjectStat`, `StartRunExecution`, `RunUnitMessage`, `CancelRunExecution`,
  `ExecutionPermit`, `ConcurrencyPolicy`, `ExecutionStarted`, `ExecutionCallbacks`,
  `ExecutionPolicyConfig`, `ServingRequest`, and `ServingGrant` in the registry. Ports
  contain no SQLAlchemy, SQL, filesystem path, S3, DynamoDB, FastAPI, Polars, or HTTP request types.

- [ ] **Step 1: Write the failing runtime protocol test**

```python
def test_adapter_estrutural_satisfaz_object_store_port():
    class Store:
        put = lambda self, key, body, expected_sha256: ObjectStat(key, 0, expected_sha256)
        open = lambda self, key: nullcontext(BytesIO())
        stat = lambda self, key: None
        promote = lambda self, source_key, destination_key, expected_sha256: ObjectStat(
            destination_key, 0, expected_sha256)
        delete = lambda self, key: None
    assert isinstance(Store(), ObjectStorePort)
```

- [ ] **Step 2: Prove the ports are absent**

Run: `uv run pytest packages/cnes_domain/tests/ports/test_target_ports.py -q`

Expected: collection FAIL importing `cnes_domain.ports.control_plane`.

- [ ] **Step 3: Add runtime-checkable protocols with exact signatures**

Copy the registry signatures exactly and decorate all five protocols with `@runtime_checkable`.
`list_raw_manifest_chain` returns an ordered immutable `ManifestRef` chain beginning at one FULL
and ending at the selected head, never candidates from two agents/snapshot chains; its positive
limit bounds the base read and callers fail closed if the chain would exceed it.
`list_waiting_runs_for_dependency` returns only `WAITING_INPUTS` runs whose immutable dependency
tuple contains the exact source/subtype/competence; its bounded discovery results are always
revalidated by canonical run key before orchestration.
`list_recoverable_runs(now, limit)` returns a bounded deterministic `(created_at,tenant_id,run_id)` order of
strongly revalidated `WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED` Runs. `put_run_units` performs a
CAS on `expected_run_state`: absent units are inserted atomically; an exact byte-identical replay
returns the stored tuple; any missing/extra/different unit conflicts without a partial write.
`finalize_run_cancellation` CAS-checks `CANCEL_REQUESTED`, atomically marks every nonterminal unit
`CANCELED`, finalizes the Run `CANCELED`, and emits its outbox event.
`reserve_run_dispatch` is the only dispatch-ID allocator. Given one ordered non-empty wave, it
atomically returns the existing unexpired `RESERVED|STARTED` dispatch for that same wave; it rejects
a different wave while any dispatch or unit lease is live. After the active dispatch is terminal,
or its lease expired and no unit has a live lease, it increments the persisted generation and
derives lowercase 16-hex `dispatch_id` from `(tenant_id,run_id,wave_id,generation,unit_ids)`.
`bind_run_dispatch` CAS-binds exactly one execution ref and renews the dispatch lease; byte-identical
replay returns the same object and a different ref conflicts. `finish_run_dispatch` CAS-transitions
the matching active dispatch to `TERMINAL` and persists its `DispatchOutcome`; byte-identical
finish replay returns the same object and a different outcome conflicts. `get_active_run_dispatch` returns only the current
unexpired `RESERVED|STARTED` record. A `ClaimRunUnit` carries the allocated `dispatch_id` and succeeds only
when it matches that active dispatch, its unit belongs to `unit_ids`, and the parent Run is
`PROCESSING`; it stores the claimed dispatch ID on the unit. Commit/fail require that same dispatch
ID in addition to owner/fence, and condition-check both parent `PROCESSING` and the active dispatch.
`StartRunExecution.wave_id` and `.dispatch_id` are required lowercase 16-hex. `wave_id` identifies
the stable logical ordered unit set; `dispatch_id` identifies one persisted generation of that wave.
Unit IDs are non-empty and unique, and concurrency is positive. `ExecutionPermit` and
`PublicationPermit` require matching tenant/run, positive concurrency where applicable, and
non-negative policy version/fence. `ExecutionPermit.binding_context` is opaque and optional; local
policy leaves it null, while billing may carry its own typed object without redefining this domain
permit. `PublicationPermit.binding_context` follows the same rule: local leaves it null and an
integrated billing adapter condition-checks the typed account/version/fence context in the same
publication transaction. `RunUnitMessage` validates nonblank identity/owner, matching lowercase 16-hex wave/dispatch,
UTC `now`, and positive lease seconds. Put
object/serving request values in the same focused module as their port. Use only `TYPE_CHECKING`
imports where a concrete import is unnecessary.

- [ ] **Step 4: Verify types, imports, and coverage**

Run: `uv run ruff check packages/cnes_domain/src/cnes_domain/ports packages/cnes_domain/tests/ports && uv run pytest packages/cnes_domain/tests/ports/test_target_ports.py --cov=cnes_domain.ports --cov-branch --cov-fail-under=100 -q`

Expected: PASS with no import of an infrastructure/framework package.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/ports \
  packages/cnes_domain/tests/ports/test_target_ports.py
git commit -m "feat(domain): define target data plane ports"
```

### Task 8: CND-013 — Shared adapter contract suites

**Files:**
- Create: `packages/cnes_infra/tests/contracts/clock.py`
- Create: `packages/cnes_infra/tests/contracts/control_plane_contract.py`
- Create: `packages/cnes_infra/tests/contracts/object_store_contract.py`
- Create: `packages/cnes_infra/tests/contracts/test_contract_harness.py`
- Modify: none

**Interfaces:**
- Consumes: CND-010 and CND-012.
- Produces: `MutableClock.now() -> datetime`, `MutableClock.advance(delta: timedelta) -> None`; `control_plane_cases() -> tuple[ControlPlaneCase, ...]`; `object_store_cases() -> tuple[ObjectStoreCase, ...]`; each case is a frozen dataclass with `name: str` and `run(adapter, clock) -> None`.

- [ ] **Step 1: Write a failing harness self-test**

```python
@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_fake_adapter_expoe_todas_as_invariantes(case, fake_control_plane, clock):
    case.run(fake_control_plane, clock)
```

- [ ] **Step 2: Prove the case catalog is absent**

Run: `uv run pytest packages/cnes_infra/tests/contracts/test_contract_harness.py -q`

Expected: collection FAIL importing `control_plane_contract`.

- [ ] **Step 3: Encode every local adapter invariant as executable cases**

The catalog must cover direct membership lookup; revoked-agent Job-claim rejection; RunUnit claim
only while its parent Run is `PROCESSING`; atomic claim/fence increment; same logical ID on retry;
byte-identical `put_run_units` replay, divergent replay conflict, and no partial unit write; immutable
dataset version; pointer CAS plus atomic Run
`PUBLISHED|PUBLISHED_DEGRADED` finalization and missing-source persistence; same-hash idempotency
replay; different-hash conflict; expired logical idempotency replacement while the row remains;
lease expiry based on the injected clock; outbox creation in the same mutation; object immutability;
deterministic raw-chain selection/order with no cross-agent mixing; hash/size stat; safe traversal
rejection; bounded waiting/recoverable Run discovery with canonical revalidation; atomic
`CANCEL_REQUESTED` unit cancellation/Run finalization; optional-normalize
`SUCCEEDED_DEGRADED` handling; canonical `run_dependency_key` use; dispatch reservation replay,
generation advance after terminal/expired-safe recovery, conflicting-wave rejection, bind replay,
and finish CAS; claim plus commit/fail rejection for a stale/missing dispatch ID or non-PROCESSING
parent; and promote verification.
`test_contract_harness.py`
provides in-memory fakes solely to prove the cases themselves detect a deliberately broken adapter.

- [ ] **Step 4: Verify the harness**

Run: `uv run ruff check packages/cnes_infra/tests/contracts && uv run pytest packages/cnes_infra/tests/contracts/test_contract_harness.py -q`

Expected: PASS; each mutation applied to the broken fake causes the named case to fail.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/tests/contracts
git commit -m "test(infra): define adapter conformance suites"
```

### Task 9: CND-014 — Profile configuration and Phase 1 serial integration

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/profiles.py`
- Create: `packages/cnes_domain/tests/test_profiles.py`
- Modify: `packages/cnes_contracts/src/cnes_contracts/__init__.py`
- Modify: `packages/cnes_contracts/src/cnes_contracts/export.py`
- Modify: `packages/cnes_domain/src/cnes_domain/__init__.py`
- Modify: `packages/cnes_domain/src/cnes_domain/ports/__init__.py`
- Modify: `packages/cnes_infra/pyproject.toml`
- Modify: `docs/contracts/schemas/*.json` (generated)
- Modify: `pyproject.toml` only if test markers need registration
- Modify: `uv.lock` only if dependency resolution changes

**Interfaces:**
- Consumes: accepted CND-010–013 commits.
- Produces: `RuntimeProfile(LOCAL, AWS)`, `AuthMode(LOCAL, OIDC)`, `BillingMode(DISABLED, STRIPE)`, and frozen `ProfileSettings(profile, tenant_id, data_dir, auth_mode, billing_mode, oidc_issuer)`; `parse_profile(env: Mapping[str, str]) -> ProfileSettings`.

- [ ] **Step 1: Write failing local-safety tests**

```python
def test_local_exige_tenant_e_rejeita_billing_remoto(tmp_path):
    with pytest.raises(ValidationError, match="local_billing_disabled"):
        ProfileSettings(profile="local", tenant_id="354130", data_dir=tmp_path,
                        auth_mode="local", billing_mode="stripe")


def test_oidc_exige_issuer(tmp_path):
    with pytest.raises(ValidationError, match="oidc_issuer_required"):
        ProfileSettings(profile="local", tenant_id="354130", data_dir=tmp_path,
                        auth_mode="oidc", billing_mode="disabled")
```

- [ ] **Step 2: Prove the profile contract is absent**

Run: `uv run pytest packages/cnes_domain/tests/test_profiles.py -q`

Expected: FAIL importing `cnes_domain.profiles`.

- [ ] **Step 3: Add settings and integrate accepted shared surfaces**

`parse_profile` defaults to `PROFILE=local`, `AUTH_MODE=local`, `BILLING_MODE=disabled`, `DATA_DIR=data`, and requires a six-digit server-side `TENANT_ID`. Local rejects Stripe; OIDC requires `OIDC_ISSUER`; the AWS enum is modeled but no AWS application is composed in this plan. Add only accepted public exports, add all CND-011 models to `cnes_contracts.export.MODELS`, declare direct `boto3` and `polars` dependencies in `cnes_infra` plus `moto[dynamodb,s3]` in root dev dependencies so CND-021–023 branches can test without editing shared manifests, lock, and regenerate schemas.

- [ ] **Step 4: Run the Phase 1 gate**

Run: `uv run ruff check packages/cnes_contracts packages/cnes_domain packages/cnes_infra/tests/contracts && uv run pytest packages/cnes_contracts packages/cnes_domain packages/cnes_infra/tests/contracts --cov --cov-config=pyproject.toml -q && uv run python scripts/gen_contracts.py`

Expected: PASS at 100% package branch coverage and `git diff --exit-code docs/contracts/schemas` after generated files are staged.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/profiles.py \
  packages/cnes_domain/tests/test_profiles.py packages/cnes_contracts/src/cnes_contracts \
  packages/cnes_domain/src/cnes_domain/__init__.py \
  packages/cnes_domain/src/cnes_domain/ports/__init__.py \
  packages/cnes_infra/pyproject.toml docs/contracts/schemas pyproject.toml uv.lock
git commit -m "feat(config): integrate target profile contracts"
```

## Phase 2 — control-plane and object-store adapters

### Task 10: CND-020 — SQLite control-plane adapter

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_schema.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_adapter.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_claims.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_idempotency.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/sqlite_publication.py`
- Create: `packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py`
- Create: `packages/cnes_infra/tests/control_plane/test_sqlite_races.py`
- Modify: none

**Interfaces:**
- Consumes: `ControlPlanePort`, CND-010 models/commands, and CND-013 `control_plane_cases()`.
- Produces: `SQLiteControlPlane(database_path: Path, clock: Callable[[], datetime])`; `initialize() -> None`; a complete `ControlPlanePort` implementation; SQLite file `state/cnesdata.sqlite3` when composed with the default local profile.

- [ ] **Step 1: Write failing contract and race tests**

```python
@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_sqlite_control_plane_contract(case, sqlite_control_plane, clock):
    case.run(sqlite_control_plane, clock)


def test_duas_conexoes_nao_reclamam_a_mesma_unit(sqlite_factory, claim):
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: sqlite_factory().claim_run_unit(claim), range(2)))
    assert sum(result is not None for result in results) == 1
```

- [ ] **Step 2: Prove the adapter is absent**

Run: `uv run pytest packages/cnes_infra/tests/control_plane/test_sqlite_adapter.py -q`

Expected: collection FAIL importing `cnes_infra.control_plane.sqlite_adapter`.

- [ ] **Step 3: Add schema, transactions, claims, idempotency, and publication**

Use `sqlite3.connect(..., isolation_level=None)`, `PRAGMA foreign_keys=ON`, `PRAGMA journal_mode=WAL`, busy timeout 5000 ms, UTC ISO-8601 strings, JSON sorted with compact separators, and `BEGIN IMMEDIATE` for claims/CAS. Create focused tables `tenants`, `memberships`, `agents`, `jobs`, `raw_manifest_index`, `runs`, `run_dependencies`, `run_units`, `run_dispatches`, `dataset_versions`, `dataset_pointers`, `access_requests`, `idempotency_records`, and `outbox_events`; primary/unique keys mirror logical identity. Every audited mutation inserts `outbox_events` before COMMIT. Job claims condition on eligible state, expired-or-null lease, and the current non-revoked agent. Unit claims strongly check the parent Run is `PROCESSING`, the supplied `dispatch_id` identifies the active `RESERVED|STARTED` dispatch containing the unit, then condition on eligible state/lease, store `RunUnit.dispatch_id`, and atomically increment `attempt`/`fencing_token`; there is no agent check for a central RunUnit. Commit/fail condition on the same parent state, active dispatch ID, stored unit dispatch ID, owner, and fence, so cancellation or dispatch supersession rejects a stale write. `put_run_units` performs the registry CAS/idempotent replay rules in one `BEGIN IMMEDIATE`. Dispatch reserve/bind/finish use `BEGIN IMMEDIATE`: reservation replays the same live wave, blocks another live wave or any live unit lease, and increments persisted generation only after terminal or expired-safe recovery; bind is one-ref idempotent CAS; finish is active-dispatch CAS. A final optional NORMALIZE failure stores `SUCCEEDED_DEGRADED` and updates `Run.missing_sources` in the same transaction. Cancellation marks all nonterminal units and the Run `CANCELED` with one outbox transaction. Completing a Job stores its immutable manifest ID/key and chain metadata in `raw_manifest_index` in the same transaction. `list_raw_manifest_chain` deterministically chooses the newest valid head by `(created_at,agent_id,snapshot_id)`, follows only that head's FULL/DELTA ancestry, and returns oldest-to-newest refs without combining agents. `run_dependencies` keys come only from `run_dependency_key`; they support bounded waiting and recoverable-run lookup, and every returned row is reloaded from `runs` and revalidated. `publish_dataset` conditions the Run on `PUBLISHING`, inserts the immutable version, CAS-updates `dataset_pointers` against `expected_version_id`, finalizes the Run to the command's `PUBLISHED|PUBLISHED_DEGRADED` state/missing sources, and inserts the supplied outbox event in one transaction; any failed condition rolls back all four mutations.

The SQLite recoverable-run index includes exactly `WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED`.

- [ ] **Step 4: Run contracts, race tests, lint, and coverage**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane packages/cnes_infra/tests/control_plane && uv run pytest packages/cnes_infra/tests/control_plane -m "not postgres" --cov=cnes_infra.control_plane --cov-branch --cov-fail-under=100 -q`

Expected: PASS with one winner in every claim/publisher race and 100% package branch coverage.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane \
  packages/cnes_infra/tests/control_plane
git commit -m "feat(infra): add SQLite control plane"
```

### Task 11: CND-021 — DynamoDB control-plane adapter

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_keys.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_codec.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_adapter.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_claims.py`
- Create: `packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_publication.py`
- Create: `packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py`
- Create: `packages/cnes_infra/tests/control_plane/test_dynamodb_stale_gsi.py`
- Modify: none

**Interfaces:**
- Consumes: `ControlPlanePort`, CND-010 models/commands, CND-013 `control_plane_cases()`, and a boto3-compatible low-level DynamoDB client supplied by the caller.
- Produces: `DynamoDBControlPlane(client: botocore.client.BaseClient, table_name: str, clock: Callable[[], datetime])` implementing the complete `ControlPlanePort`; pure key functions `tenant_key`, `membership_key`, `agent_key`, `job_key`, `run_key`, `unit_key`, `dispatch_key`, `version_key`, `pointer_key`, `idempotency_key`, and `outbox_key` matching spec §8.2.

- [ ] **Step 1: Write failing contract and stale-GSI tests**

```python
@pytest.mark.parametrize("case", control_plane_cases(), ids=lambda case: case.name)
def test_dynamodb_control_plane_contract(case, dynamodb_control_plane, clock):
    case.run(dynamodb_control_plane, clock)


def test_job_descoberto_em_gsi_e_revalidado_na_chave_base(adapter, stale_candidate):
    adapter.query_candidates = lambda *args: (stale_candidate,)
    adapter.revoke_agent("354130", "agent-01")
    assert adapter.claim_job(claim(stale_candidate)) is None
```

- [ ] **Step 2: Prove the adapter is absent**

Run: `uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_adapter.py -q`

Expected: collection FAIL importing `cnes_infra.control_plane.dynamodb_adapter`.

- [ ] **Step 3: Add single-table conditional/transactional behavior**

Encode Decimal-free JSON-compatible items and the exact PK/SK baseline from spec §8.2. Use base-table `GetItem(ConsistentRead=True)` for membership/agent authorization. Job claims use state/lease/fence conditions plus an agent `ConditionCheck`; RunUnit claims instead condition-check the parent Run is `PROCESSING` and the supplied `dispatch_id` identifies the active dispatch containing that unit, persist that ID on the unit, and never require an agent. Commit/fail transactions condition-check the parent Run, active dispatch, stored unit dispatch ID, owner, and fence; billing may add its companion fence to the same transaction without weakening these conditions. Store the current `RunDispatch` as a strongly read base item; transactional reserve reuses the same live wave, rejects overlap, and advances its persisted generation only after terminal or expired-safe recovery with no live unit lease. Bind is a one-ref idempotent CAS and finish is an active-dispatch CAS. Job completion transactionally stores the manifest ref plus immutable chain-index item; `list_raw_manifest_chain` may use a GSI only to find head candidates, strongly rereads the selected base items, follows one agent/snapshot ancestry, and returns oldest-to-newest refs. Build immutable dependency lookup PKs only with `run_dependency_key`; use them for `list_waiting_runs_for_dependency`, while a bounded recoverable-run index supplies candidate IDs for `list_recoverable_runs`; every candidate is strongly reread and state-revalidated. `put_run_units` uses one transaction with a Run-state condition and all absent unit Puts; on replay it strongly reads the entire stored tuple and returns only byte-identical units, otherwise conflicts. Optional-normalize degradation updates the unit and parent missing-source set atomically. Cancellation transactionally condition-checks `CANCEL_REQUESTED`, cancels every nonterminal unit, finalizes the Run, and writes outbox; reject a run whose unit set exceeds the documented DynamoDB transaction bound instead of partially canceling. Idempotency evaluates `expires_at` in conditions even when the TTL item still exists. Dataset publish uses one transaction for a Run `PUBLISHING` condition/final-state update, immutable version `Put`, pointer CAS `Update`, and outbox `Put`; TTL is set only on idempotency garbage collection. GSI results are candidate IDs only and are always revalidated against the base item.

The DynamoDB recoverable-run index includes exactly `WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED`.

- [ ] **Step 4: Run DynamoDB Local contracts and coverage**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_* packages/cnes_infra/tests/control_plane/test_dynamodb_* && uv run pytest packages/cnes_infra/tests/control_plane/test_dynamodb_* --cov=cnes_infra.control_plane --cov-branch --cov-fail-under=100 -q`

Expected: PASS against the moto test double, including an expired-but-present TTL item and stale candidate tests; CND-025 repeats the same cases against DynamoDB Local.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/control_plane/dynamodb_* \
  packages/cnes_infra/tests/control_plane/test_dynamodb_*
git commit -m "feat(infra): add DynamoDB control plane"
```

### Task 12: CND-022 — Filesystem and S3 object-store adapters

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/object_store/filesystem.py`
- Create: `packages/cnes_infra/src/cnes_infra/object_store/s3.py`
- Create: `packages/cnes_infra/tests/object_store/test_filesystem.py`
- Create: `packages/cnes_infra/tests/object_store/test_s3.py`
- Modify: none

**Interfaces:**
- Consumes: CND-012 `ObjectStorePort`, `ObjectStat`; CND-013 `object_store_cases()`; injected boto3-compatible S3 client.
- Produces: `FilesystemObjectStore(root: Path)` and `S3ObjectStore(client: botocore.client.BaseClient, bucket: str, prefix: str = "")`, both implementing `ObjectStorePort`; all keys are identical POSIX logical keys.

- [ ] **Step 1: Write failing conformance and traversal tests**

```python
@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_filesystem_contract(case, tmp_path, clock):
    case.run(FilesystemObjectStore(tmp_path), clock)


def test_rejeita_escape_do_data_dir(tmp_path):
    store = FilesystemObjectStore(tmp_path)
    with pytest.raises(InvalidObjectKey, match="object_key_outside_root"):
        store.put("../secret", BytesIO(b"x"), hashlib.sha256(b"x").hexdigest())


@pytest.mark.parametrize("case", object_store_cases(), ids=lambda case: case.name)
def test_s3_contract(case, s3_store, clock):
    case.run(s3_store, clock)
```

- [ ] **Step 2: Prove the adapter is absent**

Run: `uv run pytest packages/cnes_infra/tests/object_store/test_filesystem.py packages/cnes_infra/tests/object_store/test_s3.py -q`

Expected: collection FAIL importing `cnes_infra.object_store.filesystem`.

- [ ] **Step 3: Add immutable, hash-verifying filesystem operations**

Share a pure logical-key validator. Filesystem `put` streams into a sibling `.<name>.partial`, fsyncs, verifies bytes/hash, then uses `os.replace`. S3 `put` supplies `IfNoneMatch="*"`, stores `sha256` metadata, and verifies `ContentLength`/metadata with `HeadObject`; map HTTP 412 to same-hash idempotent success or `ObjectConflict`. Filesystem promote copies through a partial destination. S3 promote reads the source stream and performs the same conditional `PutObject` to the destination so correctness does not rely on overwrite-prone `CopyObject`. Neither adapter removes attempt objects during promotion.

- [ ] **Step 4: Run contract and coverage gates**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/object_store packages/cnes_infra/tests/object_store && uv run pytest packages/cnes_infra/tests/object_store --cov=cnes_infra.object_store --cov-branch --cov-fail-under=100 -q`

Expected: PASS, including interrupted partial-write cleanup and immutable-key conflict cases.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/object_store \
  packages/cnes_infra/tests/object_store
git commit -m "feat(infra): add filesystem and S3 object stores"
```

### Task 13: CND-023 — Local and S3 Object Lock audit sinks

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/audit/local_sink.py`
- Create: `packages/cnes_infra/src/cnes_infra/audit/s3_object_lock_sink.py`
- Create: `packages/cnes_infra/tests/audit/test_local_sink.py`
- Create: `packages/cnes_infra/tests/audit/test_s3_object_lock_sink.py`
- Modify: none

**Interfaces:**
- Consumes: CND-010 `OutboxEvent`; CND-012 `AuditSinkPort`.
- Produces: `LocalAuditSink(root: Path, parquet_batch_size: int = 1000)` and `S3ObjectLockAuditSink(client: botocore.client.BaseClient, bucket: str, retention_days: int)`; both implement `append(event)`. Local paths remain daily JSONL/Parquet; S3 key is `audit/<tenant>/<yyyy>/<mm>/<dd>/<event_id>.json`.

- [ ] **Step 1: Write a failing append/replay test**

```python
def test_append_repetido_nao_duplica_evento(tmp_path, event):
    sink = LocalAuditSink(tmp_path)
    sink.append(event)
    sink.append(event)
    lines = list((tmp_path / "audit/354130/2026/08/23/events.jsonl").open())
    assert len(lines) == 1


def test_s3_evento_usa_object_lock_compliance(s3_sink, event, s3_client):
    s3_sink.append(event)
    call = s3_client.put_object.call_args.kwargs
    assert call["ObjectLockMode"] == "COMPLIANCE"
    assert call["IfNoneMatch"] == "*"
```

- [ ] **Step 2: Prove the sink is absent**

Run: `uv run pytest packages/cnes_infra/tests/audit/test_local_sink.py packages/cnes_infra/tests/audit/test_s3_object_lock_sink.py -q`

Expected: collection FAIL importing `cnes_infra.audit.local_sink`.

- [ ] **Step 3: Add append-only, idempotent local delivery**

Local: serialize one compact sorted JSON object per line and maintain a sibling SQLite delivery index keyed by `event_id`; hold an advisory lock, fsync JSONL before committing the index, and materialize immutable Polars Parquet batches without deleting JSONL. S3: require bucket Object Lock configuration at initialization, write one deterministic immutable event object with `IfNoneMatch="*"`, `ObjectLockMode="COMPLIANCE"`, UTC retention date `created_at + retention_days`, content SHA-256 metadata, and treat an existing same-hash key as idempotent success.

- [ ] **Step 4: Verify crash and restart behavior**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/audit packages/cnes_infra/tests/audit && uv run pytest packages/cnes_infra/tests/audit --cov=cnes_infra.audit --cov-branch --cov-fail-under=100 -q`

Expected: PASS; replay after reopening produces no duplicate line.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/audit packages/cnes_infra/tests/audit
git commit -m "feat(audit): add local and Object Lock sinks"
```

### Task 14: CND-024 — Outbox dispatcher and recovery

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py`
- Create: `packages/cnes_domain/tests/test_outbox_dispatcher.py`
- Modify: none

**Interfaces:**
- Consumes: `ControlPlanePort.pending_outbox`, `mark_outbox_delivered`, `AuditSinkPort.append`, and CND-010 `OutboxEvent`.
- Produces: `DispatchResult(delivered: int, failed: int)`; `dispatch_once(control_plane: ControlPlanePort, sink: AuditSinkPort, now: datetime, limit: int = 100) -> DispatchResult`.

- [ ] **Step 1: Write the failing recovery test**

```python
def test_falha_do_sink_mantem_evento_pendente(control_plane, event, now):
    control_plane.create_job(job_fixture(), event)
    sink = FailingSink()
    assert dispatch_once(control_plane, sink, now).failed == 1
    assert control_plane.pending_outbox(10) == (event,)
```

- [ ] **Step 2: Prove the dispatcher is absent**

Run: `uv run pytest packages/cnes_domain/tests/test_outbox_dispatcher.py -q`

Expected: FAIL importing `cnes_domain.outbox_dispatcher`.

- [ ] **Step 3: Add bounded, idempotent dispatch**

Read at most `limit` events ordered by `(created_at,event_id)`. For each event call `sink.append`; mark delivered only after success; catch and count sink exceptions without mutating canonical state and without preventing later events from being attempted. Reject `limit < 1`.

- [ ] **Step 4: Verify coverage**

Run: `uv run ruff check packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py packages/cnes_domain/tests/test_outbox_dispatcher.py && uv run pytest packages/cnes_domain/tests/test_outbox_dispatcher.py --cov=cnes_domain.outbox_dispatcher --cov-branch --cov-fail-under=100 -q`

Expected: PASS with 100% branch coverage.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/outbox_dispatcher.py \
  packages/cnes_domain/tests/test_outbox_dispatcher.py
git commit -m "feat(domain): dispatch control plane outbox"
```

### Task 15: CND-025 — Full adapter conformance and Phase 2 serial integration

**Files:**
- Create: `tests/integration/test_local_adapter_matrix.py`
- Create: `tests/integration/test_aws_adapter_matrix.py`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/control_plane/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/object_store/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/audit/__init__.py`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `.github/workflows/python-quality.yml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: accepted CND-020–024.
- Produces: SQLite/DynamoDB Local and filesystem/S3-compatible conformance matrix; public exports for both control planes, both object stores, and both audit sinks; CI markers `local_profile`, `dynamodb_local`, and `s3_integration`. This task tests adapters only; it does not compose an AWS application profile.

- [ ] **Step 1: Write the failing integrated matrix test**

```python
@pytest.mark.local_profile
def test_restart_preserva_estado_e_objeto(tmp_path, tenant, event):
    state = tmp_path / "state/cnesdata.sqlite3"
    SQLiteControlPlane(state, utc_now).initialize()
    SQLiteControlPlane(state, utc_now).put_tenant(tenant)
    store = FilesystemObjectStore(tmp_path / "data")
    store.put("raw/354130/CNES_LOCAL/2026-07/s1/a.parquet", BytesIO(b"x"), sha(b"x"))
    assert SQLiteControlPlane(state, utc_now).get_tenant("354130") == tenant
    assert store.stat("raw/354130/CNES_LOCAL/2026-07/s1/a.parquet") is not None


@pytest.mark.parametrize("backend", ["sqlite", "dynamodb_local"])
def test_publicacao_atomica_em_ambos_control_planes(control_plane_for, backend, publish):
    pointer = control_plane_for(backend).publish_dataset(publish)
    assert pointer.version_id == publish.version.version_id
```

- [ ] **Step 2: Prove exports/marker are not integrated**

Run: `uv run pytest tests/integration/test_local_adapter_matrix.py tests/integration/test_aws_adapter_matrix.py -q`

Expected: FAIL importing the public adapter exports or warn/fail for unknown emulator markers.

- [ ] **Step 3: Integrate only accepted local adapters**

Reuse the locked `boto3`/moto dependencies explicitly added by CND-014; add only emulator/testcontainer dependencies still required by the integration tests and all three markers. Add Compose profile `aws-test` with services `dynamodb-local` and `aws-emulator`; expose DynamoDB Local as `DYNAMODB_ENDPOINT_URL=http://localhost:18000` and the S3-compatible emulator as `AWS_ENDPOINT_URL=http://localhost:4566`, with `AWS_REGION=us-east-1`, test credentials, and bucket `cnesdata-test`. CI starts exactly `docker compose --profile aws-test up -d dynamodb-local aws-emulator`, runs the matrix, then tears that profile down. These services are test/development only and are never dependencies of `PROFILE=local`.

- [ ] **Step 4: Run the Phase 2 gate**

Run: `uv sync --locked && uv run ruff check packages/cnes_infra tests/integration/test_local_adapter_matrix.py tests/integration/test_aws_adapter_matrix.py && uv run pytest packages/cnes_infra tests/integration/test_local_adapter_matrix.py tests/integration/test_aws_adapter_matrix.py -m "not postgres" --cov --cov-config=pyproject.toml -q`

Expected: PASS at the existing 100% package branch threshold against all four adapters.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_local_adapter_matrix.py tests/integration/test_aws_adapter_matrix.py \
  packages/cnes_infra pyproject.toml uv.lock docker-compose.yml \
  .github/workflows/python-quality.yml
git commit -m "test(infra): integrate adapter conformance matrix"
```

## Phase 3 — raw ingestion and Edge Agent protocol

### Task 16: CND-030 — Raw validation, delta-chain policy, and immutable registration

**Files:**
- Create: `apps/central_api/src/central_api/services/raw_ingestion.py`
- Create: `apps/central_api/src/central_api/services/delta_policy.py`
- Create: `apps/central_api/tests/services/test_raw_ingestion.py`
- Create: `apps/central_api/tests/services/test_delta_policy.py`
- Modify: none

**Interfaces:**
- Consumes: `RawManifest`, `manifest_sha256`, `ObjectStorePort`, `RawManifestRecord`, `ControlPlanePort.get_job`, `ControlPlanePort.complete_job`, and `ControlPlanePort.latest_succeeded_job(tenant_id, agent_id, source_type, file_subtype, competencia) -> Job | None` added to the CND-012 port before CND-020 implementation.
- Produces: `ResyncReason(StrEnum)` values `SEQUENCE_GAP`, `BASE_UNKNOWN`, `HASH_CHAIN_MISMATCH`, `SCHEMA_INCOMPATIBLE`, `AGENT_RESYNC_REQUIRED`, `BASE_TOO_OLD`, `CHAIN_TOO_LONG`; frozen `RawAcceptance(accepted, manifest_id, full_resync_required, reason)`; frozen `RegisterRawManifest(tenant_id: str, agent_id: str, job_id: str, owner: str, fencing_token: int, manifest: RawManifest, manifest_bytes: bytes, now: datetime)`; `DeltaPolicy(max_base_age=timedelta(days=7), max_chain_length=30)`; `AcceptedManifest = Callable[[RawManifestRecord], None]`; `RawIngestionService(control_plane, object_store, policy, accepted_manifest=noop)`; `register(command: RegisterRawManifest) -> RawAcceptance`.

- [ ] **Step 1: Write failing gap/age/hash tests**

```python
@pytest.mark.parametrize("mutation,reason", [
    ({"sequence": 4}, ResyncReason.SEQUENCE_GAP),
    ({"previous_manifest_sha256": "f" * 64}, ResyncReason.HASH_CHAIN_MISMATCH),
])
def test_delta_invalido_solicita_full(base_manifest, mutation, reason, service, now):
    delta = base_manifest.model_copy(update={"snapshot_mode": "DELTA", **mutation})
    result = service.register(register_command(delta, now))
    assert result == RawAcceptance(False, delta.manifest_id, True, reason)
```

Also test wrong tenant/agent/job identity, a manifest whose source/subtype/competence differs from
the claimed Job, stale fencing token, non-LEASED Job, and canonical manifest bytes that do not match
the parsed model. Every case fails before an object/index/outbox mutation.

- [ ] **Step 2: Prove the service is absent**

Run: `uv run pytest apps/central_api/tests/services/test_delta_policy.py -q`

Expected: collection FAIL importing `central_api.services.delta_policy`.

- [ ] **Step 3: Add fail-closed policy and registration transaction order**

Strongly load `command.job_id` and require the exact authenticated tenant/agent, `LEASED` owner,
current fence, source/subtype/competence, and canonical `manifest_bytes`; do not infer a Job from
source fields or accept tenant/agent authority from the manifest body. For FULL require sequence 1.
For DELTA load the latest succeeded job and its manifest object,
validate snapshot/base IDs, sequence `previous+1`, previous manifest canonical hash, equal schema
version, base age `<=7 days`, and delta count `<30`; limits may be reduced by configuration but
never disabled or raised. Verify the referenced raw object stat equals manifest hash/size before
writing the immutable manifest JSON key
`raw/<tenant>/<source>/<competencia>/<snapshot_id>/manifest.json`. Build the exact
`RawManifestRecord` projection including that key and canonical manifest hash, then complete the Job
so its result fields, chain index, and `raw.manifest.accepted` outbox commit atomically. Any
validation failure writes no manifest/index and leaves the dataset pointer unchanged.
Only after that commit returns, invoke `accepted_manifest(record)`. Scheduling callback failure is
sanitized/logged and does not undo or falsely reject the durable raw acceptance; the outbox plus
CND-060 bounded waiting-run recovery retries launch.

- [ ] **Step 4: Verify service coverage**

Run: `uv run ruff check apps/central_api/src/central_api/services apps/central_api/tests/services && uv run pytest apps/central_api/tests/services --cov=central_api.services --cov-branch --cov-fail-under=90 -q`

Expected: PASS at the app 90% coverage gate.

- [ ] **Step 5: Commit**

```bash
git add apps/central_api/src/central_api/services apps/central_api/tests/services
git commit -m "feat(ingestion): validate raw manifests and delta chains"
```

### Task 17: CND-032 — Go manifest v1 and full-resync handling

**Files:**
- Create: `apps/dump_agent_go/internal/manifest/raw.go`
- Create: `apps/dump_agent_go/internal/manifest/raw_test.go`
- Modify: `apps/dump_agent_go/internal/delta/store.go`
- Modify: `apps/dump_agent_go/internal/delta/store_test.go`
- Modify: `apps/dump_agent_go/internal/queue/envelope.go`
- Modify: `apps/dump_agent_go/internal/queue/envelope_test.go`
- Modify: `apps/dump_agent_go/internal/worker/executor.go`
- Modify: `apps/dump_agent_go/internal/worker/executor_rundelta_test.go`
- Modify: none of `go.mod`, `go.sum`, or generated API files

**Interfaces:**
- Consumes: current `delta.Store`, `delta.PendingTx`, `JobExecutor.RunDelta`, SHA-256 tee, and CND-011 manifest JSON shape.
- Produces: Go `manifest.Raw` with JSON tags matching `RawManifest`; `manifest.Build(BuildRequest) (Raw, error)`; `delta.ChainHead(SourceKey) (snapshotID string, sequence uint32, manifestSHA256 string, createdAt time.Time, ok bool, err error)`; queue envelope fields `JobID string`, `FencingToken uint64`, `ManifestJSON []byte`, and `ManifestSHA256 string`; `FullResync(reason string) error` that clears only the named source key after server acknowledgement.

- [ ] **Step 1: Write failing manifest/chain tests**

```go
func TestBuildDeltaCarriesBaseSequenceAndHash(t *testing.T) {
	got, err := manifest.Build(deltaRequest())
	require.NoError(t, err)
	assert.Equal(t, "DELTA", got.SnapshotMode)
	assert.Equal(t, uint32(2), got.Sequence)
	assert.Equal(t, "base-1", *got.BaseSnapshotID)
	assert.Len(t, *got.PreviousManifestSHA256, 64)
}
```

- [ ] **Step 2: Prove the package is absent**

Run: `cd apps/dump_agent_go && go test -race ./internal/manifest ./internal/delta ./internal/worker`

Expected: compile FAIL because `internal/manifest` does not exist.

- [ ] **Step 3: Add exact manifest JSON and durable chain metadata**

Use RFC3339 UTC timestamps, canonical lowercase SHA-256, manifest version 1, source/file subtype
enums matching Python, and object layout from spec §6.1. Copy the claimed `job_id` and current
`fencing_token` into the durable envelope outside `ManifestJSON`; retries submit the identical
`RawManifestSubmission`, and a newly claimed fence produces a new envelope rather than mutating the
old one. Persist the chain head in bbolt only after the raw-manifest API acknowledgement; a
transport/upload failure retains the prior head. A full-resync response marks the next extraction
FULL and clears committed delta fingerprints only for that `SourceKey`; it never deletes the
durable outbound envelope before acknowledgement.

- [ ] **Step 4: Run Go race and coverage gates**

Run: `cd apps/dump_agent_go && go test -race -count=1 -coverprofile=coverage.out ./... && grep -v -E "internal/apiclient/generated\.go|cmd/|internal/service/|_windows\.go:" coverage.out > coverage.filtered.out && go tool cover -func=coverage.filtered.out | tail -1`

Expected: PASS and filtered total coverage at least 65%.

- [ ] **Step 5: Commit**

```bash
git add apps/dump_agent_go/internal/manifest apps/dump_agent_go/internal/delta \
  apps/dump_agent_go/internal/queue apps/dump_agent_go/internal/worker
git commit -m "feat(edge): emit raw manifest v1 chains"
```

### Task 18: CND-033 — DATASUS national raw adapter

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/ingestion/datasus_cnes_transport.py`
- Create: `packages/cnes_infra/src/cnes_infra/ingestion/datasus_cnes_raw.py`
- Create: `packages/cnes_infra/tests/ingestion/test_datasus_cnes_transport.py`
- Create: `packages/cnes_infra/tests/ingestion/test_datasus_cnes_raw.py`
- Modify: none

**Interfaces:**
- Consumes: CND-011 `RawManifest`, CND-012 `ObjectStorePort`, current `CircuitBreaker`, `requests.Session`, and the CND-002 `cnes-nacional-v1.parquet` source contract.
- Produces: `DatasusCnesRequest(tenant_id, competencia, file_subtype, snapshot_id, agent_id, agent_version)`; `DatasusCnesTransportPort.fetch(request) -> Iterator[Mapping[str, object]]`; `DatasusCnesRawAdapter(transport, store, clock).extract(request) -> RawManifest`.

- [ ] **Step 1: Write failing adapter tests against a transport fake**

```python
def test_adapter_produz_o_mesmo_raw_contract(request, transport, store, clock):
    transport.rows = fixture_rows("cnes-nacional-v1.parquet")
    manifest = DatasusCnesRawAdapter(transport, store, clock.now).extract(request)
    assert manifest.source_type == "CNES_NACIONAL"
    assert manifest.agent_id == "system-datasus"
    assert manifest.snapshot_mode == SnapshotMode.FULL
    assert store.stat(manifest.object_key).sha256 == manifest.object_sha256
```

- [ ] **Step 2: Prove the adapter is absent**

Run: `uv run pytest packages/cnes_infra/tests/ingestion/test_datasus_cnes_raw.py -q`

Expected: collection FAIL importing `cnes_infra.ingestion.datasus_cnes_raw`.

- [ ] **Step 3: Add deterministic raw generation and the approved transport**

Write rows in fixed source-column order with Polars, Zstandard-compressed Parquet, stable null types, and no business reconciliation. The concrete transport must use the official DATASUS distribution endpoint, pagination/file checksum semantics, authentication requirements, and field mapping ratified in the governing spec amendment described under **External decision gate**; do not infer query parameters from the current single-establishment `CnesOficialWebAdapter` and do not retain BigQuery as fallback.

- [ ] **Step 4: Verify adapter determinism and HTTP failure behavior**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/ingestion/datasus_cnes_* packages/cnes_infra/tests/ingestion/test_datasus_cnes_* && uv run pytest packages/cnes_infra/tests/ingestion/test_datasus_cnes_* --cov=cnes_infra.ingestion.datasus_cnes_raw --cov-branch --cov-fail-under=100 -q`

Expected: PASS once the decision gate is resolved; identical fixture rows produce identical Parquet/object hashes.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/ingestion/datasus_cnes_* \
  packages/cnes_infra/tests/ingestion/test_datasus_cnes_*
git commit -m "feat(ingestion): add DATASUS CNES raw adapter"
```

### Task 19: CND-031 — Target raw-upload and Edge Job API

**Files:**
- Create: `apps/central_api/src/central_api/routes/raw_jobs.py`
- Create: `apps/central_api/src/central_api/routes/raw_manifests.py`
- Create: `apps/central_api/src/central_api/schemas/raw_api.py`
- Create: `apps/central_api/tests/routes/test_raw_jobs.py`
- Create: `apps/central_api/tests/routes/test_raw_manifests.py`
- Modify: none

**Interfaces:**
- Consumes: CND-020 SQLite behavior behind `ControlPlanePort`, CND-030 `RawIngestionService`, and server-resolved `tenant_id`/authenticated `agent_id` dependencies.
- Produces: `GET /api/v1/edge/jobs/next`, `POST /api/v1/edge/jobs/{job_id}/heartbeat`, `POST /api/v1/edge/raw-manifests`; request `RawManifestSubmission(job_id: str, fencing_token: int, manifest: RawManifest)`; response `RawManifestResponse(accepted: bool, manifest_id: str, full_resync_required: bool, reason: str | None)`.

- [ ] **Step 1: Write failing tenant/fence/resync route tests**

```python
def test_raw_manifest_rejeita_tenant_do_body_divergente(client, submission):
    body = submission.model_copy(update={
        "manifest": submission.manifest.model_copy(update={"tenant_id": "999999"}),
    }).model_dump(mode="json")
    response = client.post("/api/v1/edge/raw-manifests", json=body,
                           headers=agent_headers("354130", "agent-01"))
    assert response.status_code == 403
    assert response.json()["detail"] == "tenant_mismatch"
```

- [ ] **Step 2: Prove routes are absent**

Run: `uv run pytest apps/central_api/tests/routes/test_raw_jobs.py apps/central_api/tests/routes/test_raw_manifests.py -q`

Expected: FAIL because the test app cannot include `raw_jobs.router`/`raw_manifests.router`.

- [ ] **Step 3: Add thin FastAPI routes with injected ports**

Routes obtain `ControlPlanePort`, `RawIngestionService`, fixed tenant, and agent identity through
`Depends` callables local to the route modules so feature tests override them without editing
`deps.py`. Job discovery returns IDs only; claim revalidates the canonical job/agent. Heartbeat
requires matching owner/fence. Manifest registration builds `RegisterRawManifest` from authenticated
tenant/agent plus the explicit submission Job/fence; it rejects tenant/agent/job mismatch before
object access and maps each `ResyncReason` to HTTP 409 plus the typed resync response.

- [ ] **Step 4: Verify route coverage**

Run: `uv run ruff check apps/central_api/src/central_api/routes/raw_* apps/central_api/src/central_api/schemas/raw_api.py apps/central_api/tests/routes/test_raw_* && uv run pytest apps/central_api/tests/routes/test_raw_* --cov=central_api.routes.raw_jobs --cov=central_api.routes.raw_manifests --cov-branch --cov-fail-under=90 -q`

Expected: PASS and no route imports SQLite, filesystem, SQLAlchemy, or MinIO directly.

- [ ] **Step 5: Commit**

```bash
git add apps/central_api/src/central_api/routes/raw_jobs.py \
  apps/central_api/src/central_api/routes/raw_manifests.py \
  apps/central_api/src/central_api/schemas/raw_api.py apps/central_api/tests/routes/test_raw_*
git commit -m "feat(api): expose target raw ingestion protocol"
```

### Task 20: CND-034 — End-to-end raw ingestion and Phase 3 serial integration

**Files:**
- Create: `tests/integration/test_local_raw_ingestion.py`
- Create: `tests/integration/test_national_raw_ingestion.py`
- Create: `apps/central_api/src/central_api/services/national_ingestion.py`
- Create: `apps/central_api/tests/services/test_national_ingestion.py`
- Modify: `apps/central_api/src/central_api/app.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `apps/central_api/pyproject.toml`
- Modify: `packages/cnes_infra/pyproject.toml`
- Modify: `packages/cnes_infra/src/cnes_infra/ingestion/__init__.py`
- Modify: `packages/cnes_contracts/src/cnes_contracts/__init__.py`
- Modify: `docs/openapi.json` (generated)
- Modify: `docs/contracts/openapi.json` (generated)
- Modify: `docs/contracts/schemas/*.json` (generated)
- Modify: `apps/dump_agent_go/internal/apiclient/generated.go` (generated)
- Modify: `apps/dump_agent_go/internal/apiclient/overlay.yaml`
- Modify: `apps/dump_agent_go/internal/apiclient/adapter.go`
- Modify: `uv.lock`

**Interfaces:**
- Consumes: accepted CND-030–033 and local adapters.
- Produces: `NationalRefreshRequest(tenant_id: str, competencia: str, snapshot_id: str, idempotency_key: str)`; `NationalIngestionService(control_plane, raw_adapter, raw_ingestion, clock).refresh(request) -> RawAcceptance`; local DI graph, generated Python/Go HTTP contract, one full plus one delta Edge-to-filesystem flow, and one centrally claimed DATASUS-to-raw flow with restart persistence.

- [ ] **Step 1: Write the failing end-to-end test**

```python
@pytest.mark.local_profile
def test_full_e_delta_chegam_ao_raw_sem_mudar_dataset_ativo(local_stack, edge_fixture):
    full = local_stack.upload_and_register(edge_fixture.full)
    delta = local_stack.upload_and_register(edge_fixture.delta(previous=full))
    assert full.accepted and delta.accepted
    assert local_stack.store.stat(edge_fixture.full.object_key) is not None
    assert local_stack.control_plane.get_dataset_pointer("354130", "cnes") is None
```

Add a national-path test proving the service idempotently ensures the internal active Agent
`system-datasus`, creates and claims the deterministic CNES_NACIONAL Job, passes that job's current
fence plus the adapter manifest to `RawIngestionService`, and stores a chain-indexed raw manifest.
Neither service may complete a Job directly or bypass raw validation.

- [ ] **Step 2: Prove composition and generated client are stale**

Run: `uv run pytest tests/integration/test_local_raw_ingestion.py -m local_profile -q && cd apps/dump_agent_go && go test ./internal/apiclient`

Expected: FAIL because the app does not compose target adapters/routes and the generated Go client lacks raw-manifest methods.

- [ ] **Step 3: Compose local ports and regenerate shared artifacts**

In `deps.py`, build `ProfileSettings`, `SQLiteControlPlane(data_dir.parent / "state/cnesdata.sqlite3")`, `FilesystemObjectStore(data_dir)`, `RawIngestionService`, and `NationalIngestionService`; store only Protocol-typed values on `app.state`. The national service creates/claims a normal canonical Job owned by the internal active Agent `system-datasus`, calls the CND-033 adapter, canonicalizes the manifest bytes, and registers through `RegisterRawManifest`; it never writes the chain index or succeeds the Job itself. Include the new routers in `app.py`. Add direct package dependencies, lock them, generate both OpenAPI files, generate Pydantic schemas, update the Go overlay, regenerate `generated.go`, and wire the CND-032 queued manifest acknowledgement/full-resync path. Migration mode may still include legacy routers, but `PROFILE=local` must never construct the legacy engine or MinIO wrapper.
The Go adapter sends its durable envelope as the exact `RawManifestSubmission(job_id,
fencing_token,manifest)` and derives neither job nor fence from mutable process state during retry.

- [ ] **Step 4: Run the Phase 3 gate**

Run: `uv run pytest tests/integration/test_local_raw_ingestion.py tests/integration/test_national_raw_ingestion.py apps/central_api/tests/routes/test_raw_* apps/central_api/tests/services/test_national_ingestion.py -m local_profile -q && uv run ruff check apps/central_api packages/cnes_infra && cd apps/dump_agent_go && go test -race -count=1 ./... && cd ../web_dashboard && bun run codegen`

Expected: PASS; generated artifacts have no unstaged drift after rerunning their generators.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_local_raw_ingestion.py \
  tests/integration/test_national_raw_ingestion.py apps/central_api \
  packages/cnes_infra packages/cnes_contracts/src/cnes_contracts \
  docs/openapi.json docs/contracts apps/dump_agent_go/internal/apiclient uv.lock
git commit -m "feat(local): integrate raw ingestion vertical"
```

## Phase 4 — jobs, runs, fan-out, and atomic publication

### Task 21: CND-040 — Edge Job lifecycle service

**Files:**
- Create: `apps/central_api/src/central_api/services/job_lifecycle.py`
- Create: `apps/central_api/tests/services/test_job_lifecycle.py`
- Modify: none

**Interfaces:**
- Consumes: CND-010 commands/errors, `ControlPlanePort`, and server-resolved agent identity.
- Produces: `JobLifecycle(control_plane, clock)` with `claim(command: ClaimJob) -> Job | None`, `renew(command: RenewJobLease) -> Job`, `complete(command: CompleteJob) -> Job`, `fail(command: FailJob) -> Job`, and `request_cancel(command: CancelJob) -> Job`; every mutation constructs a typed outbox event.

- [ ] **Step 1: Write failing revoked-agent and stale-fence tests**

```python
def test_agente_revogado_nao_reclama_job(service, revoked_agent, pending_job):
    with pytest.raises(AgentRevoked):
        service.claim(claim_for(pending_job, revoked_agent))


def test_complete_rejeita_fence_antigo(service, leased_job):
    with pytest.raises(FenceRejected):
        service.complete(complete_for(leased_job, fencing_token=leased_job.fencing_token - 1))
```

- [ ] **Step 2: Prove the service is absent**

Run: `uv run pytest apps/central_api/tests/services/test_job_lifecycle.py -q`

Expected: collection FAIL importing `central_api.services.job_lifecycle`.

- [ ] **Step 3: Add lifecycle orchestration without storage knowledge**

Sanitize failure codes to `^[A-Z0-9_]{1,64}$`, cap attempts at the configured positive maximum, refuse completion by revoked agents, and emit `job.claimed`, `job.lease_renewed`, `job.succeeded`, `job.failed`, or `job.cancel_requested`. Never inspect SQLite rows or filesystem paths in the service.

- [ ] **Step 4: Verify negative paths and coverage**

Run: `uv run ruff check apps/central_api/src/central_api/services/job_lifecycle.py apps/central_api/tests/services/test_job_lifecycle.py && uv run pytest apps/central_api/tests/services/test_job_lifecycle.py --cov=central_api.services.job_lifecycle --cov-branch --cov-fail-under=90 -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/central_api/src/central_api/services/job_lifecycle.py \
  apps/central_api/tests/services/test_job_lifecycle.py
git commit -m "feat(api): enforce Edge Job lifecycle"
```

### Task 22: CND-041 — Run planner, fan-out, and fan-in

**Files:**
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/planner.py`
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/fan_in.py`
- Create: `packages/cnes_domain/tests/orchestration/test_planner.py`
- Create: `packages/cnes_domain/tests/orchestration/test_fan_in.py`
- Modify: none

**Interfaces:**
- Consumes: `Run`, its server-selected `RunDependency` tuple, `RunUnit`, `RunDispatch`, `RunStage`, deterministic `unit_id(RunUnitIdentity(...))`, and state enums.
- Produces: frozen `RawManifestRef(manifest_id: str, manifest_key: str, source_type: str, file_subtype: str, partition: str)`; `PlanRequest(run: Run, manifests: tuple[RawManifestRef, ...], deployment_limit: int)`; `RunPlan(run: Run, units: tuple[RunUnit, ...], missing_required: tuple[str, ...], missing_optional: tuple[str, ...], deployment_limit: int)`; `plan_run(request: PlanRequest) -> RunPlan`; `ready_units(plan: RunPlan, now: datetime) -> tuple[RunUnit, ...]`; `logical_wave_id(units: tuple[RunUnit, ...]) -> str`; `execution_request(plan: RunPlan, dispatch: RunDispatch, max_concurrency: int) -> StartRunExecution`; `FanInDecision(state: RunState, missing_sources: tuple[str, ...], publish_ready: bool)`; `decide_fan_in(plan: RunPlan) -> FanInDecision`. The generic planner contains no hard-coded dataset/source table and never allocates a dispatch generation.

- [ ] **Step 1: Write failing deterministic fan-out/fan-in tests**

```python
def test_planner_cria_uma_unit_por_source_subtype_partition(request):
    first, second = plan_run(request), plan_run(request)
    assert first.units == second.units
    assert len({unit.unit_id for unit in first.units}) == len(first.units)
    normalize = tuple(u for u in first.units if u.stage is RunStage.NORMALIZE)
    reconcile = next(u for u in first.units if u.stage is RunStage.RECONCILE)
    materialize = next(u for u in first.units if u.stage is RunStage.MATERIALIZE)
    assert reconcile.depends_on_unit_ids == tuple(u.unit_id for u in normalize)
    assert materialize.depends_on_unit_ids == (reconcile.unit_id,)


def test_fonte_opcional_ausente_degrada_sem_fingir_completo(run, succeeded_required):
    decision = decide_fan_in(completed_plan(run, succeeded_required))
    assert decision.state == RunState.PUBLISHING
    assert decision.publish_ready is True
    assert decision.missing_sources == ("CNES_NACIONAL/CNES_VINCULO",)


def test_retry_mantem_wave_logica_e_request_usa_dispatch_reservado(plan, now):
    first_ready = ready_units(plan, now)
    retried_ready = ready_units(with_retryable_attempt(plan), now)
    assert logical_wave_id(retried_ready) == logical_wave_id(first_ready)
    dispatch = reserved_dispatch(first_ready, dispatch_id="fedcba9876543210")
    request = execution_request(plan, dispatch, max_concurrency=4)
    assert request.dispatch_id == dispatch.dispatch_id
    assert request.unit_ids == dispatch.unit_ids


def test_lease_ativa_bloqueia_nova_dispatch(plan, now):
    assert ready_units(with_live_lease(plan, now), now) == ()
```

Also add `test_planner_rejeita_manifesto_nao_declarado_no_run` and a parameterized fan-in matrix
showing that the generic functions obey arbitrary required/optional `RunDependency` values rather
than recognizing CNES names. Add `test_so_normalize_fica_ready_na_primeira_onda`,
`test_reconcile_espera_todas_as_normalizacoes_congeladas`,
`test_materialize_espera_reconcile`, `test_stage_altera_unit_id`, and
`test_required_ausente_nao_cria_dag_executavel`. Add tests that an expired `LEASED` unit becomes
ready with next attempt, a live lease anywhere in the Run blocks every new dispatch, an optional
normalization `SUCCEEDED_DEGRADED` satisfies reconciliation while a required final failure fails the
Run, and a downstream final failure always fails the Run.

- [ ] **Step 2: Prove orchestration modules are absent**

Run: `uv run pytest packages/cnes_domain/tests/orchestration -q`

Expected: collection FAIL importing `cnes_domain.orchestration`.

- [ ] **Step 3: Add the deterministic three-stage DAG and explicit dependency evaluation**

`plan_run` accepts only immutable manifest sidecar refs matching one of `run.dependencies`, groups
them by source/subtype/partition, and never fans out per row. An undeclared source/subtype,
duplicate manifest/key, or mixed partition chain is a contract error. If any required dependency
is absent, return `RunPlan(units=(), missing_required=(...))` with `WAITING_INPUTS` semantics; do
not create an executable partial DAG. Once all required inputs exist, freeze the optional inputs
present at that moment in `missing_optional` and
create exactly this acyclic graph:

1. one `NORMALIZE` unit per source/subtype/partition, with direct raw `input_manifests` and no
   predecessor IDs;
2. one `RECONCILE` unit with null source/subtype and every normalization unit ID as predecessors;
3. one `MATERIALIZE` unit with null source/subtype and only the reconciliation unit as predecessor.

Sort normalization units by `(source_type,file_subtype,partition)` and append reconciliation then
materialization; reject more than 20 total units so DynamoDB cancellation plus Run/outbox stays
within one 25-item transaction. IDs include `RunStage`; repeated planning over the same frozen
inputs is byte-stable. `ready_units(plan, now)` first returns empty when any unit has a non-expired
`LEASED` lease, guaranteeing at most one active dispatch per Run. Otherwise it considers
`PENDING`, `FAILED_RETRYABLE`, and expired `LEASED` units whose predecessors are satisfied;
`SUCCEEDED` and `SUCCEEDED_DEGRADED` are satisfied predecessor states, with degraded predecessors
contributing no manifests. Downstream inputs are resolved later only from committed `ManifestRef`s.
`logical_wave_id` is the first 16 lowercase hex characters of SHA-256 over the ordered ready unit
IDs, so the same logical unit set retains one wave ID across retries. The caller must reserve that
wave through `ControlPlanePort.reserve_run_dispatch` before building an executor request.
`execution_request` accepts that persisted `RunDispatch`, rejects a non-`RESERVED|STARTED`
dispatch, tenant/run mismatch, a unit absent from the persisted plan, or a `wave_id` different from
`logical_wave_id` over the dispatch's own ordered unit tuple,
and copies the allocated `dispatch_id` verbatim. It carries only the dispatch's ordered unit IDs and
clamps `max_concurrency` to `min(len(dispatch.unit_ids), plan.deployment_limit, max_concurrency)`
while requiring both limits positive. It never hashes attempts or invents a generation. Reject an
empty ready wave instead of starting a no-op execution.

`decide_fan_in` returns FAILED for a final-failed required normalization or any final-failed
downstream stage. A final-failed optional normalization is persisted as `SUCCEEDED_DEGRADED`, adds
that dependency to `Run.missing_sources`, satisfies the reconciliation edge, and contributes no
input manifests. Return WAITING_INPUTS before a complete required input set; PROCESSING while the
DAG has ready/running/retryable work; and PUBLISHING with `publish_ready=True` only after the unique
materialization unit succeeds. An absent optional dependency is recorded in stable
`source_type/file_subtype` order and leads to `PUBLISHED_DEGRADED` only after the publisher commits;
the planner itself never labels an uncommitted run published. Dataset-specific requirements are
selected server-side when `Run` is created and never supplied by a browser. CNES values are frozen
in CND-060 `SourcePipeline`; later source plugins add values without modifying this generic DAG.

- [ ] **Step 4: Verify properties and coverage**

Run: `uv run ruff check packages/cnes_domain/src/cnes_domain/orchestration packages/cnes_domain/tests/orchestration && uv run pytest packages/cnes_domain/tests/orchestration --cov=cnes_domain.orchestration --cov-branch --cov-fail-under=100 -q`

Expected: PASS with Hypothesis input-order invariance.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_domain/src/cnes_domain/orchestration \
  packages/cnes_domain/tests/orchestration
git commit -m "feat(domain): plan deterministic processing runs"
```

### Task 23: CND-042 — Local worker-pool and AWS executor adapters

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/executor/local_pool.py`
- Create: `packages/cnes_infra/src/cnes_infra/executor/step_functions.py`
- Create: `packages/cnes_infra/tests/executor/test_local_pool.py`
- Create: `packages/cnes_infra/tests/executor/test_step_functions.py`
- Modify: none

**Interfaces:**
- Consumes: CND-012 `ProcessorExecutorPort` and `RunUnitMessage`, CND-041 `RunUnit`.
- Produces: `RunUnitHandler = Callable[[RunUnitMessage], RunUnit]`;
  `LocalWorkerPool(handler: RunUnitHandler, owner: str, clock: Callable[[], datetime],
  lease_seconds: int)` and `StepFunctionsExecutor(client: botocore.client.BaseClient,
  state_machine_arn: str)`, both implementing `start(request: StartRunExecution) -> str` and
  `cancel(request: CancelRunExecution) -> None`, plus `status(execution_ref: str) -> ExecutionStatus`. Local `start` returns
  `local:<run_id>:<dispatch_id>`; AWS `start` returns the Standard execution ARN for that dispatch.
  Effective concurrency is the request's already-clamped `max_concurrency`.

- [ ] **Step 1: Write failing concurrency/cancel tests**

```python
def test_pool_nunca_excede_limite(unit_batch):
    probe = ConcurrencyProbe()
    handler = recording_handler(unit_batch, probe)
    pool = LocalWorkerPool(handler, "local-worker", utc_now, lease_seconds=300)
    ref = pool.start(StartRunExecution(tenant_id="354130", run_id="run-1",
        wave_id="0123456789abcdef",
        dispatch_id="fedcba9876543210",
        unit_ids=tuple(unit.unit_id for unit in unit_batch), max_concurrency=2))
    pool.close()
    assert ref == "local:run-1:fedcba9876543210"
    assert probe.maximum == 2


def test_step_functions_envia_ids_e_max_concurrency(sfn_client, request):
    executor = StepFunctionsExecutor(
        sfn_client, "arn:aws:states:us-east-1:1:stateMachine:cnes")
    ref = executor.start(request)
    payload = json.loads(sfn_client.start_execution.call_args.kwargs["input"])
    assert payload == {"tenant_id": request.tenant_id, "run_id": request.run_id,
                       "wave_id": request.wave_id,
                       "dispatch_id": request.dispatch_id,
                       "unit_ids": list(request.unit_ids),
                       "max_concurrency": request.max_concurrency}
    assert ref.startswith("arn:aws:states:")


def test_status_mapeia_estado_terminal(sfn_client, executor):
    sfn_client.describe_execution.return_value = {"status": "FAILED"}
    assert executor.status("arn:aws:states:us-east-1:1:execution:cnes:x") is ExecutionStatus.FAILED
```

- [ ] **Step 2: Prove the executor is absent**

Run: `uv run pytest packages/cnes_infra/tests/executor/test_local_pool.py packages/cnes_infra/tests/executor/test_step_functions.py -q`

Expected: collection FAIL importing `cnes_infra.executor.local_pool` or `step_functions`.

- [ ] **Step 3: Add storage-neutral local and Step Functions execution**

Local: create `ThreadPoolExecutor(max_workers=request.max_concurrency)`, construct one validated
`RunUnitMessage` per ordered unit ID, and call the injected handler as `handler(message)`. Reject invalid
wave/dispatch/unit IDs or limits and deduplicate active `(run_id,dispatch_id,unit_id)`. Track a
per-run `threading.Event`; cancel sets it best-effort. Local `status` reports `RUNNING` while any
future for the ref is live and then `SUCCEEDED|FAILED|CANCELED` from the recorded batch result. AWS:
call `StartExecution` with a deterministic
tenant/run/dispatch execution name and JSON containing only tenant/run/wave/dispatch/unit IDs plus
`max_concurrency`; AWS-012 owns the Standard Inline
Map/ECS state-machine definition. This permits distinct normalize, reconcile, and materialize
dispatches without a Step Functions name collision, and idempotent replay of the same reserved
dispatch returns the existing execution ref. Cancel calls `StopExecution` only when
`execution_ref` is present; `status` maps `DescribeExecution` states to the four canonical values.
Neither adapter mutates Run/RunUnit state; fencing remains in the control plane.

- [ ] **Step 4: Verify concurrency and coverage**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/executor packages/cnes_infra/tests/executor && uv run pytest packages/cnes_infra/tests/executor --cov=cnes_infra.executor --cov-branch --cov-fail-under=100 -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/executor packages/cnes_infra/tests/executor
git commit -m "feat(infra): add processing executor adapters"
```

### Task 24: CND-043 — Unit attempt execution and fenced commit

**Files:**
- Create: `apps/data_processor/src/data_processor/orchestration/attempt_store.py`
- Create: `apps/data_processor/src/data_processor/orchestration/unit_worker.py`
- Create: `apps/data_processor/src/data_processor/orchestration/unit_handler.py`
- Create: `apps/data_processor/tests/orchestration/test_unit_worker.py`
- Create: `tests/chaos/test_stale_unit_fence.py`
- Modify: none

**Interfaces:**
- Consumes: `ControlPlanePort`, `ObjectStorePort`, CND-041 staged units, and processor callback `Callable[[RunUnit, ObjectStorePort], tuple[OutputManifest, ...]]`. CND-060 owns the source-stage controller that dispatches from `RunUnit.stage`, passes the attempt-scoped store to exactly one stage, and unwraps `NormalizeResult.manifests`, `(ReconcileResult.reconciliation_manifest, ReconcileResult.divergence_manifest)`, or `MaterializeResult.manifests` into this callback boundary.
- Produces: `attempt_object_key(prefix: str, logical_key: str) -> str`; `AttemptObjectStore(delegate: ObjectStorePort, prefix: str, inputs: Mapping[str, str] = {})` with `with_inputs(inputs: Mapping[str, str]) -> AttemptObjectStore`; writes/current-output stats map to `tmp/<tenant>/<run_id>/<unit_id>/<attempt>/<logical_key>`, while reads are allowed only through an explicit logical-to-physical input map; frozen `UnitWorkerDependencies(control_plane: ControlPlanePort, store: ObjectStorePort, processor: Callable[[RunUnit, ObjectStorePort], tuple[OutputManifest, ...]], clock: Callable[[], datetime])`; frozen `UnitWorkerPolicy(max_attempts: int = 3, after_persist: Callable[[RunUnit], None] = noop)`; `UnitWorker(dependencies: UnitWorkerDependencies, policy: UnitWorkerPolicy | None = None)` constructs the frozen default policy internally; `execute(command: ClaimRunUnit) -> RunUnit`; `RunUnitCommandHandler(worker: UnitWorker)` with `handle(message: RunUnitMessage) -> RunUnit`; manifest sidecar `tmp/<tenant>/<run_id>/<unit_id>/<attempt>/manifests/<manifest_id>.json`; fenced `CommitRunUnit.output_manifests: tuple[ManifestRef, ...]` in deterministic logical object-key order.

- [ ] **Step 1: Write failing stale-worker test**

```python
def test_worker_atrasado_nao_commita_output(worker, reclaimed_unit, stale_command):
    worker.processor = lambda unit, attempt_store: (valid_output(unit, attempt_store),)
    with pytest.raises(FenceRejected):
        worker.execute(stale_command)
    assert reclaimed_unit.output_manifests == ()


def test_handler_converte_ids_em_claim_exato(worker, now):
    handler = RunUnitCommandHandler(worker)
    message = RunUnitMessage(
        tenant_id="354130", run_id="run-1", wave_id="0123456789abcdef",
        dispatch_id="fedcba9876543210", unit_id="unit-1", owner="worker-1",
        now=now, lease_seconds=300)
    handler.handle(message)
    assert worker.last_command == ClaimRunUnit(
        tenant_id="354130", run_id="run-1", unit_id="unit-1",
        dispatch_id="fedcba9876543210", owner="worker-1", now=now, lease_seconds=300)


def test_dispatch_antigo_nao_commita_apos_supersede(worker, old_dispatch_command):
    worker.processor = lambda unit, attempt_store: (valid_output(unit, attempt_store),)
    worker.control_plane.activate_new_dispatch("run-1", "1111111111111111")
    with pytest.raises(FenceRejected, match="stale_dispatch"):
        worker.execute(old_dispatch_command)
```

Also prove that `put("normalized/...")` writes only below the current attempt prefix,
`open("raw/...")` resolves only after `with_inputs({logical: physical})`, a non-allowlisted read is
rejected, and an input mapping cannot target the current unit's output prefix. Assert
`after_persist` receives the fresh unit returned by a successful commit or persisted failure, and
is never called when a fence rejects the state update itself. Assert a retryable processor failure
becomes `FAILED_FINAL` on the configured final attempt and cannot create a fourth claim.

- [ ] **Step 2: Prove the worker is absent**

Run: `uv run pytest apps/data_processor/tests/orchestration/test_unit_worker.py tests/chaos/test_stale_unit_fence.py -q`

Expected: collection FAIL importing `data_processor.orchestration.unit_worker`.

- [ ] **Step 3: Add claim-process-validate-commit order**

`RunUnitCommandHandler.handle` validates nonblank IDs/owner, lowercase 16-hex wave/dispatch IDs,
UTC `now`, and positive lease seconds, builds
the exact `ClaimRunUnit`, and returns `worker.execute(command)`; it contains no stage/storage logic.
The local pool and AWS process entrypoint both call this handler shape. Claim first; calculate the
attempt prefix and pass an input-empty `AttemptObjectStore` to the
processor so stage requests keep exact final logical `target_keys` while every physical write
remains under `tmp/`. `put` always writes below the current attempt. `open` may read only an exact
logical key allowlisted by `with_inputs`; `stat` first checks a current-attempt output, then an
allowlisted input. Reject traversal, absolute keys, duplicate logical mappings, and physical input
keys below the current unit's attempt prefix. `promote` is unavailable to stage code and `delete`
can delete only a current-attempt output.

Require a non-empty output tuple, reject duplicate manifest IDs/object keys, sort by logical object
key, and validate every manifest schema/hash against `attempt_store.stat(manifest.object_key)`.
Serialize each manifest canonically to its immutable attempt-sidecar key and create the
corresponding `ManifestRef`; then issue one `commit_run_unit`. Require same owner/current fence and
the claimed unit's exact `dispatch_id`; the control-plane transaction also requires parent
`PROCESSING` and that same active `RunDispatch`, then stores all refs atomically. `fail_run_unit`
uses the same dispatch/Run/owner/fence conditions. Thus an old completion after supersession or
after `CANCEL_REQUESTED` is rejected even before cancellation finalization. On a
retryable exception below the positive `max_attempts`, write a sanitized retryable failure/outbox
event and retain temporary objects. At the limit, `fail_run_unit` atomically persists
`SUCCEEDED_DEGRADED` plus the parent missing-source marker only for an optional `NORMALIZE` unit;
it persists `FAILED_FINAL` for required normalization or either downstream stage. Retries reuse `unit_id`
with the incremented attempt. The worker never chooses a stage, resolves a
predecessor, or starts a downstream wave. Only after `commit_run_unit` or `fail_run_unit` returns,
invoke `after_persist(persisted_unit)`; those responsibilities remain in the CND-060 controller so
the same fenced worker is reusable in local and AWS profiles. Resolve `policy or
UnitWorkerPolicy()` once at construction and validate the resolved value; the two value objects keep
the worker constructor below the repository's four-argument limit without hiding dependencies in
globals or evaluating a constructor call in a default argument.

- [ ] **Step 4: Verify unit and chaos suites**

Run: `uv run ruff check apps/data_processor/src/data_processor/orchestration apps/data_processor/tests/orchestration tests/chaos/test_stale_unit_fence.py && uv run pytest apps/data_processor/tests/orchestration tests/chaos/test_stale_unit_fence.py -q`

Expected: PASS; stale output exists only under `tmp/` and is never committed.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/orchestration \
  apps/data_processor/tests/orchestration tests/chaos/test_stale_unit_fence.py
git commit -m "feat(processor): fence unit attempt commits"
```

### Task 25: CND-044 — Atomic dataset publisher

**Files:**
- Create: `apps/data_processor/src/data_processor/orchestration/publisher.py`
- Create: `apps/data_processor/tests/orchestration/test_publisher.py`
- Create: `tests/chaos/test_publisher_pointer_recovery.py`
- Modify: none

**Interfaces:**
- Consumes: `ObjectStorePort`, `ControlPlanePort.publish_dataset`, `RunManifest`, `DatasetVersion`,
  `PublishDataset`, `PublicationPermit`, and satisfied `RunUnit`s.
- Produces: `PublicationPolicy = Callable[[Run], PublicationPermit]`;
  `allow_publication(run: Run) -> PublicationPermit`; `PublishRequest(run: Run,
  units: tuple[RunUnit, ...], expected_version_id: str | None, now: datetime)`;
  `PublishResult(version: DatasetVersion, pointer: DatasetPointer, run_manifest: RunManifest)`;
  `DatasetPublisher(store: ObjectStorePort, control_plane: ControlPlanePort,
  publication_policy: PublicationPolicy = allow_publication)`;
  `publish(request: PublishRequest) -> PublishResult`.

- [ ] **Step 1: Write failing pre-CAS recovery test**

```python
def test_falha_antes_do_cas_preserva_pointer(publisher, request, current_pointer):
    publisher.store.fail_after_promotions = True
    with pytest.raises(ObjectStoreError):
        publisher.publish(request)
    assert publisher.control_plane.get_dataset_pointer("354130", "cnes") == current_pointer


def test_policy_forte_roda_imediatamente_antes_da_transacao(publisher, request):
    permit = PublicationPermit(
        tenant_id=request.run.tenant_id, run_id=request.run.run_id,
        policy_version=7, fencing_token=3)
    publisher.publication_policy = Mock(return_value=permit)
    publisher.publish(request)
    assert publisher.control_plane.calls[-1].command.publication_permit is permit
```

- [ ] **Step 2: Prove the publisher is absent**

Run: `uv run pytest apps/data_processor/tests/orchestration/test_publisher.py -q`

Expected: collection FAIL importing `data_processor.orchestration.publisher`.

- [ ] **Step 3: Implement the seven publication steps exactly**

Require every unit to be `SUCCEEDED|SUCCEEDED_DEGRADED`, every succeeded unit to have non-empty
`output_manifests`, every degraded unit to have none, exactly one succeeded
`MATERIALIZE` unit, and `run.state=PUBLISHING`; open each ref's immutable manifest sidecar, validate
its canonical `OutputManifest` and matching `manifest_id`, and reject duplicate IDs/object keys
across units. For each manifest call
`store.promote(attempt_object_key(unit_attempt_prefix, manifest.object_key), manifest.object_key,
manifest.object_sha256)` and stat/hash the final key. Write immutable
`reconciliation/<tenant>/<competencia>/<run_id>/run-manifest.json`; build immutable
`DatasetVersion(version_id=run_id)`; choose `PUBLISHED_DEGRADED` iff `run.missing_sources` is
non-empty, otherwise `PUBLISHED`. Immediately before constructing/calling `publish_dataset`, invoke `publication_policy(run)` exactly
once and validate that the returned permit has the same tenant/run. Pass that same instance into
`PublishDataset.publication_permit`; no route/coordinator/caller supplies authorization captured
earlier. `allow_publication` returns the matching local permit with policy version/fence zero. A
billing policy may perform a strong read and place its typed `PublicationGuard` in
`binding_context`; local leaves it `None`. The integrated adapter transaction rechecks the guard's
account/version/fence without redefining `PublicationPermit`. The `publish_dataset` CAS atomically stores the version,
advances the pointer, finalizes the Run with its missing sources, and creates
`reconciliation.published` outbox; return only after rereading the active pointer and Run. On CAS
conflict raise `PointerConflict`; never overwrite another winner or revert a committed pointer.

- [ ] **Step 4: Verify publisher and crash coverage**

Run: `uv run ruff check apps/data_processor/src/data_processor/orchestration/publisher.py apps/data_processor/tests/orchestration/test_publisher.py tests/chaos/test_publisher_pointer_recovery.py && uv run pytest apps/data_processor/tests/orchestration/test_publisher.py tests/chaos/test_publisher_pointer_recovery.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/orchestration/publisher.py \
  apps/data_processor/tests/orchestration/test_publisher.py \
  tests/chaos/test_publisher_pointer_recovery.py
git commit -m "feat(processor): publish datasets with pointer CAS"
```

### Task 26: CND-045 — Claims, fences, publishers, and audit crash gate

**Files:**
- Create: `tests/property/test_local_control_plane_races.py`
- Create: `tests/chaos/test_local_publication_crashes.py`
- Create: `tests/chaos/test_outbox_audit_replay.py`
- Modify: `pyproject.toml`
- Modify: `.github/workflows/python-quality.yml`

**Interfaces:**
- Consumes: integrated CND-040–044 and the local adapter matrix.
- Produces: serial Phase 4 evidence for dual claim, stale fence, dual publisher, pre-CAS failure, post-CAS/pre-audit crash, and replay.

- [ ] **Step 1: Write the failing post-commit audit test**

```python
@pytest.mark.chaos
def test_crash_apos_pointer_antes_do_sink_reenvia_sem_republicar(stack, request):
    result = stack.publisher.publish(request)
    stack.sink.fail = True
    assert stack.dispatch_once().failed == 1
    stack.restart(); stack.sink.fail = False
    assert stack.dispatch_once().delivered == 1
    assert stack.pointer() == result.pointer
    assert stack.sink.count("reconciliation.published") == 1
```

- [ ] **Step 2: Prove the integrated crash suite is missing**

Run: `uv run pytest tests/property/test_local_control_plane_races.py tests/chaos/test_local_publication_crashes.py tests/chaos/test_outbox_audit_replay.py -q`

Expected: FAIL before the new tests/harness are added.

- [ ] **Step 3: Add deterministic barriers and CI markers**

Use `threading.Barrier` rather than sleeps for the two-actor races. Repeat each race 100 times, assert exactly one winner, and preserve losing temporary/final immutable objects for diagnostic cleanup. Register existing `race`/`chaos` markers only if missing and add these local tests to CI; no Compose/AWS emulator changes belong here.

- [ ] **Step 4: Run the Phase 4 gate**

Run: `uv run ruff check tests/property/test_local_control_plane_races.py tests/chaos/test_local_publication_crashes.py tests/chaos/test_outbox_audit_replay.py && uv run pytest tests/property/test_local_control_plane_races.py tests/chaos/test_local_publication_crashes.py tests/chaos/test_outbox_audit_replay.py -q`

Expected: PASS for 100 repetitions per race.

- [ ] **Step 5: Commit**

```bash
git add tests/property/test_local_control_plane_races.py \
  tests/chaos/test_local_publication_crashes.py tests/chaos/test_outbox_audit_replay.py \
  pyproject.toml .github/workflows/python-quality.yml
git commit -m "test(local): gate fencing and publication recovery"
```

## Phase 5 — CNES processing vertical slice

### Task 27: CND-050 — Normalize `CNES_LOCAL` with delta reconstruction

**Files:**
- Create: `apps/data_processor/src/data_processor/pipeline/delta_reconstruction.py`
- Create: `apps/data_processor/src/data_processor/pipeline/normalize_cnes_local.py`
- Create: `apps/data_processor/tests/pipeline/test_delta_reconstruction.py`
- Create: `apps/data_processor/tests/pipeline/test_normalize_cnes_local.py`
- Modify: none

**Interfaces:**
- Consumes: `cnes_contracts.manifests.processing.NormalizeRequest`, `NormalizeResult`, CND-011 raw/output manifests, `ObjectStorePort`, and the existing Polars CNES local column mappings (move no legacy SQL behavior).
- Produces: `normalize_cnes_local(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult`; normalized schema `cnes-normalized-v1` with source natural-key columns plus `_source_manifest_id`, `_source_snapshot_id`, `_source_type`, `_normalized_at`.

- [ ] **Step 1: Write failing delta reconstruction/golden tests**

```python
def test_delta_aplica_iud_sobre_full_sem_perder_proveniencia(store, request):
    result = normalize_cnes_local(request, store)
    assert len(result.manifests) == 1
    frame = read_output(store, result.manifests[0])
    assert frame.select("CNES").to_series().to_list() == ["1234567", "7654321"]
    assert set(frame["_source_type"]) == {"CNES_LOCAL"}
```

- [ ] **Step 2: Prove the normalizer is absent**

Run: `uv run pytest apps/data_processor/tests/pipeline/test_delta_reconstruction.py apps/data_processor/tests/pipeline/test_normalize_cnes_local.py -q`

Expected: collection FAIL importing the pipeline modules.

- [ ] **Step 3: Add deterministic Polars normalization**

Require exactly one `request.target_keys` entry equal to
`normalized/<tenant>/CNES_LOCAL/<competencia>/<run_id>/cnes_local.parquet`. Reconstruct from the
known FULL base by applying DELTAs in sequence/hash order; reject duplicate/gap/reorder before
materialization; deletes use the CND-002 natural key and I/U replace that key. Normalize
strings/dates/codes/nullability using expressions and `with_columns`, never mutate inputs, sort by
natural key, write Zstandard Parquet to `request.target_keys[0]`, and return
`NormalizeResult(manifests=(verified_manifest,))` after stat/hash verification.

- [ ] **Step 4: Verify app coverage and golden bytes**

Run: `uv run ruff check apps/data_processor/src/data_processor/pipeline apps/data_processor/tests/pipeline && uv run pytest apps/data_processor/tests/pipeline/test_delta_reconstruction.py apps/data_processor/tests/pipeline/test_normalize_cnes_local.py --cov=data_processor.pipeline --cov-branch --cov-fail-under=90 -q`

Expected: PASS; two runs over identical inputs have identical rows/schema/hash.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/pipeline \
  apps/data_processor/tests/pipeline/test_delta_reconstruction.py \
  apps/data_processor/tests/pipeline/test_normalize_cnes_local.py
git commit -m "feat(processor): normalize local CNES snapshots"
```

### Task 28: CND-051 — Normalize `CNES_NACIONAL`

**Files:**
- Create: `apps/data_processor/src/data_processor/pipeline/normalize_cnes_nacional.py`
- Create: `apps/data_processor/tests/pipeline/test_normalize_cnes_nacional.py`
- Modify: none

**Interfaces:**
- Consumes: `NormalizeRequest`, `NormalizeResult`, CND-033 raw schema, `ObjectStorePort`.
- Produces: `normalize_cnes_nacional(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult`, also using normalized schema `cnes-normalized-v1` and the same provenance columns.

- [ ] **Step 1: Write the failing national golden test**

```python
def test_nacional_gera_schema_normalizado_compativel(store, request):
    result = normalize_cnes_nacional(request, store)
    assert len(result.manifests) == 1
    frame = read_output(store, result.manifests[0])
    assert frame.schema == EXPECTED_CNES_NORMALIZED_SCHEMA
    assert set(frame["_source_type"]) == {"CNES_NACIONAL"}
```

- [ ] **Step 2: Prove the function is absent**

Run: `uv run pytest apps/data_processor/tests/pipeline/test_normalize_cnes_nacional.py -q`

Expected: collection FAIL importing `normalize_cnes_nacional`.

- [ ] **Step 3: Add source-only mapping and provenance**

Require exactly one `request.target_keys` entry equal to
`normalized/<tenant>/CNES_NACIONAL/<competencia>/<run_id>/cnes_nacional.parquet`. Map the frozen
DATASUS source columns to the exact CND-050 normalized schema; no BigQuery client/cache import is
allowed. Pad CNES/CBO codes, parse competência/dates strictly, preserve source nulls, sort by
natural key, write the exact `request.target_keys[0]`, and return
`NormalizeResult(manifests=(verified_manifest,))`.

- [ ] **Step 4: Verify compatibility and coverage**

Run: `uv run ruff check apps/data_processor/src/data_processor/pipeline/normalize_cnes_nacional.py apps/data_processor/tests/pipeline/test_normalize_cnes_nacional.py && uv run pytest apps/data_processor/tests/pipeline/test_normalize_cnes_nacional.py --cov=data_processor.pipeline.normalize_cnes_nacional --cov-branch --cov-fail-under=90 -q`

Expected: PASS and schema equality with CND-050.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/pipeline/normalize_cnes_nacional.py \
  apps/data_processor/tests/pipeline/test_normalize_cnes_nacional.py
git commit -m "feat(processor): normalize national CNES raw"
```

### Task 29: CND-052 — Reconcile CNES competence

**Files:**
- Create: `apps/data_processor/src/data_processor/pipeline/reconcile_cnes.py`
- Create: `apps/data_processor/tests/pipeline/test_reconcile_cnes.py`
- Modify: none

**Interfaces:**
- Consumes: generic `ReconcileRequest.normalized_manifests`, `ReconcileResult`, CND-050/051 `cnes-normalized-v1` manifests, and the CND-002 locked precedence fixture.
- Produces: `reconcile_cnes(request: ReconcileRequest, store: ObjectStorePort) -> ReconcileResult`; schemas `cnes-reconciliation-v1` and `cnes-divergence-v1`.

- [ ] **Step 1: Write failing precedence/evidence tests**

```python
def test_local_vence_conflito_e_nacional_preenche_null(store, request):
    result = reconcile_cnes(request, store)
    rows = read_output(store, result.reconciliation_manifest)
    divergences = read_output(store, result.divergence_manifest)
    assert rows.filter(pl.col("CNES") == "1234567")["NOME_PROFISSIONAL"][0] == "LOCAL"
    assert divergences.select("selected_source").unique().item() == "CNES_LOCAL"
```

- [ ] **Step 2: Prove the reconciler is absent**

Run: `uv run pytest apps/data_processor/tests/pipeline/test_reconcile_cnes.py -q`

Expected: collection FAIL importing `data_processor.pipeline.reconcile_cnes`.

- [ ] **Step 3: Add exact precedence, evidence, divergences, and KPIs**

Require `reconciliation_key` and `divergence_key` to equal
`reconciliation/<tenant>/<competencia>/<run_id>/cnes.parquet` and
`.../cnes_divergences.parquet`. Validate that `normalized_manifests` contains exactly one
`CNES_LOCAL` manifest and zero or one `CNES_NACIONAL` manifest, with no other source.
Full-outer-join by the frozen natural key. For each canonical value select non-null local,
otherwise national; emit a divergence row for unequal non-null values with `field_name`,
`local_value`, `national_value`, `selected_value`, `selected_source`, and both manifest IDs.
Preserve competence, source evidence, row counts, match/local-only/national-only/conflict counts,
and sort outputs deterministically before verified Parquet writes.

- [ ] **Step 4: Verify golden reconciliation**

Run: `uv run ruff check apps/data_processor/src/data_processor/pipeline/reconcile_cnes.py apps/data_processor/tests/pipeline/test_reconcile_cnes.py && uv run pytest apps/data_processor/tests/pipeline/test_reconcile_cnes.py --cov=data_processor.pipeline.reconcile_cnes --cov-branch --cov-fail-under=90 -q`

Expected: PASS; no unexplained row/KPI difference from CND-002 fixtures.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/pipeline/reconcile_cnes.py \
  apps/data_processor/tests/pipeline/test_reconcile_cnes.py
git commit -m "feat(processor): reconcile CNES competence"
```

### Task 30: CND-053 — Materialize minimal serving JSON

**Files:**
- Create: `apps/data_processor/src/data_processor/pipeline/materialize_cnes.py`
- Create: `apps/data_processor/tests/pipeline/test_materialize_cnes.py`
- Modify: none

**Interfaces:**
- Consumes: `MaterializeRequest`, `MaterializeResult`, CND-052 manifests/KPIs, and `ServingDocument`.
- Produces: `materialize_cnes(request: MaterializeRequest, store: ObjectStorePort) -> MaterializeResult`; serving key `serving/<tenant>/<run_id>/overview.json`.

- [ ] **Step 1: Write failing privacy/schema test**

```python
def test_serving_exclui_campos_pessoais(store, request):
    result = materialize_cnes(request, store)
    assert len(result.manifests) == len(result.documents) == 1
    body = result.documents[0].model_dump(mode="json")
    rendered = json.dumps(body)
    assert all(field not in rendered for field in ("CPF", "CNS", "NOME_PROFISSIONAL"))
    assert body["document_name"] == "overview"
    assert body["run_id"] == request.run_id
```

- [ ] **Step 2: Prove materializer is absent**

Run: `uv run pytest apps/data_processor/tests/pipeline/test_materialize_cnes.py -q`

Expected: collection FAIL importing `materialize_cnes`.

- [ ] **Step 3: Add the versioned screen document**

Require exactly one `request.target_keys` entry and that it ends in `/overview.json` for CNES. Payload keys are exactly `competencia`, `kpis`, `divergence_counts`, `missing_sources`, and `source_freshness`; aggregate with Polars before JSON serialization; exclude row-level personal values; emit sorted UTF-8 JSON plus newline to `request.target_keys[0]`. After hash/stat verification, return `MaterializeResult(manifests=(verified_manifest,), documents=(ServingDocument(schema_version="cnes-serving-v1", document_name="overview", tenant_id=request.tenant_id, run_id=request.run_id, generated_at=request.generated_at, payload=payload),))`.

- [ ] **Step 4: Verify privacy, determinism, and coverage**

Run: `uv run ruff check apps/data_processor/src/data_processor/pipeline/materialize_cnes.py apps/data_processor/tests/pipeline/test_materialize_cnes.py && uv run pytest apps/data_processor/tests/pipeline/test_materialize_cnes.py --cov=data_processor.pipeline.materialize_cnes --cov-branch --cov-fail-under=90 -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add apps/data_processor/src/data_processor/pipeline/materialize_cnes.py \
  apps/data_processor/tests/pipeline/test_materialize_cnes.py
git commit -m "feat(processor): materialize CNES serving JSON"
```

### Task 31: CND-054 — Golden and shadow comparison gate

**Files:**
- Create: `scripts/compare_cnes_data_plane.py`
- Create: `scripts/compare_cnes_data_plane_test.py`
- Create: `tests/integration/test_cnes_vertical_golden.py`
- Create: `docs/baselines/cnes-local-shadow-report.json`
- Modify: `.github/workflows/shadow-e2e.yml`

**Interfaces:**
- Consumes: CND-002 fixtures and CND-050–053 functions.
- Produces: `ComparisonDifference(layer, key, field, expected, actual, rule)`; `compare_outputs(expected: Path, actual: Path) -> tuple[ComparisonDifference, ...]`; zero-difference accepted report or differences carrying a non-empty approved rule ID.

- [ ] **Step 1: Write failing exact-comparison test**

```python
def test_diferenca_sem_regra_aprovada_falha(tmp_path):
    differences = compare_outputs(write_expected(tmp_path), write_changed(tmp_path))
    with pytest.raises(UnexplainedDifference, match="field=row_count"):
        require_explained(differences, approved_rules={})
```

- [ ] **Step 2: Prove comparison module is absent**

Run: `uv run pytest scripts/compare_cnes_data_plane_test.py tests/integration/test_cnes_vertical_golden.py -q`

Expected: FAIL importing `scripts.compare_cnes_data_plane`.

- [ ] **Step 3: Add row/KPI/divergence exact comparison**

Compare schema, row identity, every canonical value, divergence evidence, counts, and serving payload after normalizing only timestamps/run IDs declared volatile in the fixture manifest. Do not use tolerances or statistical proximity. Emit the checked report with input hashes and approved rule IDs.

- [ ] **Step 4: Run the Phase 5 gate**

Run: `uv run pytest scripts/compare_cnes_data_plane_test.py tests/integration/test_cnes_vertical_golden.py -q && uv run python scripts/compare_cnes_data_plane.py --fixtures docs/fixtures/data-plane --output docs/baselines/cnes-local-shadow-report.json`

Expected: PASS with zero unexplained differences.

- [ ] **Step 5: Commit**

```bash
git add scripts/compare_cnes_data_plane.py scripts/compare_cnes_data_plane_test.py \
  tests/integration/test_cnes_vertical_golden.py \
  docs/baselines/cnes-local-shadow-report.json .github/workflows/shadow-e2e.yml
git commit -m "test(processor): gate CNES golden parity"
```

## Phase 6 — local product slice

### Task 32: CND-060 — Local composition roots

**Files:**
- Create: `apps/central_api/src/central_api/composition.py`
- Create: `apps/central_api/tests/test_local_composition.py`
- Create: `apps/central_api/src/central_api/services/run_planning.py`
- Create: `apps/central_api/tests/services/test_run_planning.py`
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/source_catalog.py`
- Create: `packages/cnes_domain/tests/orchestration/test_source_catalog.py`
- Create: `apps/data_processor/src/data_processor/pipeline/source_registry.py`
- Create: `apps/data_processor/tests/pipeline/test_source_registry.py`
- Create: `apps/data_processor/src/data_processor/pipeline/stage_processor.py`
- Create: `apps/data_processor/tests/pipeline/test_stage_processor.py`
- Create: `apps/data_processor/src/data_processor/orchestration/coordinator.py`
- Create: `apps/data_processor/tests/orchestration/test_coordinator.py`
- Create: `apps/data_processor/src/data_processor/composition.py`
- Create: `apps/data_processor/tests/test_local_composition.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `apps/data_processor/src/data_processor/main.py`

**Interfaces:**
- Consumes: `ProfileSettings`, `SQLiteControlPlane`, `FilesystemObjectStore`, `LocalWorkerPool`, `LocalAuditSink`, `dispatch_once`, raw/orchestration/processing services, and CND-050–053 stage functions.
- Produces in `cnes_domain.orchestration.source_catalog`: frozen `SubtypeLayout(source_type: str, file_subtype: str, normalized_filenames: tuple[str, ...])`; frozen `PipelineLayout(normalized: tuple[SubtypeLayout, ...], reconciliation_filename: str, divergence_filename: str, serving_documents: tuple[str, ...])`; frozen `PipelineDefinition(pipeline_id: str, source_types: tuple[str, ...], dependencies: tuple[RunDependency, ...], layout: PipelineLayout)`; `SourceCatalog(definitions: tuple[PipelineDefinition, ...])` with `for_source(source_type: str) -> PipelineDefinition` and `for_pipeline(pipeline_id: str) -> PipelineDefinition`; `build_source_catalog() -> SourceCatalog`.
- Produces in `data_processor.pipeline.source_registry`: `NormalizeStage = Callable[[NormalizeRequest, ObjectStorePort], NormalizeResult]`, `ReconcileStage = Callable[[ReconcileRequest, ObjectStorePort], ReconcileResult]`, `MaterializeStage = Callable[[MaterializeRequest, ObjectStorePort], MaterializeResult]`; frozen `SourcePipeline(definition: PipelineDefinition, normalize: NormalizeStage, reconcile: ReconcileStage, materialize: MaterializeStage)` with read-only proxy properties for pipeline ID, source types, dependencies, and layout; `SourceRegistry(catalog: SourceCatalog, pipelines: tuple[SourcePipeline, ...])` with `for_source(source_type: SourceType) -> SourcePipeline` and `for_pipeline(pipeline_id: str) -> SourcePipeline`.
- Produces in `data_processor.pipeline.stage_processor`: `UnsupportedUnitSource(ValueError)`; `attempt_prefix_from_manifest_key(ref: ManifestRef) -> str`; `StageProcessor(control_plane: ControlPlanePort, source_store: ObjectStorePort, source_registry: SourceRegistry, clock: Callable[[], datetime])`; `__call__(unit: RunUnit, attempt_store: AttemptObjectStore) -> tuple[OutputManifest, ...]`.
- Produces in `data_processor.orchestration.coordinator`: `allow_execution(run: Run, dispatch: RunDispatch, requested_limit: int) -> ExecutionPermit`; `noop_execution_started(run: Run, request: StartRunExecution, execution_ref: str, permit: ExecutionPermit) -> None`; frozen `CoordinatorDependencies(control_plane: ControlPlanePort, executor: ProcessorExecutorPort, publisher: DatasetPublisher, clock: Callable[[], datetime])`; `CoordinatorResult(state: RunState, execution_ref: str | None, published: bool)`; `PipelineCoordinator(dependencies: CoordinatorDependencies, execution: ExecutionPolicyConfig)`; `resume(tenant_id: str, run_id: str) -> CoordinatorResult`; `recover(limit: int = 100) -> tuple[CoordinatorResult, ...]`.
- Produces in `central_api.services.run_planning`: frozen `RunPlanningDependencies(control_plane: ControlPlanePort, object_store: ObjectStorePort, executor: ProcessorExecutorPort, source_catalog: SourceCatalog)`; `RunLaunchResult(run: Run, plan: RunPlan | None, execution_ref: str | None)`; `RunPlanningService(dependencies: RunPlanningDependencies, execution: ExecutionPolicyConfig, clock: Callable[[], datetime])`; `launch(tenant_id: str, run_id: str) -> RunLaunchResult`; `recover(limit: int = 100) -> tuple[RunLaunchResult, ...]`; `on_raw_manifest_accepted(record: RawManifestRecord, limit: int = 100) -> None`.
- Produces in `central_api.composition`: `LocalRuntime(control_plane: ControlPlanePort, object_store: ObjectStorePort, executor: ProcessorExecutorPort, audit_sink: AuditSinkPort, raw_ingestion: RawIngestionService, source_catalog: SourceCatalog, run_planning: RunPlanningService)`; `build_local_runtime(settings: ProfileSettings, clock: Callable[[], datetime]) -> LocalRuntime`.
- Produces in `data_processor.composition`: `normalize_cnes(request: NormalizeRequest, store: ObjectStorePort) -> NormalizeResult`; `build_source_registry(catalog: SourceCatalog | None = None) -> SourceRegistry`; `LocalProcessorRuntime(control_plane: ControlPlanePort, object_store: ObjectStorePort, executor: ProcessorExecutorPort, publisher: DatasetPublisher, source_registry: SourceRegistry, stage_processor: StageProcessor, coordinator: PipelineCoordinator, unit_worker: UnitWorker, unit_handler: RunUnitCommandHandler)`; `build_local_processor_runtime(settings: ProfileSettings, clock: Callable[[], datetime]) -> LocalProcessorRuntime`.

- [ ] **Step 1: Write failing cloud/legacy-free composition tests**

```python
def test_local_runtime_nao_constroi_postgres_minio_aws(monkeypatch, settings):
    for name in ("sqlalchemy.create_engine", "minio.Minio", "boto3.client"):
        monkeypatch.setattr(name, lambda *args, **kwargs: pytest.fail(name), raising=False)
    runtime = build_local_runtime(settings, utc_now)
    assert isinstance(runtime.control_plane, SQLiteControlPlane)
    assert isinstance(runtime.object_store, FilesystemObjectStore)


def test_registry_cnes_expoe_um_bundle_para_as_duas_fontes():
    catalog = build_source_catalog()
    registry = build_source_registry(catalog)
    local = registry.for_source(SourceType.CNES_LOCAL)
    nacional = registry.for_source(SourceType.CNES_NACIONAL)
    assert local is nacional
    assert registry.for_pipeline("cnes") is local
    assert local.pipeline_id == "cnes"
    assert local.definition is catalog.for_pipeline("cnes")
    assert local.source_types == ("CNES_LOCAL", "CNES_NACIONAL")
    assert local.dependencies == (
        RunDependency("CNES_LOCAL", "CNES_VINCULO", True),
        RunDependency("CNES_NACIONAL", "CNES_VINCULO", False),
    )
    assert local.layout == PipelineLayout(
        normalized=(
            SubtypeLayout("CNES_LOCAL", "CNES_VINCULO",
                          ("cnes_local.parquet",)),
            SubtypeLayout("CNES_NACIONAL", "CNES_VINCULO",
                          ("cnes_nacional.parquet",)),
        ),
        reconciliation_filename="cnes.parquet",
        divergence_filename="cnes_divergences.parquet",
        serving_documents=("overview",),
    )


def test_catalogo_rejeita_colisao_global_de_normalized_filename():
    first = PipelineDefinition(
        pipeline_id="sihd", source_types=("SIHD",),
        dependencies=(RunDependency("SIHD", "SIH", True),),
        layout=PipelineLayout(
            normalized=(SubtypeLayout("SIHD", "SIH", ("shared.parquet",)),),
            reconciliation_filename="sihd.parquet",
            divergence_filename="sihd_divergences.parquet",
            serving_documents=("sihd-overview",)))
    second = PipelineDefinition(
        pipeline_id="bpa", source_types=("BPA_MAG",),
        dependencies=(RunDependency("BPA_MAG", "BPA_C", True),),
        layout=PipelineLayout(
            normalized=(SubtypeLayout("BPA_MAG", "BPA_C", ("shared.parquet",)),),
            reconciliation_filename="bpa.parquet",
            divergence_filename="bpa_divergences.parquet",
            serving_documents=("bpa-overview",)))
    with pytest.raises(CatalogConflict, match="normalized_filename_collision:shared.parquet"):
        SourceCatalog((first, second))


def test_permit_e_a_mesma_instancia_entregue_ao_callback(coordinator_dependencies, run):
    permit = ExecutionPermit(tenant_id=run.tenant_id, run_id=run.run_id,
        max_concurrency=2, policy_version=7, fencing_token=11, binding_context=object())
    started = Mock()
    coordinator = PipelineCoordinator(
        coordinator_dependencies,
        ExecutionPolicyConfig(2, 300, ExecutionCallbacks(Mock(return_value=permit), started)))
    coordinator.resume(run.tenant_id, run.run_id)
    assert started.call_args.args[3] is permit


@pytest.mark.parametrize("state", [RunState.PUBLISHED, RunState.PUBLISHED_DEGRADED,
                                    RunState.FAILED, RunState.CANCELED])
def test_launch_terminal_e_noop_read_only(service, state, persisted_run):
    terminal = persisted_run.model_copy(update={"state": state})
    service.control_plane.seed_run(terminal)
    result = service.launch(terminal.tenant_id, terminal.run_id)
    assert result.run.state is state
    assert service.control_plane.mutations == []
```

Add stage-controller tests proving the exact logical targets, raw versus predecessor physical input
mapping, no undeclared reads, dispatch of one and only one function for each `RunStage`, and
`test_stage_processor_rejeita_source_type_persistido_fora_do_catalogo` expecting
`UnsupportedUnitSource`. Add
coordinator tests for three executor waves, no reconciliation before all frozen normalization units
succeed, retry dispatch, the exact reserve → start → bind ordering, RESERVED replay after a crash
before bind, generation advance after a terminal execution with no unit claim, no overlapping active
dispatch, final failure, cancellation recovery, single CAS publication, and degraded publication
when the frozen optional source is absent. A resume of an already terminal Run is a read-only no-op.
Add run-planning tests for required-input waiting, raw-chain reconstruction from sidecars,
immutable unit persistence before executor start, optional-source freeze, replay idempotency,
launch failure, the complete launch state matrix, recovery of bounded
`WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED` runs, including publication CAS replay, and resumption of only the bounded waiting runs
matching an accepted dependency. Assert `on_raw_manifest_accepted(...) is None`.

- [ ] **Step 2: Prove composition modules are absent**

Run: `uv run pytest packages/cnes_domain/tests/orchestration/test_source_catalog.py apps/central_api/tests/test_local_composition.py apps/central_api/tests/services/test_run_planning.py apps/data_processor/tests/pipeline/test_source_registry.py apps/data_processor/tests/test_local_composition.py -q`

Expected: collection FAIL importing the composition modules.

- [ ] **Step 3: Add profile-selected roots and migrate local bootstrap**

For `PROFILE=local`, initialize `state/cnesdata.sqlite3`, create the configured data directory, seed
exactly the configured tenant, and return only local adapters. `SourceCatalog.__init__` rejects an
empty definition set, empty source/dependency/layout tuples, a blank/duplicate `pipeline_id`,
ownership of one source string by two definitions, a dependency/layout pair outside the definition,
duplicate subtype layouts, unsafe filenames, duplicate reconciliation/serving names, or a required
dependency with no layout. It also builds one global index of every normalized filename and rejects
reuse across different subtype layouts or pipeline definitions; normalized target keys therefore
cannot collide when later source bundles are registered. `SourceRegistry` requires exactly one stage bundle for every catalog
definition, rejects extra/missing definitions, and keeps the shared definition object unchanged.

`build_source_catalog` creates the one CNES `PipelineDefinition` with the two source
types/dependencies from the assertion, layouts
`(CNES_LOCAL,CNES_VINCULO)->("cnes_local.parquet",)` and
`(CNES_NACIONAL,CNES_VINCULO)->("cnes_nacional.parquet",)`, reconciliation names
`cnes.parquet`/`cnes_divergences.parquet`, and serving document `overview`.
`build_source_registry(catalog)` attaches the three CND-050–053 stage functions to that exact
definition. `normalize_cnes` dispatches only `CNES_LOCAL` to `normalize_cnes_local` and
`CNES_NACIONAL` to `normalize_cnes_nacional`; anything else raises `UnsupportedSourceType`.

`StageProcessor` loads the canonical `Run`, chooses the pipeline by `run.dataset_name`, and checks
that the unit belongs to that run. For `NORMALIZE`, it loads each `RawManifest` sidecar from the
unit's direct refs through `source_store`, validates the exact source/subtype/partition chain,
converts the persisted value exactly with `SourceType(unit.source_type)`, and translates
`TypeError|ValueError` into `UnsupportedUnitSource(unit.source_type)` before registry lookup,
allowlists every raw `manifest.object_key -> manifest.object_key`, and builds normalized target
keys from the source/subtype layout. For `RECONCILE` and `MATERIALIZE`, it loads only succeeded
predecessor units, opens every referenced `OutputManifest` sidecar, derives each immutable physical
object as `<predecessor-attempt-prefix>/<manifest.object_key>`, and passes that exact map through
`attempt_store.with_inputs`. It builds reconciliation and serving keys solely from `PipelineLayout`,
constructs the canonical request with run/unit/attempt identity and the injected clock, invokes
exactly the function selected by `RunStage`, then unwraps and validates the result tuple. It rejects
missing/extra predecessor refs, wrong layers, paths outside a predecessor attempt, layout mismatch,
and a stage result not matching its request.

`RunPlanningService.launch` begins with a strong Run read and implements this closed state matrix:

- `PLANNED|WAITING_INPUTS`: resolve the definition by `dataset_name`, require its dependencies to
  equal the catalog's server-owned tuple, call `list_raw_manifest_chain` for each dependency, open
  and validate every full `RawManifest` chain, and create planner refs with `partition="all"`.
  Missing required input conditionally moves/keeps the Run in `WAITING_INPUTS` and starts nothing.
  Otherwise call `plan_run`, issue one `PutRunUnits(expected_run_state=run.state, units=plan.units)`,
  require exact byte-identical replay, then CAS the Run to `PROCESSING` with frozen
  `missing_optional`. A crash between unit persistence and transition replays those exact bytes.
- `PROCESSING`: never resolve raw inputs or re-plan. Load the immutable persisted unit tuple, build
  the in-memory `RunPlan` from it plus the Run's frozen missing sources, and start or recover only a
  ready/retry wave through the dispatch protocol below.
- `PUBLISHING`: return the strong read without starting an executor; the processor coordinator owns
  publication.
- `PUBLISHED|PUBLISHED_DEGRADED|FAILED|CANCELED`: return a read-only no-op result.
- `CANCEL_REQUESTED`: best-effort cancel the active executor ref (including `None` for an unbound
  reservation), finish that dispatch as `CANCELED` when present, then call the single
  `finalize_run_cancellation` CAS that cancels every nonterminal unit and finalizes the Run.

Run creation copies the selected pipeline's immutable dependencies into `Run.dependencies`; no API
request accepts dependency overrides. `RunPlanningService.recover` samples `now = clock()` once and
iterates exactly the strongly revalidated deterministic result of
`list_recoverable_runs(now, limit)` and calls `launch` for each
`WAITING_INPUTS|PROCESSING|PUBLISHING|CANCEL_REQUESTED` candidate. API `launch` returns a
`PUBLISHING` no-op, while processor `resume` replays publisher validation/CAS. `on_raw_manifest_accepted` calls
`list_waiting_runs_for_dependency` and launches only those exact candidates, then has an explicit
`return None`; periodic recovery therefore covers a post-commit callback failure.

`PipelineCoordinator.resume` uses the same state matrix for processor-owned states: terminal Runs
are read-only, `CANCEL_REQUESTED` follows the cancellation sequence above, `PROCESSING` uses only
persisted units, and `PUBLISHING` invokes `DatasetPublisher` once under its CAS. After every
persisted unit result it strongly reloads Run/units, evaluates CND-041 fan-in, transitions a final
required-normalize or any final downstream failure to `FAILED`, treats an optional normalization
`SUCCEEDED_DEGRADED` as a satisfied edge with no manifest, transitions successful fan-in to
`PUBLISHING`, and publishes only after the unique materialization unit succeeds.

Both launchers use this exact dispatch protocol, with no overlapping active dispatch per Run:

1. Load `get_active_run_dispatch` first. Only when it returns `None`, call
   `ready_units(plan, now)`; a live unit lease returns no work. Compute `wave_id` with
   `logical_wave_id`, then call
   `reserve_run_dispatch(ReserveRunDispatch(tenant_id,run_id,wave_id,unit_ids,now,
   execution.dispatch_lease_seconds))` before any executor call.
2. A recovered, unexpired `RESERVED` dispatch is never replaced merely because no unit claimed it.
   Call `execution.callbacks.policy(run, dispatch, execution.deployment_limit)` exactly once,
   validate tenant/run and
   positive authorized concurrency, and retain the returned `ExecutionPermit`. Local
   `allow_execution` returns policy/fence zero and `binding_context=None`.
3. Build `execution_request(plan, dispatch, permit.max_concurrency)` and repeat `executor.start` with
   the same persisted `dispatch_id`; the executor returns the same ref. This closes the crash window
   after cloud start but before bind. A start exception leaves `RESERVED` durable for recovery.
   After start, call `bind_run_dispatch(BindRunDispatch(...))`, then call
   `execution.callbacks.started(run, request, execution_ref, permit)` with that exact permit object (`is`, not
   a copy). The callback returns `None`. Billing imports the domain `ExecutionPermit`; it may put its
   typed authorization object in `binding_context` but must not redefine this contract.
4. For recovered `STARTED`, call `executor.status(execution_ref)`. `RUNNING` is a no-op. For
   `SUCCEEDED|FAILED|CANCELED`, first persist `FinishRunDispatch(..., outcome=...)`, reload Run and
   units, and resume fan-in. If no unit was ever claimed, the same logical wave is still ready;
   `reserve_run_dispatch` advances persisted `generation` and returns a new deterministic
   `dispatch_id`, so a failed Standard execution cannot permanently reuse its old name.
5. Concurrent reserve/bind/finish conflicts reload the active dispatch. Reuse only an identical
   wave/ref; reject any different live wave/ref. If start returned a ref but bind cannot establish
   that same ref, best-effort cancel it and finish the still-owned reservation as `CANCELED`; the
   next recovery advances generation even when no unit claimed. A new generation is allowed only
   after terminal, or after dispatch-lease expiry with no live unit lease; an expired unbound
   reservation is therefore replaced deterministically. Claims carry `dispatch_id` and require the matching active dispatch plus Run
   `PROCESSING`, so superseded messages cannot execute.

`PipelineCoordinator.recover` samples `now = clock()` once, iterates
`list_recoverable_runs(now, limit)`, and calls `resume`, including
`CANCEL_REQUESTED`; repeated recovery is idempotent at every CAS boundary. Wire
`UnitWorker.after_persist` to `coordinator.resume`. Construct `RunUnitCommandHandler(unit_worker)`
and inject `unit_handler.handle` into `LocalWorkerPool`, so every local `RunUnitMessage` becomes the
exact `ClaimRunUnit` and no adapter reaches into a worker. Store the registry, stage processor,
coordinator, worker, and handler on `LocalProcessorRuntime`; `run_processor(runtime)` uses only this
already-composed graph.

Both processes call the same package-owned `build_source_catalog`; the API never imports the data
processor app, and `SourceRegistry` adds only processor callables to the same definitions. The API
runtime stores `run_planning`; its raw-ingestion accepted-event handler invokes
`run_planning.on_raw_manifest_accepted` only after the manifest/Job/outbox transaction commits.
`RunAuthorizationService` (added by the billing plan for both disabled and Stripe modes) calls
`run_planning.launch` after the canonical Run transaction; no route writes units or calls the
executor directly.

`deps.py` and processor `main.py` call the builders rather than constructing engines/MinIO.
Preserve a separate migration-mode branch for current tests, but never silently fall back from local
to legacy on error. `PROFILE=aws` raises `ProfileNotImplemented("aws_runtime_plan_required")` here.

- [ ] **Step 4: Verify local process startup**

Run: `PROFILE=local TENANT_ID=354130 DATA_DIR=$(mktemp -d) AUTH_MODE=local BILLING_MODE=disabled uv run pytest packages/cnes_domain/tests/orchestration/test_source_catalog.py apps/central_api/tests/test_local_composition.py apps/central_api/tests/services/test_run_planning.py apps/data_processor/tests/pipeline/test_source_registry.py apps/data_processor/tests/pipeline/test_stage_processor.py apps/data_processor/tests/orchestration/test_coordinator.py apps/data_processor/tests/test_local_composition.py -q`

Expected: PASS with no network/service dependency.

- [ ] **Step 5: Commit**

```bash
git add apps/central_api/src/central_api/composition.py \
  apps/central_api/src/central_api/services/run_planning.py \
  apps/central_api/tests/services/test_run_planning.py \
  apps/central_api/tests/test_local_composition.py apps/central_api/src/central_api/deps.py \
  packages/cnes_domain/src/cnes_domain/orchestration/source_catalog.py \
  packages/cnes_domain/tests/orchestration/test_source_catalog.py \
  apps/data_processor/src/data_processor/pipeline/source_registry.py \
  apps/data_processor/src/data_processor/pipeline/stage_processor.py \
  apps/data_processor/tests/pipeline/test_source_registry.py \
  apps/data_processor/tests/pipeline/test_stage_processor.py \
  apps/data_processor/src/data_processor/orchestration/coordinator.py \
  apps/data_processor/tests/orchestration/test_coordinator.py \
  apps/data_processor/src/data_processor/composition.py \
  apps/data_processor/tests/test_local_composition.py apps/data_processor/src/data_processor/main.py
git commit -m "feat(local): compose filesystem and SQLite runtime"
```

### Task 33: CND-061 — Local password auth and optional generic OIDC

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/auth/local_credentials.py`
- Create: `packages/cnes_infra/src/cnes_infra/auth/local_auth.py`
- Create: `packages/cnes_infra/tests/auth/test_local_auth.py`
- Create: `apps/central_api/src/central_api/routes/local_auth.py`
- Create: `apps/central_api/tests/routes/test_local_auth.py`
- Modify: none

**Interfaces:**
- Consumes: fixed local tenant settings, `ControlPlanePort.get_membership`, current `JWKSValidator` for optional OIDC.
- Produces: SQLite `local_users(user_id,email,password_hash,salt,created_at,disabled_at)` in the same state DB; `hash_password(password: str, salt: bytes) -> bytes` using `hashlib.scrypt(n=2**14,r=8,p=1,dklen=32)`; `LocalAuthService.authenticate(email, password) -> AuthenticatedPrincipal`; `OidcMembershipResolver.resolve(claims) -> AuthenticatedPrincipal`; routes `POST /api/v1/auth/local/login`, `POST /api/v1/auth/logout`, `GET /api/v1/auth/me` using HttpOnly/SameSite=Lax session cookies.

- [ ] **Step 1: Write failing password and fixed-tenant tests**

```python
def test_login_nao_aceita_tenant_do_browser(client, seeded_user):
    response = client.post("/api/v1/auth/local/login", json={
        "email": seeded_user.email, "password": "correta", "tenant_id": "999999"})
    assert response.status_code == 422


def test_hash_e_salt_nunca_sao_retornados(client, seeded_user):
    response = login(client, seeded_user)
    assert "password_hash" not in response.text and "salt" not in response.text
```

- [ ] **Step 2: Prove local auth is absent**

Run: `uv run pytest packages/cnes_infra/tests/auth/test_local_auth.py apps/central_api/tests/routes/test_local_auth.py -q`

Expected: collection FAIL importing `local_credentials`/`local_auth`.

- [ ] **Step 3: Add constant-time local auth and server-side membership resolution**

Normalize email with trim/casefold, validate password length 12–128, generate 16 random salt bytes, compare with `hmac.compare_digest`, apply one dummy scrypt for unknown users, and never log credentials. Resolve the tenant only from `ProfileSettings.tenant_id`; both local and OIDC modes require a direct membership lookup before returning principal. OIDC claims cannot supply/override tenant. Sessions contain opaque random IDs; store only session hash and expiry in SQLite.

- [ ] **Step 4: Verify auth security and coverage**

Run: `uv run ruff check packages/cnes_infra/src/cnes_infra/auth/local_* apps/central_api/src/central_api/routes/local_auth.py packages/cnes_infra/tests/auth/test_local_auth.py apps/central_api/tests/routes/test_local_auth.py && uv run pytest packages/cnes_infra/tests/auth/test_local_auth.py apps/central_api/tests/routes/test_local_auth.py -q`

Expected: PASS, including disabled user, bad password, expired session, absent membership, and OIDC tenant-claim mismatch cases.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/auth/local_* \
  packages/cnes_infra/tests/auth/test_local_auth.py \
  apps/central_api/src/central_api/routes/local_auth.py \
  apps/central_api/tests/routes/test_local_auth.py
git commit -m "feat(auth): add local password profile"
```

### Task 34: CND-062 — Authorized serving BFF

**Files:**
- Create: `apps/central_api/src/central_api/services/serving_access.py`
- Create: `apps/central_api/src/central_api/routes/serving.py`
- Create: `apps/central_api/tests/services/test_serving_access.py`
- Create: `apps/central_api/tests/routes/test_serving.py`
- Modify: none

**Interfaces:**
- Consumes: `ControlPlanePort.get_membership`, `get_dataset_pointer`, `get_dataset_version`, `ObjectStorePort`, `ServingRequest`, `ServingGrant`, fixed tenant/principal dependencies.
- Produces: `LocalServingAccess(control_plane).authorize(request: ServingRequest) -> ServingGrant`; `GET /api/v1/dashboard/serving/{dataset_name}/{document_name}` streaming authorized JSON with `ETag=<sha256>`, `X-Dataset-Version`, and `Cache-Control: private, max-age=30`.

- [ ] **Step 1: Write failing denial/no-fallback tests**

```python
def test_serving_ausente_nao_cai_para_versao_antiga(client, active_pointer):
    delete_active_serving_object(active_pointer)
    response = client.get("/api/v1/dashboard/serving/cnes/overview")
    assert response.status_code == 503
    assert response.json()["detail"] == "active_serving_unavailable"
```

- [ ] **Step 2: Prove serving modules are absent**

Run: `uv run pytest apps/central_api/tests/services/test_serving_access.py apps/central_api/tests/routes/test_serving.py -q`

Expected: collection FAIL importing `central_api.services.serving_access`.

- [ ] **Step 3: Add pointer-only authorization and streaming**

Authorize by direct `(tenant_id,user_id)` membership lookup, resolve only `POINTER#CURRENT`, load its immutable version/run manifest, and allow only serving-layer keys listed there. Reject `raw`, `normalized`, `reconciliation`, arbitrary object keys, tenant input, and path traversal. Local BFF streams the JSON object; it does not create public/signed filesystem URLs.

- [ ] **Step 4: Verify security and coverage**

Run: `uv run ruff check apps/central_api/src/central_api/services/serving_access.py apps/central_api/src/central_api/routes/serving.py apps/central_api/tests/services/test_serving_access.py apps/central_api/tests/routes/test_serving.py && uv run pytest apps/central_api/tests/services/test_serving_access.py apps/central_api/tests/routes/test_serving.py --cov=central_api.services.serving_access --cov=central_api.routes.serving --cov-branch --cov-fail-under=90 -q`

Expected: PASS, including unauthorized/cross-tenant/raw-key/missing-serving cases.

- [ ] **Step 5: Commit**

```bash
git add apps/central_api/src/central_api/services/serving_access.py \
  apps/central_api/src/central_api/routes/serving.py \
  apps/central_api/tests/services/test_serving_access.py \
  apps/central_api/tests/routes/test_serving.py
git commit -m "feat(api): serve authorized active dataset JSON"
```

### Task 35: CND-063 — Dashboard serving-JSON migration

**Files:**
- Create: `apps/web_dashboard/src/api/hooks/useServingOverview.ts`
- Create: `apps/web_dashboard/tests/unit/api/hooks/useServingOverview.test.tsx`
- Modify: `apps/web_dashboard/src/api/client.ts`
- Modify: `apps/web_dashboard/src/routes/_app.overview.tsx`
- Modify: `apps/web_dashboard/src/components/overview/KpiGrid.tsx`
- Modify: `apps/web_dashboard/tests/mocks/handlers.ts`
- Modify: none of `package.json`, `bun.lock`, or generated API types

**Interfaces:**
- Consumes: CND-053 serving document and CND-062 route; no `tenantId` input.
- Produces: `ServingOverview` TypeScript type mirroring `ServingDocument.payload`; `useServingOverview(): UseQueryResult<ServingOverview>`; overview route no longer invokes legacy overview/faturamento endpoints.

- [ ] **Step 1: Write failing no-tenant/no-legacy request test**

```tsx
test("busca_apenas_serving_ativo_sem_header_de_tenant", async () => {
  const seen = vi.fn(); server.use(http.get("/api/v1/dashboard/serving/cnes/overview",
    ({ request }) => { seen(request.headers.get("X-Tenant-Id")); return HttpResponse.json(doc); }));
  const { result } = renderHook(() => useServingOverview(), { wrapper: wrap });
  await waitFor(() => expect(result.current.isSuccess).toBe(true));
  expect(seen).toHaveBeenCalledWith(null);
});
```

- [ ] **Step 2: Prove the hook is absent**

Run: `cd apps/web_dashboard && bun run test -- useServingOverview.test.tsx`

Expected: FAIL resolving `@/api/hooks/useServingOverview`.

- [ ] **Step 3: Add serving hook and replace legacy dashboard calls**

Add `apiFetch` option `sendTenantHeader?: never` by removing tenant header support from the new hook path; fetch `/dashboard/serving/cnes/overview`; render KPI/divergence/freshness/missing-source status from the serving payload. Preserve Portuguese UI text, loading/error/503 states, and never expose raw/reconciliation object keys in rendered data.

- [ ] **Step 4: Run dashboard quality gates**

Run: `cd apps/web_dashboard && bun run format:check && bun run lint && bun run typecheck && bun run test && bun run build && bun run bundle:check`

Expected: PASS at 80% line/70% branch coverage and all bundle budgets.

- [ ] **Step 5: Commit**

```bash
git add apps/web_dashboard/src/api/client.ts \
  apps/web_dashboard/src/api/hooks/useServingOverview.ts \
  apps/web_dashboard/src/routes/_app.overview.tsx \
  apps/web_dashboard/src/components/overview/KpiGrid.tsx \
  apps/web_dashboard/tests/unit/api/hooks/useServingOverview.test.tsx \
  apps/web_dashboard/tests/mocks/handlers.ts
git commit -m "feat(dashboard): consume active serving JSON"
```

### Task 36: CND-064 — Local acceptance, backup/restore, and serial integration gate

**Files:**
- Create: `scripts/local_backup.py`
- Create: `scripts/local_backup_test.py`
- Create: `tests/integration/test_local_profile_acceptance.py`
- Create: `tests/chaos/test_local_backup_restore.py`
- Modify: `apps/central_api/src/central_api/app.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `apps/central_api/pyproject.toml`
- Modify: `apps/data_processor/pyproject.toml`
- Modify: `packages/cnes_infra/pyproject.toml`
- Modify: `packages/cnes_contracts/src/cnes_contracts/__init__.py`
- Modify: `packages/cnes_domain/src/cnes_domain/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Modify: `docs/openapi.json` (generated)
- Modify: `docs/contracts/openapi.json` (generated)
- Modify: `docs/contracts/schemas/*.json` (generated)
- Modify: `apps/dump_agent_go/internal/apiclient/generated.go` (generated)
- Modify: `apps/web_dashboard/src/api/generated.ts` (generated)
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `apps/web_dashboard/bun.lock` only if regeneration changes it
- Modify: `docker-compose.yml`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/python-quality.yml`
- Modify: `.github/workflows/dump-agent-go.yml`
- Modify: `.github/workflows/web-dashboard.yml`

**Interfaces:**
- Consumes: all accepted CND-000–064 foundation tasks; CND-021, the S3 portions of CND-022/023/025, and the Step Functions adapter from CND-042 remain adapter-only and are not composed into the local runtime. AWS-012 owns state-machine/ECS composition.
- Produces: fully composed local app; `create_backup(state_db: Path, data_dir: Path, target: Path, now: datetime) -> BackupManifest`; `restore_backup(archive: Path, state_db: Path, data_dir: Path) -> None`; acceptance evidence for restart, full/delta, fencing, publication recovery, auth/serving, and non-derivable state restore.

- [ ] **Step 1: Write failing acceptance and restore tests**

```python
@pytest.mark.local_profile
def test_backup_restaura_usuarios_memberships_agents_e_access_decisions(stack, tmp_path):
    archive = stack.backup(tmp_path / "backup.tar.zst")
    stack.destroy_instance(); stack.restore(archive); stack.restart()
    assert stack.login("gestor@local", "senha-de-teste").status_code == 200
    assert stack.membership("gestor") is not None
    assert stack.agent("agent-01").state == AgentState.ACTIVE
    assert stack.access_request("request-01").state == AccessRequestState.APPROVED
```

- [ ] **Step 2: Prove local acceptance is not yet wired**

Run: `uv run pytest tests/integration/test_local_profile_acceptance.py tests/chaos/test_local_backup_restore.py -m local_profile -q`

Expected: FAIL because routes/exports/generated contracts/Compose are not integrated and backup functions are absent.

- [ ] **Step 3: Integrate shared surfaces and implement verified backups**

Use SQLite's online backup API into a staging directory, copy immutable data/audit objects, write per-file SHA-256/size plus backup version/tenant/timestamp, then atomically finalize the archive. Restore into empty targets only, verify every hash before replacement, and refuse a tenant mismatch. Compose adds a `local` profile with only API, processor, and dashboard plus mounted `state/` and `data/`; it must not start PostgreSQL, MinIO, Keycloak, or AWS services. Include local auth/serving routers, regenerate all shared contracts/clients, and add the full acceptance matrix to CI.

- [ ] **Step 4: Run every final local gate**

Run: `uv sync --locked && uv run ruff check . && uv run pytest packages --cov --cov-config=pyproject.toml -q && uv run pytest apps --cov --cov-config=.coveragerc -q && uv run pytest tests/integration/test_local_profile_acceptance.py tests/chaos/test_local_backup_restore.py -m local_profile -q && cd apps/dump_agent_go && go test -race -count=1 -coverprofile=coverage.out ./... && cd ../web_dashboard && bun install --frozen-lockfile && bun run lint && bun run typecheck && bun run test && bun run build && bun run bundle:check`

Expected: packages at 100% branch, Python apps at 90% line, Go filtered coverage at least 65%, dashboard at 80% line/70% branch, and the acceptance matrix PASS after a process restart and backup restore.

- [ ] **Step 5: Commit**

```bash
git add scripts/local_backup.py scripts/local_backup_test.py \
  tests/integration/test_local_profile_acceptance.py tests/chaos/test_local_backup_restore.py \
  apps packages docs/openapi.json docs/contracts pyproject.toml uv.lock \
  docker-compose.yml .github/workflows
git commit -m "feat(local): complete CNES local profile acceptance"
```

## Dependency and worktree dispatch notes

- Serial: CND-000 → CND-001 → CND-002; CND-003 may follow CND-000 in parallel with CND-001 only if it does not touch the baseline report.
- Wave 1: CND-010 and CND-011 are independent; CND-012 follows both; CND-013 follows CND-012; CND-014 is the serial integration owner after both contract branches are accepted.
- Adapter wave: CND-020, CND-021, CND-022, and CND-023 are independent after CND-013/CND-014 dependency preparation. CND-024 follows both control planes and both sinks. CND-025 is serial and integrates the complete SQLite/DynamoDB Local/filesystem/S3-compatible matrix.
- Raw wave: CND-030, CND-032, and CND-033 can run independently after their listed contracts exist. CND-031 follows CND-030. CND-034 is serial and owns DI, dependency manifests, generated OpenAPI/schema, and generated Go client changes.
- Orchestration wave: CND-041 starts as soon as CND-010/CND-011 are merged and is independent of adapters; CND-042 follows CND-012/CND-041. CND-040 waits for CND-020/CND-021/CND-030. CND-043 follows CND-040/CND-041; CND-044 follows CND-022/CND-024/CND-043; CND-045 is the serial race/crash gate.
- Processing wave: CND-050 and CND-051 are independent because their shared request/result types are frozen in CND-011. CND-052 → CND-053 → CND-054 is serial.
- Product wave: CND-060, CND-061, and CND-062 own disjoint feature files after serving schema freeze; CND-060 freezes `SourceCatalog`, `SourcePipeline`/`SourceRegistry`, run planning, stage dispatch, and the initial CNES bundle before any SIHD/BPA/SIA processing worktree is dispatched. Those later source lanes create source-owned stage functions plus disjoint package definition files; their serial controller integration extends the shared catalog and registry. CND-063 follows CND-062; CND-064 is the only final integration owner for app bootstrap, package manifests/locks, generated artifacts, CI, and Compose.
- DynamoDB/S3 adapters and S3 Object Lock audit delivery are completed here in CND-021–025; Step Functions/ECS, AWS application composition, AWS OIDC/signed serving, billing, SIHD/BPA/SIA source parity, legacy deletion, historical export, and cutover remain outside this plan.

## External decision gate

The approved data-plane design mandates an official DATASUS adapter but does not specify the official bulk endpoint/file catalog, authentication, pagination/archive checksum behavior, or a field mapping for both establishments and professional links. The current repository's `CnesOficialWebAdapter` performs only a single-establishment existence check and cannot satisfy CND-033. Before dispatching CND-033, amend the governing spec with that concrete source contract; CND-033 and therefore CND-034/CND-051 onward remain blocked until it is ratified. No implementation worker may guess the endpoint or retain BigQuery as a fallback.

## Self-review results

- **Spec coverage:** CND-000–003, 010–014, 020–025, 030–034, 040–045, 050–054, and 060–064 each map to one independently reviewable task. AWS application composition, billing, source parity, cutover, and removal are explicitly excluded. Delta limits, leases/fencing, degraded fan-in, atomic pointer/outbox publication, local isolation, authorized serving, audit replay, and backup/restore all have concrete tests.
- **Specificity scan:** No vague deferred-work phrase or wildcard task remains. The only blocked work is the explicit DATASUS source decision gate above; its adapter interfaces and tests are nevertheless fixed.
- **Type consistency:** All later tasks use the canonical request/result types from `cnes_contracts.manifests.processing`; normalization and materialization results carry non-empty tuples so source plugins may emit multiple typed artifacts. They use the `SourceType` enum from `cnes_contracts.manifests.raw`, target PEP 544 ports from `cnes_domain.ports`, package-owned `PipelineDefinition`/`SourceCatalog`, the app-level `SourcePipeline` stage registry, and the composition signatures from CND-060. `Job`, `Run`, and staged `RunUnit` remain distinct; `version_id=run_id` is consistent from publisher through serving.
