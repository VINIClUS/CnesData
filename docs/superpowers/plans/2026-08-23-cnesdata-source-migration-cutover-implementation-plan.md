# CnesData Source Migration and Cutover Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate SIHD, BPA, and SIA to the immutable Parquet data plane, prove historical
equivalence, cut every product read and write to the active `DatasetPointer`, export required
legacy history, and remove PostgreSQL, MinIO, BigQuery, and Keycloak from runtime safely.

**Architecture:** Each retained source is implemented as an isolated data-processor plugin that
consumes the canonical raw-manifest and stage contracts from the Data Plane plan. The three
source plugins are independent until a controller-owned registry checkpoint. All later migration
steps are serial: historical shadow evidence, pointer-only product cutover, legacy-write stop,
verified immutable export, legacy removal, and final profile acceptance. No removal can begin
while a retained source or rollback/export gate still depends on the path being removed.

**Tech Stack:** Python 3.13, Pydantic v2, Polars, PyArrow/Parquet, FastAPI, pytest, Hypothesis,
Go 1.26.2 Edge Agent, React 18, TypeScript, TanStack Query/Router, Vitest, Playwright, SQLite,
filesystem, DynamoDB/S3 test adapters, Ruff, mypy, Bun.

**Spec:**
[Data Plane Parquet and Orchestration](../specs/2026-08-16-parquet-data-plane-orchestration-design.md),
[Billing and Entitlements](../specs/2026-08-16-stripe-billing-entitlements-design.md), and
[Execution and Parallel Worktrees](../specs/2026-08-23-cnesdata-redesign-execution-design.md).

## Global Constraints

- Start from the latest green `develop` commit containing `CND-064`; do not substitute an
  unmerged dependency branch.
- Do not dispatch any source lane while the external DATASUS bulk contract required by `CND-033`
  remains unratified. `CND-064` and this plan stay blocked behind that explicit decision gate;
  no worker may infer endpoints, authentication, archive/checksum behavior, or field mappings.
- `SRC-010`, `SRC-011`, and `SRC-012` may run in three independent worktrees. Retain a fourth,
  controller-owned lane for reviews, shared-file integration, and wave verification.
- Source agents may edit only their source module, source-specific adapters, source-specific
  tests, and source-specific fixtures. They must not edit composition roots, package exports,
  dependency manifests, lockfiles, generated schemas, CI, or deployment files.
- Source stages use the exact processing requests/results, `RunDependency`, and `SourcePipeline`
  registry frozen by `CND-041`/`CND-060`. They must not create a competing planner/pipeline
  abstraction or import a SQLAlchemy `Engine`.
- Raw inputs and every normalized, reconciliation, and serving output are immutable and carry
  tenant, source, competence, run, schema, provenance, row-count, and SHA-256 metadata.
- Preserve sensitive source fields only in private raw/normalized objects when the frozen source
  contract requires them. Serving JSON must never contain patient name, CNS, CPF, date of birth,
  or other screen-unneeded personal data.
- Unknown reference codes are explicit quality/divergence rows with counters. A normalizer must
  not silently discard a source row because a dimension lookup failed.
- Golden and shadow acceptance is exact after the versioned canonicalization rules. Statistical
  proximity, percentage tolerances, and unexplained row-count differences are failures.
- `MIG-011` through `MIG-015` are serial controller tasks. In particular, `MIG-014` never runs in
  parallel with code that reads from or writes to PostgreSQL, MinIO, BigQuery, or Keycloak.
- Production AWS resource creation remains outside this application plan and requires the
  dedicated deployment specification. AWS adapter/emulator acceptance stays in scope.
- Current repository limits remain binding: Python 3.13; Portuguese test names; functions at
  most 50 lines; complexity at most 10; line width at most 100; files at most 500 lines; package
  coverage 100% branch where enforced; app coverage 90% line; Go race tests and 65% coverage;
  dashboard lint, typecheck, tests, build, and relevant Playwright coverage.
- Every task begins with a failing behavior test, runs targeted checks before the commit, opens a
  PR against `develop`, and is integrated only after specification and code-quality review.
- Do not create downstream worktrees before all `Depends on` commits are present in `develop`.

## Dependency and Worktree Schedule

| Wave | Worktree A | Worktree B | Worktree C | Controller lane |
|---|---|---|---|---|
| S1 | `SRC-010` SIHD | `SRC-011` BPA | `SRC-012` SIA | Review each source PR |
| S2 | — | — | — | Register plugins and run source matrix |
| M1 | `MIG-010` shadow evidence | — | — | Review evidence and gate cutover |
| M2 | `MIG-011` pointer cutover | — | — | Full product verification |
| M3 | `MIG-012` stop writes | — | — | Observe target-only runtime |
| M4 | `MIG-013` export history | — | — | Verify every manifest and hash |
| M5 | `MIG-014` remove legacy | — | — | Dependency/config/document scan |
| M6 | `MIG-015` final acceptance | — | — | Sign final evidence matrix |

The source wave is the only parallel wave in this plan. `MIG-010` consumes the integrated source
registry, and every later task consumes the previous task's committed evidence.

---

### Task 1: `SRC-010` — Migrate SIHD end to end

**Depends on:** `CND-064`. This task freezes its own anonymized SIHD fixtures and hashes before
implementation; `CND-002` contains CNES fixtures only.

**Worktree:** `feat/src-010-sihd-parquet-parity`

**Allowed files:**

- Create: `apps/data_processor/src/data_processor/sources/sihd/__init__.py`
- Create: `apps/data_processor/src/data_processor/sources/sihd/contract.py`
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sihd.py`
- Create: `packages/cnes_domain/tests/orchestration/source_definitions/test_sihd.py`
- Create: `apps/data_processor/src/data_processor/sources/sihd/normalize.py`
- Create: `apps/data_processor/src/data_processor/sources/sihd/reconcile.py`
- Create: `apps/data_processor/src/data_processor/sources/sihd/serving.py`
- Modify: `apps/data_processor/src/data_processor/adapters/sihd_local_adapter.py`
- Create: `apps/data_processor/tests/sources/sihd/test_normalize.py`
- Create: `apps/data_processor/tests/sources/sihd/test_reconcile.py`
- Create: `apps/data_processor/tests/sources/sihd/test_serving.py`
- Create: `apps/data_processor/tests/fixtures/sihd/raw_manifests.json`
- Create: `apps/data_processor/tests/fixtures/sihd/raw_internacao_rows.json`
- Create: `apps/data_processor/tests/fixtures/sihd/raw_proc_aih_rows.json`
- Create: `apps/data_processor/tests/fixtures/sihd/expected_normalized.json`
- Create: `apps/data_processor/tests/fixtures/sihd/expected_serving.json`
- Create: `apps/data_processor/tests/fixtures/sihd/fixture-manifest.json`

**Forbidden files:** `apps/data_processor/src/data_processor/composition.py`, every
`pyproject.toml`, `uv.lock`, package `__init__.py` files outside the SIHD directory, generated
schemas, Docker/CI files, and all BPA/SIA modules.

**Consumed interfaces:** the Data Plane plan's canonical `NormalizeRequest`,
`NormalizeResult`, `ReconcileRequest`, `ReconcileResult`, `MaterializeRequest`,
`MaterializeResult`, `ObjectStorePort`, `RawManifest`, `OutputManifest`, and `RunDependency`
types, plus CND-060 `SubtypeLayout` and `PipelineLayout`.
Each source also consumes CND-060 `PipelineDefinition`; its definition lives in a distinct
`cnes_domain.orchestration.source_definitions.<source>` file so the three worktrees do not touch a
shared catalog or package export.

**Produced interfaces:**

```python
SIHD_DEPENDENCIES = (
    RunDependency("SIHD", "SIHD_INTERNACAO", True),
    RunDependency("SIHD", "SIHD_PROC_AIH", True),
)
SIHD_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout("SIHD", "SIHD_INTERNACAO",
                      ("internacoes.parquet", "quality_issues_internacao.parquet")),
        SubtypeLayout("SIHD", "SIHD_PROC_AIH",
                      ("procedimentos_aih.parquet", "quality_issues_proc_aih.parquet")),
    ),
    reconciliation_filename="sihd.parquet",
    divergence_filename="sihd_divergences.parquet",
    serving_documents=("overview",),
)
SIHD_DEFINITION = PipelineDefinition(
    pipeline_id="sihd", source_types=("SIHD",),
    dependencies=SIHD_DEPENDENCIES, layout=SIHD_LAYOUT,
)

def normalize_sihd(
    request: NormalizeRequest,
    store: ObjectStorePort,
) -> NormalizeResult: ...

def reconcile_sihd(
    request: ReconcileRequest,
    store: ObjectStorePort,
) -> ReconcileResult: ...

def materialize_sihd(
    request: MaterializeRequest,
    store: ObjectStorePort,
) -> MaterializeResult: ...
```

The three constants are defined in the package-owned `source_definitions/sihd.py`;
`sources/sihd/contract.py` imports them by direct module path and does not duplicate their values.

The normalized record key is deterministic from `num_aih`, procedure code, competence, and the
source-row discriminator. Normalized Parquet preserves source provenance and amounts in integer
cents. Serving output contains aggregate counts, totals, dates, competence, and divergence
summaries only. Both required raw subtypes always have immutable manifests; a source table with
zero rows is represented by a verified empty Parquet, not by omitting the manifest.

Each SIHD normalization unit contains exactly one subtype chain, matching CND-041 fan-out. For
`SIHD_INTERNACAO`, `NormalizeRequest.target_keys` is the ordered pair
`normalized/<tenant>/SIHD/<competencia>/<run_id>/internacoes.parquet` and
`.../quality_issues_internacao.parquet`; for `SIHD_PROC_AIH`, it is
`.../procedimentos_aih.parquet` and `.../quality_issues_proc_aih.parquet`.
`NormalizeResult.manifests` matches its request pair and uses `source_type=SIHD`. Reconciliation
requires all four successful normalized manifests. The planner supplies
`reconciliation/<tenant>/<competencia>/<run_id>/sihd.parquet` and
`.../sihd_divergences.parquet` as the only reconciliation destinations.
`MaterializeRequest.target_keys` contains exactly
`serving/<tenant>/<run_id>/overview.json` for dataset `sihd`, and `MaterializeResult` returns one
serving manifest plus `ServingDocument(document_name="overview", ...)`.

- [ ] **Step 1: Add failing SIHD normalization tests**

  Write tests named
  `test_normaliza_sihd_com_chave_estavel_e_proveniencia`,
  `test_rejeita_manifesto_sihd_de_competencia_divergente`, and
  `test_registra_codigo_referencia_desconhecido_sem_descartar_linha` in
  `apps/data_processor/tests/sources/sihd/test_normalize.py`. Assert `SIHD_LAYOUT` exactly matches
  the produced-interface registry and has one entry per `SIHD_DEPENDENCIES` member.

  The first test loads `raw_manifests.json`, `raw_internacao_rows.json`, and
  `raw_proc_aih_rows.json`, writes both raw Parquets through the in-memory contract fixture,
  invokes `normalize_sihd` once per subtype unit, and asserts exact normalized rows,
  output key, schema version, row count, and SHA-256. The second asserts a typed contract error.
  The third asserts one quality row and unchanged input/output accounting. Freeze fixture hashes
  in a source-owned `fixture-manifest.json` and fail the test if either raw subtype fixture changes
  without the reviewed expected outputs changing in the same PR.

- [ ] **Step 2: Run the RED test**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sihd/test_normalize.py -q
  ```

  Expected: FAIL with `ModuleNotFoundError: data_processor.sources.sihd`; a fixture-setup failure
  is not the expected RED state.

- [ ] **Step 3: Implement the pure SIHD normalizer**

  Add package `source_definitions/sihd.py` with the exact immutable definition and make app
  `contract.py` import its dependency/layout constants. Keep
  `SihdLocalAdapter` as a pure Polars mapping helper. Remove any implicit Gold persistence
  expectation and add strict schema/type validation. Implement `normalize_sihd` to:

  1. require `source_type=SIHD`, one competence, and exactly one chain whose subtype belongs to
     `SIHD_DEPENDENCIES`;
  2. read only manifest-declared immutable raw objects;
  3. dispatch by subtype and normalize CNES, AIH, SIGTAP, CID, dates, and monetary values
     deterministically;
  4. validate that subtype's exact ordered target pair and write only those keys through
     `ObjectStorePort`;
  5. return the two ordered `result.manifests` only after re-reading object stats and verifying
     SHA-256.

- [ ] **Step 4: Add and run reconciliation and serving tests**

  In `test_reconcile.py`, assert exact totals by CNES/procedure/competence, explicit quality
  evidence, and idempotent bytes for the same request. In `test_serving.py`, assert
  `sihd/overview.json` matches `expected_serving.json` and recursively contains none of
  `paciente_nome`, `paciente_cns`, `cpf`, or `data_nascimento`.

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sihd \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sihd.py -q
  uv run ruff check apps/data_processor/src/data_processor/sources/sihd \
    apps/data_processor/tests/sources/sihd \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sihd.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sihd.py
  uv run mypy apps/data_processor/src/data_processor/sources/sihd
  ```

  Expected: all tests pass, Ruff exits 0, and mypy reports no errors.

- [ ] **Step 5: Verify source-boundary coverage and commit**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sihd \
    --cov=data_processor.sources.sihd --cov-branch --cov-report=term-missing \
    --cov-fail-under=90
  git diff --check
  git status --short
  ```

  Expected: at least 90% app line coverage, branch/negative cases visible, no whitespace errors,
  and only allowed paths changed.

  Commit:

  ```bash
  git add apps/data_processor/src/data_processor/sources/sihd \
    apps/data_processor/src/data_processor/adapters/sihd_local_adapter.py \
    apps/data_processor/tests/sources/sihd \
    apps/data_processor/tests/fixtures/sihd \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sihd.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sihd.py
  git commit -m "feat(data-processor): migrate SIHD to parquet pipeline"
  ```

---

### Task 2: `SRC-011` — Migrate BPA end to end

**Depends on:** `CND-064`. This task freezes its own anonymized BPA fixtures and hashes before
implementation. It is independent of `SRC-010` and `SRC-012`.

**Worktree:** `feat/src-011-bpa-parquet-parity`

**Allowed files:**

- Create: `apps/data_processor/src/data_processor/sources/bpa/__init__.py`
- Create: `apps/data_processor/src/data_processor/sources/bpa/contract.py`
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/bpa.py`
- Create: `packages/cnes_domain/tests/orchestration/source_definitions/test_bpa.py`
- Create: `apps/data_processor/src/data_processor/sources/bpa/normalize.py`
- Create: `apps/data_processor/src/data_processor/sources/bpa/reconcile.py`
- Create: `apps/data_processor/src/data_processor/sources/bpa/serving.py`
- Modify: `apps/data_processor/src/data_processor/adapters/bpa_adapter.py`
- Create: `apps/data_processor/tests/sources/bpa/test_normalize.py`
- Create: `apps/data_processor/tests/sources/bpa/test_reconcile.py`
- Create: `apps/data_processor/tests/sources/bpa/test_serving.py`
- Create: `apps/data_processor/tests/fixtures/bpa/raw_manifest_bpa_c.json`
- Create: `apps/data_processor/tests/fixtures/bpa/raw_manifest_bpa_i.json`
- Create: `apps/data_processor/tests/fixtures/bpa/raw_rows.json`
- Create: `apps/data_processor/tests/fixtures/bpa/expected_normalized.json`
- Create: `apps/data_processor/tests/fixtures/bpa/expected_serving.json`
- Create: `apps/data_processor/tests/fixtures/bpa/fixture-manifest.json`

**Forbidden files:** the same shared surfaces as Task 1, plus SIHD/SIA paths.

**Produced interface:**

```python
BPA_DEPENDENCIES = (
    RunDependency("BPA_MAG", "BPA_C", True),
    RunDependency("BPA_MAG", "BPA_I", True),
)
BPA_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout("BPA_MAG", "BPA_C",
                      ("bpa_c.parquet", "quality_issues_bpa_c.parquet")),
        SubtypeLayout("BPA_MAG", "BPA_I",
                      ("bpa_i.parquet", "quality_issues_bpa_i.parquet")),
    ),
    reconciliation_filename="bpa.parquet",
    divergence_filename="bpa_divergences.parquet",
    serving_documents=("overview", "by-establishment"),
)
BPA_DEFINITION = PipelineDefinition(
    pipeline_id="bpa", source_types=("BPA_MAG",),
    dependencies=BPA_DEPENDENCIES, layout=BPA_LAYOUT,
)

def normalize_bpa(
    request: NormalizeRequest,
    store: ObjectStorePort,
) -> NormalizeResult: ...

def reconcile_bpa(
    request: ReconcileRequest,
    store: ObjectStorePort,
) -> ReconcileResult: ...

def materialize_bpa(
    request: MaterializeRequest,
    store: ObjectStorePort,
) -> MaterializeResult: ...
```

The three constants are defined in package-owned `source_definitions/bpa.py`; the app contract
imports them without duplication.

The target schema uses natural CNES, SIGTAP, CBO, CID, and competence keys. It does not persist
PostgreSQL surrogate keys and does not import `_BPADimLookup` backed by SQL. BPA-C and BPA-I keep
distinct provenance while contributing to a common ambulatory-production reconciliation table.
Both required subtype manifests exist even when one extracted table has zero rows.

Each BPA normalization unit contains exactly one subtype chain. `BPA_C` uses the ordered targets
`normalized/<tenant>/BPA_MAG/<competencia>/<run_id>/bpa_c.parquet` and
`.../quality_issues_bpa_c.parquet`; `BPA_I` uses `.../bpa_i.parquet` and
`.../quality_issues_bpa_i.parquet`. `NormalizeResult.manifests` matches its request pair and uses
`source_type=BPA_MAG`; reconciliation requires all four successful manifests. The planner supplies
`reconciliation/<tenant>/<competencia>/<run_id>/bpa.parquet` and
`.../bpa_divergences.parquet` as the only reconciliation destinations. Materialization requires
the exact ordered keys `serving/<tenant>/<run_id>/overview.json` and
`.../by-establishment.json` for dataset `bpa` and returns two manifests/documents with unique
names `overview` and `by-establishment`.

- [ ] **Step 1: Add failing BPA-C/BPA-I contract tests**

  Add tests named `test_normaliza_bpa_c_e_bpa_i_sem_chaves_sql`,
  `test_rejeita_cadeia_com_subtipo_nao_declarado`, and
  `test_preserva_linha_com_referencia_desconhecida_como_divergencia`. Assert `BPA_LAYOUT` exactly
  matches the produced-interface registry and has one entry per dependency.

  Assert deterministic `source_record_id`, canonical codes, integer quantities, null-date
  handling, per-subtype counts, and a quality record instead of the existing silent `continue`.
  Freeze SHA-256 and row counts for every BPA fixture in `fixture-manifest.json`; the verifier
  fails if source or expected bytes change independently.

- [ ] **Step 2: Run the RED test**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/bpa/test_normalize.py -q
  ```

  Expected: FAIL because `data_processor.sources.bpa` does not exist.

- [ ] **Step 3: Implement normalization without database lookups**

  Add package `source_definitions/bpa.py` with the exact immutable definition and make app
  `contract.py` import its dependency/layout constants. Refactor
  `bpa_adapter.py` into pure transformations and implement `normalize_bpa`.
  Require exactly one chain for `BPA_C` or `BPA_I`; reject mixed-subtype, cross-tenant, or
  mixed-competence requests. Validate that subtype's exact ordered target pair and write only its
  data and quality artifacts below:

  ```text
  normalized/<tenant>/BPA_MAG/<competencia>/<run_id>/bpa_c.parquet
  normalized/<tenant>/BPA_MAG/<competencia>/<run_id>/bpa_i.parquet
  normalized/<tenant>/BPA_MAG/<competencia>/<run_id>/quality_issues_<subtype>.parquet
  ```

  Do not put patient CNS/CPF in any serving projection. Preserve required private fields only in
  normalized output under the object-store authorization policy.

- [ ] **Step 4: Implement reconciliation and serving with exact tests**

  Reconciliation groups by tenant, competence, CNES, SIGTAP, CBO, and BPA subtype and records
  reported versus accepted quantities without collapsing provenance. Serving materializes
  `bpa/overview.json` and `bpa/by-establishment.json` with bounded arrays and schema metadata.

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/bpa \
    packages/cnes_domain/tests/orchestration/source_definitions/test_bpa.py -q
  uv run ruff check apps/data_processor/src/data_processor/sources/bpa \
    apps/data_processor/src/data_processor/adapters/bpa_adapter.py \
    apps/data_processor/tests/sources/bpa \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/bpa.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_bpa.py
  uv run mypy apps/data_processor/src/data_processor/sources/bpa
  ```

  Expected: exact fixture equality; no skipped source rows; all quality commands exit 0.

- [ ] **Step 5: Verify and commit**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/bpa \
    --cov=data_processor.sources.bpa --cov-branch --cov-report=term-missing \
    --cov-fail-under=90
  git diff --check
  git status --short
  ```

  Commit:

  ```bash
  git add apps/data_processor/src/data_processor/sources/bpa \
    apps/data_processor/src/data_processor/adapters/bpa_adapter.py \
    apps/data_processor/tests/sources/bpa \
    apps/data_processor/tests/fixtures/bpa \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/bpa.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_bpa.py
  git commit -m "feat(data-processor): migrate BPA to parquet pipeline"
  ```

---

### Task 3: `SRC-012` — Migrate SIA end to end

**Depends on:** `CND-064`. This task freezes its own anonymized SIA fixtures and hashes before
implementation. It is independent of `SRC-010` and `SRC-011`.

**Worktree:** `feat/src-012-sia-parquet-parity`

**Allowed files:**

- Create: `apps/data_processor/src/data_processor/sources/sia/__init__.py`
- Create: `apps/data_processor/src/data_processor/sources/sia/contract.py`
- Create: `packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sia.py`
- Create: `packages/cnes_domain/tests/orchestration/source_definitions/test_sia.py`
- Create: `apps/data_processor/src/data_processor/sources/sia/normalize.py`
- Create: `apps/data_processor/src/data_processor/sources/sia/reference_data.py`
- Create: `apps/data_processor/src/data_processor/sources/sia/reconcile.py`
- Create: `apps/data_processor/src/data_processor/sources/sia/serving.py`
- Modify: `apps/data_processor/src/data_processor/adapters/sia_adapter.py`
- Modify: `apps/data_processor/src/data_processor/adapters/sia_dim_sync.py`
- Create: `apps/data_processor/tests/sources/sia/test_normalize.py`
- Create: `apps/data_processor/tests/sources/sia/test_reference_data.py`
- Create: `apps/data_processor/tests/sources/sia/test_reconcile.py`
- Create: `apps/data_processor/tests/sources/sia/test_serving.py`
- Create: `apps/data_processor/tests/fixtures/sia/raw_manifests.json`
- Create: `apps/data_processor/tests/fixtures/sia/raw_rows.json`
- Create: `apps/data_processor/tests/fixtures/sia/expected_normalized.json`
- Create: `apps/data_processor/tests/fixtures/sia/expected_serving.json`
- Create: `apps/data_processor/tests/fixtures/sia/fixture-manifest.json`

**Forbidden files:** the shared surfaces from Task 1, plus SIHD/BPA paths.

**Produced interface:**

```python
SIA_DEPENDENCIES = (
    RunDependency("SIA_LOCAL", "SIA_APA", True),
    RunDependency("SIA_LOCAL", "SIA_BPI", True),
    RunDependency("SIA_LOCAL", "SIA_BPIHST", True),
    RunDependency("SIA_LOCAL", "DIM_SIGTAP", True),
    RunDependency("SIA_LOCAL", "DIM_MUNICIPIO", True),
)
SIA_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout("SIA_LOCAL", "SIA_APA",
                      ("apa.parquet", "quality_issues_sia_apa.parquet")),
        SubtypeLayout("SIA_LOCAL", "SIA_BPI",
                      ("bpi.parquet", "quality_issues_sia_bpi.parquet")),
        SubtypeLayout("SIA_LOCAL", "SIA_BPIHST",
                      ("bpihst.parquet", "quality_issues_sia_bpihst.parquet")),
        SubtypeLayout("SIA_LOCAL", "DIM_SIGTAP",
                      ("reference_sigtap.parquet", "quality_issues_dim_sigtap.parquet")),
        SubtypeLayout("SIA_LOCAL", "DIM_MUNICIPIO",
                      ("reference_municipio.parquet", "quality_issues_dim_municipio.parquet")),
    ),
    reconciliation_filename="sia.parquet",
    divergence_filename="sia_divergences.parquet",
    serving_documents=("overview", "by-establishment"),
)
SIA_DEFINITION = PipelineDefinition(
    pipeline_id="sia", source_types=("SIA_LOCAL",),
    dependencies=SIA_DEPENDENCIES, layout=SIA_LAYOUT,
)

def normalize_sia(
    request: NormalizeRequest,
    store: ObjectStorePort,
) -> NormalizeResult: ...

def reconcile_sia(
    request: ReconcileRequest,
    store: ObjectStorePort,
) -> ReconcileResult: ...

def materialize_sia(
    request: MaterializeRequest,
    store: ObjectStorePort,
) -> MaterializeResult: ...
```

The three constants are defined in package-owned `source_definitions/sia.py`; the app contract
imports them without duplication.

SIA consumes `SIA_APA`, `SIA_BPI`, `SIA_BPIHST`, `DIM_SIGTAP`, and `DIM_MUNICIPIO`. Reference
data becomes versioned Parquet; `sia_dim_sync.py` must no longer write a SQL table. BPI and
BPIHST retain distinct provenance. Exact duplicate candidates are reported; no unapproved
current-versus-history precedence rule is introduced. The Edge contract emits all five required
manifests; a missing optional DBF becomes a verified zero-row Parquet for its fixed slot rather
than a missing dependency.

Each SIA normalization unit contains exactly one of the five subtype chains. Its ordered targets
below `normalized/<tenant>/SIA_LOCAL/<competencia>/<run_id>/` are the subtype data key and a
subtype-specific quality key: `apa.parquet`/`quality_issues_sia_apa.parquet`,
`bpi.parquet`/`quality_issues_sia_bpi.parquet`,
`bpihst.parquet`/`quality_issues_sia_bpihst.parquet`,
`reference_sigtap.parquet`/`quality_issues_dim_sigtap.parquet`, or
`reference_municipio.parquet`/`quality_issues_dim_municipio.parquet`.
`NormalizeResult.manifests` matches its request pair and uses `source_type=SIA_LOCAL`;
reconciliation requires all ten successful manifests. The planner supplies
`reconciliation/<tenant>/<competencia>/<run_id>/sia.parquet` and
`.../sia_divergences.parquet` as the only reconciliation destinations. Materialization requires
the exact ordered keys `serving/<tenant>/<run_id>/overview.json` and
`.../by-establishment.json` for dataset `sia` and returns two manifests/documents with unique
names `overview` and `by-establishment`.

- [ ] **Step 1: Add failing five-subtype normalization tests**

  Test exact type/canonicalization behavior for all five subtypes, missing optional DBF files,
  malformed dates, oversized numeric values already clamped by the Edge source contract, and
  cross-competence manifest rejection. Assert no `Engine`, SQLAlchemy, or SQL text is accepted by
  the new reference-data adapter. Assert `SIA_LAYOUT` exactly matches the produced-interface
  registry and has one entry per dependency. Freeze SHA-256 and row counts for all raw/expected SIA fixtures
  in `fixture-manifest.json`; a missing DBF case still includes its zero-row manifest and Parquet.

- [ ] **Step 2: Run the RED test**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sia/test_normalize.py \
    apps/data_processor/tests/sources/sia/test_reference_data.py -q
  ```

  Expected: FAIL because `data_processor.sources.sia` does not exist and the current
  `sia_dim_sync.py` is SQL-backed.

- [ ] **Step 3: Implement SIA normalization and reference Parquet**

  Add package `source_definitions/sia.py` with the exact immutable definition and make app
  `contract.py` import its dependency/layout constants. Require exactly one
  chain whose subtype belongs to `SIA_DEPENDENCIES`; reject a mixed-subtype
  request. Replace SQL dimension synchronization with pure creation of
  `reference_sigtap.parquet` and `reference_municipio.parquet`. Dispatch by subtype, validate its
  exact ordered data/quality target pair, and write only those keys. Every member of
  `result.manifests` records the contributing raw manifest hashes.

- [ ] **Step 4: Implement reconciliation and minimal serving**

  Reconcile source quantities and approved values by natural keys while retaining subtype and
  provenance. Materialize `sia/overview.json` and `sia/by-establishment.json`. Add a recursive
  PII-deny assertion and a test proving BPIHST is neither silently discarded nor silently
  preferred over BPI.

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sia \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sia.py -q
  uv run ruff check apps/data_processor/src/data_processor/sources/sia \
    apps/data_processor/src/data_processor/adapters/sia_adapter.py \
    apps/data_processor/src/data_processor/adapters/sia_dim_sync.py \
    apps/data_processor/tests/sources/sia \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sia.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sia.py
  uv run mypy apps/data_processor/src/data_processor/sources/sia
  ```

  Expected: all checks pass and normalized row accounting equals input, quality, and explicit
  duplicate counts.

- [ ] **Step 5: Verify and commit**

  Run:

  ```bash
  uv run pytest apps/data_processor/tests/sources/sia \
    --cov=data_processor.sources.sia --cov-branch --cov-report=term-missing \
    --cov-fail-under=90
  git diff --check
  git status --short
  ```

  Commit:

  ```bash
  git add apps/data_processor/src/data_processor/sources/sia \
    apps/data_processor/src/data_processor/adapters/sia_adapter.py \
    apps/data_processor/src/data_processor/adapters/sia_dim_sync.py \
    apps/data_processor/tests/sources/sia \
    apps/data_processor/tests/fixtures/sia \
    packages/cnes_domain/src/cnes_domain/orchestration/source_definitions/sia.py \
    packages/cnes_domain/tests/orchestration/source_definitions/test_sia.py
  git commit -m "feat(data-processor): migrate SIA to parquet pipeline"
  ```

---

### Task 4: Source-wave integration checkpoint (controller-owned)

**Depends on:** merged `SRC-010`, `SRC-011`, and `SRC-012` PRs plus AWS plan Task 8 and Billing
plan Task 6 on green `develop`. This is the next fixed item in the controller-owned composition
queue and must preserve the already-composed local, AWS, and billing execution policies.

**Worktree:** controller integration worktree; run serially.

**Files:**

- Modify: `apps/data_processor/src/data_processor/composition.py`
- Modify: `apps/data_processor/tests/test_local_composition.py`
- Create: `apps/data_processor/tests/pipeline/test_retained_source_registry.py`
- Create: `apps/data_processor/tests/pipeline/test_retained_source_pipeline_e2e.py`
- Modify: `packages/cnes_domain/src/cnes_domain/orchestration/source_catalog.py`
- Modify: `packages/cnes_domain/tests/orchestration/test_source_catalog.py`
- Modify: `apps/data_processor/pyproject.toml` only if the reviewed source modules introduced an
  already-approved dependency
- Modify: `uv.lock` only through `uv lock`

The controller first extends package-owned `build_source_catalog()` with exactly
`SIHD_DEFINITION`, `BPA_DEFINITION`, and `SIA_DEFINITION`, then attaches these exact
`SourcePipeline` bundles to the `SourceRegistry` frozen in `CND-060`:

```python
SourcePipeline(
    definition=source_catalog.for_pipeline("sihd"),
    normalize=normalize_sihd,
    reconcile=reconcile_sihd,
    materialize=materialize_sihd,
)
SourcePipeline(
    definition=source_catalog.for_pipeline("bpa"),
    normalize=normalize_bpa,
    reconcile=reconcile_bpa,
    materialize=materialize_bpa,
)
SourcePipeline(
    definition=source_catalog.for_pipeline("sia"),
    normalize=normalize_sia,
    reconcile=reconcile_sia,
    materialize=materialize_sia,
)
```

A duplicate source owner is a startup error; an unregistered retained source is an acceptance
error. Each `pipeline_id` is also the canonical `Run.dataset_name`, `RunManifest.dataset_name`,
and `DatasetPointer.dataset_name`; no alias table translates `sihd`, `bpa`, or `sia` at serving
time.

- [ ] **Step 1: Add failing registry and retained-pipeline tests**

  Assert `build_source_registry()` resolves `CNES_LOCAL`, `CNES_NACIONAL`, `SIHD`, `BPA_MAG`,
  and `SIA_LOCAL`. Assert the retained bundles contain `normalize_sihd`/`reconcile_sihd`/
  `materialize_sihd`, the corresponding BPA functions, and the corresponding SIA functions,
  all with the frozen two-parameter callable signatures and no SQLAlchemy `Engine`. Assert each
  bundle carries its exact `*_LAYOUT`, every dependency has exactly one matching subtype layout,
  and the generic CND-060 `StageProcessor` derives every documented normalized, reconciliation,
  divergence, and serving target key without source-name conditionals. Assert the
  exact two SIHD, two BPA, and five SIA required dependencies above and prove a zero-row source
  subtype is present rather than classified as missing. Also assert that the catalog rejects a
  normalized filename reused by two subtype layouts, because that would create two immutable
  producers for the same logical target.

  In `test_retained_source_pipeline_e2e.py`, parameterize `sihd`, `bpa`, and `sia`. For each
  pipeline, seed the real SQLite/filesystem test adapters with one accepted raw-manifest chain for
  every declared dependency, create a `PLANNED` Run, and call the canonical
  `RunPlanningService.launch`. Drain the resulting NORMALIZE dispatch through the real
  `RunUnitCommandHandler`, call `PipelineCoordinator.recover`, and repeat for RECONCILE and
  MATERIALIZE. Record each `(wave_id, dispatch_id)` and require exactly three logical waves,
  successful byte-identical DAG replay, terminal `RunState.PUBLISHED`, a verified
  `RunManifest`, and a `DatasetPointer` whose `dataset_name`, `version_id`, object hashes, and
  serving keys match the source definition. The test must fail if a stage callable is missing,
  any unit receives another tenant/run, the source string cannot be converted to `SourceType`,
  or publication happens before every non-degraded predecessor is terminal.

- [ ] **Step 2: Run RED, integrate, and run GREEN**

  Run before the registry edit:

  ```bash
  uv run pytest apps/data_processor/tests/pipeline/test_retained_source_registry.py \
    apps/data_processor/tests/pipeline/test_retained_source_pipeline_e2e.py -q
  ```

  Expected: FAIL listing the three missing source keys.

  Add the three definitions to `build_source_catalog()` and the three `SourcePipeline` bundles to
  `build_source_registry()`, then run:

  ```bash
  uv run pytest apps/data_processor/tests/test_local_composition.py \
    apps/data_processor/tests/pipeline/test_retained_source_registry.py \
    apps/data_processor/tests/pipeline/test_retained_source_pipeline_e2e.py \
    apps/data_processor/tests/sources \
    packages/cnes_domain/tests/orchestration/test_source_catalog.py -q
  uv run ruff check apps/data_processor/src apps/data_processor/tests \
    packages/cnes_domain/src/cnes_domain/orchestration/source_catalog.py \
    packages/cnes_domain/tests/orchestration/test_source_catalog.py
  uv run mypy apps/data_processor/src
  ```

  Expected: all source plugins and the integrated registry pass.

- [ ] **Step 3: Commit the integration checkpoint**

  ```bash
  git add apps/data_processor/src/data_processor/composition.py \
    apps/data_processor/tests/test_local_composition.py \
    apps/data_processor/tests/pipeline/test_retained_source_registry.py \
    apps/data_processor/tests/pipeline/test_retained_source_pipeline_e2e.py \
    packages/cnes_domain/src/cnes_domain/orchestration/source_catalog.py \
    packages/cnes_domain/tests/orchestration/test_source_catalog.py \
    apps/data_processor/pyproject.toml uv.lock
  git commit -m "chore(data-processor): integrate retained source plugins"
  ```

  If neither dependency file changed, omit it from `git add`; never stage an unrelated lockfile
  change.

---

### Task 5: `MIG-010` — Produce historical shadow equivalence evidence

**Depends on:** `CND-054`, the source-wave integration checkpoint, and all retained source PRs.

**Worktree:** `test/mig-010-historical-shadow-report`

**Files:**

- Create: `apps/data_processor/src/data_processor/migration/__init__.py`
- Create: `apps/data_processor/src/data_processor/migration/equivalence.py`
- Create: `apps/data_processor/tests/migration/test_equivalence.py`
- Create: `scripts/run_historical_shadow.py`
- Create: `scripts/run_historical_shadow_test.py`
- Create: `docs/fixtures/migration/equivalence-contract-v1.json`
- Modify: `scripts/shadow_diff.py`

**Produced interfaces:**

```python
class ComparisonStatus(StrEnum):
    MATCH = "MATCH"
    EXPLAINED = "EXPLAINED"
    MISMATCH = "MISMATCH"

@dataclass(frozen=True)
class MetricComparison:
    metric: str
    legacy_value: int | str | None
    candidate_value: int | str | None
    status: ComparisonStatus
    rule_id: str | None

@dataclass(frozen=True)
class SourceEquivalenceReport:
    tenant_id: str
    source_type: SourceType
    competencia: Competencia
    legacy_sha256: str
    candidate_version_id: DatasetVersionId
    comparisons: tuple[MetricComparison, ...]

    @property
    def accepted(self) -> bool: ...

def compare_shadow_run(
    *,
    contract: EquivalenceContract,
    legacy: Mapping[str, Scalar],
    candidate: Mapping[str, Scalar],
) -> tuple[MetricComparison, ...]: ...
```

The JSON contract enumerates exact canonical sort/key normalization and approved rule-change IDs.
It must not contain numeric tolerances. `EXPLAINED` is valid only when the differing metric names
an approved, documented rule ID in this file.

- [ ] **Step 1: Add failing exact-equivalence tests**

  Cover exact matches, an approved rule change, an unexplained mismatch, missing metrics,
  duplicate keys, order-insensitive canonical rows, and report serialization with input hashes.
  Add a test that rejects contract fields named `tolerance`, `percent`, or `epsilon`.

- [ ] **Step 2: Run RED**

  ```bash
  uv run pytest apps/data_processor/tests/migration/test_equivalence.py \
    scripts/run_historical_shadow_test.py -q
  ```

  Expected: FAIL because the migration equivalence module and CLI do not exist.

- [ ] **Step 3: Implement comparison and the historical runner**

  Extend `shadow_diff.py` to return structured row/key differences instead of only a cell count.
  The CLI accepts explicit `--tenant`, repeatable `--source`, `--from-competencia`,
  `--to-competencia`, `--legacy-root`, `--candidate-root`, and `--report-root`. For every candidate
  competence, it creates the canonical `PLANNED` Run, invokes `RunPlanningService.launch`, drains
  NORMALIZE/RECONCILE/MATERIALIZE through the composed worker entrypoint, calls
  `PipelineCoordinator.recover` between dispatches, and reads the candidate only through the
  resulting active `DatasetPointer`. It writes one immutable JSON report per source and competence,
  then writes a signed-off aggregate containing every report SHA-256.

  A run exits 1 when any required source/competence is absent, `MISMATCH` exists, a manifest hash
  cannot be verified, or a generated report would overwrite an existing object.

- [ ] **Step 4: Verify the fixture matrix and commit**

  ```bash
  uv run pytest apps/data_processor/tests/migration/test_equivalence.py \
    scripts/run_historical_shadow_test.py scripts/shadow_diff_test.py -q
  uv run ruff check apps/data_processor/src/data_processor/migration \
    apps/data_processor/tests/migration scripts/run_historical_shadow.py \
    scripts/run_historical_shadow_test.py scripts/shadow_diff.py
  uv run mypy apps/data_processor/src/data_processor/migration \
    scripts/run_historical_shadow.py
  git diff --check
  ```

  Expected: all commands pass and the negative fixture makes the CLI return 1 in its test.

  Commit:

  ```bash
  git add apps/data_processor/src/data_processor/migration \
    apps/data_processor/tests/migration scripts/run_historical_shadow.py \
    scripts/run_historical_shadow_test.py scripts/shadow_diff.py \
    docs/fixtures/migration/equivalence-contract-v1.json
  git commit -m "test(migration): prove historical source equivalence"
  ```

**Gate evidence before `MIG-011`:** run the CLI over the approved historical range for every
retained tenant/source; attach the aggregate report URI, hash, exact command, dependency commit,
and zero-unexplained-mismatch result to the `MIG-010` PR or issue.

---

### Task 6: `MIG-011` — Cut product reads to the active `DatasetPointer`

**Depends on:** accepted `MIG-010` evidence and `CND-064` pointer-only serving implementation.

**Worktree:** `feat/mig-011-pointer-only-product-cutover`

**Files:**

- Modify: `apps/central_api/src/central_api/routes/overview.py`
- Modify: `apps/central_api/tests/test_overview_routes.py`
- Modify: `apps/central_api/src/central_api/routes/serving.py`
- Modify: `apps/central_api/tests/routes/test_serving.py`
- Create: `apps/central_api/tests/routes/test_pointer_only_cutover.py`
- Modify: `apps/web_dashboard/src/api/hooks/useServingOverview.ts`
- Modify: `apps/web_dashboard/tests/unit/api/hooks/useServingOverview.test.tsx`
- Modify: `apps/web_dashboard/tests/mocks/handlers.ts`
- Create: `apps/web_dashboard/tests/e2e/dashboard.spec.ts`

These are controller/integration-owned files. The CND local-product slice may already have added
the new serving client; this task disables the old overview/faturamento product-read routes and
makes the existing CND-062 serving route the only product-read source. Agent status and run
status remain operational `ControlPlanePort` reads and are deliberately outside this dataset
cutover. This task must not duplicate the serving route or access port.

- [ ] **Step 1: Add failing pointer-only API tests**

  Configure previous and current versions for each dataset `cnes`, `sihd`, `bpa`, and `sia`.
  Assert every serving response resolves only its authorized `CURRENT` pointer, emits the selected
  version in `X-Dataset-Version`, and never invokes `DashboardRepo` SQL methods. Assert missing
  active content returns 503 and does not fall back to PostgreSQL or an older version. Assert the
  retired legacy overview/faturamento routes return 410 without constructing a SQL repository.

- [ ] **Step 2: Add failing dashboard tests**

  Extend the CND-063 serving handlers and test the existing `useServingOverview` hook. Assert it
  calls only `/api/v1/dashboard/serving/cnes/overview`, sends no tenant header, handles 503, and
  issues no legacy overview/faturamento request. In Playwright, atomically switch `CURRENT` and
  assert the next query refresh renders only the new version without mixing assets from two runs.

- [ ] **Step 3: Run RED**

  ```bash
  uv run pytest apps/central_api/tests/routes/test_pointer_only_cutover.py \
    apps/central_api/tests/test_overview_routes.py \
    apps/central_api/tests/routes/test_serving.py -q
  cd apps/web_dashboard && bun run test -- useServingOverview.test.tsx
  ```

  Expected: API tests show a legacy route still reading SQL and/or the new multi-dataset serving
  matrix is incomplete; frontend assertions expose any legacy request.

- [ ] **Step 4: Remove fallback behavior and verify**

  Keep the CND-062 authorization service and route as the single BFF. Resolve dataset names only
  from the allowlisted source registry, never from an object key; do not accept a browser-supplied
  tenant or expose object-store credentials. Disable the legacy overview/faturamento handlers with
  410 responses and remove their repository construction. Configure the existing hook's bounded
  refetch/invalidation behavior so a pointer switch cannot retain mixed-run UI state.

  Run:

  ```bash
  uv run pytest apps/central_api/tests/routes/test_pointer_only_cutover.py \
    apps/central_api/tests/test_overview_routes.py \
    apps/central_api/tests/routes/test_serving.py -q
  uv run ruff check apps/central_api/src apps/central_api/tests
  uv run mypy apps/central_api/src
  cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test && bun run build
  cd apps/web_dashboard && bunx playwright test tests/e2e/dashboard.spec.ts
  ```

  Expected: all commands pass; the negative API test proves no legacy fallback.

- [ ] **Step 5: Commit**

  ```bash
  git add apps/central_api/src/central_api/routes/overview.py \
    apps/central_api/src/central_api/routes/serving.py \
    apps/central_api/tests/test_overview_routes.py \
    apps/central_api/tests/routes/test_serving.py \
    apps/central_api/tests/routes/test_pointer_only_cutover.py \
    apps/web_dashboard/src/api/hooks/useServingOverview.ts \
    apps/web_dashboard/tests/unit/api/hooks/useServingOverview.test.tsx \
    apps/web_dashboard/tests/mocks/handlers.ts \
    apps/web_dashboard/tests/e2e/dashboard.spec.ts
  git commit -m "feat(product): cut reads to active dataset pointer"
  ```

---

### Task 7: `MIG-012` — Stop all new PostgreSQL and MinIO writes

**Depends on:** `MIG-011` deployed and observed with accepted pointer-only smoke evidence.

**Worktree:** `feat/mig-012-stop-legacy-writes`

**Files:**

- Create: `apps/central_api/tests/routes/test_target_only_ingestion.py`
- Modify: `apps/central_api/src/central_api/routes/extractions.py`
- Modify: `apps/central_api/src/central_api/routes/jobs.py`
- Modify: `apps/central_api/tests/routes/test_e2e_upload_register.py`
- Modify: `apps/central_api/tests/routes/test_jobs_register_v2.py`
- Modify: `apps/data_processor/src/data_processor/consumer.py`
- Modify: `apps/data_processor/src/data_processor/poll.py`
- Create: `apps/data_processor/tests/test_target_only_processor.py`
- Modify: `.env.example` (integration-owned)
- Modify: `docker-compose.yml` (integration-owned)

Do not delete the legacy data or migration exporter in this task. The goal is a provable write
fence and an observation window before export/removal.

- [ ] **Step 1: Add failing write-spy tests**

  Inject spies that raise on `extractions_repo`, SQL repository writes, `MinioWrapper`, and
  `MinioObjectStorage`. Exercise full upload registration, job completion, processing, and
  publication through target ports. Assert the target control plane and object store receive the
  operations and every legacy spy has zero calls.

- [ ] **Step 2: Run RED**

  ```bash
  uv run pytest apps/central_api/tests/routes/test_target_only_ingestion.py \
    apps/data_processor/tests/test_target_only_processor.py -q
  ```

  Expected: FAIL showing at least one direct legacy call or import.

- [ ] **Step 3: Remove legacy write routing**

  Rewire the remaining route/consumer/poller paths exclusively through the already integrated
  control-plane and object-store ports. The old endpoint shape may return a migration response,
  but it must not mint a MinIO URL, insert landing rows, or persist Gold facts. Configure local
  development composition with SQLite/filesystem; keep emulator-specific AWS composition in its
  existing profile. Do not introduce a reversible runtime flag that can silently reactivate
  legacy writes after this gate.

- [ ] **Step 4: Verify target-only ingestion and commit**

  ```bash
  uv run pytest apps/central_api/tests/routes/test_target_only_ingestion.py \
    apps/central_api/tests/routes/test_e2e_upload_register.py \
    apps/central_api/tests/routes/test_jobs_register_v2.py \
    apps/data_processor/tests/test_target_only_processor.py -q
  uv run ruff check apps/central_api/src apps/central_api/tests \
    apps/data_processor/src apps/data_processor/tests
  uv run mypy apps/central_api/src apps/data_processor/src
  docker compose config
  git diff --check
  ```

  Expected: all commands pass; rendered Compose has no active application dependency on the
  legacy services in the target-only profile.

  Commit:

  ```bash
  git add apps/central_api/src/central_api/routes/extractions.py \
    apps/central_api/src/central_api/routes/jobs.py \
    apps/central_api/tests/routes/test_target_only_ingestion.py \
    apps/central_api/tests/routes/test_e2e_upload_register.py \
    apps/central_api/tests/routes/test_jobs_register_v2.py \
    apps/data_processor/src/data_processor/consumer.py \
    apps/data_processor/src/data_processor/poll.py \
    apps/data_processor/tests/test_target_only_processor.py \
    .env.example docker-compose.yml
  git commit -m "feat(migration): stop legacy data-plane writes"
  ```

**Operational gate before `MIG-013`:** record the cutover timestamp, last PostgreSQL transaction
marker, last MinIO object timestamp, target run/version, and observation result showing no later
legacy writes.

---

### Task 8: `MIG-013` — Export required legacy history to immutable Parquet

**Depends on:** accepted `MIG-012` write-fence evidence.

**Worktree:** `feat/mig-013-export-legacy-history`

**Files:**

- Create: `packages/cnes_contracts/src/cnes_contracts/legacy_export.py`
- Create: `packages/cnes_contracts/tests/test_legacy_export.py`
- Create: `packages/cnes_infra/src/cnes_infra/migration/__init__.py`
- Create: `packages/cnes_infra/src/cnes_infra/migration/legacy_export.py`
- Create: `packages/cnes_infra/src/cnes_infra/migration/verify_export.py`
- Create: `packages/cnes_infra/tests/migration/test_legacy_export.py`
- Create: `packages/cnes_infra/tests/migration/test_verify_export.py`
- Create: `scripts/export_legacy_history.py`
- Create: `scripts/verify_legacy_history.py`
- Create: `scripts/tests/test_export_legacy_history_cli.py`

**Produced contract:**

```python
class LegacyExportObject(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    object_key: str
    source_kind: Literal["POSTGRES_TABLE", "MINIO_OBJECT"]
    source_name: str
    competencia: str | None
    row_count: int | None = Field(ge=0)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

class LegacyHistoryExportManifest(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True)

    manifest_version: Literal[1]
    tenant_id: str
    export_id: str
    stopped_writes_at: datetime
    postgres_marker: str
    minio_marker: str
    objects: tuple[LegacyExportObject, ...]
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
```

Exported keys use
`raw/<tenant>/LEGACY_EXPORT/<competencia>/<export_id>/<source_name>.parquet`; the immutable
manifest is beside the objects. `LEGACY_EXPORT` is archival input, never an active source plugin
or serving dataset.

- [ ] **Step 1: Add failing contract and exporter tests**

  Cover stable canonical JSON hashing, empty tables, chunk-boundary stability, timezone/date and
  decimal conversion, object-copy hashing, duplicate target refusal, partial-export resume with
  the same export ID, different-payload conflict, and tamper detection.

- [ ] **Step 2: Run RED**

  ```bash
  uv run pytest packages/cnes_contracts/tests/test_legacy_export.py \
    packages/cnes_infra/tests/migration \
    scripts/tests/test_export_legacy_history_cli.py -q
  ```

  Expected: FAIL because the export contract and implementation do not exist.

- [ ] **Step 3: Implement bounded, idempotent export**

  Stream each approved PostgreSQL table in stable primary-key order into bounded Parquet parts;
  never load the whole table into memory. Copy each retained MinIO object through a streaming
  reader. For every part, write to an attempt key, compute SHA-256, publish once to the final key,
  verify stat/hash, and append the object entry. Write the final manifest only after every object
  verifies. Retry with identical inputs returns the same entries; changed bytes conflict.

  Keep legacy client imports confined to `cnes_infra.migration.legacy_export`; the verifier uses
  only `ObjectStorePort` and remains useful after `MIG-014` deletes the exporter.

- [ ] **Step 4: Verify implementation and commit**

  ```bash
  uv run pytest packages/cnes_contracts/tests/test_legacy_export.py \
    packages/cnes_infra/tests/migration \
    scripts/tests/test_export_legacy_history_cli.py -q
  uv run ruff check packages/cnes_contracts/src/cnes_contracts/legacy_export.py \
    packages/cnes_contracts/tests/test_legacy_export.py \
    packages/cnes_infra/src/cnes_infra/migration \
    packages/cnes_infra/tests/migration scripts/export_legacy_history.py \
    scripts/verify_legacy_history.py scripts/tests/test_export_legacy_history_cli.py
  uv run mypy packages/cnes_contracts/src/cnes_contracts/legacy_export.py \
    packages/cnes_infra/src/cnes_infra/migration
  git diff --check
  ```

  Expected: all commands pass, including corrupted-object and duplicate-target negatives.

  Commit:

  ```bash
  git add packages/cnes_contracts/src/cnes_contracts/legacy_export.py \
    packages/cnes_contracts/tests/test_legacy_export.py \
    packages/cnes_infra/src/cnes_infra/migration \
    packages/cnes_infra/tests/migration scripts/export_legacy_history.py \
    scripts/verify_legacy_history.py scripts/tests/test_export_legacy_history_cli.py
  git commit -m "feat(migration): export immutable legacy history"
  ```

**Operational gate before `MIG-014`:** execute export from the frozen legacy markers, run
`scripts/verify_legacy_history.py` from a separate process using target-store credentials only,
and attach manifest URI/hash, object count, row counts, total bytes, and zero verification errors.

---

### Task 9: `MIG-014` — Remove all legacy runtime dependencies

**Depends on:** verified `MIG-013` manifest and an approved rollback decision that uses immutable
exports and target backups rather than re-enabling legacy writes.

**Worktree:** `refactor/mig-014-remove-legacy-runtime`

**This task is destructive and serial.** Resolve the exact paths with read-only searches first;
delete only the reviewed inventory below. Do not delete approved architecture specs or immutable
export evidence merely because they describe the removed systems historically.

**Create first:**

- Create: `tests/acceptance/test_no_legacy_runtime.py`
- Create: `scripts/check_no_legacy_runtime.py`
- Create: `scripts/tests/test_check_no_legacy_runtime.py`

**Delete after the test is RED:**

- Delete: `apps/cnes_db_migrator/`
- Delete: `docker-compose.keycloak/`
- Delete: `packages/cnes_infra/alembic.ini`
- Delete: `packages/cnes_infra/src/cnes_infra/alembic/`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/dashboard_models.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/dim_lookup.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/extractions_repo.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/object_storage.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/query_counter.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/rls.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/schema_v2.py`
- Delete: `packages/cnes_infra/src/cnes_infra/storage/repositories/`
- Delete: `packages/cnes_infra/src/cnes_infra/ingestion/db_client.py`
- Delete: `packages/cnes_infra/src/cnes_infra/migration/legacy_export.py`
- Delete: `packages/cnes_infra/tests/test_extractions_repo_mint.py`
- Delete: `packages/cnes_infra/tests/test_migration_014.py`
- Delete: `packages/cnes_infra/tests/alembic/`
- Delete: `packages/cnes_infra/tests/fixtures/seed_postgres.py`
- Delete: `packages/cnes_infra/tests/ingestion/test_db_client.py`
- Delete: `packages/cnes_infra/tests/storage/repositories/`
- Delete: `packages/cnes_infra/tests/storage/test_dashboard_models.py`
- Delete: `packages/cnes_infra/tests/storage/test_dim_lookup.py`
- Delete: `packages/cnes_infra/tests/storage/test_extractions_repo_v2.py`
- Delete: `packages/cnes_infra/tests/storage/test_migration_015_dashboard_users.py`
- Delete: `packages/cnes_infra/tests/storage/test_migration_016_access_requests.py`
- Delete: `packages/cnes_infra/tests/storage/test_object_storage.py`
- Delete: `packages/cnes_infra/tests/storage/test_query_counter.py`
- Delete: `packages/cnes_infra/tests/storage/test_rls.py`
- Delete: `packages/cnes_infra/tests/storage/test_schema_v2.py`
- Delete: `apps/central_api/src/central_api/repositories/agent_status_repo.py`
- Delete: `apps/central_api/src/central_api/repositories/dashboard_repo.py`
- Delete: `apps/central_api/src/central_api/repositories/dashboard_repo_overview.py`
- Delete: `apps/central_api/tests/repositories/`
- Delete: `scripts/export_legacy_history.py`
- Delete: `docs/data-dictionary-firebird-bigquery.md`
- Delete: `docs/migration-plan-gold-v2.md`

**Modify in the serial integration lane:**

- Modify: `packages/cnes_infra/src/cnes_infra/config.py`
- Modify: `packages/cnes_infra/src/cnes_infra/__init__.py`
- Modify: `packages/cnes_infra/src/cnes_infra/telemetry.py`
- Modify: `packages/cnes_infra/tests/conftest.py`
- Modify: `packages/cnes_infra/pyproject.toml`
- Modify: `apps/central_api/src/central_api/app.py`
- Modify: `apps/central_api/src/central_api/deps.py`
- Modify: `apps/central_api/src/central_api/repositories/__init__.py`
- Modify: `apps/data_processor/src/data_processor/config.py`
- Modify: `apps/data_processor/src/data_processor/main.py`
- Modify: `apps/data_processor/pyproject.toml`
- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `.env.example`
- Modify: `docker-compose.yml`
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `docs/architecture.md`
- Modify: `docs/project-context.md`
- Modify: `docs/roadmap.md`
- Modify: `.github/workflows/ci.yml`

The deletion list is a baseline from pre-redesign `develop`. Before deletion, add any later-created
runtime/config file containing a forbidden dependency to the reviewed PR inventory. Do not broaden
the deletion to source specs, migration evidence, or Firebird/DBF Edge extraction code.

- [ ] **Step 1: Add the failing absence scanner and acceptance test**

  `check_no_legacy_runtime.py` scans runtime Python/Go/TypeScript imports, dependency manifests,
  environment examples, Compose/charts, CI, and operational docs. It rejects:

  ```text
  sqlalchemy, psycopg, psycopg2, asyncpg, alembic, minio,
  basedosdados, google-cloud-bigquery, google-cloud-bigquery-storage,
  google-cloud-bigquery-connection, pandas-gbq,
  DB_URL, MINIO_, GCP_PROJECT_ID, KEYCLOAK_
  ```

  Scope exemptions are explicit paths for approved design specs, export/equivalence evidence, and
  the scanner's own deny list. String suppression comments are forbidden.

  The acceptance test also imports local and AWS composition roots with network clients replaced
  by target-adapter fakes, proving startup does not import a removed package.

- [ ] **Step 2: Run RED and capture the inventory**

  ```bash
  uv run pytest tests/acceptance/test_no_legacy_runtime.py \
    scripts/tests/test_check_no_legacy_runtime.py -q
  uv run python scripts/check_no_legacy_runtime.py
  ```

  Expected: FAIL with concrete legacy dependency, environment, deployment, and documentation
  paths. Save this output in the PR description as the removal inventory.

- [ ] **Step 3: Delete legacy modules and remove dependency/configuration surfaces**

  Use `apply_patch` for text edits and reviewed file deletions. Remove the PostgreSQL/MinIO
  composition and middleware, SQL instrumentation, Alembic application, old Gold repositories,
  BigQuery extras, Keycloak development service, and obsolete docs. Preserve generic OIDC,
  mTLS/Edge identity, target SQLite/DynamoDB/S3/filesystem adapters, the post-export verifier,
  and Firebird/DBF source extraction.

  Regenerate `uv.lock` from the reduced dependency graph; do not hand-edit it.

- [ ] **Step 4: Run absence, profile, and full regression verification**

  ```bash
  uv lock
  uv run python scripts/check_no_legacy_runtime.py
  uv run pytest tests/acceptance/test_no_legacy_runtime.py \
    scripts/tests/test_check_no_legacy_runtime.py -q
  uv run ruff check .
  uv run mypy packages apps/central_api/src apps/data_processor/src
  uv run pytest -q
  cd apps/dump_agent_go && go test -race ./...
  cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test && bun run build
  docker compose config
  git diff --check
  ```

  Expected: deny scan returns zero findings; all suites pass; rendered Compose contains no
  PostgreSQL, MinIO, Keycloak, or database migrator service.

- [ ] **Step 5: Commit the reviewed removal**

  ```bash
  git add -A
  git status --short
  git diff --cached --stat
  git commit -m "refactor(platform): remove legacy runtime dependencies"
  ```

  Before committing, compare `git status --short` with the reviewed inventory. Any unreviewed
  deletion stops the task.

---

### Task 10: `MIG-015` — Run and persist the final local/AWS acceptance matrix

**Depends on:** merged `MIG-014`, `AWS-014`, and `BIL-024` whenever the AWS release sets
`BILLING_MODE=stripe`. An AWS release must not claim Stripe billing support without `BIL-024`.

**Worktree:** `test/mig-015-final-acceptance-matrix`

**Files:**

- Create: `tests/acceptance/test_local_profile.py`
- Create: `tests/acceptance/test_aws_profile.py`
- Create: `tests/acceptance/test_retained_sources_aws.py`
- Create: `tests/acceptance/test_source_matrix.py`
- Create: `tests/acceptance/test_cutover_recovery.py`
- Create: `scripts/run_acceptance_matrix.py`
- Create: `scripts/tests/test_run_acceptance_matrix.py`
- Create: `docs/runbooks/cutover-acceptance.md`
- Modify: `.github/workflows/ci.yml` (integration-owned)

**Produced evidence schema:**

```python
@dataclass(frozen=True)
class AcceptanceCaseResult:
    profile: Literal["local", "aws"]
    case_id: str
    dependency_commit: str
    status: Literal["PASS", "FAIL", "SKIP_NOT_RELEASED"]
    evidence_sha256: str
    duration_ms: int

@dataclass(frozen=True)
class AcceptanceMatrixResult:
    commit_sha: str
    source_types: tuple[str, ...]
    cases: tuple[AcceptanceCaseResult, ...]

    @property
    def releasable(self) -> bool: ...
```

`SKIP_NOT_RELEASED` is allowed only for Stripe cases when the release manifest explicitly sets
AWS billing to disabled. It is forbidden for data plane, tenancy, authentication, source, audit,
backup/restore, fencing, publication, or legacy-absence cases.

- [ ] **Step 1: Add failing matrix-runner tests**

  Test deterministic case discovery, missing-case failure, duplicate case IDs, failed-case exit 1,
  signed evidence hashing, and the Stripe-only skip rule. Require matrix cases for:

  - local restart persistence, full/delta ingestion, gap-triggered full resync, stale fence denial,
    publisher crash recovery, pointer-only authorized dashboard, and backup/restore;
  - AWS cross-tenant denial, stale-GSI revalidation, transactional publication/outbox replay,
    executor failure/retry, signed serving expiry, and audit delivery;
  - SIHD, BPA, and SIA golden/shadow parity for the approved competence range;
  - SIHD, BPA, and SIA through the DynamoDB, S3, and Step Functions emulators for three persisted
    waves (`NORMALIZE`, `RECONCILE`, `MATERIALIZE`), ending at the active serving pointer;
  - zero legacy runtime dependencies and zero post-cutover legacy writes;
  - Stripe trial/renewal/failure/cancellation/revocation only when billing is released.

- [ ] **Step 2: Run RED**

  ```bash
  uv run pytest scripts/tests/test_run_acceptance_matrix.py \
    tests/acceptance/test_local_profile.py \
    tests/acceptance/test_aws_profile.py \
    tests/acceptance/test_retained_sources_aws.py \
    tests/acceptance/test_source_matrix.py \
    tests/acceptance/test_cutover_recovery.py -q
  ```

  Expected: FAIL because the final runner and matrix tests do not exist.

- [ ] **Step 3: Implement the matrix runner and CI job**

  The runner accepts `--commit`, `--profile local|aws|all`, `--billing disabled|stripe`, and
  `--evidence-dir`. It executes the named suites, captures structured results, hashes raw logs,
  writes immutable JSON evidence, and exits nonzero if `releasable` is false. It never changes a
  dataset pointer or external environment.

  Add a non-production CI job that runs local acceptance and emulator-backed AWS acceptance.
  Parameterize `test_retained_sources_aws.py` over SIHD, BPA, and SIA; for each source, seed its
  immutable raw manifest in S3, persist and advance all three waves through DynamoDB and the Step
  Functions emulator, then assert that the published serving manifest is exactly the object
  selected by the active `DatasetPointer`. The test must use the real source registry and stage
  processors, not mocks or a source-specific shortcut.
  Production endpoint/IAM verification remains gated by the deployment specification.

- [ ] **Step 4: Run the complete final verification**

  ```bash
  uv run python scripts/check_no_legacy_runtime.py
  uv run pytest tests/acceptance -q
  uv run python scripts/run_acceptance_matrix.py \
    --commit "$(git rev-parse HEAD)" --profile all --billing disabled \
    --evidence-dir .artifacts/acceptance
  uv run ruff check .
  uv run mypy packages apps/central_api/src apps/data_processor/src
  uv run pytest -q
  cd apps/dump_agent_go && go test -race ./...
  cd apps/web_dashboard && bun run lint && bun run typecheck && bun run test && bun run build
  docker compose config
  git diff --check
  ```

  Expected: every mandatory matrix case is `PASS`, deny scan is empty, and all repository quality
  gates pass. For a Stripe-enabled release, rerun with `--billing stripe` against the approved
  test-clock environment and require all Stripe cases to pass.

- [ ] **Step 5: Commit the final acceptance harness**

  ```bash
  git add tests/acceptance scripts/run_acceptance_matrix.py \
    scripts/tests/test_run_acceptance_matrix.py \
    docs/runbooks/cutover-acceptance.md .github/workflows/ci.yml
  git commit -m "test(platform): add final cutover acceptance matrix"
  ```

## Final Release Gate

The redesign is complete only after the integrated `develop` commit has:

1. accepted immutable shadow/equivalence evidence for every retained source and competence;
2. pointer-only product reads with no fallback;
3. recorded proof that legacy writes stopped before export;
4. a separately verified immutable legacy-export manifest;
5. zero forbidden runtime/config/deployment dependencies;
6. a fully passing local/AWS acceptance matrix, plus Stripe test-clock evidence when released.

Individual source worktree success is not sufficient. The controller records the final commit SHA,
matrix evidence URI/hash, export manifest URI/hash, shadow aggregate URI/hash, and release decision
on the `MIG-015` issue before any production rollout proceeds.
