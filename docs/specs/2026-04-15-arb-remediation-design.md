# ARB Remediation Design — CnesData Pipeline V3 Readiness

**Date:** 2026-04-15
**Status:** APPROVED
**Scope:** Full ARB remediation across 4 phases

## Context

ARB assessment identified 9 critical structural vulnerabilities in the CnesData pipeline:
CBO in fato_vinculo PK causing phantom duplicates, God Adapter, Polars vendor lock-in
in ports, chatty I/O in processor, OOM risk on download, missing circuit breaker,
black-box orchestration (no tracing), volatile natural keys, and hardcoded network assumptions.

V3 (FastAPI microservices) horizon: ~1-2 months. Investments in clean interfaces and
repository segregation will be directly reused.

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| CBO handling | Snapshot Replace (delete-and-load) | Preserves CBO in PK for legitimate multi-role; eliminates phantoms via atomic replace |
| DELETE scope | tenant_id + competencia + fonte | Source independence: LOCAL reprocessing cannot destroy NACIONAL data |
| Adapter split | 3 repositories + UnitOfWork | Direct path to V3 microservices; each repo = future service boundary |
| Port interfaces | `Iterable[dict]` (not `pl.DataFrame`) | Polars stays as internal analytical engine; zero vendor lock-in at boundaries |
| Processor decoupling | Claim-Check (enrich Job) + JobMetadataPort fallback | Eliminates chatty I/O; processor receives all metadata in Job dataclass |
| Download resilience | httpx streaming + circuit breaker with exponential backoff | Solves OOM and network fragility simultaneously |
| Observability | OpenTelemetry spans at contour points | Minimal instrumentation; leverages existing trace_context column in jobs table |
| mTLS | Config-level (env vars for certs), not code-level | Infrastructure concern; code just supports TLS config params |

---

## Phase 1 — Snapshot Replace + Business Integrity Tests

### Problem

`PostgresAdapter._upsert_chunks()` uses `ON CONFLICT (tenant_id, competencia, cpf, cnes, cbo)`.
When DATASUS reclassifies ACS (CBO 515105) to ACE (CBO 515110), the upsert **inserts a new row**
because CBO changed. The old-CBO row persists — professional appears duplicated in the same
competencia/establishment, invalidating municipal capacity reports.

### Solution: Snapshot Replace

Keep CBO in PK (professional can legitimately hold multiple CBOs). Replace upsert with
atomic delete-and-load:

```
BEGIN TRANSACTION
  DELETE FROM gold.fato_vinculo
    WHERE tenant_id = :tid
      AND competencia = :comp
      AND fontes ? :fonte
  ;
  BULK INSERT (complete snapshot from Parquet)
COMMIT
```

**Atomicity:** delete + insert in same transaction. Failure = clean rollback.
**Idempotency:** reprocessing same Parquet produces identical result.

### Changes to PostgresAdapter

New method `_snapshot_replace_vinculos` replaces `_upsert_chunks` for vinculos:

```python
def _snapshot_replace_vinculos(
    self, con, competencia: str, fonte: str, rows: list[dict],
) -> None:
    con.execute(
        text(
            "DELETE FROM gold.fato_vinculo "
            "WHERE tenant_id = :tid AND competencia = :comp "
            "AND fontes ? :fonte"
        ),
        {"tid": get_tenant_id(), "comp": competencia, "fonte": fonte},
    )
    for chunk in _chunked(rows, _CHUNK_SIZE):
        con.execute(insert(fato_vinculo).values(chunk))
```

`gravar_profissionais` flow becomes:
1. Extract `fonte` from DataFrame (first value of FONTE column)
2. `_upsert_chunks` for `dim_profissional` (keeps upsert — dimension is SCD Type 1)
3. `_snapshot_replace_vinculos` for `fato_vinculo` (delete-and-load)

### Alembic Migration: 006_add_gin_index_fontes.py

GIN index on JSONB `fontes` column for DELETE performance with `? :fonte`:

```sql
CREATE INDEX idx_fato_vinculo_fontes ON gold.fato_vinculo USING GIN (fontes);
```

### Business Integrity Tests

| Test | Validates |
|------|-----------|
| `test_reclassificacao_cbo_nao_cria_fantasma` | Ingest CBO=515105, then CBO=515110 for same (cpf, cnes, comp). Result: 1 row, not 2. |
| `test_profissional_multiplos_cbos_legitimos_preservados` | Professional with 2 real CBOs in same facility. Result: 2 rows (both in snapshot). |
| `test_snapshot_replace_idempotente` | Process same Parquet 2x. Result: identical count. |
| `test_fonte_local_nao_destroi_nacional` | LOCAL snapshot does not delete NACIONAL records. |
| `test_fonte_nacional_nao_destroi_local` | Inverse. |
| `test_delete_insert_atomico_em_falha` | Force INSERT error (FK violation). Validate DELETE was rolled back. |
| `test_competencia_isolada` | Snapshot for 2026-03 does not affect 2026-02 vinculos. |
| `test_profissional_troca_estabelecimento_na_mesma_competencia` | CPF moves from CNES A to CNES B. Correct: 1 vinculo at CNES B, zero at CNES A. |

### Schema

No schema changes. PK remains `(tenant_id, competencia, cpf, cnes, cbo)`.

---

## Phase 2 — Split God Adapter + Generic Interfaces

### Problem

`PostgresAdapter` (204 lines) is a God Object:
1. Receives `pl.DataFrame` — couples infra to analytical engine
2. Contains business parsing (`SUS` "S"->True, `FONTE`->JSONB) in `_build_*_rows`
3. Mixes 3 domains (profissional, estabelecimento, vinculo) in one class

Ports (`StoragePort`, `repository.py`) accept/return `pl.DataFrame` — domain-level vendor lock-in.

### Solution: 3 Repositories + UnitOfWork

**Directory structure:**

```
packages/cnes_infra/src/cnes_infra/storage/
  repositories/
    __init__.py
    profissional_repo.py      # NEW
    estabelecimento_repo.py   # NEW
    vinculo_repo.py           # NEW (with snapshot replace from Phase 1)
    unit_of_work.py           # NEW
  schema.py                   # unchanged
  job_queue.py                # unchanged
  landing.py                  # unchanged
```

`postgres_adapter.py` is deleted after migration complete.

**Ports (rewritten, Polars-free):**

```python
# cnes_domain/ports/storage.py

class ProfissionalStoragePort(Protocol):
    def gravar(self, rows: Iterable[dict]) -> int: ...

class EstabelecimentoStoragePort(Protocol):
    def gravar(self, rows: Iterable[dict]) -> int: ...

class VinculoStoragePort(Protocol):
    def snapshot_replace(
        self, competencia: str, fonte: str, rows: Iterable[dict],
    ) -> int: ...

class UnitOfWorkPort(Protocol):
    profissionais: ProfissionalStoragePort
    estabelecimentos: EstabelecimentoStoragePort
    vinculos: VinculoStoragePort
    def __enter__(self) -> Self: ...
    def __exit__(self, *exc) -> None: ...
```

**UnitOfWork implementation:**

```python
class PostgresUnitOfWork:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    def __enter__(self) -> Self:
        self._con = self._engine.connect()
        self._tx = self._con.begin()
        self.profissionais = ProfissionalRepository(self._con)
        self.estabelecimentos = EstabelecimentoRepository(self._con)
        self.vinculos = VinculoRepository(self._con)
        return self

    def __exit__(self, exc_type, *_) -> None:
        if exc_type:
            self._tx.rollback()
        else:
            self._tx.commit()
        self._con.close()
```

### Move business logic to Transformer

What lives in `_build_vinculo_rows` (L137-166) is transformation, not persistence:
- `SUS "S"->True` / `"N"->False`
- `FONTE "LOCAL"->{"LOCAL": True}` (JSONB construction)
- Column rename upper->lower

This migrates to a new `cnes_domain/processing/row_mapper.py` module
(not transformer.py — transformer handles validation/dedup, row_mapper handles
schema-to-persistence mapping: column rename, type coercion, JSONB construction).
Repositories receive `Iterable[dict]` ready for INSERT — zero Polars knowledge.

### Tests

| Test | Validates |
|------|-----------|
| `test_unit_of_work_commit_atomico` | Prof + vinculo in same tx |
| `test_unit_of_work_rollback_em_falha` | Vinculo error reverts profissional |
| `test_vinculo_repo_snapshot_replace` | Isolated delete+insert |
| `test_profissional_repo_upsert_idempotente` | Re-gravar same CPF no duplicates |
| `test_row_mapper_sus_booleano` | "S"->True, "N"->False, None->None |
| `test_row_mapper_fonte_jsonb` | "LOCAL"->{"LOCAL": True} |

---

## Phase 3 — Claim-Check + Resilient Download

### Problem

`processor.py` (L65-97) accesses DB directly via `select(raw_payload.c.object_key)` and
`select(raw_payload.c.competencia)`:
1. Imports `raw_payload` (SQLAlchemy Table) and `select` — framework leaks into application
2. 2 extra queries per job (Chatty I/O)
3. Download (L89-97) loads entire payload into `BytesIO` — OOM on large payloads
4. `httpx.get(url, timeout=120.0)` without retry/backoff — single point of failure

### Solution A: Claim-Check — Enrich Job Dataclass

`raw_payload` already has `object_key` and `competencia`. Both are known before processor runs.
Populate them on `acquire_completed_job` via JOIN:

```python
@dataclass(frozen=True)
class Job:
    id: uuid.UUID
    status: str
    source_system: str
    tenant_id: str
    payload_id: uuid.UUID
    object_key: str | None = None       # NEW
    competencia: str | None = None      # NEW
    created_at: datetime | None = None
    attempt_count: int = 0
    max_retries: int = 3
    machine_id: str | None = None
    lease_expires_at: datetime | None = None
    heartbeat_at: datetime | None = None
```

`acquire_completed_job` does a single JOIN:

```python
stmt = (
    select(jobs, raw_payload.c.object_key, raw_payload.c.competencia)
    .join(raw_payload, jobs.c.payload_id == raw_payload.c.id)
    .where(jobs.c.status == "COMPLETED")
    .order_by(jobs.c.completed_at)
    .limit(1)
    .with_for_update(skip_locked=True)
)
```

One query instead of three. Processor receives everything in Job.

**Functions deleted from processor.py:** `_get_object_key`, `_get_competencia`.
**Imports removed from processor.py:** `select`, `raw_payload`, `Engine`.

### Solution B: Streaming Download

Replace in-memory `httpx.get` with streaming + temp file:

```python
def _download_parquet(
    url: str,
    breaker: CircuitBreaker,
    chunk_size: int = 64 * 1024,
) -> pl.DataFrame:
    if url.startswith("null://"):
        raise ValueError("null_storage url_not_downloadable")

    def _fetch() -> Path:
        tmp = Path(tempfile.mktemp(suffix=".parquet"))
        with httpx.stream("GET", url, timeout=30.0) as resp:
            resp.raise_for_status()
            raw = io.BytesIO()
            for chunk in resp.iter_bytes(chunk_size):
                raw.write(chunk)
        data = raw.getvalue()
        if data[:2] == b"\x1f\x8b":
            data = gzip.decompress(data)
        tmp.write_bytes(data)
        return tmp

    tmp_path = breaker.call(_fetch)
    try:
        return pl.read_parquet(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)
```

### Solution C: Circuit Breaker with Exponential Backoff

Extend existing `CircuitBreaker` (backward compatible — new params have defaults):

```python
class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        service_name: str = "external",
        base_delay: float = 1.0,     # NEW
        max_delay: float = 30.0,     # NEW
        reset_after: float = 60.0,   # NEW (HALF-OPEN)
    ) -> None: ...
```

Adds:
- Exponential backoff: `min(base_delay * 2^(failures-1), max_delay)`
- HALF-OPEN state: after `reset_after` seconds, allow one probe call

### Processor Refactored

```python
def process_job(
    uow_factory: Callable[[], UnitOfWorkPort],
    storage: ObjectStoragePort,
    job: Job,
    breaker: CircuitBreaker | None = None,
) -> None:
    if not job.object_key:
        raise ValueError(f"object_key_missing job_id={job.id}")
    if not job.competencia:
        raise ValueError(f"competencia_missing job_id={job.id}")

    breaker = breaker or CircuitBreaker(service_name="minio")
    download_url = storage.get_presigned_download_url(MINIO_BUCKET, job.object_key)
    df = _download_parquet(download_url, breaker)

    with uow_factory() as uow:
        uow.profissionais.gravar(prof_rows)
        uow.vinculos.snapshot_replace(job.competencia, fonte, vinculo_rows)
```

**Removed from processor:** `Engine`, `select`, `raw_payload`, `PostgresAdapter`.
**Processor depends only on:** `UnitOfWorkPort`, `ObjectStoragePort`, `CircuitBreaker`.

### Tests

| Test | Validates |
|------|-----------|
| `test_acquire_completed_popula_object_key_e_competencia` | JOIN brings fields into Job |
| `test_process_job_sem_engine_direto` | Processor does not import Engine |
| `test_download_com_retry_apos_falha_transitoria` | Circuit breaker retries with backoff |
| `test_download_abre_circuito_apos_threshold` | 3 failures -> CircuitBreakerAberto |
| `test_half_open_permite_tentativa_apos_reset` | After reset_after, allows 1 probe |
| `test_download_streaming_chunks` | Mock httpx.stream, verify chunked reads |

---

## Phase 4 — Observability + V3-Ready Contracts

### Problem

1. Logging via `logging.getLogger(__name__)` — impossible to correlate a specific job end-to-end
2. Ports in `repository.py` return `pl.DataFrame` — vendor lock-in blocking microservices
3. No distributed tracing — at scale, failures are black boxes

### Solution A: OpenTelemetry Tracing

Minimal instrumentation at contour points:

```python
# cnes_domain/observability.py
from opentelemetry import trace

tracer = trace.get_tracer("cnesdata")
```

**Instrumented spans:**

| Span | Where | Attributes |
|------|-------|------------|
| `process_job` | processor.py entry | job_id, competencia, source_system |
| `download_parquet` | _download_parquet | url, content_length, compressed |
| `transform` | transformar() | rows_in, rows_out, rules_applied |
| `snapshot_replace` | VinculoRepository | competencia, fonte, deleted, inserted |
| `upsert_profissionais` | ProfissionalRepository | rows_count |

**Propagation:** `trace_context` column already exists in `jobs` table (job_queue.py L58).
Currently NULL. Populate with W3C TraceContext on `enqueue()`, extract on
`acquire_completed_job()` — end-to-end tracing from ingestion agent to persistence.

### Solution B: Clean Ports (no Polars)

**Before (repository.py):**
```python
class ProfissionalRepository(Protocol):
    def listar_profissionais(self, ...) -> pl.DataFrame: ...
```

**After:**
```python
class ProfissionalRepository(Protocol):
    def listar_profissionais(self, ...) -> Iterable[dict]: ...
```

Ingestion adapters (`CnesLocalAdapter`, `SihdLocalAdapter`) continue using Polars internally,
call `.to_dicts()` at output boundary. Consumer never knows Polars exists.

### Solution C: TLS Configuration

mTLS is infrastructure config (Docker Compose / Kubernetes), not application code.

Code changes:
- `httpx.Client` supports `verify`, `cert` params via env vars
- New env vars: `MINIO_CA_CERT`, `MINIO_CLIENT_CERT`, `MINIO_CLIENT_KEY`
- SQLAlchemy: `?sslmode=verify-full&sslcert=...&sslkey=...&sslrootcert=...`

mTLS enforcement is ops concern. Code provides config hooks. Document in README.

### New Dependencies

```toml
# cnes_domain pyproject.toml
opentelemetry-api >= 1.20

# data_processor pyproject.toml
opentelemetry-sdk >= 1.20
opentelemetry-exporter-otlp >= 1.20
```

### Tests

| Test | Validates |
|------|-----------|
| `test_span_process_job_registra_atributos` | Span exports job_id, competencia |
| `test_trace_context_propagado_via_job` | enqueue -> acquire preserves trace_id |
| `test_repository_port_nao_importa_polars` | Import guard: polars absent from ports module |
| `test_adapter_retorna_iterable_dict` | CnesLocalAdapter.listar_profissionais returns Iterable[dict] |

---

## Phase Summary

| Phase | Deliverable | Files created/modified | Migration |
|-------|-------------|----------------------|-----------|
| **1** | Snapshot Replace + integrity tests | `postgres_adapter.py`, 8 new tests | `006_gin_index_fontes.py` |
| **2** | 3 repos + UoW, Polars decoupled | 5 new (repos/), `storage.py`, `transformer.py`, delete `postgres_adapter.py` | None |
| **3** | Claim-Check, streaming, circuit breaker | `job_queue.py`, `processor.py`, `circuit_breaker.py`, `config.py` | None |
| **4** | OpenTelemetry, clean ports, TLS config | `observability.py`, `repository.py`, adapters, `pyproject.toml` | None |

## Dependency Order

```
Phase 1 (CBO fix) --> Phase 2 (repositories) --> Phase 3 (processor) --> Phase 4 (observability)
```

Phase 1 is independent. Phase 2 uses VinculoRepository that Phase 1 creates.
Phase 3 depends on clean interfaces from Phase 2. Phase 4 instruments already-clean code.
