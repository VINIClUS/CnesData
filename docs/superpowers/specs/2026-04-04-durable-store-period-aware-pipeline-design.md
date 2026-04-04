# Spec AB — Durable Audit Store & Period-Aware Pipeline

**Date:** 2026-04-04
**Status:** Approved
**Scope:** Replace CSV/XLSX as primary storage with DuckDB; enforce period-awareness for Firebird access; guarantee audit period invariant; degrade gracefully when local data is unavailable.

---

## Goals

1. DuckDB is the single source of truth for all pipeline data across all periods.
2. Firebird is consulted only for the current calendar period (or on-demand sync-in). Past periods load exclusively from DuckDB.
3. CSV archiving (`historico/YYYY-MM/*.csv`) is eliminated. CSV/XLSX become on-demand exports.
4. Audits are always conducted between data from the same period. Cross-checks only execute when both local and national data are available for the requested period.
5. When local data is unavailable for a past period, the pipeline continues partially: national data is fetched and stored, audit stages degrade gracefully, pipeline run status is recorded.

---

## Out of Scope

- HR/payroll DuckDB storage (HR file ingestion unchanged).
- PEC ESUS integration (Spec C).
- SIAH integration (Spec D).
- Inter-period comparison / diff feature (future).

---

## Data Layer — New DuckDB Gold Tables

Four new tables join the existing Gold schema.

### `gold.profissionais_processados`

Replaces `snapshot_local_prof.parquet` and `Relatorio_Profissionais_CNES.csv`.

```sql
CREATE TABLE IF NOT EXISTS gold.profissionais_processados (
    competencia        VARCHAR NOT NULL,
    cpf                VARCHAR NOT NULL,
    cnes               VARCHAR NOT NULL,
    cns                VARCHAR,
    nome_profissional  VARCHAR,
    sexo               VARCHAR(1),
    cbo                VARCHAR,
    tipo_vinculo       VARCHAR,
    sus                VARCHAR(1),
    ch_total           INTEGER,
    ch_ambulatorial    INTEGER,
    ch_outras          INTEGER,
    ch_hospitalar      INTEGER,
    fonte              VARCHAR,
    alerta_status_ch   VARCHAR,
    descricao_cbo      VARCHAR,
    gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (competencia, cpf, cnes)
)
```

### `gold.estabelecimentos`

Replaces `snapshot_local_estab.parquet`.

```sql
CREATE TABLE IF NOT EXISTS gold.estabelecimentos (
    competencia        VARCHAR NOT NULL,
    cnes               VARCHAR NOT NULL,
    nome_fantasia      VARCHAR,
    tipo_unidade       VARCHAR,
    cnpj_mantenedora   VARCHAR,
    natureza_juridica  VARCHAR,
    cod_municipio      VARCHAR,
    vinculo_sus        VARCHAR(1),
    fonte              VARCHAR,
    gravado_em         TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (competencia, cnes)
)
```

### `gold.cbo_lookup`

Replaces `snapshot_cbo_lookup.json`.

```sql
CREATE TABLE IF NOT EXISTS gold.cbo_lookup (
    competencia  VARCHAR NOT NULL,
    codigo_cbo   VARCHAR NOT NULL,
    descricao    VARCHAR,
    PRIMARY KEY (competencia, codigo_cbo)
)
```

### `gold.pipeline_runs`

New. Tracks availability and completion status per period.

```sql
CREATE TABLE IF NOT EXISTS gold.pipeline_runs (
    competencia          VARCHAR PRIMARY KEY,
    local_disponivel     BOOLEAN NOT NULL DEFAULT FALSE,
    nacional_disponivel  BOOLEAN NOT NULL DEFAULT FALSE,
    hr_disponivel        BOOLEAN NOT NULL DEFAULT FALSE,
    status               VARCHAR NOT NULL,
    iniciado_em          TIMESTAMP,
    concluido_em         TIMESTAMP
)
```

**Status values:**

| `local_disponivel` | `nacional_disponivel` | `status` |
|---|---|---|
| TRUE | TRUE | `'completo'` |
| TRUE | FALSE | `'parcial'` |
| FALSE | TRUE | `'sem_dados_locais'` |
| FALSE | FALSE | `'sem_dados'` |

---

## Period-Awareness

### `periodo_atual() -> str`

Added to `src/storage/competencia_utils.py`. Returns the current calendar month as `YYYY-MM` from `date.today()`. No configuration, no override. This is the single gatekeeper for Firebird access.

### Firebird access rule

Firebird may be consulted **only when** `state.competencia_str == periodo_atual()`. For all other periods, the pipeline loads from DuckDB (or parquet backfill). This rule is enforced inside `IngestaoLocalStage`.

### `--force-reingestao` interaction

When `force_reingestao=True` AND `competencia != periodo_atual()`:
- Firebird is **not** consulted (past period, always off-limits).
- National data is re-fetched from BigQuery.
- If a parquet backup exists, it backfills DuckDB.
- Logs `"force_reingestao_ignorado_periodo_passado competencia=YYYY-MM"` as WARNING.

`--force-reingestao` retains full effect only for the current period.

---

## Pipeline Stage Changes

### `PipelineState` — new fields

```python
local_disponivel: bool = True       # False when no local data exists for a past period
nacional_disponivel: bool = False   # True after IngestaoNacionalStage succeeds
```

### `IngestaoLocalStage` — major change

Priority order for loading local data:

1. **DuckDB has rows** for `(competencia, *, *)` in `gold.profissionais_processados` → load from DuckDB, set `state.local_disponivel = True`, `state.snapshot_carregado = True`.
2. **DuckDB empty, parquet snapshot exists** → backfill DuckDB from parquet, then load, set `state.local_disponivel = True`, `state.snapshot_carregado = True`. *(one-time migration per period)*
3. **Neither exists, `competencia == periodo_atual()`** → query Firebird, set `state.local_disponivel = True`.
4. **Neither exists, `competencia != periodo_atual()`** → set `state.local_disponivel = False`, log `"dados_locais_indisponiveis competencia=YYYY-MM"`, return.

Constructor signature changes from `IngestaoLocalStage(historico_dir)` to `IngestaoLocalStage(historico_dir, db_loader)`.

### `ProcessamentoStage` — minor change

Guards on `state.local_disponivel`. If `False`, returns immediately.

### `SnapshotLocalStage` — major change

Writes processed data to DuckDB (`gold.profissionais_processados`, `gold.estabelecimentos`, `gold.cbo_lookup`) instead of reading from parquet. Parquet files are retained as file-level backup but are never read by the pipeline going forward. Skips if `state.snapshot_carregado = True` (data already in DuckDB).

### `IngestaoNacionalStage` — minor change

After successful ingestion, sets `state.nacional_disponivel = True`. Runs regardless of `state.local_disponivel`.

### `AuditoriaLocalStage` — minor change

Guards on `state.local_disponivel`. If `False`, returns immediately.

### `AuditoriaNacionalStage` — minor change

- **Stage runs when:** `state.local_disponivel OR state.nacional_disponivel`
- **Cross-check rules (RQ006–RQ011) execute when:** `state.local_disponivel AND state.nacional_disponivel`
- **Neither available:** returns immediately.

Raises `PeriodoInvariantError` (defensive guard) if both flags are `True` but competency strings diverge.

### `MetricasStage` — minor change

Guards local-dependent metric calculations on `state.local_disponivel`. Persists what is available. Does not write `gold.pipeline_runs` (that is `ExportacaoStage`'s responsibility).

### `ExportacaoStage` — major change

**Removed:**
- `_exportar_csvs()` — no CSV output.
- `_arquivar_csvs()` — no CSV archiving to `historico/`.
- `_gerar_relatorio()` call — no XLSX written at pipeline time.

**Added:**
- Writes `gold.pipeline_runs` row with final status and `concluido_em` timestamp.

**Retained:**
- `salvar_snapshot()` (JSON evolution tracker).
- All `gravar_metricas()`, `gravar_auditoria()`, `gravar_glosas()` calls.

### `main.py` — constructor update

`IngestaoLocalStage(config.HISTORICO_DIR)` → `IngestaoLocalStage(config.HISTORICO_DIR, db_loader)`.

---

## XLSX On-Demand Export

### `exportar_xlsx_periodo(competencia: str, duckdb_path: Path) -> bytes`

Added to `src/export/report_generator.py`. Queries DuckDB for the given period, builds an in-memory XLSX (via `openpyxl`), returns bytes. Does not write to disk.

Tabs:
- `Profissionais` — from `gold.profissionais_processados WHERE competencia = ?`
- One tab per rule — from `gold.glosas_profissional WHERE competencia = ? AND regra = ?`
- `Métricas` — from `gold.metricas_avancadas WHERE competencia = ?`

**Dashboard:** `st.download_button` added to `scripts/pages/5_Metricas.py`.

---

## `HistoricoReader` Changes

- `carregar_registros(regra, competencia)` → replaced by `carregar_glosas_periodo(regra, competencia)`. Same signature and return type; reads from `gold.glosas_profissional` instead of CSV files.
- `CSV_MAP` constant removed.
- New methods: `carregar_profissionais(competencia)`, `carregar_estabelecimentos(competencia)`, `carregar_pipeline_run(competencia)`.

All dashboard pages that call `carregar_registros` switch to `carregar_glosas_periodo` (one-line change per call site).

---

## Migration Path (Existing Parquet Files)

On first `IngestaoLocalStage` execution for a period that has parquet but no DuckDB rows (priority 2 above), the stage:
1. Loads from parquet via existing `carregar_snapshot()`.
2. Writes all three DuckDB tables.
3. Sets `state.snapshot_carregado = True`.

Parquet files remain on disk as cold backup. No batch migration script needed — migration happens lazily, period by period, on first pipeline access.

---

## Testing Strategy

### New test modules

| Module | Coverage |
|---|---|
| `tests/storage/test_competencia_utils.py` | `periodo_atual()` format and correctness |
| `tests/storage/test_database_loader.py` | DDL for 4 new tables; `gravar_profissionais`, `gravar_estabelecimentos`, `gravar_cbo_lookup`, `gravar_pipeline_run` |
| `tests/storage/test_historico_reader.py` | `carregar_glosas_periodo`; `carregar_profissionais`; `carregar_pipeline_run` |
| `tests/pipeline/stages/test_ingestao_local.py` | DuckDB-first; parquet backfill; past-period no-op; current-period Firebird call |
| `tests/pipeline/stages/test_snapshot_local_stage.py` | DuckDB writes; skip when `snapshot_carregado=True` |
| `tests/pipeline/stages/test_auditoria_nacional.py` | OR guard; AND cross-checks; neither no-op; `PeriodoInvariantError` |
| `tests/pipeline/stages/test_exportacao.py` | No CSV written; `pipeline_runs` row written with correct status |
| `tests/export/test_report_generator.py` | `exportar_xlsx_periodo` returns valid bytes; correct tab names |

### Existing tests requiring updates

- `test_ingestao_local.py` — add DuckDB fixture; update constructor calls.
- `test_database_loader.py` — add new table DDL assertions.
- `test_historico_reader.py` — replace `carregar_registros` with `carregar_glosas_periodo`.
- `test_exportacao.py` — assert CSVs not written; assert `pipeline_runs` written.

### Integration test (marked `integration`)

Full pipeline run for current period (mocked Firebird + real DuckDB in `tmp_path`). Second run for same period: assert Firebird not called (DuckDB-first path).

---

## File Map

| Action | File |
|---|---|
| Modify | `src/storage/competencia_utils.py` |
| Modify | `src/storage/database_loader.py` |
| Modify | `src/storage/historico_reader.py` |
| Modify | `src/pipeline/state.py` |
| Modify | `src/pipeline/stages/ingestao_local.py` |
| Modify | `src/pipeline/stages/snapshot_local.py` |
| Modify | `src/pipeline/stages/ingestao_nacional.py` |
| Modify | `src/pipeline/stages/auditoria_local.py` |
| Modify | `src/pipeline/stages/auditoria_nacional.py` |
| Modify | `src/pipeline/stages/metricas.py` |
| Modify | `src/pipeline/stages/exportacao.py` |
| Modify | `src/main.py` |
| Modify | `src/export/report_generator.py` |
| Modify | `scripts/pages/5_Metricas.py` |
| Modify | `tests/pipeline/stages/test_ingestao_local.py` |
| Modify | `tests/pipeline/stages/test_snapshot_local_stage.py` |
| Modify | `tests/pipeline/stages/test_exportacao.py` |
| Modify | `tests/storage/test_database_loader.py` |
| Modify | `tests/storage/test_historico_reader.py` |
| Modify | `tests/export/test_report_generator.py` |
| Create | `tests/pipeline/stages/test_auditoria_nacional.py` |
