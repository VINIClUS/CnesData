# Pipeline Persistence Redesign

**Date:** 2026-04-05  
**Status:** Approved

## Problem

Historical/backfill pipeline runs using only national CNES data save nothing to DuckDB except `pipeline_runs`. Root causes:

1. `ProcessamentoStage` returns early when `local_disponivel=False` → `df_processado` empty.
2. `IngestaoNacionalStage._computar_fingerprint` calls `state.df_processado` which is empty → fingerprint logic broken for national-only runs.
3. `MetricasStage` returns early when `local_disponivel=False`.
4. `ExportacaoStage` gates all persistence on `state.local_disponivel`.
5. `delta_local_snapshot` only computed on `force_reingestao=True` with existing snapshot.
6. `profissionais_processados` and `estabelecimentos` have `fonte VARCHAR` — can't represent a professional present in both local and national sources for the same competency.
7. `cbo_lookup` includes `competencia` column — duplicates entries unnecessarily.

## Scope

Both schema redesign and pipeline gate fixes land together. Migration strategy: DROP and rebuild (data recovered by re-running competencies).

---

## Architecture

### Stage Order (after redesign)

```
1. IngestaoLocalStage           unchanged
2. ProcessamentoStage           unchanged name; guard already returns early if not local_disponivel
3. SnapshotLocalStage           unchanged
4. IngestaoNacionalStage        fix: fingerprint uses df_nacional when df_processado empty
5. ProcessamentoNacionalStage   NEW: normalizes df_nacional → df_processado when local absent
6. AuditoriaLocalStage          unchanged (already handles dual paths)
7. AuditoriaNacionalStage       unchanged
8. MetricasStage                remove early-return guard on local_disponivel
9. ExportacaoStage              replace local_disponivel gates with df_processado.empty check
```

### ProcessamentoNacionalStage

New file: `src/pipeline/stages/processamento_nacional.py`

Activates only when `not state.local_disponivel and state.nacional_disponivel`.

Responsibility: normalize `state.df_nacional` into the same column schema as `df_processado`, set `fontes = ['nacional']`, assign to `state.df_processado`.

Column mapping from `df_nacional` to `df_processado` schema follows `cnes_nacional_adapter` output column names. Reuses transformer cleaning functions where applicable.

---

## Schema Changes

### profissionais_processados

Change `fonte VARCHAR` → `fontes VARCHAR[]` (DuckDB list type).

Primary key: `(cns, competencia)`. If `cns` is null, fall back to `(cpf, competencia)`.

Upsert on conflict:
```sql
INSERT INTO gold.profissionais_processados (..., fontes)
VALUES (..., ['nacional'])
ON CONFLICT (cns, competencia)
DO UPDATE SET fontes = list_distinct(list_concat(fontes, EXCLUDED.fontes))
```

Query pattern "all professionals in competency X":
```sql
SELECT * FROM gold.profissionais_processados WHERE competencia = '2026-03'
```

Query pattern "full history of professional P":
```sql
SELECT competencia, fontes FROM gold.profissionais_processados WHERE cns = '?'
ORDER BY competencia
```

### estabelecimentos

Same change: `fonte VARCHAR` → `fontes VARCHAR[]`, PK `(cnes, competencia)`.

### cbo_lookup

Remove `competencia` column. Add `created_at TIMESTAMP DEFAULT now()`.

On each pipeline run: `INSERT OR IGNORE` new (cbo, descricao) pairs. Existing rows untouched.

### delta_local_snapshot

No schema change. New trigger: compute every run where `df_processado` is not empty (local or national), comparing current competency's `df_processado` against previous competency's rows in `gold.profissionais_processados`. Cross-source comparison is intentional — a professional disappearing from local but appearing nationally is a valid signal. Store even when all deltas are zero.

---

## Persistence Logic

### ExportacaoStage

Replace every `if state.local_disponivel:` guard with `if not state.df_processado.empty:`.

`gravar_pipeline_run` remains unconditional (unchanged).

### MetricasStage

Remove early-return `if not state.local_disponivel: return`.

### DatabaseLoader.gravar_profissionais / gravar_estabelecimentos

Change from `INSERT OR REPLACE` to upsert with `ON CONFLICT DO UPDATE SET fontes = list_distinct(list_concat(...))`. This allows the same (cns, competencia) to accumulate sources from separate pipeline runs without data loss.

---

## IngestaoNacionalStage Fingerprint Fix

Current: `_computar_fingerprint(state.df_processado)` at entry — fails silently when df_processado is empty (national-only run, Processamento hasn't run yet).

Fix: `df = state.df_processado if not state.df_processado.empty else state.df_nacional`  
Use `df` for fingerprint computation. If both are empty, fingerprint is `None` (forces fresh load).

---

## Dashboard / CLI Consistency

No code changes needed for CLI vs dashboard gap. The difference is `@st.cache_data(ttl=300)` in the dashboard. After a backfill pipeline run, the dashboard cache must expire (5 minutes) or Streamlit must be restarted. This is documented, not fixed in code.

`HistoricoReader` methods (`carregar_profissionais`, `carregar_estabelecimentos`, `carregar_cbo_descricoes`) query `WHERE competencia = ?` — unchanged. `fontes` column is returned as-is (DuckDB list → Python list).

---

## Migration

`DatabaseLoader.__init__` (or `_criar_schema`): for each of the three changed tables, run `DROP TABLE IF EXISTS gold.<table>` before `CREATE TABLE`. Data is rebuilt by re-running pipeline per competency.

---

## Files Changed

| Action | File |
|---|---|
| Modify | `src/pipeline/stages/processamento.py` (add guard: return early if not local_disponivel, keep name) |
| New | `src/pipeline/stages/processamento_nacional.py` |
| Fix | `src/pipeline/stages/ingestao_nacional.py` (fingerprint) |
| Fix | `src/pipeline/stages/metricas.py` (remove early return) |
| Fix | `src/pipeline/stages/exportacao.py` (remove local gates) |
| Fix | `src/pipeline/stages/snapshot_local.py` (delta trigger) |
| Fix | `src/storage/database_loader.py` (schema + upsert) |
| Fix | `src/pipeline/orchestrator.py` (stage list + new stage) |

## Out of Scope

- Dashboard cache invalidation automation
- Backfill CLI command (user re-runs pipeline per competency manually)
- `profissionais_processados` → `profissionais_dim` + `profissionais_presenca` split (over-engineering for current query volume)
