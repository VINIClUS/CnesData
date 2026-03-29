---
name: Performance baseline and optimizations — 2026-03-22
description: Full profiling pass on CnesData pipeline. Records bottlenecks found, changes applied/rejected, and thresholds. Updated after second analysis pass.
type: project
---

## Baseline context

- Local data volume: 367 vínculos (Firebird, município 354130)
- National data: BigQuery, potentially 10K+ rows
- Pipeline: Firebird extraction → BigQuery fetch → transform → 11 audit rules → CSV + Excel export
- Test suite: 313 passing at analysis time

## Bottlenecks identified — first pass (2026-03-22)

| File | Location | Issue | Type |
|------|----------|-------|------|
| transformer.py:97-98 | `_aplicar_rq003_flag_carga_horaria` | `.apply(lambda)` per row | CPU |
| transformer.py:96 | same function | `df.copy()` at entry (caller already owns data) | Memory |
| rules_engine.py:182 | `detectar_folha_fantasma` | `.map(python_fn)` per row | CPU |
| cnes_local_adapter.py:52,73,97 | 3 public methods | `.rename().copy()` — rename already returns new DF | Memory |
| cnes_nacional_adapter.py:59,82 | 2 public methods | same `.rename().copy()` pattern | Memory |
| web_client.py:123,155,181,207 | 4 fetch methods | `.copy()` after `bd.read_sql()` | Memory |
| report_generator.py:224-228 | `_formatar_cabecalho` | O(rows×cols) full column scan for width | CPU |

## Optimizations APPLIED (first pass)

### 1 — transformer.py: `_aplicar_rq003_flag_carga_horaria`
- Before: `df_out = df.copy()` then `.apply(lambda ch: ... if ch == 0 else ...)`
- After: mutate argument in place (`df["ALERTA_STATUS_CH"] = np.where(df["CH_TOTAL"] == 0, ...)`)
- Why faster: `np.where` is C-level vectorized; no per-row Python call; no extra full copy
- Safety: caller (`transformar`) passes a DataFrame it already owns (result of rq002 copy)

### 2 — cnes_local_adapter.py: removed `.copy()` after `.rename()` in 3 methods
- `listar_profissionais`, `listar_estabelecimentos`, `listar_equipes`
- `DataFrame.rename(inplace=False)` always returns a new object; the `.copy()` was a 3rd allocation

### 3 — cnes_nacional_adapter.py: removed `.copy()` after `.rename()` in 2 methods
- `listar_estabelecimentos`, `listar_profissionais`
- Same reason as above

### 4 — rules_engine.py: `detectar_folha_fantasma` vectorized
- Before: `frozenset` lookups inside a Python function called via `.map()` — O(n) with Python overhead
- After: two `.isin()` calls (C-level) then `.loc` assignment; no Python per row
- Pattern: compute `mascara_ausente` and `mascara_inativo` separately, assign via `.loc`, then filter

### 5 — report_generator.py: `_formatar_cabecalho` column width scan capped
- Before: iterated ALL cells in each column (O(rows × cols))
- After: `amostra = list(coluna)[:101]` — header + first 100 rows only
- Why safe: column width is determined by typical content, not outliers in row 5000

## Optimizations REJECTED (first pass)

### web_client.py — remove `.copy()` after BigQuery fetch
- Reason: test `TestQualidade::test_resultado_e_copia_independente` explicitly contracts that
  the returned DF must NOT be the same object as the one from `bd.read_sql()`.
  This is an intentional defensive copy — the test documents it as a behavioural requirement, not an implementation detail.

### cnes_client.py `_enriquecer_com_equipe` — remove pre-merge copies
- Reason: CLAUDE.md convention: "Data transformations work on `.copy()`, never mutate originals."
  The private function receives DataFrames as arguments. Even though callers don't reuse them,
  removing the copies would violate the project's stated style rule. Impact is negligible at 367 rows.

## Remaining opportunities identified — second pass (2026-03-22)

| Priority | File | Location | Issue | Impact |
|----------|------|----------|-------|--------|
| P1 | transformer.py | `transformar()` lines 131-134 | `_COLUNAS_TEXTO` loop calls `.astype(str)` on every column including numeric ones unnecessarily; CNES/COD_MUNICIPIO not in raw output columns | Low-medium |
| P1 | transformer.py | `_aplicar_rq002_validar_cpf` line 64 | Double `.astype(str).str.strip()` — CPF already stripped in step 1 of `transformar` | Low |
| P2 | rules_engine.py | `detectar_estabelecimentos_fantasma` line 209 | `.astype(str).str.strip()` computed twice: once for `cnes_nacionais` frozenset and once for `resultado` filter | Medium at 10K+ rows |
| P2 | rules_engine.py | `detectar_estabelecimentos_ausentes_local` lines 231,238 | Same double `.astype(str).str.strip()` pattern on CNES column | Medium |
| P2 | rules_engine.py | `detectar_profissionais_fantasma` lines 258,261 | Same double `.astype(str).str.strip()` pattern on CNS column | Medium |
| P2 | rules_engine.py | `detectar_profissionais_ausentes_local` lines 288,294 | Same pattern on CNS column | Medium |
| P3 | cnes_local_adapter.py | `listar_estabelecimentos` line 71 | `_extrair()` returns cache, then `.str.strip().str.zfill(7)` applied again on CNES (already done in `listar_profissionais`) | Low |
| P3 | rules_engine.py | `detectar_multiplas_unidades` | merge on CPF after groupby could be replaced with a direct boolean index filter | Low |
| P4 | main.py | lines 197-201 | `df_estab_local[["CNES","TIPO_UNIDADE"]]` merge in main for RQ-005; TIPO_UNIDADE should come from the standardized schema directly | Structural |
| P4 | web_client.py | `_SQL_EQUIPES` line 64 | `SELECT *` fetches all columns from equipe table; only 3-4 are used | Minor at municipality scale |

## Not worth optimizing

- `evolution_tracker.py`: JSON file I/O on tiny snapshots. Trivial.
- `csv_exporter.py`: single `df.to_csv()` call. Already optimal.
- `_aplicar_rq002_validar_cpf` `.copy()` return: the returned DF is immediately mutated by rq003
  (column assignment). Removing the `.copy()` risks SettingWithCopyWarning in pandas. Keep it.
- All functions operating on 367-row local data: even worst-case Python loops finish in <5ms.
- `_adicionar_recomendacao` in report_generator.py: one `.copy()` + scalar assignment. Negligible.

## Third pass findings — 2026-03-27 (after cascade_resolver + DatabaseLoader added)

### New bottleneck: cascade_resolver.py — resolver_lag_rq006 (HIGH)
- Sequential HTTP loop: one DATASUS API call per ghost establishment, 0.5s sleep between calls
- 10 ghost CNES = 5s minimum wait, 100 = 50s. Dominated by network RTT + enforced sleep
- Retry policy adds up to (1+2+4)×3 = 21s per failed host before fallback
- No parallelism, no result caching across pipeline runs
- Fix (not yet applied): asyncio/ThreadPoolExecutor with shared semaphore; pickle cache with TTL per CNES

### New bottleneck: DatabaseLoader — multiple connections per run (LOW)
- Opens/closes a new duckdb.connect() for each of inicializar_schema(), gravar_metricas(),
  gravar_auditoria() ×3 calls = 5 separate open/close cycles on a file-based DuckDB
- At DuckDB file sizes involved this is negligible in absolute terms, but pattern is inconsistent
  with single-connection-per-pipeline discipline used for Firebird

### Confirmed remaining P2 opportunities from second pass (still not applied)
- rules_engine.py: 4 functions (RQ-006 through RQ-009) each recompute `.astype(str).str.strip()`
  twice — once to build the frozenset, once on the comparison side. At 10K+ national rows these
  double-normalize ~20K–40K cells redundantly. Fix: normalize to a local variable once per function.
- transformer.py: `_COLUNAS_TEXTO` tuple contains TIPO_UNIDADE and COD_MUNICIPIO which are NOT
  present in SCHEMA_PROFISSIONAL (the raw output from `extrair_profissionais`). The `if coluna in
  resultado.columns` guard prevents a crash but causes 2 pointless in-check iterations every run.

### Confirmed NOT-yet-applied P1 opportunities from second pass
- transformer.py `_aplicar_rq002_validar_cpf` line 64: comment says CPF is already stripped,
  but the variable `cpf_str = df["CPF"]` is used directly without re-stripping — the note
  in the second pass about "double strip" was a misread; no actual P1 remaining here.

## Data volume thresholds

- Under 10K rows national: current implementation is fast enough even before optimization
- At 10K+ national rows: ghost detection vectorization and column-width cap become measurably relevant
- At 100K+ rows: consider O6 (chunked BigQuery fetch) from the optimization guide
- cascade_resolver becomes the pipeline's slowest step if RQ-006 returns > ~5 ghost establishments
