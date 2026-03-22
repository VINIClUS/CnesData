---
name: Performance baseline and optimizations — 2026-03-22
description: First profiling pass on CnesData pipeline. Records bottlenecks found, changes applied, changes rejected, and reasons.
type: project
---

## Baseline context

- Local data volume: 367 vínculos (Firebird, município 354130)
- National data: BigQuery, potentially 10K+ rows
- Pipeline: Firebird extraction → BigQuery fetch → transform → 11 audit rules → CSV + Excel export
- Test suite: 313 passing at analysis time

## Bottlenecks identified (all files read 2026-03-22)

| File | Location | Issue | Type |
|------|----------|-------|------|
| transformer.py:97-98 | `_aplicar_rq003_flag_carga_horaria` | `.apply(lambda)` per row | CPU |
| transformer.py:96 | same function | `df.copy()` at entry (caller already owns data) | Memory |
| rules_engine.py:182 | `detectar_folha_fantasma` | `.map(python_fn)` per row | CPU |
| cnes_local_adapter.py:52,73,97 | 3 public methods | `.rename().copy()` — rename already returns new DF | Memory |
| cnes_nacional_adapter.py:59,82 | 2 public methods | same `.rename().copy()` pattern | Memory |
| web_client.py:123,155,181,207 | 4 fetch methods | `.copy()` after `bd.read_sql()` | Memory |
| report_generator.py:224-228 | `_formatar_cabecalho` | O(rows×cols) full column scan for width | CPU |

## Optimizations APPLIED

### 1 — transformer.py: `_aplicar_rq003_flag_carga_horaria`
- **Before:** `df_out = df.copy()` then `.apply(lambda ch: ... if ch == 0 else ...)`
- **After:** mutate argument in place (`df["ALERTA_STATUS_CH"] = np.where(df["CH_TOTAL"] == 0, ...)`)
- **Why faster:** `np.where` is C-level vectorized; no per-row Python call; no extra full copy
- **Safety:** caller (`transformar`) passes a DataFrame it already owns (result of rq002 copy)

### 2 — cnes_local_adapter.py: removed `.copy()` after `.rename()` in 3 methods
- `listar_profissionais`, `listar_estabelecimentos`, `listar_equipes`
- `DataFrame.rename(inplace=False)` always returns a new object; the `.copy()` was a 3rd allocation

### 3 — cnes_nacional_adapter.py: removed `.copy()` after `.rename()` in 2 methods
- `listar_estabelecimentos`, `listar_profissionais`
- Same reason as above

### 4 — rules_engine.py: `detectar_folha_fantasma` vectorized
- **Before:** `frozenset` lookups inside a Python function called via `.map()` — O(n) with Python overhead
- **After:** two `.isin()` calls (C-level) then `.loc` assignment; no Python per row
- Pattern: compute `mascara_ausente` and `mascara_inativo` separately, assign via `.loc`, then filter

### 5 — report_generator.py: `_formatar_cabecalho` column width scan capped
- **Before:** iterated ALL cells in each column (O(rows × cols))
- **After:** `amostra = list(coluna)[:101]` — header + first 100 rows only
- **Why safe:** column width is determined by typical content, not outliers in row 5000

## Optimizations REJECTED

### web_client.py — remove `.copy()` after BigQuery fetch
- **Reason:** test `TestQualidade::test_resultado_e_copia_independente` explicitly contracts that
  the returned DF must NOT be the same object as the one from `bd.read_sql()`.
  This is an intentional defensive copy — the test documents it as a behavioural requirement, not an implementation detail.
- **Rule:** "If a single test fails, revert."

### cnes_client.py `_enriquecer_com_equipe` — remove pre-merge copies
- **Reason:** CLAUDE.md convention: "Data transformations work on `.copy()`, never mutate originals."
  The private function receives DataFrames as arguments. Even though callers don't reuse them,
  removing the copies would violate the project's stated style rule. Impact is negligible at 367 rows.

## Not worth optimizing

- `evolution_tracker.py`: JSON file I/O on tiny snapshots. Trivial.
- `csv_exporter.py`: single `df.to_csv()` call. Already optimal.
- `_aplicar_rq002_validar_cpf` `.copy()` return: the returned DF is immediately mutated by rq003
  (column assignment). Removing the `.copy()` risks SettingWithCopyWarning in pandas. Keep it.
- All functions operating on 367-row local data: even worst-case Python loops finish in <5ms.

## Data volume thresholds

- Under 10K rows national: current implementation is fast enough even before optimization
- At 10K+ national rows: ghost detection vectorization and column-width cap become measurably relevant
- At 100K+ rows: consider O6 (chunked BigQuery fetch) from the optimization guide
