---
name: audit-rule
description: >
  Scaffolds a new CNES audit rule (RQ-NNN) end-to-end using strict TDD.
  Use when the user says "add RQ-NNN", "implement rule RQ-NNN", "nova regra RQ-NNN",
  or "criar regra de auditoria". Covers: reading data_dictionary.md, writing failing
  tests, implementing the function, wiring in main.py.
  Does NOT trigger for debugging existing rules or modifying transformations.
---

# Audit Rule Implementation Workflow

## Required arg
`RQ_ID` — rule identifier, e.g. `RQ-012` or `WP-005`.

## Step 1 — Read the spec

Read the relevant section in `data_dictionary.md` for `RQ_ID`. Extract:
- **Anomaly definition**: what condition triggers a finding
- **Source tables / DataFrames**: which inputs (local CNES, nacional, RH)
- **Domain constants**: CBO sets, TP_UNID_ID sets, status values
- **Exclusion clauses**: cascade filters, tipo_excluir params

Do NOT proceed until the spec is clear. If `data_dictionary.md` has no entry for `RQ_ID`, ask the user to add it first.

## Step 2 — Plan the function signature

Choose the function name and signature pattern:

| Rule type | Pattern | Example |
|---|---|---|
| Single-source filter | `def detectar_<nome>(df: pd.DataFrame, ...) -> pd.DataFrame` | RQ-003-B |
| CNES vs nacional | `def detectar_<nome>(df_local: pd.DataFrame, df_nacional: pd.DataFrame, ...) -> pd.DataFrame` | RQ-006, RQ-008, RQ-010 |
| CNES vs RH | `def detectar_<nome>(df_cnes: pd.DataFrame, df_rh: pd.DataFrame) -> pd.DataFrame` | WP-003, WP-004 |

Rules:
- ≤4 parameters. Extract domain constants as module-level `Final[frozenset[str]]`.
- Always return a `pd.DataFrame` (empty if no anomalies).
- Never mutate inputs — work on `.copy()`.

## Step 3 — Write tests FIRST (Red phase)

Add a new test class to `tests/analysis/test_rules_engine.py`.

**Mandatory test cases** (adapt names to the rule):

```python
# ─────────────────────────────────────────────────────────────────────────────
# Grupo N: detectar_<nome>() — RQ-NNN
# ─────────────────────────────────────────────────────────────────────────────

class TestDetectar<Nome>:

    def test_detecta_caso_positivo(self):
        """<Condition that triggers anomaly> → encontrado no resultado."""
        ...
        assert len(resultado) == 1
        assert resultado["<key_col>"].iloc[0] == "<expected>"

    def test_nao_detecta_caso_negativo(self):
        """<Condition where no anomaly exists> → resultado vazio."""
        ...
        assert resultado.empty

    def test_retorna_vazio_quando_entrada_vazia(self):
        assert detectar_<nome>(pd.DataFrame()).empty  # or pass empty dfs

    def test_colunas_preservadas(self):
        """Colunas de origem preservadas no resultado."""
        ...
        assert "<expected_col>" in resultado.columns

    def test_logging_registra_contagem(self, caplog):
        with caplog.at_level(logging.INFO, logger="analysis.rules_engine"):
            detectar_<nome>(...)
        assert "rq-<nnn>" in caplog.text.lower() or "<keyword>" in caplog.text.lower()
```

Use minimal factory helpers like existing `_df_cnes()`, `_df_rh()`. If new helpers are needed, add them near the test class with a `# Helpers para RQ-NNN` comment.

Run tests to confirm they **fail**:
```bash
./venv/Scripts/python.exe -m pytest tests/analysis/test_rules_engine.py::TestDetectar<Nome> -v
```

## Step 4 — Implement (Green phase)

Add the function to `src/analysis/rules_engine.py`:

1. **Add any new domain constants** near line 31 with the existing constants block, with a `# fonte: data_dictionary.md — RQ-NNN` comment.
2. **Write the function** following this template:

```python
def detectar_<nome>(df: pd.DataFrame, ...) -> pd.DataFrame:
    """RQ-NNN: <one-line description in Portuguese>.

    Args:
        df: DataFrame transformado com colunas <list key cols>.
    Returns:
        Subconjunto de df com as anomalias detectadas.
    """
    if df.empty:
        return df.copy()
    # ... logic using .copy(), no mutation ...
    logger.info("RQ-NNN: %d <anomalia(s)> detectada(s).", len(resultado))
    return resultado
```

3. **Update the module docstring** at the top of `rules_engine.py` to list the new rule.

Run tests to confirm **Green**:
```bash
./venv/Scripts/python.exe -m pytest tests/analysis/test_rules_engine.py::TestDetectar<Nome> -v
```

## Step 5 — Lint

```bash
./venv/Scripts/ruff.exe check src/analysis/rules_engine.py tests/analysis/test_rules_engine.py
```

Fix any violations before proceeding.

## Step 6 — Wire in main.py

1. Import the new function in `src/main.py`.
2. Call it in the appropriate pipeline step (after the relevant data source is loaded).
3. Pass the result to the report/export layer consistent with existing rule outputs.

Check how an existing similar rule is wired (e.g., `detectar_estabelecimentos_fantasma` for national-comparison rules).

## Step 7 — Full test suite

```bash
./venv/Scripts/python.exe -m pytest tests/ -m "not integration" -q --tb=short
```

All tests must pass before declaring done.

## Hard constraints (from CLAUDE.md)

- ≤50 lines per function body
- ≤4 parameters
- No inline comments — only "why" comments for non-obvious business rules
- Parameterized queries only if SQL is involved (no string interpolation)
- All user-facing strings and docstrings in **Portuguese**; business logic in **English**
