# Phase 1: Snapshot Replace + Business Integrity Tests

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace vinculo upsert with atomic delete-and-load (snapshot replace) so CBO reclassifications overwrite instead of duplicating, and add business integrity tests that prevent regression.

**Architecture:** `gravar_profissionais` keeps upsert for `dim_profissional` (SCD Type 1) but switches `fato_vinculo` to snapshot replace: DELETE all vinculos matching `(tenant_id, competencia, fonte)` then bulk INSERT the complete Parquet snapshot, all within one transaction. A GIN index on `fontes` JSONB makes the DELETE fast.

**Tech Stack:** SQLAlchemy Core, PostgreSQL JSONB `?` operator, Alembic, pytest + docker-compose (integration tests)

**Spec:** `docs/specs/2026-04-15-arb-remediation-design.md` — Phase 1 section

---

### Task 1: Alembic Migration — GIN Index on fontes

**Files:**
- Create: `packages/cnes_infra/src/cnes_infra/alembic/versions/006_gin_index_fontes.py`

- [ ] **Step 1: Write the migration**

```python
"""GIN index on fato_vinculo.fontes for snapshot replace DELETE performance.

Revision ID: 006
Revises: 005
Create Date: 2026-04-15
"""
from collections.abc import Sequence

from alembic import op

revision: str = "006"
down_revision: str = "005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "idx_fato_vinculo_fontes",
        "fato_vinculo",
        ["fontes"],
        schema="gold",
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index(
        "idx_fato_vinculo_fontes",
        "fato_vinculo",
        schema="gold",
    )
```

- [ ] **Step 2: Verify migration applies against test DB**

Requires docker-compose postgres running:

```bash
docker compose up -d postgres
# wait for healthy, then:
DB_URL="postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test" \
  python -c "
from alembic import command
from alembic.config import Config
cfg = Config()
cfg.set_main_option('script_location', 'packages/cnes_infra/src/cnes_infra/alembic')
cfg.set_main_option('sqlalchemy.url', 'postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test')
command.upgrade(cfg, 'head')
print('migration OK')
"
```

Expected: `migration OK` with no errors.

- [ ] **Step 3: Verify GIN index exists**

```bash
psql "postgresql://cnesdata:cnesdata_test@localhost:5433/cnesdata_test" \
  -c "SELECT indexname FROM pg_indexes WHERE tablename='fato_vinculo' AND indexdef LIKE '%gin%';"
```

Expected: `idx_fato_vinculo_fontes` in output.

- [ ] **Step 4: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/alembic/versions/006_gin_index_fontes.py
git commit -m "feat(infra): add GIN index on fato_vinculo.fontes for snapshot replace"
```

---

### Task 2: Add `_snapshot_replace_vinculos` to PostgresAdapter

**Files:**
- Modify: `packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py`

**Context:** The current `gravar_profissionais` (L31-48) calls `_upsert_chunks` for both `dim_profissional` and `fato_vinculo`. We keep upsert for `dim_profissional` but replace the `fato_vinculo` path with snapshot replace.

- [ ] **Step 1: Write failing unit test for `_snapshot_replace_vinculos`**

Add to `packages/cnes_infra/tests/storage/test_postgres_adapter_unit.py`:

```python
class TestSnapshotReplaceVinculos:
    def test_executa_delete_e_insert(self, adapter, engine):
        con = engine.begin.return_value.__enter__.return_value
        rows = [
            {
                "tenant_id": "123456", "competencia": "2025-01",
                "cpf": "12345678901", "cnes": "1234567", "cbo": "225125",
                "tipo_vinculo": "EP", "sus": True, "ch_total": 40,
                "ch_ambulatorial": 20, "ch_outras": 0, "ch_hospitalar": 20,
                "fontes": {"LOCAL": True},
            },
        ]
        adapter._snapshot_replace_vinculos(con, "2025-01", "LOCAL", rows)
        calls = con.execute.call_args_list
        assert len(calls) == 2
        delete_sql = str(calls[0][0][0])
        assert "DELETE" in delete_sql
        assert "fontes" in delete_sql
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter_unit.py::TestSnapshotReplaceVinculos -v
```

Run from `packages/cnes_infra`. Expected: FAIL with `AttributeError: 'PostgresAdapter' object has no attribute '_snapshot_replace_vinculos'`.

- [ ] **Step 3: Implement `_snapshot_replace_vinculos`**

Add to `PostgresAdapter` class in `postgres_adapter.py` (after `_upsert_chunks`, around L115):

```python
    def _snapshot_replace_vinculos(
        self,
        con,
        competencia: str,
        fonte: str,
        rows: list[dict],
    ) -> None:
        con.execute(
            text(
                "DELETE FROM gold.fato_vinculo "
                "WHERE tenant_id = :tid "
                "AND competencia = :comp "
                "AND fontes ? :fonte"
            ),
            {"tid": get_tenant_id(), "comp": competencia, "fonte": fonte},
        )
        for chunk in _chunked(rows, _CHUNK_SIZE):
            con.execute(insert(fato_vinculo).values(chunk))
```

- [ ] **Step 4: Run test to verify it passes**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter_unit.py::TestSnapshotReplaceVinculos -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py
git add packages/cnes_infra/tests/storage/test_postgres_adapter_unit.py
git commit -m "feat(infra): add _snapshot_replace_vinculos method to PostgresAdapter"
```

---

### Task 3: Wire `gravar_profissionais` to use snapshot replace for vinculos

**Files:**
- Modify: `packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py:31-48`

**Context:** `gravar_profissionais` currently calls `_upsert_chunks(con, fato_vinculo, vinculo_rows, "vinculo")` on L43. We change this to `_snapshot_replace_vinculos`.

- [ ] **Step 1: Write failing unit test**

Add to `TestGravarProfissionais` in `test_postgres_adapter_unit.py`:

```python
    def test_vinculo_usa_snapshot_replace(self, adapter, engine, df_profissionais):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df_profissionais)
        calls = [str(c[0][0]) for c in con.execute.call_args_list]
        has_delete = any("DELETE" in c for c in calls)
        has_on_conflict_vinculo = any(
            "ON CONFLICT" in c and "competencia" in c for c in calls
        )
        assert has_delete, "should use DELETE for fato_vinculo"
        assert not has_on_conflict_vinculo, "should not use ON CONFLICT for fato_vinculo"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter_unit.py::TestGravarProfissionais::test_vinculo_usa_snapshot_replace -v
```

Expected: FAIL — currently uses `ON CONFLICT` for vinculos.

- [ ] **Step 3: Modify `gravar_profissionais`**

Replace the body of `gravar_profissionais` (L31-48) with:

```python
    def gravar_profissionais(self, competencia: str, df: pl.DataFrame) -> None:
        t0 = time.perf_counter()
        prof_rows = self._build_profissional_rows(df)
        vinculo_rows = self._build_vinculo_rows(competencia, df)
        fonte = df["FONTE"][0]
        with self._engine.begin() as con:
            self._upsert_chunks(con, dim_profissional, prof_rows, "profissional")
            self._snapshot_replace_vinculos(
                con, competencia, fonte, vinculo_rows,
            )
        elapsed = time.perf_counter() - t0
        logger.info(
            "gravar_profissionais dim=%d fato=%d elapsed=%.2fs",
            len(prof_rows), len(vinculo_rows), elapsed,
        )
```

Key change: line with `_upsert_chunks(con, fato_vinculo, ...)` replaced by `_snapshot_replace_vinculos(con, competencia, fonte, vinculo_rows)`.

- [ ] **Step 4: Run all unit tests**

```bash
./venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter_unit.py -v
```

Expected: ALL PASS. Existing `test_executa_dois_inserts` may need update since call count changes (DELETE + INSERT = 2 for vinculo, + 1 for profissional = 3 total). Update it:

```python
    def test_executa_tres_statements(self, adapter, engine, df_profissionais):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df_profissionais)
        assert con.execute.call_count == 3
```

Rename from `test_executa_dois_inserts` to `test_executa_tres_statements`.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py
git add packages/cnes_infra/tests/storage/test_postgres_adapter_unit.py
git commit -m "refactor(infra): wire gravar_profissionais to snapshot replace for vinculos"
```

---

### Task 4: Integration test — CBO reclassification does not create phantoms

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

**Context:** This is the critical test that proves the CBO bug is fixed. It requires the docker-compose PostgreSQL. The existing conftest at `packages/cnes_infra/tests/storage/conftest.py` provides `adapter` and `pg_engine` fixtures with tenant_id `355030` and truncates tables between tests.

- [ ] **Step 1: Write the failing test**

Add to `test_postgres_adapter.py`:

```python
@pytest.mark.integration
def test_reclassificacao_cbo_nao_cria_fantasma(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())

    df_acs = _df_prof(cpf="11111111111", cnes="1234567")
    df_acs = df_acs.with_columns(pl.lit("515105").alias("CBO"))
    adapter.gravar_profissionais("2026-01", df_acs)

    df_ace = _df_prof(cpf="11111111111", cnes="1234567")
    df_ace = df_ace.with_columns(pl.lit("515110").alias("CBO"))
    adapter.gravar_profissionais("2026-01", df_ace)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' AND competencia = '2026-01'"
        )).scalar()
    assert count == 1, f"expected 1 vinculo after CBO reclassification, got {count}"
```

- [ ] **Step 2: Run test (requires docker-compose postgres)**

```bash
docker compose up -d postgres
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_reclassificacao_cbo_nao_cria_fantasma -v
```

Expected: PASS (snapshot replace deletes old CBO row then inserts new one).

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): add CBO reclassification phantom detection test"
```

---

### Task 5: Integration test — Legitimate multi-CBO professional preserved

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Write the test**

```python
@pytest.mark.integration
def test_profissional_multiplos_cbos_legitimos_preservados(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())

    df_medico = _df_prof(cpf="11111111111", cnes="1234567")
    df_medico = df_medico.with_columns(pl.lit("225125").alias("CBO"))
    df_professor = _df_prof(cpf="11111111111", cnes="1234567")
    df_professor = df_professor.with_columns(pl.lit("234110").alias("CBO"))
    df_both = pl.concat([df_medico, df_professor])

    adapter.gravar_profissionais("2026-01", df_both)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' AND competencia = '2026-01'"
        )).scalar()
    assert count == 2, f"expected 2 legitimate CBO vinculos, got {count}"
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_profissional_multiplos_cbos_legitimos_preservados -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify legitimate multi-CBO professional not collapsed"
```

---

### Task 6: Integration test — Snapshot replace is idempotent

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Write the test**

```python
@pytest.mark.integration
def test_snapshot_replace_idempotente(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())

    df = pl.concat([
        _df_prof(cpf="11111111111"),
        _df_prof(cpf="22222222222"),
    ])
    adapter.gravar_profissionais("2026-01", df)
    adapter.gravar_profissionais("2026-01", df)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
    assert count == 2, f"expected 2 vinculos after idempotent re-process, got {count}"
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_snapshot_replace_idempotente -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify snapshot replace idempotency"
```

---

### Task 7: Integration test — Source isolation (LOCAL does not destroy NACIONAL)

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Write the test**

```python
@pytest.mark.integration
def test_fonte_local_nao_destroi_nacional(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())

    df_nac = _df_prof(cpf="11111111111", fonte="NACIONAL")
    adapter.gravar_profissionais("2026-01", df_nac)

    df_local = _df_prof(cpf="22222222222", fonte="LOCAL")
    adapter.gravar_profissionais("2026-01", df_local)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
        nac = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01' AND fontes ? 'NACIONAL'"
        )).scalar()
        loc = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01' AND fontes ? 'LOCAL'"
        )).scalar()
    assert count == 2, f"expected 2 total vinculos, got {count}"
    assert nac == 1, f"expected 1 NACIONAL vinculo, got {nac}"
    assert loc == 1, f"expected 1 LOCAL vinculo, got {loc}"
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_fonte_local_nao_destroi_nacional -v
```

Expected: PASS.

- [ ] **Step 3: Write the inverse test**

```python
@pytest.mark.integration
def test_fonte_nacional_nao_destroi_local(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())

    df_local = _df_prof(cpf="11111111111", fonte="LOCAL")
    adapter.gravar_profissionais("2026-01", df_local)

    df_nac = _df_prof(cpf="22222222222", fonte="NACIONAL")
    adapter.gravar_profissionais("2026-01", df_nac)

    with pg_engine.connect() as con:
        count = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-01'"
        )).scalar()
    assert count == 2, f"expected 2 total vinculos, got {count}"
```

- [ ] **Step 4: Run both tests**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py -k "fonte" -v
```

Expected: BOTH PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify source isolation in snapshot replace"
```

---

### Task 8: Integration test — Atomicity on failure (rollback preserves data)

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

**Context:** If the INSERT after DELETE fails (e.g. FK violation), the DELETE must also rollback. We test this by inserting valid data first, then attempting a snapshot replace with an invalid CPF that violates the CHECK constraint (`cpf ~ '^\d{11}$'`).

- [ ] **Step 1: Write the test**

```python
@pytest.mark.integration
def test_delete_insert_atomico_em_falha(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab())
    adapter.gravar_profissionais("2026-01", _df_prof())

    with pg_engine.connect() as con:
        before = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo"
        )).scalar()
    assert before == 1

    df_bad = _df_prof(cpf="BADCPF00000")
    with pytest.raises(Exception):
        adapter.gravar_profissionais("2026-01", df_bad)

    with pg_engine.connect() as con:
        after = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo"
        )).scalar()
    assert after == before, (
        f"DELETE should have been rolled back; before={before} after={after}"
    )
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_delete_insert_atomico_em_falha -v
```

Expected: PASS. The `BADCPF00000` fails dim_profissional CHECK constraint, rolling back the entire transaction including the fato_vinculo DELETE.

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify atomic rollback on failed snapshot replace"
```

---

### Task 9: Integration test — Competencia isolation

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Write the test**

```python
@pytest.mark.integration
def test_competencia_isolada(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-02", _df_estab())

    adapter.gravar_profissionais("2026-02", _df_prof(cpf="11111111111"))
    adapter.gravar_profissionais("2026-03", _df_prof(cpf="22222222222"))

    with pg_engine.connect() as con:
        feb = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-02'"
        )).scalar()
        mar = con.execute(text(
            "SELECT COUNT(*) FROM gold.fato_vinculo "
            "WHERE competencia = '2026-03'"
        )).scalar()
    assert feb == 1, f"expected 1 vinculo in 2026-02, got {feb}"
    assert mar == 1, f"expected 1 vinculo in 2026-03, got {mar}"
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_competencia_isolada -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify competencia isolation in snapshot replace"
```

---

### Task 10: Integration test — Professional changes establishment

**Files:**
- Modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Add second establishment helper and write the test**

```python
@pytest.mark.integration
def test_profissional_troca_estabelecimento(adapter, pg_engine):
    adapter.gravar_estabelecimentos("2026-01", _df_estab(cnes="1234567"))
    adapter.gravar_estabelecimentos("2026-01", _df_estab(cnes="7654321"))

    df_cnes_a = _df_prof(cpf="11111111111", cnes="1234567")
    adapter.gravar_profissionais("2026-01", df_cnes_a)

    df_cnes_b = _df_prof(cpf="11111111111", cnes="7654321")
    adapter.gravar_profissionais("2026-01", df_cnes_b)

    with pg_engine.connect() as con:
        rows = con.execute(text(
            "SELECT cnes FROM gold.fato_vinculo "
            "WHERE cpf = '11111111111' AND competencia = '2026-01'"
        )).fetchall()
    cnes_list = [r[0] for r in rows]
    assert "7654321" in cnes_list, "new CNES should be present"
    assert "1234567" not in cnes_list, "old CNES should have been replaced"
```

- [ ] **Step 2: Run test**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py::test_profissional_troca_estabelecimento -v
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add packages/cnes_infra/tests/storage/test_postgres_adapter.py
git commit -m "test(infra): verify professional facility transfer via snapshot replace"
```

---

### Task 11: Run full test suite and fix regressions

**Files:**
- Possibly modify: `packages/cnes_infra/tests/storage/test_postgres_adapter.py`

- [ ] **Step 1: Run full unit test suite**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter_unit.py -v
```

Expected: ALL PASS. If `test_executa_dois_inserts` fails, it was renamed in Task 3.

- [ ] **Step 2: Run full integration test suite**

```bash
docker compose up -d postgres
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py -v --tb=short
```

Expected: ALL PASS.

Check specifically that existing tests still pass:
- `test_gravar_profissionais_upsert_nao_duplica_rows` — should still pass (snapshot replace is idempotent)
- `test_fontes_jsonb_merge_em_conflito` — **THIS WILL CHANGE BEHAVIOR**. Previously, JSONB merged on conflict (`fontes || EXCLUDED.fontes`). With snapshot replace, each fonte gets its own rows. This test may need updating to reflect that `dim_profissional` still merges fontes (upsert) but `fato_vinculo` now replaces.

If `test_fontes_jsonb_merge_em_conflito` fails for fato_vinculo, update it to verify the JSONB merge behavior only on `dim_profissional`:

```python
@pytest.mark.integration
def test_fontes_jsonb_merge_em_conflito(adapter, pg_engine):
    adapter.gravar_profissionais("2026-01", _df_prof(fonte="LOCAL"))
    adapter.gravar_profissionais("2026-01", _df_prof(fonte="NACIONAL"))
    with pg_engine.connect() as con:
        fontes = con.execute(text(
            "SELECT fontes FROM gold.dim_profissional"
        )).scalar()
    assert fontes.get("LOCAL") is True
    assert fontes.get("NACIONAL") is True
```

- [ ] **Step 3: Run lint**

```bash
./venv/Scripts/ruff.exe check packages/cnes_infra/src/ packages/cnes_infra/tests/
```

Expected: no errors.

- [ ] **Step 4: Commit any regression fixes**

```bash
git add -u
git commit -m "fix(infra): update existing tests for snapshot replace behavior"
```

---

### Task 12: Verify existing `gravar_estabelecimentos` unchanged

**Files:** None (verification only)

`gravar_estabelecimentos` uses inline `on_conflict_do_update` (L50-77), NOT `_upsert_chunks`. It was never routed through the vinculo conflict keys. Confirm it still works.

- [ ] **Step 1: Run establishment-specific integration tests**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/storage/test_postgres_adapter.py -k "estabelecimento" -v
```

Expected: ALL PASS. No changes needed — `gravar_estabelecimentos` is not affected by snapshot replace.

---

### Task 13: Dead code cleanup in `_upsert_chunks`

**Files:**
- Modify: `packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py:82-115`

**Context:** The `"vinculo"` branch in `conflict_keys` (L91-107) is now dead code — `_upsert_chunks` is only called with `kind="profissional"`. Remove it.

- [ ] **Step 1: Simplify `_upsert_chunks`**

Replace the method (L82-115):

```python
    def _upsert_chunks(self, con, table, rows: list[dict], kind: str) -> None:
        conflict_keys = {
            "profissional": {
                "index_elements": ["tenant_id", "cpf"],
                "set_": {
                    "fontes": text(f"{table.name}.fontes || EXCLUDED.fontes"),
                    "atualizado_em": text("NOW()"),
                },
            },
        }
        cfg = conflict_keys[kind]
        for chunk in _chunked(rows, _CHUNK_SIZE):
            con.execute(
                insert(table)
                .values(chunk)
                .on_conflict_do_update(**cfg),
            )
```

Removed: entire `"vinculo"` dict (L91-107) with its 5-column index_elements and 8-field set_.

- [ ] **Step 2: Run all tests**

```bash
cd packages/cnes_infra
../../venv/Scripts/python.exe -m pytest tests/ -v --tb=short
```

Expected: ALL PASS.

- [ ] **Step 3: Run lint**

```bash
./venv/Scripts/ruff.exe check packages/cnes_infra/src/ packages/cnes_infra/tests/
```

Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add packages/cnes_infra/src/cnes_infra/storage/postgres_adapter.py
git commit -m "refactor(infra): remove dead vinculo upsert config from _upsert_chunks"
```
