"""Teste do gen_shadow_seed_sql — gera SQL seed FB compatível."""
from pathlib import Path

from scripts.gen_shadow_seed_sql import (
    generate_cnes_seed,
    render_insert_stmts,
)


def test_generate_cnes_seed_produz_sql_com_inserts(tmp_path: Path) -> None:
    out = tmp_path / "cnes_seed.sql"
    generate_cnes_seed(out, seed=42, rows_per_table=10)
    content = out.read_text(encoding="utf-8")

    assert "CREATE TABLE LFCES018" in content
    assert "CREATE TABLE LFCES004" in content
    assert "CREATE TABLE LFCES021" in content

    assert content.count("INSERT INTO LFCES018") >= 10
    assert content.count("INSERT INTO LFCES004") >= 10

    assert "COMMIT" in content


def test_generate_cnes_seed_deterministico(tmp_path: Path) -> None:
    out_a = tmp_path / "a.sql"
    out_b = tmp_path / "b.sql"
    generate_cnes_seed(out_a, seed=42, rows_per_table=5)
    generate_cnes_seed(out_b, seed=42, rows_per_table=5)
    assert out_a.read_bytes() == out_b.read_bytes(), "output must be deterministic"


def test_render_insert_stmts_escapa_aspas_simples() -> None:
    rows = [{"NOME": "João O'Silva", "ID": 1}]
    stmts = render_insert_stmts("TB", rows)
    assert "O''Silva" in stmts
    assert "João O''Silva" in stmts
