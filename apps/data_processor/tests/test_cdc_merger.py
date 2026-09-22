"""Tests for cdc_merger module — _op routing + DELETE inline."""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import polars as pl
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from data_processor.cdc_merger import (
    FatalError,
    has_op_column,
    merge_delta,
)


def test_has_op_column_true():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    assert has_op_column(df) is True


def test_has_op_column_false():
    df = pl.DataFrame({"CNES": ["1"]})
    assert has_op_column(df) is False


def test_merge_delta_buckets_iud():
    df = pl.DataFrame({
        "CNES": ["1", "2", "3"],
        "NOME_FANTA": ["A", "B", None],
        "_op": ["I", "U", "D"],
    })
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts == {
        "inserts": 1, "updates": 1, "deletes": 1, "applied": 0,
    }
    conn.execute.assert_called_once()


def test_merge_delta_with_apply_iu_fn_calls_callback():
    df = pl.DataFrame({
        "CNES": ["1", "2"],
        "NOME_FANTA": ["A", "B"],
        "_op": ["I", "U"],
    })
    conn = MagicMock()
    captured: list[pl.DataFrame] = []

    def apply_iu(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    counts = merge_delta(df, conn, "cnes", "estabelecimentos", apply_iu)
    assert counts == {
        "inserts": 1, "updates": 1, "deletes": 0, "applied": 2,
    }
    assert len(captured) == 1
    assert "_op" not in captured[0].columns
    assert len(captured[0]) == 2


def test_merge_delta_no_callback_skips_apply():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["I"]})
    conn = MagicMock()
    counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts["applied"] == 0
    assert counts["inserts"] == 1


def test_merge_delta_callback_only_deletes_applied_zero():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["D"]})
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=1)
    captured: list[pl.DataFrame] = []

    def apply_iu(df_iu: pl.DataFrame) -> int:
        captured.append(df_iu)
        return len(df_iu)

    counts = merge_delta(df, conn, "cnes", "estabelecimentos", apply_iu)
    assert counts["applied"] == 0
    assert counts["deletes"] == 1
    assert captured == []


def test_merge_delta_unknown_op_raises():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["X"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_op"):
        merge_delta(df, conn, "cnes", "estabelecimentos")


def test_merge_delta_unknown_source_intent_raises():
    df = pl.DataFrame({"CNES": ["1"], "_op": ["D"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_source_intent"):
        merge_delta(df, conn, "xyz", "abc")


def test_merge_delta_cnes_equipes_raises_unknown_source_intent():
    """gold.dim_equipe is never created by any migration - routing this
    intent used to raise UndefinedTable at execution time; it must raise
    the same FatalError as any other unrouted (source, intent)."""
    df = pl.DataFrame({"SEQ_EQUIPE": ["1"], "_op": ["D"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_source_intent"):
        merge_delta(df, conn, "cnes", "equipes")


def test_merge_delta_bpa_linhas_raises_unknown_source_intent():
    """fato_producao_ambulatorial's only natural key includes job_id,
    which merge_delta never receives - a correct DELETE is unconstructable
    from what this function is given, so the intent is unrouted."""
    df = pl.DataFrame({"CPF": ["1"], "_op": ["D"]})
    conn = MagicMock()
    with pytest.raises(FatalError, match="unknown_source_intent"):
        merge_delta(df, conn, "bpa", "linhas")


def test_merge_delta_delete_no_op_logs(caplog):
    df = pl.DataFrame({"CNES": ["404"], "_op": ["D"]})
    conn = MagicMock()
    conn.execute.return_value = MagicMock(rowcount=0)
    with caplog.at_level("INFO"):
        counts = merge_delta(df, conn, "cnes", "estabelecimentos")
    assert counts["deletes"] == 0
    assert any("delete_no_op" in r.message for r in caplog.records)


# --- postgres-marked: proves the SQL actually parses/executes against
# Gold v2, not just that FatalError fires for unrouted intents. ---

_PG_URL = os.getenv(
    "PG_TEST_URL",
    "postgresql+psycopg://cnesdata:cnesdata_test@localhost:5433/cnesdata_test",
)


@pytest.fixture(scope="module")
def pg_engine():
    engine = create_engine(_PG_URL)
    try:
        with engine.connect() as con:
            con.execute(text("SELECT 1"))
    except Exception:
        pytest.skip(
            f"postgres indisponivel em {_PG_URL}; "
            "rode 'docker compose up -d' primeiro",
        )
    cfg = Config()
    cfg.set_main_option("script_location", "cnes_infra:alembic")
    cfg.set_main_option("sqlalchemy.url", _PG_URL)
    command.upgrade(cfg, "head")
    yield engine
    engine.dispose()


@pytest.mark.postgres
class TestMergeDeltaCnesProfissionaisPostgres:
    def test_delete_remove_vinculo_via_cpf_hash_e_cbo_subquery(
        self, pg_engine,
    ) -> None:
        from data_processor.cdc_merger import _cpf_hash

        raw_cpf = "12345678901"
        cpf_hash = _cpf_hash(raw_cpf)
        cnes = "9999901"
        cod_cbo = "225930"

        with pg_engine.begin() as conn:
            sk_municipio = conn.execute(text(
                "INSERT INTO gold.dim_municipio "
                "(ibge6, ibge7, nome, uf) VALUES "
                "('999999', '9999999', 'TESTE', 'SP') "
                "RETURNING sk_municipio",
            )).scalar_one()
            sk_estab = conn.execute(text(
                "INSERT INTO gold.dim_estabelecimento "
                "(cnes, nome, tp_unid, sk_municipio) VALUES "
                "(:cnes, 'TESTE', 5, :sk_municipio) "
                "RETURNING sk_estabelecimento",
            ), {"cnes": cnes, "sk_municipio": sk_municipio}).scalar_one()
            sk_cbo = conn.execute(text(
                "INSERT INTO gold.dim_cbo (cod_cbo, descricao) VALUES "
                "(:cod_cbo, 'TESTE') RETURNING sk_cbo",
            ), {"cod_cbo": cod_cbo}).scalar_one()
            sk_prof = conn.execute(text(
                "INSERT INTO gold.dim_profissional (cpf_hash, nome) VALUES "
                "(:cpf_hash, 'TESTE') RETURNING sk_profissional",
            ), {"cpf_hash": cpf_hash}).scalar_one()
            job_id = conn.execute(text(
                "INSERT INTO gold.fato_vinculo_cnes "
                "(sk_profissional, sk_estabelecimento, sk_cbo, "
                " sk_competencia, job_id, fonte_sistema, extracao_ts) "
                "VALUES (:sk_prof, :sk_estab, :sk_cbo, 73, "
                " gen_random_uuid(), 'CNES_LOCAL', NOW()) "
                "RETURNING job_id",
            ), {
                "sk_prof": sk_prof, "sk_estab": sk_estab, "sk_cbo": sk_cbo,
            }).scalar_one()

            df = pl.DataFrame({
                "CPF_PROF": [raw_cpf], "CNES": [cnes], "COD_CBO": [cod_cbo],
                "_op": ["D"],
            })
            counts = merge_delta(df, conn, "cnes", "profissionais")
            assert counts["deletes"] == 1

            remaining = conn.execute(text(
                "SELECT COUNT(*) FROM gold.fato_vinculo_cnes "
                "WHERE job_id = :job_id",
            ), {"job_id": job_id}).scalar_one()
            assert remaining == 0

            conn.execute(text(
                "DELETE FROM gold.dim_profissional WHERE sk_profissional = :sk",
            ), {"sk": sk_prof})
            conn.execute(text(
                "DELETE FROM gold.dim_cbo WHERE sk_cbo = :sk",
            ), {"sk": sk_cbo})
            conn.execute(text(
                "DELETE FROM gold.dim_estabelecimento WHERE sk_estabelecimento = :sk",
            ), {"sk": sk_estab})
            conn.execute(text(
                "DELETE FROM gold.dim_municipio WHERE sk_municipio = :sk",
            ), {"sk": sk_municipio})
