from __future__ import annotations

import os

import polars as pl
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from data_processor.adapters.sia_dim_sync import (
    sync_dim_municipio,
    sync_dim_procedimento,
)

pytestmark = pytest.mark.postgres

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


class TestSiaDimSync:
    def test_s_cdn_sync_upsert_procs(self, pg_engine) -> None:
        df = pl.DataFrame({
            "cdn_tb": ["PROC", "PROC"],
            "cdn_it": ["0399010056", "0399010064"],
            "cdn_dscr": ["CONSULTA TESTE A", "CONSULTA TESTE B"],
            "cdn_chksm": ["00000000", "00000000"],
        })
        n = sync_dim_procedimento(pg_engine, df)
        assert n == 2

    def test_cadmun_sync(self, pg_engine) -> None:
        df = pl.DataFrame({
            "coduf": ["35"],
            "codmunic": ["399130"],
            "nome": ["CIDADE TESTE"],
            "condic": ["1"],
        })
        n = sync_dim_municipio(pg_engine, df)
        assert n == 1

        with pg_engine.begin() as conn:
            nome = conn.execute(text(
                "SELECT nome FROM gold.dim_municipio "
                "WHERE ibge6 = '399130'",
            )).scalar_one()
        assert nome == "CIDADE TESTE"

    def test_idempotente(self, pg_engine) -> None:
        df = pl.DataFrame({
            "cdn_tb": ["PROC"],
            "cdn_it": ["0399010099"],
            "cdn_dscr": ["X"],
            "cdn_chksm": ["0"],
        })
        sync_dim_procedimento(pg_engine, df)
        sync_dim_procedimento(pg_engine, df)
        with pg_engine.begin() as conn:
            count = conn.execute(text(
                "SELECT COUNT(*) FROM gold.dim_procedimento_sus "
                "WHERE cod_sigtap = '0399010099'",
            )).scalar_one()
        assert count == 1
