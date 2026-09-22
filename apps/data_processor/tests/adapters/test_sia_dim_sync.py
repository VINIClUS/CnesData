from __future__ import annotations

import os

import polars as pl
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from data_processor.adapters.sia_dim_sync import (
    _ibge7_check_digit,
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

    def test_nao_fabrica_complexidade_financiamento_modalidade(
        self, pg_engine,
    ) -> None:
        """S_CDN.DBF só carrega CDN_TB/IT/DSCR/CHKSM — esses atributos
        não existem na fonte; devem ficar NULL, não um valor inventado
        que passaria por dado real em queries downstream."""
        df = pl.DataFrame({
            "cdn_tb": ["PROC"],
            "cdn_it": ["0399010077"],
            "cdn_dscr": ["CONSULTA TESTE C"],
            "cdn_chksm": ["0"],
        })
        sync_dim_procedimento(pg_engine, df)
        with pg_engine.begin() as conn:
            row = conn.execute(text(
                "SELECT complexidade, financiamento, modalidade, "
                "competencia_vigencia_ini, competencia_vigencia_fim "
                "FROM gold.dim_procedimento_sus WHERE cod_sigtap = '0399010077'",
            )).one()
        assert row.complexidade is None
        assert row.financiamento is None
        assert row.modalidade is None
        assert row.competencia_vigencia_ini is None
        assert row.competencia_vigencia_fim is None

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

    def test_ibge7_colidindo_pula_linha_sem_abortar_o_lote(
        self, pg_engine,
    ) -> None:
        """ON CONFLICT (ibge6) só cobre um ibge6 já existente; um ibge6
        novo cujo ibge7 recomputado colide com o ibge7 (stale/incorreto)
        de OUTRA linha levanta IntegrityError na constraint própria de
        ibge7 - isso não pode derrubar o engine.begin() inteiro e
        descartar as demais linhas já sincronizadas nesta chamada."""
        syncing_ibge6 = "999997"
        stale_stored_ibge7 = syncing_ibge6 + str(_ibge7_check_digit(syncing_ibge6))
        with pg_engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM gold.dim_municipio WHERE ibge6 IN "
                "('999998', '999997', '999996', '999995')",
            ))
            conn.execute(text(
                "INSERT INTO gold.dim_municipio (ibge6, ibge7, nome, uf) "
                "VALUES ('999998', :i7, 'PRE-EXISTENTE', 'SP')",
            ), {"i7": stale_stored_ibge7})

        df = pl.DataFrame({
            "coduf": ["35", "35"],
            "codmunic": [syncing_ibge6, "999995"],
            "nome": ["COLIDE", "SEGUE_OK"],
            "condic": ["1", "1"],
        })
        n = sync_dim_municipio(pg_engine, df)
        assert n == 1

        with pg_engine.begin() as conn:
            colidiu = conn.execute(text(
                "SELECT COUNT(*) FROM gold.dim_municipio WHERE ibge6 = :i6",
            ), {"i6": syncing_ibge6}).scalar_one()
            seguiu = conn.execute(text(
                "SELECT nome FROM gold.dim_municipio WHERE ibge6 = '999995'",
            )).scalar_one()
            conn.execute(text(
                "DELETE FROM gold.dim_municipio WHERE ibge6 IN "
                "('999998', '999995', '999997')",
            ))
        assert colidiu == 0
        assert seguiu == "SEGUE_OK"

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
