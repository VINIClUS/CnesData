"""Testes unitários para PostgresAdapter (sem banco de dados real)."""
import logging
from unittest.mock import MagicMock

import polars as pl
import pytest

from cnes_domain.tenant import set_tenant_id
from cnes_infra.storage.postgres_adapter import PostgresAdapter

_TENANT_ID = "123456"


@pytest.fixture
def engine():
    mock = MagicMock()
    mock.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock.begin.return_value.__exit__ = MagicMock(return_value=False)
    return mock


@pytest.fixture
def adapter(engine):
    set_tenant_id(_TENANT_ID)
    return PostgresAdapter(engine=engine)


@pytest.fixture
def df_profissionais():
    return pl.DataFrame(
        [
            {
                "CPF": "12345678901",
                "CNS": "123456789012345",
                "NOME_PROFISSIONAL": "João Silva",
                "SEXO": "M",
                "CBO": "225125",
                "CNES": "1234567",
                "TIPO_VINCULO": "EP",
                "SUS": "S",
                "CH_TOTAL": 40,
                "CH_AMBULATORIAL": 20,
                "CH_OUTRAS": 0,
                "CH_HOSPITALAR": 20,
                "FONTE": "LOCAL",
            },
            {
                "CPF": "98765432100",
                "CNS": None,
                "NOME_PROFISSIONAL": "Maria Costa",
                "SEXO": "F",
                "CBO": "225125",
                "CNES": "1234567",
                "TIPO_VINCULO": "EP",
                "SUS": "N",
                "CH_TOTAL": 20,
                "CH_AMBULATORIAL": 20,
                "CH_OUTRAS": 0,
                "CH_HOSPITALAR": 0,
                "FONTE": "LOCAL",
            },
        ]
    )


@pytest.fixture
def df_estabelecimentos():
    return pl.DataFrame(
        [
            {
                "CNES": "1234567",
                "NOME_FANTASIA": "UBS Centro",
                "TIPO_UNIDADE": "02",
                "CNPJ_MANTENEDORA": "55293427000117",
                "NATUREZA_JURIDICA": "1023",
                "COD_MUNICIPIO": "123456",
                "VINCULO_SUS": "S",
                "FONTE": "LOCAL",
            }
        ]
    )


class TestGravarProfissionais:
    def test_executa_dois_inserts(self, adapter, engine, df_profissionais):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df_profissionais)
        assert con.execute.call_count == 2

    def test_sus_s_converte_para_true(self, adapter, df_profissionais):
        rows = adapter._build_vinculo_rows("2025-01", df_profissionais)
        sus_values = [r["sus"] for r in rows]
        assert True in sus_values

    def test_sus_n_converte_para_false(self, adapter, df_profissionais):
        rows = adapter._build_vinculo_rows("2025-01", df_profissionais)
        sus_values = [r["sus"] for r in rows]
        assert False in sus_values

    def test_sus_none_permanece_none(self, adapter):
        df = pl.DataFrame(
            [
                {
                    "CPF": "11111111111",
                    "CNS": None,
                    "NOME_PROFISSIONAL": "X",
                    "SEXO": "M",
                    "CBO": "225125",
                    "CNES": "1234567",
                    "TIPO_VINCULO": "EP",
                    "SUS": None,
                    "CH_TOTAL": 40,
                    "CH_AMBULATORIAL": 20,
                    "CH_OUTRAS": 0,
                    "CH_HOSPITALAR": 20,
                    "FONTE": "LOCAL",
                }
            ]
        )
        rows = adapter._build_vinculo_rows("2025-01", df)
        assert rows[0]["sus"] is None


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


class TestGravarEstabelecimentos:
    def test_executa_um_insert(self, adapter, engine, df_estabelecimentos):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_estabelecimentos("2025-01", df_estabelecimentos)
        assert con.execute.call_count == 1

    def test_vinculo_sus_s_converte_para_true(self, adapter, df_estabelecimentos):
        rows = adapter._build_estabelecimento_rows(df_estabelecimentos)
        assert rows[0]["vinculo_sus"] is True

    def test_vinculo_sus_none_permanece_none(self, adapter):
        df = pl.DataFrame(
            [
                {
                    "CNES": "1234567",
                    "NOME_FANTASIA": "UBS",
                    "TIPO_UNIDADE": "02",
                    "CNPJ_MANTENEDORA": "55293427000117",
                    "NATUREZA_JURIDICA": "1023",
                    "COD_MUNICIPIO": "123456",
                    "VINCULO_SUS": None,
                    "FONTE": "LOCAL",
                }
            ]
        )
        rows = adapter._build_estabelecimento_rows(df)
        assert rows[0]["vinculo_sus"] is None


class TestRegistrarPipelineRun:
    def test_nao_levanta_excecao(self, adapter):
        adapter.registrar_pipeline_run("2025-01", {"status": "ok"})

    def test_loga_em_debug(self, adapter, caplog):
        with caplog.at_level(logging.DEBUG, logger="cnes_infra.storage.postgres_adapter"):
            adapter.registrar_pipeline_run("2025-01", {"status": "ok"})
        assert "2025-01" in caplog.text
