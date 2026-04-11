"""Testes unitários para PostgresAdapter (sem banco de dados real)."""
import logging
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from storage.postgres_adapter import PostgresAdapter


@pytest.fixture()
def engine():
    mock = MagicMock()
    mock.begin.return_value.__enter__ = MagicMock(return_value=MagicMock())
    mock.begin.return_value.__exit__ = MagicMock(return_value=False)
    return mock


@pytest.fixture()
def adapter(engine):
    return PostgresAdapter(engine=engine, tenant_id="123456")


@pytest.fixture()
def df_profissionais():
    return pd.DataFrame(
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


@pytest.fixture()
def df_estabelecimentos():
    return pd.DataFrame(
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

    def test_sus_s_converte_para_true(self, adapter, engine, df_profissionais):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df_profissionais)
        _, kwargs = con.execute.call_args_list[1]
        rows = kwargs.get("parameters") or con.execute.call_args_list[1][0][1]
        sus_values = [r["sus"] for r in rows]
        assert True in sus_values

    def test_sus_n_converte_para_false(self, adapter, engine, df_profissionais):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df_profissionais)
        _, kwargs = con.execute.call_args_list[1]
        rows = kwargs.get("parameters") or con.execute.call_args_list[1][0][1]
        sus_values = [r["sus"] for r in rows]
        assert False in sus_values

    def test_sus_none_permanece_none(self, adapter, engine):
        df = pd.DataFrame(
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
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_profissionais("2025-01", df)
        _, kwargs = con.execute.call_args_list[1]
        rows = kwargs.get("parameters") or con.execute.call_args_list[1][0][1]
        assert rows[0]["sus"] is None


class TestGravarEstabelecimentos:
    def test_executa_um_insert(self, adapter, engine, df_estabelecimentos):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_estabelecimentos("2025-01", df_estabelecimentos)
        assert con.execute.call_count == 1

    def test_vinculo_sus_s_converte_para_true(self, adapter, engine, df_estabelecimentos):
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_estabelecimentos("2025-01", df_estabelecimentos)
        args = con.execute.call_args_list[0]
        rows = args[0][1] if len(args[0]) > 1 else args[1].get("parameters", [])
        assert rows[0]["vinculo_sus"] is True

    def test_vinculo_sus_none_permanece_none(self, adapter, engine):
        df = pd.DataFrame(
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
        con = engine.begin.return_value.__enter__.return_value
        adapter.gravar_estabelecimentos("2025-01", df)
        args = con.execute.call_args_list[0]
        rows = args[0][1] if len(args[0]) > 1 else args[1].get("parameters", [])
        assert rows[0]["vinculo_sus"] is None


class TestRegistrarPipelineRun:
    def test_nao_levanta_excecao(self, adapter):
        adapter.registrar_pipeline_run("2025-01", {"status": "ok"})

    def test_loga_em_debug(self, adapter, caplog):
        with caplog.at_level(logging.DEBUG, logger="storage.postgres_adapter"):
            adapter.registrar_pipeline_run("2025-01", {"status": "ok"})
        assert "2025-01" in caplog.text
