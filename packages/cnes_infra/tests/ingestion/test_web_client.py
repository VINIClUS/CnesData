"""test_web_client.py -- Testes Unitarios do Cliente BigQuery (WP-002)."""

import logging
from unittest.mock import patch

import pandas as pd
import polars as pl
import pytest

from cnes_infra.ingestion.web_client import (
    CnesWebAuthError,
    CnesWebClient,
    CnesWebError,
)

_ID_MUNICIPIO = "3541307"
_ANO = 2026
_MES = 3
_CNES = "0985333"
_BILLING_PROJECT = "projeto-teste"


def _df_estabelecimentos() -> pd.DataFrame:
    return pd.DataFrame({
        "ano": [_ANO],
        "mes": [_MES],
        "id_municipio": [_ID_MUNICIPIO],
        "id_municipio_6": ["354130"],
        "id_estabelecimento_cnes": [_CNES],
        "cnpj_mantenedora": ["55293427000117"],
        "id_natureza_juridica": ["1244"],
        "tipo_unidade": ["01"],
        "tipo_gestao": ["M"],
        "indicador_vinculo_sus": [1],
    })


def _df_profissionais() -> pd.DataFrame:
    return pd.DataFrame({
        "ano": [_ANO],
        "mes": [_MES],
        "id_municipio": [_ID_MUNICIPIO],
        "id_estabelecimento_cnes": [_CNES],
        "cartao_nacional_saude": ["702002887429583"],
        "nome": ["PROFISSIONAL TESTE"],
        "cbo_2002": ["514225"],
        "tipo_vinculo": ["01"],
        "indicador_atende_sus": [1],
        "carga_horaria_ambulatorial": [40],
        "carga_horaria_outros": [0],
        "carga_horaria_hospitalar": [0],
    })


def _df_equipes() -> pd.DataFrame:
    return pd.DataFrame({
        "ano": [_ANO],
        "mes": [_MES],
        "id_municipio": [_ID_MUNICIPIO],
        "id_estabelecimento_cnes": [_CNES],
        "id_equipe": ["0001365993"],
    })


class TestFetchEstabelecimentosQuery:

    def test_query_usa_id_municipio_exato(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_estabelecimentos()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"id_municipio = '{_ID_MUNICIPIO}'" in sql_executado

    def test_query_inclui_ano_e_mes_como_particao(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_estabelecimentos()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"ano = {_ANO}" in sql_executado
            assert f"mes = {_MES}" in sql_executado

    def test_query_seleciona_tabela_estabelecimento(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_estabelecimentos()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert "br_ms_cnes.estabelecimento" in sql_executado


class TestFetchProfissionaisQuery:

    def test_query_inclui_cartao_nacional_saude(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert "cartao_nacional_saude" in sql_executado

    def test_query_filtra_municipio_ano_mes(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"id_municipio = '{_ID_MUNICIPIO}'" in sql_executado
            assert f"ano = {_ANO}" in sql_executado
            assert f"mes = {_MES}" in sql_executado


class TestFetchProfissionaisPorEstabelecimentoQuery:

    def test_query_filtra_por_cnes(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_profissionais_por_estabelecimento(_CNES, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"id_estabelecimento_cnes = '{_CNES}'" in sql_executado

    def test_query_inclui_ano_mes(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_profissionais_por_estabelecimento(_CNES, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"ano = {_ANO}" in sql_executado
            assert f"mes = {_MES}" in sql_executado


class TestFetchEquipesQuery:

    def test_query_filtra_municipio(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_equipes()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_equipes(_ID_MUNICIPIO, _ANO, _MES)

            sql_executado: str = mock_read_sql.call_args[0][0]
            assert f"id_municipio = '{_ID_MUNICIPIO}'" in sql_executado


class TestRetorno:

    def test_fetch_estabelecimentos_retorna_dataframe(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_estabelecimentos()
            client = CnesWebClient(_BILLING_PROJECT)
            resultado = client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            assert isinstance(resultado, pl.DataFrame)
            assert len(resultado) == 1

    def test_fetch_estabelecimentos_vazio_retorna_dataframe_vazio(self, caplog):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame()
            client = CnesWebClient(_BILLING_PROJECT)

            import logging
            with caplog.at_level(logging.WARNING, logger="cnes_infra.ingestion.web_client"):
                resultado = client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            assert resultado.is_empty()
            assert "ainda não publicados" in caplog.text

    def test_fetch_profissionais_retorna_dataframe_com_cns(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)
            resultado = client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)

            assert isinstance(resultado, pl.DataFrame)
            assert "cartao_nacional_saude" in resultado.columns

    def test_fetch_profissionais_vazio_loga_warning(self, caplog):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = pd.DataFrame(
                columns=["ano", "mes", "id_municipio", "id_estabelecimento_cnes",
                         "cartao_nacional_saude", "nome", "cbo_2002", "tipo_vinculo",
                         "indicador_atende_sus", "carga_horaria_ambulatorial",
                         "carga_horaria_outros", "carga_horaria_hospitalar"]
            )
            client = CnesWebClient(_BILLING_PROJECT)
            with caplog.at_level("WARNING", logger="cnes_infra.ingestion.web_client"):
                resultado = client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)
        assert resultado.is_empty()
        assert "rows=0" in caplog.text

    def test_fetch_equipes_retorna_dataframe(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_equipes()
            client = CnesWebClient(_BILLING_PROJECT)
            resultado = client.fetch_equipes(_ID_MUNICIPIO, _ANO, _MES)

            assert isinstance(resultado, pl.DataFrame)


class TestErros:

    def test_erro_generico_bigquery_levanta_cnes_web_error(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = Exception("BigQuery internal error")
            client = CnesWebClient(_BILLING_PROJECT)

            with pytest.raises(CnesWebError):
                client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

    def test_erro_auth_levanta_cnes_web_auth_error(self):
        import google.auth.exceptions

        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.side_effect = google.auth.exceptions.DefaultCredentialsError(
                "Could not automatically determine credentials"
            )
            client = CnesWebClient(_BILLING_PROJECT)

            with pytest.raises(CnesWebAuthError):
                client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)


class TestQualidade:

    def test_resultado_e_copia_independente(self):
        df_original = _df_profissionais()
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = df_original
            client = CnesWebClient(_BILLING_PROJECT)
            resultado = client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)

            assert resultado is not df_original

    def test_logging_registra_competencia_e_contagem(self, caplog):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_profissionais()
            client = CnesWebClient(_BILLING_PROJECT)

            with caplog.at_level(logging.INFO, logger="cnes_infra.ingestion.web_client"):
                client.fetch_profissionais(_ID_MUNICIPIO, _ANO, _MES)

            log_texto = caplog.text
            assert str(_ANO) in log_texto
            assert str(_MES) in log_texto

    def test_billing_project_passado_ao_basedosdados(self):
        with patch("cnes_infra.ingestion.web_client.bd.read_sql") as mock_read_sql:
            mock_read_sql.return_value = _df_estabelecimentos()
            client = CnesWebClient(_BILLING_PROJECT)
            client.fetch_estabelecimentos(_ID_MUNICIPIO, _ANO, _MES)

            _, kwargs = mock_read_sql.call_args
            assert kwargs.get("billing_project_id") == _BILLING_PROJECT
