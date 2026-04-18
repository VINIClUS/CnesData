"""Testes do CnesNacionalAdapter."""
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl
import pytest

from cnes_domain.contracts.columns import (
    SCHEMA_ESTABELECIMENTO,
    SCHEMA_PROFISSIONAL,
)
from data_processor.adapters.cnes_nacional_adapter import CnesNacionalAdapter


def _make_estab_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id_estabelecimento_cnes": ["1234567"],
        "cnpj_mantenedora": ["55293427000117"],
        "id_natureza_juridica": ["1023"],
        "tipo_unidade": ["01"],
        "indicador_vinculo_sus": [1],
        "id_municipio_6": ["354130"],
    })


def _make_prof_df() -> pl.DataFrame:
    return pl.DataFrame({
        "id_estabelecimento_cnes": ["1234567"],
        "cartao_nacional_saude": ["123456789"],
        "nome": ["JOAO"],
        "cbo_2002": ["225125"],
        "tipo_vinculo": ["1"],
        "indicador_atende_sus": [1],
        "carga_horaria_ambulatorial": [20],
        "carga_horaria_outros": [10],
        "carga_horaria_hospitalar": [10],
    })


class TestCnesNacionalAdapterEstabelecimentos:
    def test_rejeita_competencia_none(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        with pytest.raises(ValueError, match="competencia=obrigatoria"):
            adapter.listar_estabelecimentos(competencia=None)

    def test_retorna_schema_correto(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = _make_estab_df()
        adapter._client = mock_client

        df = adapter.listar_estabelecimentos(competencia=(2026, 3))

        assert set(SCHEMA_ESTABELECIMENTO).issubset(set(df.columns))
        assert df["FONTE"][0] == "NACIONAL"

    def test_transforma_vinculo_sus_1_em_s(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = _make_estab_df()
        adapter._client = mock_client

        df = adapter.listar_estabelecimentos(competencia=(2026, 3))
        assert df["VINCULO_SUS"][0] == "S"

    def test_transforma_vinculo_sus_0_em_n(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        raw = _make_estab_df().with_columns(
            pl.lit(0).alias("indicador_vinculo_sus"),
        )
        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = raw
        adapter._client = mock_client

        df = adapter.listar_estabelecimentos(competencia=(2026, 3))
        assert df["VINCULO_SUS"][0] == "N"

    def test_usa_cache_quando_disponivel(self, tmp_path):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
            cache_dir=tmp_path,
        )
        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = _make_estab_df()
        adapter._client = mock_client

        df1 = adapter.listar_estabelecimentos(competencia=(2026, 3))
        df2 = adapter.listar_estabelecimentos(competencia=(2026, 3))

        mock_client.fetch_estabelecimentos.assert_called_once()
        assert len(df2) == len(df1)

    def test_ignora_cache_expirado(self, tmp_path):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
            cache_dir=tmp_path,
            ttl_cache_segundos=0,
        )
        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = _make_estab_df()
        adapter._client = mock_client

        adapter.listar_estabelecimentos(competencia=(2026, 3))
        adapter.listar_estabelecimentos(competencia=(2026, 3))

        assert mock_client.fetch_estabelecimentos.call_count == 2

    def test_remove_cache_corrompido(self, tmp_path):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
            cache_dir=tmp_path,
            ttl_cache_segundos=3600,
        )
        chave = "estab_354130_2026_03"
        cache_path = tmp_path / f"{chave}.pkl"
        cache_path.write_bytes(b"not_valid_pickle")

        mock_client = MagicMock()
        mock_client.fetch_estabelecimentos.return_value = _make_estab_df()
        adapter._client = mock_client

        df = adapter.listar_estabelecimentos(competencia=(2026, 3))
        assert len(df) == 1
        mock_client.fetch_estabelecimentos.assert_called_once()


class TestCnesNacionalAdapterProfissionais:
    def test_rejeita_competencia_none(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        with pytest.raises(ValueError, match="competencia=obrigatoria"):
            adapter.listar_profissionais(competencia=None)

    def test_retorna_schema_correto(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        mock_client = MagicMock()
        mock_client.fetch_profissionais.return_value = _make_prof_df()
        adapter._client = mock_client

        df = adapter.listar_profissionais(competencia=(2026, 3))

        assert set(SCHEMA_PROFISSIONAL).issubset(set(df.columns))
        assert df["FONTE"][0] == "NACIONAL"

    def test_calcula_ch_total(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        mock_client = MagicMock()
        mock_client.fetch_profissionais.return_value = _make_prof_df()
        adapter._client = mock_client

        df = adapter.listar_profissionais(competencia=(2026, 3))
        assert df["CH_TOTAL"][0] == 40

    def test_transforma_sus_1_em_s(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        mock_client = MagicMock()
        mock_client.fetch_profissionais.return_value = _make_prof_df()
        adapter._client = mock_client

        df = adapter.listar_profissionais(competencia=(2026, 3))
        assert df["SUS"][0] == "S"

    def test_transforma_sus_0_em_n(self):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
        )
        raw = _make_prof_df().with_columns(
            pl.lit(0).alias("indicador_atende_sus"),
        )
        mock_client = MagicMock()
        mock_client.fetch_profissionais.return_value = raw
        adapter._client = mock_client

        df = adapter.listar_profissionais(competencia=(2026, 3))
        assert df["SUS"][0] == "N"

    def test_sem_cache_dir_nao_cria_arquivo(self, tmp_path):
        adapter = CnesNacionalAdapter(
            billing_project_id="proj",
            id_municipio="354130",
            cache_dir=None,
        )
        mock_client = MagicMock()
        mock_client.fetch_profissionais.return_value = _make_prof_df()
        adapter._client = mock_client

        adapter.listar_profissionais(competencia=(2026, 3))
        assert not list(tmp_path.iterdir())


class TestSihdLocalAdapter:
    def _make_raw_aihs(self) -> "pl.DataFrame":
        return pl.DataFrame({
            "AH_NUM_AIH": ["12345"],
            "AH_CNES": ["1234567"],
            "AH_CMPT": ["202603"],
            "AH_PACIENTE_NOME": ["JOAO"],
            "AH_PACIENTE_NUMERO_CNS": ["123456789"],
            "AH_PACIENTE_SEXO": ["M"],
            "AH_PACIENTE_DT_NASCIMENTO": ["1980-01-01"],
            "AH_PACIENTE_MUN_ORIGEM": ["354130"],
            "AH_DIAG_PRI": ["J18"],
            "AH_DIAG_SEC": [None],
            "AH_PROC_SOLICITADO": ["0303010010"],
            "AH_PROC_REALIZADO": ["0303010010"],
            "AH_DT_INTERNACAO": ["2026-03-01"],
            "AH_DT_SAIDA": ["2026-03-10"],
            "AH_MOT_SAIDA": ["11"],
            "AH_CAR_INTERNACAO": ["01"],
            "AH_ESPECIALIDADE": ["45"],
            "AH_SITUACAO": ["0"],
            "AH_MED_SOL_DOC": ["99999999999"],
            "AH_MED_RESP_DOC": ["88888888888"],
        })

    def test_renomeia_colunas_raw_para_canonico(self):
        from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
        from cnes_domain.contracts.sihd_columns import SCHEMA_AIH
        df = SihdLocalAdapter(self._make_raw_aihs()).listar_aihs()
        assert "NUM_AIH" in df.columns
        assert "AH_NUM_AIH" not in df.columns
        assert set(SCHEMA_AIH).issubset(set(df.columns))

    def test_pad_cnes_7_digitos(self):
        from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
        df = SihdLocalAdapter(self._make_raw_aihs()).listar_aihs()
        assert df["CNES"][0] == "1234567"

    def test_strip_num_aih(self):
        from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
        raw = self._make_raw_aihs().with_columns(
            pl.lit("  12345  ").alias("AH_NUM_AIH"),
        )
        df = SihdLocalAdapter(raw).listar_aihs()
        assert df["NUM_AIH"][0] == "12345"

    def test_fonte_local(self):
        from data_processor.adapters.sihd_local_adapter import SihdLocalAdapter
        df = SihdLocalAdapter(self._make_raw_aihs()).listar_aihs()
        assert df["FONTE"][0] == "LOCAL"
