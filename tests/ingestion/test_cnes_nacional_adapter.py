"""test_cnes_nacional_adapter.py — Testes do adapter BigQuery → schema padronizado."""

from unittest.mock import MagicMock

import pandas as pd
import pytest

from ingestion.schemas import SCHEMA_PROFISSIONAL, SCHEMA_ESTABELECIMENTO
from ingestion.cnes_nacional_adapter import CnesNacionalAdapter


_COMPETENCIA = (2024, 12)

_DF_ESTAB_BQ = pd.DataFrame({
    "ano":                    [2024],
    "mes":                    [12],
    "id_municipio":           ["3541307"],
    "id_municipio_6":         ["354130"],
    "id_estabelecimento_cnes": ["0985333"],
    "cnpj_mantenedora":       ["55293427000117"],
    "id_natureza_juridica":   ["1147"],
    "tipo_unidade":           ["02"],
    "tipo_gestao":            ["M"],
    "indicador_vinculo_sus":  [1],
})

_DF_PROF_BQ = pd.DataFrame({
    "ano":                    [2024],
    "mes":                    [12],
    "id_municipio":           ["3541307"],
    "id_estabelecimento_cnes": ["0985333"],
    "cartao_nacional_saude":  ["702002887429583"],
    "nome":                   ["PROFISSIONAL TESTE"],
    "cbo_2002":               ["514225"],
    "tipo_vinculo":           ["010101"],
    "indicador_atende_sus":   [1],
    "carga_horaria_ambulatorial": [40],
    "carga_horaria_outros":   [0],
    "carga_horaria_hospitalar": [0],
})


def _adapter_com_mock(df_estab=None, df_prof=None) -> CnesNacionalAdapter:
    adapter = CnesNacionalAdapter("test-project", "3541307")
    adapter._client = MagicMock()
    adapter._client.fetch_estabelecimentos.return_value = (
        df_estab if df_estab is not None else _DF_ESTAB_BQ
    ).copy()
    adapter._client.fetch_profissionais.return_value = (
        df_prof if df_prof is not None else _DF_PROF_BQ
    ).copy()
    return adapter


class TestListarEstabelecimentos:

    def test_retorna_colunas_do_schema(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert list(resultado.columns) == list(SCHEMA_ESTABELECIMENTO)

    def test_adiciona_fonte_nacional(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert (resultado["FONTE"] == "NACIONAL").all()

    def test_cod_municipio_seis_digitos(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["COD_MUNICIPIO"].iloc[0] == "354130"

    def test_renomeia_id_estabelecimento_para_cnes(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_renomeia_cnpj_mantenedora_para_cnpj_mantenedora(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["CNPJ_MANTENEDORA"].iloc[0] == "55293427000117"

    def test_nome_fantasia_e_none(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["NOME_FANTASIA"].iloc[0] is None

    def test_normaliza_indicador_vinculo_sus_1_para_s(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["VINCULO_SUS"].iloc[0] == "S"

    def test_normaliza_indicador_vinculo_sus_zero_para_n(self):
        df_estab = _DF_ESTAB_BQ.copy()
        df_estab["indicador_vinculo_sus"] = [0]
        adapter = _adapter_com_mock(df_estab=df_estab)
        resultado = adapter.listar_estabelecimentos(_COMPETENCIA)
        assert resultado["VINCULO_SUS"].iloc[0] == "N"

    def test_passa_competencia_correta_ao_cliente(self):
        adapter = _adapter_com_mock()
        adapter.listar_estabelecimentos(_COMPETENCIA)
        adapter._client.fetch_estabelecimentos.assert_called_once_with("3541307", 2024, 12)

    def test_levanta_value_error_sem_competencia(self):
        adapter = _adapter_com_mock()
        with pytest.raises(ValueError, match="competencia"):
            adapter.listar_estabelecimentos(None)


class TestListarProfissionais:

    def test_retorna_colunas_do_schema(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert list(resultado.columns) == list(SCHEMA_PROFISSIONAL)

    def test_adiciona_fonte_nacional(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert (resultado["FONTE"] == "NACIONAL").all()

    def test_renomeia_cartao_nacional_saude_para_cns(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["CNS"].iloc[0] == "702002887429583"

    def test_renomeia_cbo_2002_para_cbo(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["CBO"].iloc[0] == "514225"

    def test_calcula_ch_total(self):
        df_prof = _DF_PROF_BQ.copy()
        df_prof["carga_horaria_ambulatorial"] = [20]
        df_prof["carga_horaria_outros"] = [10]
        df_prof["carga_horaria_hospitalar"] = [10]
        adapter = _adapter_com_mock(df_prof=df_prof)
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["CH_TOTAL"].iloc[0] == 40

    def test_normaliza_indicador_atende_sus_1_para_s(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["SUS"].iloc[0] == "S"

    def test_normaliza_indicador_atende_sus_zero_para_n(self):
        df_prof = _DF_PROF_BQ.copy()
        df_prof["indicador_atende_sus"] = [0]
        adapter = _adapter_com_mock(df_prof=df_prof)
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["SUS"].iloc[0] == "N"

    def test_cpf_e_none(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["CPF"].iloc[0] is None

    def test_nome_profissional_vem_da_coluna_nome(self):
        adapter = _adapter_com_mock()
        resultado = adapter.listar_profissionais(_COMPETENCIA)
        assert resultado["NOME_PROFISSIONAL"].iloc[0] == "PROFISSIONAL TESTE"

    def test_levanta_value_error_sem_competencia(self):
        adapter = _adapter_com_mock()
        with pytest.raises(ValueError, match="competencia"):
            adapter.listar_profissionais(None)

    def test_passa_competencia_correta_ao_cliente(self):
        adapter = _adapter_com_mock()
        adapter.listar_profissionais(_COMPETENCIA)
        adapter._client.fetch_profissionais.assert_called_once_with("3541307", 2024, 12)
