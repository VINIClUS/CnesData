"""test_cnes_local_adapter.py — Testes do adapter Firebird → schema padronizado."""

import unicodedata
from unittest.mock import MagicMock, patch

import pandas as pd

from ingestion.schemas import SCHEMA_PROFISSIONAL, SCHEMA_ESTABELECIMENTO, SCHEMA_EQUIPE
from ingestion.cnes_local_adapter import CnesLocalAdapter


_DF_FIREBIRD = pd.DataFrame({
    "CPF":                ["11716723817 "],
    "CNS":                [" 702002887429583"],
    "NOME_PROFISSIONAL":  ["ZELIA RIBEIRO"],
    "NOME_SOCIAL":        [None],
    "SEXO":               ["F"],
    "DATA_NASCIMENTO":    ["1975-04-12"],
    "CBO":                ["514225"],
    "COD_VINCULO":        ["010101"],
    "SUS_NAO_SUS":        ["S"],
    "CARGA_HORARIA_TOTAL": [40],
    "CH_AMBULATORIAL":    [40],
    "CH_OUTRAS":          [0],
    "CH_HOSPITALAR":      [0],
    "COD_CNES":           [" 0985333"],
    "ESTABELECIMENTO":    ["ESF VILA GERONIMO"],
    "COD_TIPO_UNIDADE":   ["02"],
    "COD_MUN_GESTOR":     ["354130"],
    "COD_INE_EQUIPE":     ["0001365993"],
    "NOME_EQUIPE":        ["ESF VILA GERONIMO"],
    "COD_TIPO_EQUIPE":    ["70"],
})


def _adapter_com_mock(df=None) -> tuple[CnesLocalAdapter, MagicMock]:
    df_retorno = df if df is not None else _DF_FIREBIRD
    con_mock = MagicMock()
    with patch("ingestion.cnes_local_adapter.cnes_client.extrair_profissionais", return_value=df_retorno):
        adapter = CnesLocalAdapter(con_mock)
        adapter._cache = df_retorno
    return adapter, con_mock


class TestListarProfissionais:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert list(resultado.columns) == list(SCHEMA_PROFISSIONAL)

    def test_renomeia_cod_cnes_para_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert "CNES" in resultado.columns
        assert "COD_CNES" not in resultado.columns

    def test_renomeia_cod_vinculo_para_tipo_vinculo(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert "TIPO_VINCULO" in resultado.columns
        assert "COD_VINCULO" not in resultado.columns

    def test_renomeia_carga_horaria_total_para_ch_total(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert "CH_TOTAL" in resultado.columns
        assert "CARGA_HORARIA_TOTAL" not in resultado.columns

    def test_renomeia_sus_nao_sus_para_sus(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert "SUS" in resultado.columns
        assert "SUS_NAO_SUS" not in resultado.columns

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_strip_cns(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CNS"].iloc[0] == "702002887429583"

    def test_strip_cpf(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CPF"].iloc[0] == "11716723817"

    def test_strip_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_nao_muta_dataframe_original(self):
        adapter, _ = _adapter_com_mock()
        original_cols = list(_DF_FIREBIRD.columns)
        adapter.listar_profissionais()
        assert list(_DF_FIREBIRD.columns) == original_cols


class TestListarEstabelecimentos:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert list(resultado.columns) == list(SCHEMA_ESTABELECIMENTO)

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_strip_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_deduplica_por_cnes(self):
        df_dup = pd.concat([_DF_FIREBIRD, _DF_FIREBIRD], ignore_index=True)
        adapter, _ = _adapter_com_mock(df=df_dup)
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"].nunique() == len(resultado)

    def test_cnpj_mantenedora_none_quando_indisponivel(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNPJ_MANTENEDORA"].iloc[0] is None


class TestListarEquipes:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert list(resultado.columns) == list(SCHEMA_EQUIPE)

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_exclui_linhas_sem_ine(self):
        df_sem_ine = _DF_FIREBIRD.copy()
        df_sem_ine["COD_INE_EQUIPE"] = None
        adapter, _ = _adapter_com_mock(df=df_sem_ine)
        resultado = adapter.listar_equipes()
        assert resultado.empty

    def test_deduplica_por_ine(self):
        df_dup = pd.concat([_DF_FIREBIRD, _DF_FIREBIRD], ignore_index=True)
        adapter, _ = _adapter_com_mock(df=df_dup)
        resultado = adapter.listar_equipes()
        assert resultado["INE"].nunique() == len(resultado)


class TestZeroPaddingCNES:

    def test_cnes_6_digitos_recebe_zero_a_esquerda_profissionais(self):
        df = _DF_FIREBIRD.copy()
        df["COD_CNES"] = ["985333"]
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_cnes_7_digitos_nao_muda_profissionais(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_cnes_6_digitos_recebe_zero_a_esquerda_estabelecimentos(self):
        df = _DF_FIREBIRD.copy()
        df["COD_CNES"] = ["985333"]
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"].iloc[0] == "0985333"

    def test_cnes_zfill_em_equipes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert not resultado.empty
        assert (resultado["CNES"].str.len() == 7).all()


class TestNFKDNormalizacao:

    def test_listar_profissionais_normaliza_nome_profissional_para_nfkd(self):
        df = _DF_FIREBIRD.copy()
        df["NOME_PROFISSIONAL"] = ["Atenção Básica"]
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_PROFISSIONAL"].iloc[0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_nao_altera_nome_sem_acento(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_PROFISSIONAL"].iloc[0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_normaliza_nome_social_para_nfkd(self):
        df = _DF_FIREBIRD.copy()
        df["NOME_SOCIAL"] = ["Atenção"]
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_SOCIAL"].iloc[0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_nome_social_none_permanece_nan(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert pd.isna(resultado["NOME_SOCIAL"].iloc[0])

    def test_listar_estabelecimentos_normaliza_nome_fantasia_para_nfkd(self):
        df = _DF_FIREBIRD.copy()
        df["ESTABELECIMENTO"] = ["Unidade Básica de Saúde"]
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_estabelecimentos()
        val = resultado["NOME_FANTASIA"].iloc[0]
        assert unicodedata.is_normalized("NFKD", val)


class TestCacheInterno:

    def test_extrair_profissionais_chamado_uma_vez_para_multiplas_chamadas(self):
        con_mock = MagicMock()
        with patch(
            "ingestion.cnes_local_adapter.cnes_client.extrair_profissionais",
            return_value=_DF_FIREBIRD,
        ) as mock_extrair:
            adapter = CnesLocalAdapter(con_mock)
            adapter.listar_profissionais()
            adapter.listar_estabelecimentos()
            adapter.listar_equipes()
            mock_extrair.assert_called_once()
