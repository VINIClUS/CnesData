"""test_cnes_local_adapter.py -- Testes do adapter Firebird -> schema padronizado."""

import unicodedata
from unittest.mock import MagicMock, patch

import polars as pl
from cnes_domain.contracts.columns import SCHEMA_EQUIPE, SCHEMA_ESTABELECIMENTO, SCHEMA_PROFISSIONAL
from cnes_infra.ingestion.cnes_local_adapter import CnesLocalAdapter

_DF_FIREBIRD = pl.DataFrame({
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
    with patch(
        "cnes_infra.ingestion.cnes_local_adapter"
        ".cnes_client.extrair_profissionais",
        return_value=df_retorno,
    ):
        adapter = CnesLocalAdapter(con_mock)
        adapter._cache = df_retorno
    return adapter, con_mock


class TestListarProfissionais:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado.columns == list(SCHEMA_PROFISSIONAL)

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
        assert resultado["CNS"][0] == "702002887429583"

    def test_strip_cpf(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CPF"][0] == "11716723817"

    def test_strip_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"][0] == "0985333"

    def test_nao_muta_dataframe_original(self):
        adapter, _ = _adapter_com_mock()
        original_cols = list(_DF_FIREBIRD.columns)
        adapter.listar_profissionais()
        assert list(_DF_FIREBIRD.columns) == original_cols


class TestListarEstabelecimentos:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert resultado.columns == list(SCHEMA_ESTABELECIMENTO)

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_strip_cnes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"][0] == "0985333"

    def test_deduplica_por_cnes(self):
        df_dup = pl.concat([_DF_FIREBIRD, _DF_FIREBIRD])
        adapter, _ = _adapter_com_mock(df=df_dup)
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"].n_unique() == len(resultado)

    def test_cnpj_mantenedora_none_quando_indisponivel(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNPJ_MANTENEDORA"][0] is None


class TestListarEquipes:

    def test_retorna_colunas_do_schema(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert resultado.columns == list(SCHEMA_EQUIPE)

    def test_adiciona_fonte_local(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert (resultado["FONTE"] == "LOCAL").all()

    def test_exclui_linhas_sem_ine(self):
        df_sem_ine = _DF_FIREBIRD.clone()
        df_sem_ine = df_sem_ine.with_columns(pl.lit(None).alias("COD_INE_EQUIPE"))
        adapter, _ = _adapter_com_mock(df=df_sem_ine)
        resultado = adapter.listar_equipes()
        assert resultado.is_empty()

    def test_deduplica_por_ine(self):
        df_dup = pl.concat([_DF_FIREBIRD, _DF_FIREBIRD])
        adapter, _ = _adapter_com_mock(df=df_dup)
        resultado = adapter.listar_equipes()
        assert resultado["INE"].n_unique() == len(resultado)


class TestZeroPaddingCNES:

    def test_cnes_6_digitos_recebe_zero_a_esquerda_profissionais(self):
        df = _DF_FIREBIRD.clone()
        df = df.with_columns(pl.lit("985333").alias("COD_CNES"))
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"][0] == "0985333"

    def test_cnes_7_digitos_nao_muda_profissionais(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["CNES"][0] == "0985333"

    def test_cnes_6_digitos_recebe_zero_a_esquerda_estabelecimentos(self):
        df = _DF_FIREBIRD.clone()
        df = df.with_columns(pl.lit("985333").alias("COD_CNES"))
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_estabelecimentos()
        assert resultado["CNES"][0] == "0985333"

    def test_cnes_zfill_em_equipes(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_equipes()
        assert not resultado.is_empty()
        assert (resultado["CNES"].str.len_chars() == 7).all()


class TestNFKDNormalizacao:

    def test_listar_profissionais_normaliza_nome_profissional_para_nfkd(self):
        df = _DF_FIREBIRD.clone()
        df = df.with_columns(pl.lit("Atenção Básica").alias("NOME_PROFISSIONAL"))
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_PROFISSIONAL"][0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_nao_altera_nome_sem_acento(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_PROFISSIONAL"][0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_normaliza_nome_social_para_nfkd(self):
        df = _DF_FIREBIRD.clone()
        df = df.with_columns(pl.lit("Atenção").alias("NOME_SOCIAL"))
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_profissionais()
        val = resultado["NOME_SOCIAL"][0]
        assert unicodedata.is_normalized("NFKD", val)

    def test_listar_profissionais_nome_social_none_permanece_nan(self):
        adapter, _ = _adapter_com_mock()
        resultado = adapter.listar_profissionais()
        assert resultado["NOME_SOCIAL"][0] is None

    def test_listar_estabelecimentos_normaliza_nome_fantasia_para_nfkd(self):
        df = _DF_FIREBIRD.clone()
        df = df.with_columns(pl.lit("Unidade Básica de Saúde").alias("ESTABELECIMENTO"))
        adapter, _ = _adapter_com_mock(df=df)
        resultado = adapter.listar_estabelecimentos()
        val = resultado["NOME_FANTASIA"][0]
        assert unicodedata.is_normalized("NFKD", val)


class TestCacheInterno:

    def test_extrair_profissionais_chamado_uma_vez_para_multiplas_chamadas(self):
        con_mock = MagicMock()
        with patch(
            "cnes_infra.ingestion.cnes_local_adapter.cnes_client.extrair_profissionais",
            return_value=_DF_FIREBIRD,
        ) as mock_extrair:
            adapter = CnesLocalAdapter(con_mock)
            adapter.listar_profissionais()
            adapter.listar_estabelecimentos()
            adapter.listar_equipes()
            mock_extrair.assert_called_once()
