"""Testes do CnesLocalAdapter (raw Parquet -> schema canonico)."""

import polars as pl

from cnes_domain.contracts.columns import (
    SCHEMA_EQUIPE,
    SCHEMA_ESTABELECIMENTO,
    SCHEMA_PROFISSIONAL,
)
from data_processor.adapters.cnes_local_adapter import CnesLocalAdapter


def _raw_profissionais() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF_PROF": [" 12345678901 "],
        "COD_CNS": [" 123456 "],
        "NOME_PROF": ["JO\u00c3O"],
        "NO_SOCIAL": [None],
        "SEXO": ["M"],
        "DATA_NASC": ["1990-01-01"],
        "COD_CBO": ["225125"],
        "IND_VINC": ["1"],
        "TP_SUS_NAO_SUS": ["S"],
        "CARGA_HORARIA_TOTAL": [40],
        "CG_HORAAMB": [20],
        "CGHORAOUTR": [10],
        "CGHORAHOSP": [10],
        "CNES": [" 1234567 "],
        "NOME_FANTA": ["UBS CENTRAL"],
        "TP_UNID_ID": ["1"],
        "CODMUNGEST": ["354130"],
    })


def _raw_estabelecimentos() -> pl.DataFrame:
    return pl.DataFrame({
        "CNES": [" 123 "],
        "NOME_FANTA": ["UBS CENTRAL"],
        "TP_UNID_ID": ["1"],
        "CODMUNGEST": ["354130"],
        "CNPJ_MANT": ["55293427000117"],
    })


def _raw_equipes() -> pl.DataFrame:
    return pl.DataFrame({
        "SEQ_EQUIPE": ["999"],
        "INE": ["0001234567"],
        "DS_AREA": ["EQUIPE ALFA"],
        "TP_EQUIPE": ["70"],
        "COD_MUN": ["354130"],
    })


class TestListarProfissionais:
    def test_renomeia_colunas_raw_para_canonico(self):
        df = CnesLocalAdapter(_raw_profissionais()).listar_profissionais()
        assert "CPF" in df.columns
        assert "CPF_PROF" not in df.columns
        assert "TIPO_VINCULO" in df.columns
        assert "IND_VINC" not in df.columns

    def test_adiciona_fonte_local(self):
        df = CnesLocalAdapter(_raw_profissionais()).listar_profissionais()
        assert "FONTE" in df.columns
        assert df["FONTE"][0] == "LOCAL"

    def test_strip_cpf(self):
        df = CnesLocalAdapter(_raw_profissionais()).listar_profissionais()
        assert df["CPF"][0] == "12345678901"

    def test_strip_cns(self):
        df = CnesLocalAdapter(_raw_profissionais()).listar_profissionais()
        assert df["CNS"][0] == "123456"

    def test_pad_cnes_7_digitos(self):
        raw = _raw_profissionais().with_columns(
            pl.lit("123").alias("CNES"),
        )
        df = CnesLocalAdapter(raw).listar_profissionais()
        assert df["CNES"][0] == "0000123"

    def test_schema_profissional_completo(self):
        df = CnesLocalAdapter(_raw_profissionais()).listar_profissionais()
        assert tuple(df.columns) == SCHEMA_PROFISSIONAL

    def test_nfkd_normaliza_nome(self):
        raw = _raw_profissionais().with_columns(
            pl.lit("JOS\u00c9").alias("NOME_PROF"),
        )
        df = CnesLocalAdapter(raw).listar_profissionais()
        nome = df["NOME_PROFISSIONAL"][0]
        assert "\u00c9" not in nome
        assert "E" in nome or "e" in nome


class TestListarEstabelecimentos:
    def test_renomeia_colunas(self):
        df = CnesLocalAdapter(
            _raw_estabelecimentos(),
        ).listar_estabelecimentos()
        assert "NOME_FANTASIA" in df.columns
        assert "NOME_FANTA" not in df.columns
        assert "TIPO_UNIDADE" in df.columns
        assert "COD_MUNICIPIO" in df.columns

    def test_pad_cnes(self):
        df = CnesLocalAdapter(
            _raw_estabelecimentos(),
        ).listar_estabelecimentos()
        assert df["CNES"][0] == "0000123"

    def test_schema_estabelecimento_completo(self):
        df = CnesLocalAdapter(
            _raw_estabelecimentos(),
        ).listar_estabelecimentos()
        assert tuple(df.columns) == SCHEMA_ESTABELECIMENTO

    def test_fonte_local(self):
        df = CnesLocalAdapter(
            _raw_estabelecimentos(),
        ).listar_estabelecimentos()
        assert df["FONTE"][0] == "LOCAL"


class TestListarEquipes:
    def test_renomeia_colunas(self):
        df = CnesLocalAdapter(_raw_equipes()).listar_equipes()
        assert "INE" in df.columns
        assert "NOME_EQUIPE" in df.columns
        assert "TIPO_EQUIPE" in df.columns
        assert "COD_MUNICIPIO" in df.columns

    def test_cnes_from_seq_equipe(self):
        df = CnesLocalAdapter(_raw_equipes()).listar_equipes()
        assert df["CNES"][0] == "0000999"

    def test_schema_equipe_completo(self):
        df = CnesLocalAdapter(_raw_equipes()).listar_equipes()
        assert tuple(df.columns) == SCHEMA_EQUIPE

    def test_fonte_local(self):
        df = CnesLocalAdapter(_raw_equipes()).listar_equipes()
        assert df["FONTE"][0] == "LOCAL"
