"""Testes de branches não cobertas no transformer."""

import polars as pl

from cnes_domain.processing.transformer import transformar


class TestTransformarSemColunasTexto:

    def test_df_sem_colunas_texto_nao_falha(self):
        df = pl.DataFrame({
            "CH_TOTAL": [40],
            "NUM": [1],
        })
        resultado = transformar(df)
        assert resultado.height == 1
        assert "ALERTA_STATUS_CH" in resultado.columns

    def test_df_so_numericos_alerta_ch_calculado(self):
        df = pl.DataFrame({"CH_TOTAL": [0], "X": [99]})
        resultado = transformar(df)
        assert resultado["ALERTA_STATUS_CH"][0] == "ATIVO_SEM_CH"


class TestTransformarSemColunasCPF:

    def test_df_sem_cpf_nao_aplica_rq002(self):
        df = pl.DataFrame({
            "CNES": ["0985333"],
            "CH_TOTAL": [20],
        })
        resultado = transformar(df)
        assert resultado.height == 1

    def test_df_sem_cpf_nao_adiciona_coluna_cpf(self):
        df = pl.DataFrame({"CH_TOTAL": [10], "CNES": ["123"]})
        resultado = transformar(df)
        assert "CPF" not in resultado.columns


class TestTransformarSemColunasEquipe:

    def test_df_sem_colunas_equipe_nao_falha(self):
        df = pl.DataFrame({
            "CPF": ["11716723817"],
            "CH_TOTAL": [30],
        })
        resultado = transformar(df)
        assert resultado.height == 1
        assert "INE" not in resultado.columns

    def test_df_com_equipe_mas_sem_ine_nao_cria_ine(self):
        df = pl.DataFrame({
            "CPF": ["11716723817"],
            "CH_TOTAL": [30],
            "NOME_EQUIPE": [None],
        })
        resultado = transformar(df)
        assert "INE" not in resultado.columns
        assert resultado["NOME_EQUIPE"][0] == "SEM EQUIPE VINCULADA"
