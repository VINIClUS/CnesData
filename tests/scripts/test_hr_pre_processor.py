"""
test_hr_pre_processor.py — Testes do pré-processador de higienização de RH

Comportamentos verificados:
  [carregar_csv_rh]
  - Lê CSV latin-1 e retorna DataFrame com PIS normalizado (str zfill 11)
  - Levanta ValueError quando coluna 'PIS' está ausente
  - Levanta ValueError quando coluna 'Nome' está ausente

  [consultar_pispasep_firebird]
  - Retorna DataFrame com colunas PISPASEP, CPF_PROF, NOME_PROF
  - Exclui registros com PISPASEP vazio

  [crosswalk_pis_cpf]
  - Match primário via PIS → ORIGEM_MATCH='PIS'
  - Match fallback via nome normalizado → ORIGEM_MATCH='NOME'
  - Registro sem match é excluído do output
  - STATUS sempre 'ATIVO' para todos os matches
  - Colunas de saída: CPF, NOME, STATUS, ORIGEM_MATCH
  - Firebird vazio → output vazio (sem crash)
  - PIS com match e nome com match: PIS tem precedência

  [salvar_hr_padronizado]
  - Grava CSV com exatamente [CPF, NOME, STATUS] (sem ORIGEM_MATCH)
  - Cria diretório pai se não existir

Estratégia de mock:
  - Firebird (fdb.Connection) mockado via MagicMock com cursor simulado
  - pd.read_csv mockado para evitar leitura de arquivo real
  - tmp_path para I/O de arquivos de saída
"""

from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from hr_pre_processor import (
    carregar_csv_rh,
    consultar_pispasep_firebird,
    crosswalk_pis_cpf,
    salvar_hr_padronizado,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _cursor_mock(linhas: list, colunas: list) -> MagicMock:
    cur = MagicMock()
    cur.description = [(c,) + (None,) * 6 for c in colunas]
    cur.fetchall.return_value = linhas
    return cur


def _con_mock(linhas: list, colunas: list) -> MagicMock:
    con = MagicMock()
    con.cursor.return_value = _cursor_mock(linhas, colunas)
    return con


def _df_firebird_padrao() -> pd.DataFrame:
    return pd.DataFrame({
        "PISPASEP": ["10604029451", "19000429237"],
        "CPF_PROF":  ["00700515852", "12103817885"],
        "NOME_PROF": ["ANTONIO ROBERTO PEREIRA PACCAS", "JANE REIS DE OLIVEIRA"],
    })


def _df_rh_padrao() -> pd.DataFrame:
    return pd.DataFrame({
        "PIS":  ["10604029451", "19000429237", "00000000000"],
        "Nome": ["ANTONIO ROBERTO PEREIRA PACCAS", "JANE REIS DE OLIVEIRA", "INEXISTENTE"],
    })


# ─────────────────────────────────────────────────────────────────────────────
# carregar_csv_rh
# ─────────────────────────────────────────────────────────────────────────────

class TestCarregarCsvRh:
    def test_retorna_dataframe_com_pis_str_zfill(self, tmp_path):
        # Arrange
        csv = tmp_path / "ponto.csv"
        csv.write_text("Matrícula,Nome,PIS,Departamento,Função\n1,ANA SILVA,9001262573,SAUDE,ACS\n", encoding="latin-1")

        # Act
        df = carregar_csv_rh(csv)

        # Assert
        assert "PIS" in df.columns
        assert df["PIS"].iloc[0] == "09001262573"

    def test_raises_quando_coluna_pis_ausente(self, tmp_path):
        # Arrange
        csv = tmp_path / "ponto.csv"
        csv.write_text("Matrícula,Nome,Departamento\n1,ANA,SAUDE\n", encoding="latin-1")

        # Act & Assert
        with pytest.raises(ValueError, match="PIS"):
            carregar_csv_rh(csv)

    def test_raises_quando_coluna_nome_ausente(self, tmp_path):
        # Arrange
        csv = tmp_path / "ponto.csv"
        csv.write_text("Matrícula,PIS,Departamento\n1,10604029451,SAUDE\n", encoding="latin-1")

        # Act & Assert
        with pytest.raises(ValueError, match="Nome"):
            carregar_csv_rh(csv)

    def test_pis_sempre_11_digitos(self, tmp_path):
        # Arrange — PIS curto (9 dígitos) deve ganhar zfill
        csv = tmp_path / "ponto.csv"
        csv.write_text("Matrícula,Nome,PIS,Departamento\n1,JOAO,123456789,SAUDE\n", encoding="latin-1")

        # Act
        df = carregar_csv_rh(csv)

        # Assert
        assert df["PIS"].iloc[0] == "00123456789"
        assert len(df["PIS"].iloc[0]) == 11


# ─────────────────────────────────────────────────────────────────────────────
# consultar_pispasep_firebird
# ─────────────────────────────────────────────────────────────────────────────

class TestConsultarPispasepFirebird:
    _COLUNAS = ["PISPASEP", "CPF_PROF", "NOME_PROF"]

    def test_retorna_dataframe_com_tres_colunas(self):
        # Arrange
        con = _con_mock(
            [("10604029451", "00700515852", "ANTONIO ROBERTO")],
            self._COLUNAS,
        )

        # Act
        df = consultar_pispasep_firebird(con)

        # Assert
        assert list(df.columns) == self._COLUNAS
        assert len(df) == 1

    def test_exclui_pispasep_vazio(self):
        # Arrange — 2 registros: 1 com PISPASEP vazio, 1 válido
        con = _con_mock(
            [("", "00014808129", "MONICA CRISTINA"), ("10604029451", "00700515852", "ANTONIO")],
            self._COLUNAS,
        )

        # Act
        df = consultar_pispasep_firebird(con)

        # Assert
        assert len(df) == 1
        assert df["PISPASEP"].iloc[0] == "10604029451"

    def test_cursor_fechado_apos_uso(self):
        # Arrange
        con = _con_mock([], self._COLUNAS)

        # Act
        consultar_pispasep_firebird(con)

        # Assert
        con.cursor.return_value.close.assert_called_once()


# ─────────────────────────────────────────────────────────────────────────────
# crosswalk_pis_cpf
# ─────────────────────────────────────────────────────────────────────────────

class TestCrosswalkPisCpf:
    def test_match_via_pis_tem_origem_pis(self):
        # Arrange
        df_rh = pd.DataFrame({"PIS": ["10604029451"], "Nome": ["ANTONIO ROBERTO PEREIRA PACCAS"]})
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert len(df) == 1
        assert df["ORIGEM_MATCH"].iloc[0] == "PIS"
        assert df["CPF"].iloc[0] == "00700515852"

    def test_match_fallback_via_nome_tem_origem_nome(self):
        # Arrange — PIS não existe no Firebird, mas nome sim
        df_rh = pd.DataFrame({"PIS": ["99999999999"], "Nome": ["JANE REIS DE OLIVEIRA"]})
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert len(df) == 1
        assert df["ORIGEM_MATCH"].iloc[0] == "NOME"
        assert df["CPF"].iloc[0] == "12103817885"

    def test_sem_match_excluido_do_output(self):
        # Arrange — PIS e nome sem correspondência no Firebird
        df_rh = pd.DataFrame({"PIS": ["00000000000"], "Nome": ["INEXISTENTE SILVA"]})
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert df.empty

    def test_status_sempre_ativo(self):
        # Arrange
        df_rh = pd.DataFrame({"PIS": ["10604029451"], "Nome": ["ANTONIO ROBERTO PEREIRA PACCAS"]})
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert (df["STATUS"] == "ATIVO").all()

    def test_colunas_de_saida_corretas(self):
        # Arrange
        df_rh = _df_rh_padrao()
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert set(df.columns) == {"CPF", "NOME", "STATUS", "ORIGEM_MATCH"}

    def test_firebird_vazio_retorna_dataframe_vazio(self):
        # Arrange
        df_rh = _df_rh_padrao()
        df_fb = pd.DataFrame(columns=["PISPASEP", "CPF_PROF", "NOME_PROF"])

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert
        assert df.empty

    def test_pis_tem_precedencia_sobre_nome(self):
        # Arrange — mesmo registro tem tanto PIS quanto nome no Firebird
        df_rh = pd.DataFrame({"PIS": ["10604029451"], "Nome": ["ANTONIO ROBERTO PEREIRA PACCAS"]})
        df_fb = _df_firebird_padrao()

        # Act
        df = crosswalk_pis_cpf(df_rh, df_fb)

        # Assert — deve ser 1 linha, não duplicado
        assert len(df) == 1
        assert df["ORIGEM_MATCH"].iloc[0] == "PIS"


# ─────────────────────────────────────────────────────────────────────────────
# salvar_hr_padronizado
# ─────────────────────────────────────────────────────────────────────────────

class TestSalvarHrPadronizado:
    def test_grava_csv_com_colunas_cpf_nome_status(self, tmp_path):
        # Arrange
        df = pd.DataFrame({
            "CPF": ["00700515852"],
            "NOME": ["ANTONIO ROBERTO PEREIRA PACCAS"],
            "STATUS": ["ATIVO"],
            "ORIGEM_MATCH": ["PIS"],
        })
        destino = tmp_path / "hr_padronizado.csv"

        # Act
        salvar_hr_padronizado(df, destino)

        # Assert
        resultado = pd.read_csv(destino, sep=";", dtype=str)
        assert list(resultado.columns) == ["CPF", "NOME", "STATUS"]

    def test_nao_inclui_coluna_origem_match(self, tmp_path):
        # Arrange
        df = pd.DataFrame({
            "CPF": ["00700515852"],
            "NOME": ["ANTONIO ROBERTO PEREIRA PACCAS"],
            "STATUS": ["ATIVO"],
            "ORIGEM_MATCH": ["PIS"],
        })
        destino = tmp_path / "hr_padronizado.csv"

        # Act
        salvar_hr_padronizado(df, destino)

        # Assert
        resultado = pd.read_csv(destino, sep=";", dtype=str)
        assert "ORIGEM_MATCH" not in resultado.columns

    def test_campos_entre_aspas_no_csv_gerado(self, tmp_path):
        # Arrange — NOME com potencial de fórmula CSV injection
        df = pd.DataFrame({
            "CPF": ["00700515852"],
            "NOME": ["=HYPERLINK(\"http://evil.com\",\"click\")"],
            "STATUS": ["ATIVO"],
            "ORIGEM_MATCH": ["PIS"],
        })
        destino = tmp_path / "hr_padronizado.csv"

        # Act
        salvar_hr_padronizado(df, destino)

        # Assert — todos os campos devem estar entre aspas duplas
        conteudo = destino.read_text(encoding="utf-8-sig")
        primeira_linha_dados = conteudo.splitlines()[1]
        assert primeira_linha_dados.startswith('"'), "campos não estão entre aspas (QUOTE_ALL ausente)"

    def test_cria_diretorio_pai_se_inexistente(self, tmp_path):
        # Arrange
        df = pd.DataFrame({"CPF": ["123"], "NOME": ["X"], "STATUS": ["ATIVO"], "ORIGEM_MATCH": ["PIS"]})
        destino = tmp_path / "subdir" / "hr_padronizado.csv"

        # Act
        salvar_hr_padronizado(df, destino)

        # Assert
        assert destino.exists()
