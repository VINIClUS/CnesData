"""test_hr_client.py -- Testes Unitarios do Parser de RH (WP-001)."""

import logging
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest
from cnes_infra.ingestion.hr_client import (
    HrSchemaError,
    carregar_folha,
    carregar_ponto,
)

_XLSX = Path("folha.xlsx")
_CSV = Path("folha.csv")
_DESCONHECIDO = Path("folha.txt")


def _df_folha_valido() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF":    ["117.167.238-17", "227.307.688-66"],
        "NOME":   ["ZELIA RIBEIRO", "VANESSA PAIXAO"],
        "STATUS": ["ATIVO", "ATIVO"],
    })


def _df_ponto_valido() -> pl.DataFrame:
    return pl.DataFrame({
        "CPF":    ["117.167.238-17"],
        "NOME":   ["ZELIA RIBEIRO"],
        "STATUS": ["PRESENTE"],
    })


class TestLeituraDeArquivo:

    def test_xlsx_chama_read_excel(self):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel") as mock_read:
            mock_read.return_value = _df_folha_valido()
            carregar_folha(_XLSX)
            mock_read.assert_called_once_with(_XLSX)

    pass


def test_csv_usa_linhas_limpas(tmp_path):
    from cnes_infra.ingestion.hr_client import COLUNAS_OBRIGATORIAS_FOLHA
    csv_path = tmp_path / "folha.csv"
    colunas = sorted(COLUNAS_OBRIGATORIAS_FOLHA) + ["DATA_ADMISSAO", "DATA_DEMISSAO"]
    header = ",".join(colunas)
    vals = {
        c: "12345678901" if "CPF" in c
        else ("NOME TESTE" if "NOME" in c
              else ("ATIVO" if "STATUS" in c
                    else "2024-01-01"))
        for c in colunas
    }
    row = ",".join(vals.values())
    csv_path.write_text(f"{header}\n{row}", encoding="utf-8")
    df = carregar_folha(csv_path)
    assert not df.is_empty()

    def test_extensao_desconhecida_levanta_hr_schema_error(self):
        with pytest.raises(HrSchemaError, match="extensão"):
            carregar_folha(_DESCONHECIDO)

    def test_carregar_ponto_xlsx_chama_read_excel(self):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel") as mock_read:
            mock_read.return_value = _df_ponto_valido()
            carregar_ponto(_XLSX)
            mock_read.assert_called_once_with(_XLSX)


class TestValidacaoDeSchema:

    def test_rejeita_folha_sem_cpf(self):
        df = _df_folha_valido().drop("CPF")
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with pytest.raises(HrSchemaError, match="CPF"):
                carregar_folha(_XLSX)

    def test_rejeita_folha_sem_nome(self):
        df = _df_folha_valido().drop("NOME")
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with pytest.raises(HrSchemaError, match="NOME"):
                carregar_folha(_XLSX)

    def test_rejeita_folha_sem_status(self):
        df = _df_folha_valido().drop("STATUS")
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with pytest.raises(HrSchemaError, match="STATUS"):
                carregar_folha(_XLSX)

    def test_mensagem_erro_lista_todas_colunas_ausentes(self):
        df = pl.DataFrame({"OUTRO": ["x"]})
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with pytest.raises(HrSchemaError) as exc_info:
                carregar_folha(_XLSX)
            msg = str(exc_info.value)
            assert "CPF" in msg
            assert "NOME" in msg
            assert "STATUS" in msg

    def test_aceita_folha_com_colunas_extras(self):
        df = _df_folha_valido().clone()
        df = df.with_columns(pl.Series("CARGO", ["ACS", "ACS"]))
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            resultado = carregar_folha(_XLSX)
        assert "CARGO" in resultado.columns


class TestNormalizacaoCpf:

    def test_remove_pontos_e_traco(self):
        df = pl.DataFrame({
            "CPF": ["117.167.238-17"],
            "NOME": ["ZELIA"],
            "STATUS": ["ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            resultado = carregar_folha(_XLSX)
        assert resultado["CPF"][0] == "11716723817"

    def test_remove_espacos(self):
        df = pl.DataFrame({
            "CPF": [" 11716723817 "],
            "NOME": ["ZELIA"],
            "STATUS": ["ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            resultado = carregar_folha(_XLSX)
        assert resultado["CPF"][0] == "11716723817"

    def test_cpf_ja_limpo_permanece_igual(self):
        df = pl.DataFrame({
            "CPF": ["11716723817"],
            "NOME": ["ZELIA"],
            "STATUS": ["ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            resultado = carregar_folha(_XLSX)
        assert resultado["CPF"][0] == "11716723817"

    def test_normaliza_todos_os_registros(self):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=_df_folha_valido()):
            resultado = carregar_folha(_XLSX)
        assert resultado["CPF"].to_list() == ["11716723817", "22730768866"]


class TestDeteccaoCpfInvalido:

    def test_loga_cpf_nulo(self, caplog):
        df = pl.DataFrame({
            "CPF": [None, "11716723817"],
            "NOME": ["NULO", "ZELIA"],
            "STATUS": ["ATIVO", "ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with caplog.at_level(logging.WARNING, logger="cnes_infra.ingestion.hr_client"):
                carregar_folha(_XLSX)
        assert "cpf_invalido" in caplog.text

    def test_loga_cpf_comprimento_incorreto(self, caplog):
        df = pl.DataFrame({
            "CPF": ["123456789", "11716723817"],
            "NOME": ["CURTO", "ZELIA"],
            "STATUS": ["ATIVO", "ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            with caplog.at_level(logging.WARNING, logger="cnes_infra.ingestion.hr_client"):
                carregar_folha(_XLSX)
        assert "cpf_invalido" in caplog.text

    def test_nao_remove_registros_com_cpf_invalido(self):
        df = pl.DataFrame({
            "CPF": [None, "123456789", "11716723817"],
            "NOME": ["A", "B", "C"],
            "STATUS": ["ATIVO", "ATIVO", "ATIVO"],
        })
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df):
            resultado = carregar_folha(_XLSX)
        assert len(resultado) == 3

    def test_cpf_valido_nao_gera_warning(self, caplog):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=_df_folha_valido()):
            with caplog.at_level(logging.WARNING, logger="cnes_infra.ingestion.hr_client"):
                carregar_folha(_XLSX)
        assert "cpf_invalido" not in caplog.text


class TestQualidade:

    def test_retorna_copia_independente(self):
        df_original = _df_folha_valido()
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df_original):
            resultado = carregar_folha(_XLSX)
        assert resultado is not df_original

    def test_nao_muta_dataframe_original(self):
        df_original = _df_folha_valido()
        cpf_antes = df_original["CPF"].to_list()
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=df_original):
            carregar_folha(_XLSX)
        assert df_original["CPF"].to_list() == cpf_antes

    def test_retorna_dataframe(self):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=_df_folha_valido()):
            resultado = carregar_folha(_XLSX)
        assert isinstance(resultado, pl.DataFrame)

    def test_logging_registra_total_carregado(self, caplog):
        with patch("cnes_infra.ingestion.hr_client.pl.read_excel", return_value=_df_folha_valido()):
            with caplog.at_level(logging.INFO, logger="cnes_infra.ingestion.hr_client"):
                carregar_folha(_XLSX)
        assert "rows=2" in caplog.text
