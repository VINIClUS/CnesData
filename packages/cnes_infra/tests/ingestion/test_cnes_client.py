"""Testes unitarios da camada de ingestao CNES."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from cnes_infra.ingestion.cnes_client import (
    COLUNAS_ESPERADAS,
    carregar_driver,
    conectar,
    extrair_lookup_cbo,
    extrair_profissionais,
)

_COLUNAS_VINCULOS: list[str] = [
    "CPF", "CNS", "NOME_PROFISSIONAL", "NOME_SOCIAL", "SEXO", "DATA_NASCIMENTO",
    "CBO", "COD_VINCULO", "SUS_NAO_SUS",
    "CARGA_HORARIA_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS", "CH_HOSPITALAR",
    "COD_CNES", "ESTABELECIMENTO", "COD_TIPO_UNIDADE", "COD_MUN_GESTOR",
]
_COLUNAS_MEMBROS: list[str] = ["CPF", "CBO", "SEQ_EQUIPE"]
_COLUNAS_EQUIPES: list[str] = ["SEQ_EQUIPE", "COD_INE_EQUIPE", "NOME_EQUIPE", "COD_TIPO_EQUIPE"]


def _criar_cursor_mock(linhas: list, nomes_colunas: list) -> MagicMock:
    cursor_mock = MagicMock()
    cursor_mock.description = [(nome,) + (None,) * 6 for nome in nomes_colunas]
    cursor_mock.fetchall.return_value = linhas
    return cursor_mock


def _criar_conexao_mock(
    linhas_vinculos: list,
    linhas_membros: list | None = None,
    linhas_equipes: list | None = None,
) -> MagicMock:
    cur_v = _criar_cursor_mock(linhas_vinculos, _COLUNAS_VINCULOS)
    cur_m = _criar_cursor_mock(linhas_membros or [], _COLUNAS_MEMBROS)
    cur_e = _criar_cursor_mock(linhas_equipes or [], _COLUNAS_EQUIPES)
    con_mock = MagicMock()
    con_mock.cursor.side_effect = [cur_v, cur_m, cur_e]
    return con_mock


def _linha_vinculos() -> tuple:
    return (
        "11716723817",
        "702002887429583",
        "ZELIA RIBEIRO",
        None,
        "F",
        "1975-04-12",
        "514225",
        "010101",
        "S",
        40,
        40,
        0,
        0,
        "0985333",
        "ESF VILA GERONIMO",
        "02",
        "354130",
    )


def _linha_valida() -> tuple:
    return _linha_vinculos()


class TestConectar:

    def test_conectar_usa_charset_win1252(self):
        with patch("cnes_infra.ingestion.cnes_client.fdb.connect") as mock_connect, \
             patch("cnes_infra.ingestion.cnes_client.carregar_driver"):
            mock_connect.return_value = MagicMock()
            conectar()
            call_kwargs = mock_connect.call_args.kwargs
            assert call_kwargs.get("charset") == "WIN1252"


class TestCarregarDriver:

    def test_levanta_file_not_found_quando_dll_ausente(self, tmp_path: Path):
        dll_inexistente = tmp_path / "fbembed_fake.dll"
        with pytest.raises(FileNotFoundError, match="DLL do Firebird"):
            carregar_driver(dll_inexistente)

    def test_chama_fdb_load_api_com_path_string(self, tmp_path: Path):
        dll_fake = tmp_path / "fbembed.dll"
        dll_fake.write_bytes(b"fake_dll_content")

        with patch("cnes_infra.ingestion.cnes_client.fdb.load_api") as mock_load_api:
            carregar_driver(dll_fake)
            mock_load_api.assert_called_once_with(str(dll_fake))

    def test_nao_chama_load_api_quando_dll_ausente(self, tmp_path: Path):
        dll_inexistente = tmp_path / "inexistente.dll"

        with patch("cnes_infra.ingestion.cnes_client.fdb.load_api") as mock_load_api:
            with pytest.raises(FileNotFoundError):
                carregar_driver(dll_inexistente)
            mock_load_api.assert_not_called()


class TestExtrairProfissionais:

    def test_retorna_dataframe_com_colunas_esperadas(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        assert isinstance(resultado, pl.DataFrame)
        assert resultado.columns == list(COLUNAS_ESPERADAS)
        assert len(resultado) == 1

    def test_colunas_base_vem_de_cursor_description(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        for coluna in _COLUNAS_VINCULOS:
            assert coluna in resultado.columns

    def test_levanta_value_error_quando_query_vazia(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[])
        with pytest.raises(ValueError, match="nao retornou dados"):
            extrair_profissionais(con_mock)

    def test_fecha_cursor_mesmo_quando_execute_falha(self):
        cursor_mock = MagicMock()
        cursor_mock.execute.side_effect = Exception("Erro simulado de SQL")
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        with pytest.raises(Exception, match="Erro simulado"):
            extrair_profissionais(con_mock)

        cursor_mock.close.assert_called_once()

    def test_fecha_cursor_mesmo_quando_fetchall_falha(self):
        cursor_mock = MagicMock()
        cursor_mock.fetchall.side_effect = Exception("Erro no fetchall")
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        with pytest.raises(Exception, match="Erro no fetchall"):
            extrair_profissionais(con_mock)

        cursor_mock.close.assert_called_once()

    def test_nao_fecha_conexao(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        extrair_profissionais(con_mock)
        con_mock.close.assert_not_called()

    def test_retorna_multiplas_linhas(self):
        con_mock = _criar_conexao_mock(
            linhas_vinculos=[_linha_vinculos(), _linha_vinculos(), _linha_vinculos()],
        )
        resultado = extrair_profissionais(con_mock)
        assert len(resultado) == 3

    def test_colunas_equipe_nulas_quando_sem_membros(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        assert "COD_INE_EQUIPE" in resultado.columns
        assert "NOME_EQUIPE" in resultado.columns
        assert "COD_TIPO_EQUIPE" in resultado.columns
        assert resultado["COD_INE_EQUIPE"][0] is None
        assert resultado["NOME_EQUIPE"][0] is None
        assert resultado["COD_TIPO_EQUIPE"][0] is None

    def test_enriquece_com_dados_de_equipe(self):
        linhas_membros = [("11716723817", "514225", 2239)]
        linhas_equipes = [(2239930, "0002239930", "ESF VILA PALMIRA", 70)]

        con_mock = _criar_conexao_mock(
            linhas_vinculos=[_linha_vinculos()],
            linhas_membros=linhas_membros,
            linhas_equipes=linhas_equipes,
        )
        resultado = extrair_profissionais(con_mock)

        assert resultado["COD_INE_EQUIPE"][0] == "0002239930"
        assert resultado["NOME_EQUIPE"][0] == "ESF VILA PALMIRA"
        assert resultado["COD_TIPO_EQUIPE"][0] == 70

    def test_sem_match_de_equipe_preserva_linha(self):
        linhas_membros = [("99999999999", "111111", 9999)]
        con_mock = _criar_conexao_mock(
            linhas_vinculos=[_linha_vinculos()],
            linhas_membros=linhas_membros,
        )
        resultado = extrair_profissionais(con_mock)

        assert len(resultado) == 1
        assert resultado["COD_INE_EQUIPE"][0] is None


class TestExtrairLookupCbo:

    def test_retorna_dict_cbo_descricao(self):
        linhas = [("515105", "AGENTE COMUNITARIO DE SAUDE"),
                  ("322255", "TECNICO EM AGENTE COMUNITARIO DE SAUDE")]
        cursor_mock = _criar_cursor_mock(linhas, ["CBO", "DESCRICAO_CBO"])
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        resultado = extrair_lookup_cbo(con_mock)

        assert isinstance(resultado, dict)
        assert resultado["515105"] == "AGENTE COMUNITARIO DE SAUDE"
        assert resultado["322255"] == "TECNICO EM AGENTE COMUNITARIO DE SAUDE"

    def test_aplica_strip_nas_chaves_e_valores(self):
        linhas = [("515105 ", " AGENTE COMUNITARIO ")]
        cursor_mock = _criar_cursor_mock(linhas, ["CBO", "DESCRICAO_CBO"])
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        resultado = extrair_lookup_cbo(con_mock)

        assert "515105" in resultado
        assert resultado["515105"] == "AGENTE COMUNITARIO"

    def test_retorna_dict_vazio_quando_tabela_vazia(self):
        cursor_mock = _criar_cursor_mock([], ["CBO", "DESCRICAO_CBO"])
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        resultado = extrair_lookup_cbo(con_mock)

        assert resultado == {}

    def test_fecha_cursor_apos_execucao(self):
        cursor_mock = _criar_cursor_mock([], ["CBO", "DESCRICAO_CBO"])
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        extrair_lookup_cbo(con_mock)

        cursor_mock.close.assert_called_once()
