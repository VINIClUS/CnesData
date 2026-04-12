"""
test_cnes_client.py — Testes Unitários da Camada de Ingestão CNES

Objetivo: verificar que cnes_client.py se comporta corretamente sem
precisar de conexão com o banco Firebird.

Estratégia de mock:
  - fdb.load_api é mockado para evitar carregamento de DLL real.
  - Cada cursor é simulado via MagicMock com .description e .fetchall()
    configurados para retornar dados controlados por cada teste.
  - extrair_profissionais() executa 3 queries (vinculos, membros, equipes),
    portanto con.cursor() retorna 3 cursors distintos via side_effect.
  - A conexão (fdb.Connection) é simulada — nunca instanciada de verdade.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.cnes_client import (
    carregar_driver,
    conectar,
    extrair_lookup_cbo,
    extrair_profissionais,
    COLUNAS_ESPERADAS,
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
    """Cria mock de conexão com 3 cursors distintos para as 3 queries."""
    cur_v = _criar_cursor_mock(linhas_vinculos, _COLUNAS_VINCULOS)
    cur_m = _criar_cursor_mock(linhas_membros or [], _COLUNAS_MEMBROS)
    cur_e = _criar_cursor_mock(linhas_equipes or [], _COLUNAS_EQUIPES)
    con_mock = MagicMock()
    con_mock.cursor.side_effect = [cur_v, cur_m, cur_e]
    return con_mock


def _linha_vinculos() -> tuple:
    """Retorna uma linha de dados válida para a query de vínculos (17 colunas)."""
    return (
        "11716723817",        # CPF
        "702002887429583",    # CNS
        "ZELIA RIBEIRO",      # NOME_PROFISSIONAL
        None,                 # NOME_SOCIAL
        "F",                  # SEXO
        "1975-04-12",         # DATA_NASCIMENTO
        "514225",             # CBO
        "010101",             # COD_VINCULO
        "S",                  # SUS_NAO_SUS
        40,                   # CARGA_HORARIA_TOTAL
        40,                   # CH_AMBULATORIAL
        0,                    # CH_OUTRAS
        0,                    # CH_HOSPITALAR
        "0985333",            # COD_CNES
        "ESF VILA GERONIMO",  # ESTABELECIMENTO
        "02",                 # COD_TIPO_UNIDADE
        "354130",             # COD_MUN_GESTOR
    )


def _linha_valida() -> tuple:
    """Compatibilidade: retorna linha de vínculos (sem colunas de equipe)."""
    return _linha_vinculos()


class TestConectar:

    def test_conectar_usa_charset_win1252(self):
        with patch("ingestion.cnes_client.fdb.connect") as mock_connect, \
             patch("ingestion.cnes_client.carregar_driver"):
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

        with patch("ingestion.cnes_client.fdb.load_api") as mock_load_api:
            carregar_driver(dll_fake)
            mock_load_api.assert_called_once_with(str(dll_fake))

    def test_nao_chama_load_api_quando_dll_ausente(self, tmp_path: Path):
        dll_inexistente = tmp_path / "inexistente.dll"

        with patch("ingestion.cnes_client.fdb.load_api") as mock_load_api:
            with pytest.raises(FileNotFoundError):
                carregar_driver(dll_inexistente)
            mock_load_api.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: extrair_profissionais()
# ─────────────────────────────────────────────────────────────────────────────

class TestExtrairProfissionais:

    def test_retorna_dataframe_com_colunas_esperadas(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        assert isinstance(resultado, pd.DataFrame)
        assert list(resultado.columns) == list(COLUNAS_ESPERADAS)
        assert len(resultado) == 1

    def test_colunas_base_vem_de_cursor_description(self):
        """
        As 17 colunas base do DataFrame devem vir de cursor.description
        da query de vínculos — os aliases SQL definem o schema de saída.
        As 3 colunas de equipe são adicionadas pelo merge Python.
        """
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        for coluna in _COLUNAS_VINCULOS:
            assert coluna in resultado.columns

    def test_levanta_value_error_quando_query_vazia(self):
        con_mock = _criar_conexao_mock(linhas_vinculos=[])
        with pytest.raises(ValueError, match="não retornou dados"):
            extrair_profissionais(con_mock)

    def test_fecha_cursor_mesmo_quando_execute_falha(self):
        """
        cursor.close() deve ser chamado no bloco finally mesmo se execute() falhar.
        Garante que recursos do banco não fiquem presos em caso de erro SQL.
        """
        cursor_mock = MagicMock()
        cursor_mock.execute.side_effect = Exception("Erro simulado de SQL")
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        with pytest.raises(Exception, match="Erro simulado"):
            extrair_profissionais(con_mock)

        cursor_mock.close.assert_called_once()

    def test_fecha_cursor_mesmo_quando_fetchall_falha(self):
        """cursor.close() deve ser chamado no finally mesmo se fetchall() falhar."""
        cursor_mock = MagicMock()
        cursor_mock.fetchall.side_effect = Exception("Erro no fetchall")
        con_mock = MagicMock()
        con_mock.cursor.return_value = cursor_mock

        with pytest.raises(Exception, match="Erro no fetchall"):
            extrair_profissionais(con_mock)

        cursor_mock.close.assert_called_once()

    def test_nao_fecha_conexao(self):
        """
        extrair_profissionais() NÃO deve fechar a conexão.
        Gerenciar o ciclo de vida da conexão é responsabilidade do main.py.
        """
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
        """
        Quando LFCES048/LFCES060 não retornam dados, as colunas de equipe
        devem existir no DataFrame com valores NaN — o transformer lida com isso.
        """
        con_mock = _criar_conexao_mock(linhas_vinculos=[_linha_vinculos()])
        resultado = extrair_profissionais(con_mock)

        assert "COD_INE_EQUIPE" in resultado.columns
        assert "NOME_EQUIPE" in resultado.columns
        assert "COD_TIPO_EQUIPE" in resultado.columns
        assert pd.isna(resultado["COD_INE_EQUIPE"].iloc[0])
        assert pd.isna(resultado["NOME_EQUIPE"].iloc[0])
        assert pd.isna(resultado["COD_TIPO_EQUIPE"].iloc[0])

    def test_enriquece_com_dados_de_equipe(self):
        """
        Quando LFCES048 e LFCES060 retornam dados compatíveis,
        o merge Python deve preencher as colunas de equipe corretamente.

        LFCES060.SEQ_EQUIPE=2239930 (nacional) → SEQ_BASE=2239
        LFCES048.SEQ_EQUIPE=2239 (local) → deve fazer match.
        """
        linhas_membros = [("11716723817", "514225", 2239)]
        linhas_equipes = [(2239930, "0002239930", "ESF VILA PALMIRA", 70)]

        con_mock = _criar_conexao_mock(
            linhas_vinculos=[_linha_vinculos()],
            linhas_membros=linhas_membros,
            linhas_equipes=linhas_equipes,
        )
        resultado = extrair_profissionais(con_mock)

        assert resultado["COD_INE_EQUIPE"].iloc[0] == "0002239930"
        assert resultado["NOME_EQUIPE"].iloc[0] == "ESF VILA PALMIRA"
        assert resultado["COD_TIPO_EQUIPE"].iloc[0] == 70

    def test_sem_match_de_equipe_preserva_linha(self):
        """
        Profissional sem correspondência em LFCES048 deve ter linha preservada
        com NaN nas colunas de equipe — nunca removido do resultado.
        """
        linhas_membros = [("99999999999", "111111", 9999)]  # CPF diferente
        con_mock = _criar_conexao_mock(
            linhas_vinculos=[_linha_vinculos()],
            linhas_membros=linhas_membros,
        )
        resultado = extrair_profissionais(con_mock)

        assert len(resultado) == 1
        assert pd.isna(resultado["COD_INE_EQUIPE"].iloc[0])


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
