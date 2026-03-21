"""
test_cnes_client.py — Testes Unitários da Camada de Ingestão CNES

Objetivo: verificar que cnes_client.py se comporta corretamente sem
precisar de conexão com o banco Firebird.

Estratégia de mock:
  - fdb.load_api é mockado para evitar carregamento de DLL real.
  - O cursor é simulado via MagicMock com .description e .fetchall()
    configurados para retornar dados controlados por cada teste.
  - A conexão (fdb.Connection) é simulada — nunca instanciada de verdade.

Por que não usar unittest.mock.patch("fdb.connect") aqui?
  extrair_profissionais() recebe uma conexão já aberta como argumento
  (injeção de dependência). Isso permite testar sem mockar o módulo fdb
  globalmente — cada teste controla exatamente o que o cursor retorna.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# conftest.py (tests/) já adicionou src/ ao sys.path
from ingestion.cnes_client import (
    carregar_driver,
    extrair_profissionais,
    COLUNAS_ESPERADAS,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _criar_cursor_mock(linhas: list, nomes_colunas: list) -> MagicMock:
    """
    Cria um mock de cursor fdb com description e fetchall configurados.

    cursor.description segue a especificação DBAPI2: sequência de tuplas
    de 7 elementos onde o índice [0] é o nome da coluna.
    """
    cursor_mock = MagicMock()
    cursor_mock.description = [(nome,) + (None,) * 6 for nome in nomes_colunas]
    cursor_mock.fetchall.return_value = linhas
    return cursor_mock


def _criar_conexao_mock(linhas: list, nomes_colunas: list) -> MagicMock:
    """Cria um mock de fdb.Connection com cursor já configurado."""
    cursor_mock = _criar_cursor_mock(linhas, nomes_colunas)
    con_mock = MagicMock()
    con_mock.cursor.return_value = cursor_mock
    return con_mock


def _linha_valida() -> tuple:
    """Retorna uma linha de dados válida com 20 colunas (COLUNAS_ESPERADAS)."""
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
        None,                 # COD_INE_EQUIPE
        None,                 # NOME_EQUIPE
        None,                 # COD_TIPO_EQUIPE
    )


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: carregar_driver()
# ─────────────────────────────────────────────────────────────────────────────

class TestCarregarDriver:

    def test_levanta_file_not_found_quando_dll_ausente(self, tmp_path: Path):
        """carregar_driver() deve levantar FileNotFoundError para DLL inexistente."""
        dll_inexistente = tmp_path / "fbembed_fake.dll"
        with pytest.raises(FileNotFoundError, match="DLL do Firebird"):
            carregar_driver(dll_inexistente)

    def test_chama_fdb_load_api_com_path_string(self, tmp_path: Path):
        """carregar_driver() deve chamar fdb.load_api() passando o path como str."""
        dll_fake = tmp_path / "fbembed.dll"
        dll_fake.write_bytes(b"fake_dll_content")

        with patch("ingestion.cnes_client.fdb.load_api") as mock_load_api:
            carregar_driver(dll_fake)
            mock_load_api.assert_called_once_with(str(dll_fake))

    def test_nao_chama_load_api_quando_dll_ausente(self, tmp_path: Path):
        """fdb.load_api() NÃO deve ser chamado quando a DLL não existe."""
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
        """Deve retornar DataFrame com as 19 colunas de COLUNAS_ESPERADAS."""
        con_mock = _criar_conexao_mock(
            linhas=[_linha_valida()],
            nomes_colunas=list(COLUNAS_ESPERADAS),
        )
        resultado = extrair_profissionais(con_mock)

        assert isinstance(resultado, pd.DataFrame)
        assert list(resultado.columns) == list(COLUNAS_ESPERADAS)
        assert len(resultado) == 1

    def test_colunas_vem_de_cursor_description(self):
        """
        Os nomes de coluna do DataFrame devem vir de cursor.description,
        não de constantes hardcoded — os aliases SQL definem o schema de saída.
        """
        colunas_customizadas = ["COL_A", "COL_B", "COL_C"]
        con_mock = _criar_conexao_mock(
            linhas=[("val_a", "val_b", "val_c")],
            nomes_colunas=colunas_customizadas,
        )
        resultado = extrair_profissionais(con_mock)

        assert list(resultado.columns) == colunas_customizadas

    def test_levanta_value_error_quando_query_vazia(self):
        """Deve levantar ValueError com mensagem descritiva quando não há dados."""
        con_mock = _criar_conexao_mock(
            linhas=[],
            nomes_colunas=list(COLUNAS_ESPERADAS),
        )
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
        con_mock = _criar_conexao_mock(
            linhas=[_linha_valida()],
            nomes_colunas=list(COLUNAS_ESPERADAS),
        )
        extrair_profissionais(con_mock)
        con_mock.close.assert_not_called()

    def test_retorna_multiplas_linhas(self):
        """Deve retornar DataFrame com o número correto de linhas."""
        con_mock = _criar_conexao_mock(
            linhas=[_linha_valida(), _linha_valida(), _linha_valida()],
            nomes_colunas=list(COLUNAS_ESPERADAS),
        )
        resultado = extrair_profissionais(con_mock)
        assert len(resultado) == 3

    def test_valores_nulos_do_left_join_sao_preservados(self):
        """
        Valores None das colunas opcionais do LEFT JOIN devem ser preservados
        no DataFrame — o transformer é responsável por preenchê-los.
        """
        con_mock = _criar_conexao_mock(
            linhas=[_linha_valida()],
            nomes_colunas=list(COLUNAS_ESPERADAS),
        )
        resultado = extrair_profissionais(con_mock)

        # As três últimas colunas (equipe) têm None na _linha_valida()
        assert resultado["COD_INE_EQUIPE"].iloc[0] is None
        assert resultado["NOME_EQUIPE"].iloc[0] is None
        assert resultado["COD_TIPO_EQUIPE"].iloc[0] is None
