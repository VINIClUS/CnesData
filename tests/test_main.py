"""
test_main.py — Testes Unitários da Orquestração do Pipeline (WP-005)

Estratégia: todos os I/O externos são mockados via patch no namespace de `main`.
Nenhum banco Firebird, arquivo real ou BigQuery é acessado.

O que é testado:
  - Relatório principal sempre exportado.
  - CSVs de auditoria exportados apenas quando DataFrames não vazios.
  - Bloco HR executado apenas quando config.FOLHA_HR_PATH está configurado.
  - Código de saída 0 em sucesso, 1 em exceção.
"""

from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest


_DF_VAZIO = pd.DataFrame()
_OUTPUT = Path("data/processed/Relatorio.csv")


def _df_cnes_minimo() -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": ["11111111111"],
        "NOME_PROFISSIONAL": ["ZELIA"],
        "CBO": ["515105"],
        "COD_CNES": ["0985333"],
        "COD_TIPO_UNIDADE": ["01"],
        "ALERTA_STATUS_CH": ["OK"],
    })


def _df_rh_minimo() -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": ["11111111111"],
        "NOME": ["ZELIA"],
        "STATUS": ["ATIVO"],
    })


@contextmanager
def _mocks(
    df_cnes=None,
    df_rh=None,
    df_multi=None,
    df_acs=None,
    df_ace=None,
    df_ghost=None,
    df_missing=None,
    folha_hr_path=None,
):
    """Configura todos os patches necessários para isolar main()."""
    df_cnes = df_cnes or _df_cnes_minimo()
    df_rh = df_rh or _df_rh_minimo()

    with patch("main.configurar_logging"), \
         patch("main.conectar", return_value=MagicMock()), \
         patch("main.extrair_profissionais", return_value=df_cnes), \
         patch("main.transformar", return_value=df_cnes), \
         patch("main.detectar_multiplas_unidades", return_value=_DF_VAZIO if df_multi is None else df_multi), \
         patch("main.auditar_lotacao_acs_tacs", return_value=_DF_VAZIO if df_acs is None else df_acs), \
         patch("main.auditar_lotacao_ace_tace", return_value=_DF_VAZIO if df_ace is None else df_ace), \
         patch("main.detectar_folha_fantasma", return_value=_DF_VAZIO if df_ghost is None else df_ghost), \
         patch("main.detectar_registro_ausente", return_value=_DF_VAZIO if df_missing is None else df_missing), \
         patch("main.carregar_folha", return_value=df_rh), \
         patch("main.exportar_csv") as mock_exportar, \
         patch("main.criar_snapshot"), \
         patch("main.salvar_snapshot"), \
         patch("main.gerar_relatorio"), \
         patch("main.config") as mock_config:

        mock_config.OUTPUT_PATH = _OUTPUT
        mock_config.FOLHA_HR_PATH = folha_hr_path
        mock_config.LOGS_DIR = Path("logs")
        mock_config.LOG_FILE = Path("logs/cnes_exporter.log")
        mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
        mock_config.COD_MUN_IBGE = "354130"
        mock_config.CNPJ_MANTENEDORA = "55293427000117"
        mock_config.RAIZ_PROJETO = Path(".")

        yield mock_exportar


class TestExportacaoPrincipal:

    def test_relatorio_principal_sempre_exportado(self):
        with _mocks() as mock_exportar:
            from main import main
            main()

        caminhos = [c.args[1] for c in mock_exportar.call_args_list]
        assert _OUTPUT in caminhos

    def test_exporta_rq003b_quando_nao_vazio(self):
        df_anomalia = _df_cnes_minimo()
        df_anomalia["QTD_UNIDADES"] = [2]
        with _mocks(df_multi=df_anomalia) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq003b_multiplas_unidades.csv" in nomes

    def test_nao_exporta_rq003b_quando_vazio(self):
        with _mocks(df_multi=_DF_VAZIO) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq003b_multiplas_unidades.csv" not in nomes

    def test_exporta_rq005_acs_quando_nao_vazio(self):
        with _mocks(df_acs=_df_cnes_minimo()) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq005_acs_tacs_incorretos.csv" in nomes

    def test_exporta_rq005_ace_quando_nao_vazio(self):
        with _mocks(df_ace=_df_cnes_minimo()) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq005_ace_tace_incorretos.csv" in nomes


class TestCrossCheckHr:

    def test_sem_folha_hr_nao_chama_carregar_folha(self):
        with patch("main.configurar_logging"), \
             patch("main.conectar", return_value=MagicMock()), \
             patch("main.extrair_profissionais", return_value=_df_cnes_minimo()), \
             patch("main.transformar", return_value=_df_cnes_minimo()), \
             patch("main.detectar_multiplas_unidades", return_value=_DF_VAZIO), \
             patch("main.auditar_lotacao_acs_tacs", return_value=_DF_VAZIO), \
             patch("main.auditar_lotacao_ace_tace", return_value=_DF_VAZIO), \
             patch("main.detectar_folha_fantasma", return_value=_DF_VAZIO), \
             patch("main.detectar_registro_ausente", return_value=_DF_VAZIO), \
             patch("main.carregar_folha") as mock_carregar, \
             patch("main.exportar_csv"), \
             patch("main.criar_snapshot"), \
             patch("main.salvar_snapshot"), \
             patch("main.gerar_relatorio"), \
             patch("main.config") as mock_config:

            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.FOLHA_HR_PATH = None
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")
            mock_config.SNAPSHOTS_DIR = Path("data/snapshots")

            from main import main
            main()

        mock_carregar.assert_not_called()

    def test_com_folha_hr_chama_carregar_folha(self):
        caminho_hr = Path("folha.xlsx")
        with patch("main.configurar_logging"), \
             patch("main.conectar", return_value=MagicMock()), \
             patch("main.extrair_profissionais", return_value=_df_cnes_minimo()), \
             patch("main.transformar", return_value=_df_cnes_minimo()), \
             patch("main.detectar_multiplas_unidades", return_value=_DF_VAZIO), \
             patch("main.auditar_lotacao_acs_tacs", return_value=_DF_VAZIO), \
             patch("main.auditar_lotacao_ace_tace", return_value=_DF_VAZIO), \
             patch("main.detectar_folha_fantasma", return_value=_DF_VAZIO), \
             patch("main.detectar_registro_ausente", return_value=_DF_VAZIO), \
             patch("main.carregar_folha", return_value=_df_rh_minimo()) as mock_carregar, \
             patch("main.exportar_csv"), \
             patch("main.criar_snapshot"), \
             patch("main.salvar_snapshot"), \
             patch("main.gerar_relatorio"), \
             patch("main.config") as mock_config:

            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.FOLHA_HR_PATH = caminho_hr
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")
            mock_config.SNAPSHOTS_DIR = Path("data/snapshots")

            from main import main
            main()

        mock_carregar.assert_called_once_with(caminho_hr)

    def test_exporta_ghost_quando_nao_vazio(self):
        df_ghost = _df_cnes_minimo().assign(MOTIVO_GHOST=["AUSENTE_NO_RH"])
        with _mocks(df_ghost=df_ghost, folha_hr_path=Path("folha.xlsx")) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" in nomes

    def test_nao_exporta_ghost_quando_vazio(self):
        with _mocks(df_ghost=_DF_VAZIO, folha_hr_path=Path("folha.xlsx")) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" not in nomes

    def test_exporta_missing_quando_nao_vazio(self):
        df_missing = _df_rh_minimo()
        with _mocks(df_missing=df_missing, folha_hr_path=Path("folha.xlsx")) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_missing_registration.csv" in nomes

    def test_sem_hr_nao_exporta_ghost_nem_missing(self):
        with _mocks(folha_hr_path=None) as mock_exportar:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" not in nomes
        assert "auditoria_missing_registration.csv" not in nomes


class TestCodigosDeSaida:

    def test_retorna_0_em_sucesso(self):
        with _mocks():
            from main import main
            assert main() == 0

    def test_retorna_1_em_environment_error(self):
        with patch("main.configurar_logging"), \
             patch("main.conectar", side_effect=EnvironmentError("sem variavel")), \
             patch("main.config") as mock_config:

            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            from main import main
            assert main() == 1

    def test_retorna_1_em_excecao_generica(self):
        with patch("main.configurar_logging"), \
             patch("main.conectar", side_effect=RuntimeError("erro inesperado")), \
             patch("main.config") as mock_config:

            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            from main import main
            assert main() == 1

    def test_conexao_fechada_mesmo_com_erro(self):
        con_mock = MagicMock()
        with patch("main.configurar_logging"), \
             patch("main.conectar", return_value=con_mock), \
             patch("main.extrair_profissionais", side_effect=ValueError("erro")), \
             patch("main.config") as mock_config:

            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            from main import main
            main()

        con_mock.close.assert_called_once()
