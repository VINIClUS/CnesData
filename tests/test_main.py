"""
test_main.py — Testes Unitários da Orquestração do Pipeline (WP-005)

Estratégia: todos os I/O externos são mockados via ExitStack + patch.
Nenhum banco Firebird, arquivo real ou BigQuery é acessado.

O que é testado:
  - Relatório principal sempre exportado.
  - CSVs de auditoria exportados apenas quando DataFrames não vazios.
  - Bloco HR executado apenas quando config.FOLHA_HR_PATH está configurado.
  - Cross-check nacional executado em toda execução.
  - Código de saída 0 em sucesso, 1 em exceção.
"""

import contextlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from cli import CliArgs

_CLI_DEFAULTS = CliArgs(
    competencia=None,
    output_dir=None,
    skip_nacional=False,
    skip_hr=False,
    verbose=False,
)


_DF_VAZIO = pd.DataFrame()
_OUTPUT = Path("data/processed/Relatorio.csv")


def _df_cnes_minimo() -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": ["11111111111"],
        "CNS": ["702002887429583"],
        "NOME_PROFISSIONAL": ["ZELIA"],
        "CBO": ["515105"],
        "CNES": ["0985333"],
        "TIPO_VINCULO": ["010101"],
        "SUS": ["S"],
        "CH_TOTAL": [40],
        "CH_AMBULATORIAL": [40],
        "CH_OUTRAS": [0],
        "CH_HOSPITALAR": [0],
        "FONTE": ["LOCAL"],
        "TIPO_UNIDADE": ["01"],
        "ALERTA_STATUS_CH": ["OK"],
    })


def _df_rh_minimo() -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": ["11111111111"],
        "NOME": ["ZELIA"],
        "STATUS": ["ATIVO"],
    })


def _df_estab_minimo() -> pd.DataFrame:
    return pd.DataFrame({
        "CNES": ["0985333"],
        "NOME_FANTASIA": ["ESF TESTE"],
        "TIPO_UNIDADE": ["02"],
        "CNPJ_MANTENEDORA": [None],
        "NATUREZA_JURIDICA": [None],
        "COD_MUNICIPIO": ["354130"],
        "VINCULO_SUS": [None],
        "FONTE": ["LOCAL"],
    })


def _mock_adapter_local(df_prof=None) -> MagicMock:
    adapter = MagicMock()
    adapter.listar_profissionais.return_value = (
        df_prof if df_prof is not None else _df_cnes_minimo()
    )
    adapter.listar_estabelecimentos.return_value = _df_estab_minimo()
    return adapter


def _mock_adapter_nacional() -> MagicMock:
    adapter = MagicMock()
    adapter.listar_profissionais.return_value = _DF_VAZIO
    adapter.listar_estabelecimentos.return_value = _DF_VAZIO
    return adapter


def _aplicar_patches(
    stack: contextlib.ExitStack,
    adapter_local: MagicMock,
    adapter_nacional: MagicMock,
    df_cnes: pd.DataFrame,
    df_rh: pd.DataFrame,
    df_multi: pd.DataFrame,
    df_acs: pd.DataFrame,
    df_ace: pd.DataFrame,
    df_ghost: pd.DataFrame,
    df_missing: pd.DataFrame,
    mock_carregar: MagicMock,
    folha_hr_path,
) -> MagicMock:
    """Aplica todos os patches no ExitStack e retorna o mock de exportar_csv."""
    mock_config = MagicMock()
    mock_config.OUTPUT_PATH = _OUTPUT
    mock_config.FOLHA_HR_PATH = folha_hr_path
    mock_config.LOGS_DIR = Path("logs")
    mock_config.LOG_FILE = Path("logs/cnes_exporter.log")
    mock_config.SNAPSHOTS_DIR = Path("data/snapshots")
    mock_config.GCP_PROJECT_ID = "test-project"
    mock_config.COMPETENCIA_ANO = 2024
    mock_config.COMPETENCIA_MES = 12
    mock_config.COD_MUN_IBGE = "354130"
    mock_config.ID_MUNICIPIO_IBGE7 = "3541307"
    mock_config.CNPJ_MANTENEDORA = "55293427000117"
    mock_config.RAIZ_PROJETO = Path(".")

    stack.enter_context(patch("main.parse_args", return_value=_CLI_DEFAULTS))
    stack.enter_context(patch("main.configurar_logging"))
    stack.enter_context(patch("main.conectar", return_value=MagicMock()))
    stack.enter_context(patch("main.CnesLocalAdapter", return_value=adapter_local))
    stack.enter_context(patch("main.CnesNacionalAdapter", return_value=adapter_nacional))
    stack.enter_context(patch("main.transformar", return_value=df_cnes))
    stack.enter_context(patch("main.detectar_multiplas_unidades", return_value=df_multi))
    stack.enter_context(patch("main.auditar_lotacao_acs_tacs", return_value=df_acs))
    stack.enter_context(patch("main.auditar_lotacao_ace_tace", return_value=df_ace))
    stack.enter_context(patch("main.detectar_folha_fantasma", return_value=df_ghost))
    stack.enter_context(patch("main.detectar_registro_ausente", return_value=df_missing))
    stack.enter_context(patch("main.carregar_folha", new=mock_carregar))
    stack.enter_context(patch("main.detectar_estabelecimentos_fantasma", return_value=_DF_VAZIO))
    stack.enter_context(patch("main.detectar_estabelecimentos_ausentes_local", return_value=_DF_VAZIO))
    stack.enter_context(patch("main.detectar_profissionais_fantasma", return_value=_DF_VAZIO))
    stack.enter_context(patch("main.detectar_profissionais_ausentes_local", return_value=_DF_VAZIO))
    stack.enter_context(patch("main.detectar_divergencia_cbo", return_value=_DF_VAZIO))
    stack.enter_context(patch("main.detectar_divergencia_carga_horaria", return_value=_DF_VAZIO))
    mock_exportar = stack.enter_context(patch("main.exportar_csv"))
    stack.enter_context(patch("main.criar_snapshot"))
    stack.enter_context(patch("main.salvar_snapshot"))
    stack.enter_context(patch("main.gerar_relatorio"))
    stack.enter_context(patch("main.config", mock_config))
    return mock_exportar


def _mocks_simples(
    df_cnes=None,
    df_rh=None,
    df_multi=None,
    df_acs=None,
    df_ace=None,
    df_ghost=None,
    df_missing=None,
    folha_hr_path=None,
    adapter_local=None,
    adapter_nacional=None,
):
    """Context manager que retorna (stack, mock_exportar, mock_carregar)."""
    df_cnes = df_cnes or _df_cnes_minimo()
    df_rh = df_rh or _df_rh_minimo()
    mock_carregar = MagicMock(return_value=df_rh)

    stack = contextlib.ExitStack()
    mock_exportar = _aplicar_patches(
        stack=stack,
        adapter_local=adapter_local if adapter_local is not None else _mock_adapter_local(df_prof=df_cnes),
        adapter_nacional=adapter_nacional if adapter_nacional is not None else _mock_adapter_nacional(),
        df_cnes=df_cnes,
        df_rh=df_rh,
        df_multi=df_multi if df_multi is not None else _DF_VAZIO,
        df_acs=df_acs if df_acs is not None else _DF_VAZIO,
        df_ace=df_ace if df_ace is not None else _DF_VAZIO,
        df_ghost=df_ghost if df_ghost is not None else _DF_VAZIO,
        df_missing=df_missing if df_missing is not None else _DF_VAZIO,
        mock_carregar=mock_carregar,
        folha_hr_path=folha_hr_path,
    )
    return stack, mock_exportar, mock_carregar


class TestExportacaoPrincipal:

    def test_relatorio_principal_sempre_exportado(self):
        with contextlib.ExitStack() as stack:
            df_cnes = _df_cnes_minimo()
            mock_exportar = _aplicar_patches(
                stack, _mock_adapter_local(), _mock_adapter_nacional(),
                df_cnes, _df_rh_minimo(),
                _DF_VAZIO, _DF_VAZIO, _DF_VAZIO, _DF_VAZIO, _DF_VAZIO,
                MagicMock(), None,
            )
            from main import main
            main()

        caminhos = [c.args[1] for c in mock_exportar.call_args_list]
        assert _OUTPUT in caminhos

    def test_exporta_rq003b_quando_nao_vazio(self):
        df_anomalia = _df_cnes_minimo()
        df_anomalia["QTD_UNIDADES"] = [2]
        stack, mock_exportar, _ = _mocks_simples(df_multi=df_anomalia)
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq003b_multiplas_unidades.csv" in nomes

    def test_nao_exporta_rq003b_quando_vazio(self):
        stack, mock_exportar, _ = _mocks_simples(df_multi=_DF_VAZIO)
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq003b_multiplas_unidades.csv" not in nomes

    def test_exporta_rq005_acs_quando_nao_vazio(self):
        stack, mock_exportar, _ = _mocks_simples(df_acs=_df_cnes_minimo())
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq005_acs_tacs_incorretos.csv" in nomes

    def test_exporta_rq005_ace_quando_nao_vazio(self):
        stack, mock_exportar, _ = _mocks_simples(df_ace=_df_cnes_minimo())
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_rq005_ace_tace_incorretos.csv" in nomes


class TestCrossCheckHr:

    def test_sem_folha_hr_nao_chama_carregar_folha(self):
        stack, _, mock_carregar = _mocks_simples(folha_hr_path=None)
        with stack:
            from main import main
            main()

        mock_carregar.assert_not_called()

    def test_com_folha_hr_chama_carregar_folha(self):
        caminho_hr = Path("folha.xlsx")
        stack, _, mock_carregar = _mocks_simples(folha_hr_path=caminho_hr)
        with stack:
            from main import main
            main()

        mock_carregar.assert_called_once_with(caminho_hr)

    def test_exporta_ghost_quando_nao_vazio(self):
        df_ghost = _df_cnes_minimo().assign(MOTIVO_GHOST=["AUSENTE_NO_RH"])
        stack, mock_exportar, _ = _mocks_simples(
            df_ghost=df_ghost, folha_hr_path=Path("folha.xlsx")
        )
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" in nomes

    def test_nao_exporta_ghost_quando_vazio(self):
        stack, mock_exportar, _ = _mocks_simples(
            df_ghost=_DF_VAZIO, folha_hr_path=Path("folha.xlsx")
        )
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" not in nomes

    def test_exporta_missing_quando_nao_vazio(self):
        stack, mock_exportar, _ = _mocks_simples(
            df_missing=_df_rh_minimo(), folha_hr_path=Path("folha.xlsx")
        )
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_missing_registration.csv" in nomes

    def test_sem_hr_nao_exporta_ghost_nem_missing(self):
        stack, mock_exportar, _ = _mocks_simples(folha_hr_path=None)
        with stack:
            from main import main
            main()

        nomes = [c.args[1].name for c in mock_exportar.call_args_list]
        assert "auditoria_ghost_payroll.csv" not in nomes
        assert "auditoria_missing_registration.csv" not in nomes


class TestCrossCheckNacional:

    def test_chama_adapter_nacional_com_competencia(self):
        adapter_nacional_inst = _mock_adapter_nacional()
        stack, _, _ = _mocks_simples(adapter_nacional=adapter_nacional_inst)
        with stack:
            from main import main
            main()

        adapter_nacional_inst.listar_profissionais.assert_called_once_with((2024, 12))
        adapter_nacional_inst.listar_estabelecimentos.assert_called_once_with((2024, 12))


class TestCodigosDeSaida:

    def test_retorna_0_em_sucesso(self):
        stack, _, _ = _mocks_simples()
        with stack:
            from main import main
            assert main() == 0

    def test_retorna_1_em_environment_error(self):
        with contextlib.ExitStack() as stack:
            mock_config = MagicMock()
            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            stack.enter_context(patch("main.parse_args", return_value=_CLI_DEFAULTS))
            stack.enter_context(patch("main.configurar_logging"))
            stack.enter_context(patch("main.conectar", side_effect=EnvironmentError("sem variavel")))
            stack.enter_context(patch("main.config", mock_config))

            from main import main
            assert main() == 1

    def test_retorna_1_em_excecao_generica(self):
        with contextlib.ExitStack() as stack:
            mock_config = MagicMock()
            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            stack.enter_context(patch("main.parse_args", return_value=_CLI_DEFAULTS))
            stack.enter_context(patch("main.configurar_logging"))
            stack.enter_context(patch("main.conectar", side_effect=RuntimeError("erro inesperado")))
            stack.enter_context(patch("main.config", mock_config))

            from main import main
            assert main() == 1

    def test_conexao_fechada_mesmo_com_erro(self):
        con_mock = MagicMock()
        adapter_local = MagicMock()
        adapter_local.listar_profissionais.side_effect = ValueError("erro")

        with contextlib.ExitStack() as stack:
            mock_config = MagicMock()
            mock_config.OUTPUT_PATH = _OUTPUT
            mock_config.LOGS_DIR = Path("logs")
            mock_config.LOG_FILE = Path("logs/cnes_exporter.log")

            stack.enter_context(patch("main.parse_args", return_value=_CLI_DEFAULTS))
            stack.enter_context(patch("main.configurar_logging"))
            stack.enter_context(patch("main.conectar", return_value=con_mock))
            stack.enter_context(patch("main.CnesLocalAdapter", return_value=adapter_local))
            stack.enter_context(patch("main.config", mock_config))

            from main import main
            main()

        con_mock.close.assert_called_once()


class TestIntegracaoCli:

    def test_skip_nacional_pula_adapter_nacional(self):
        cli_args = CliArgs(
            competencia=None, output_dir=None,
            skip_nacional=True, skip_hr=False, verbose=False,
        )
        mock_nacional_cls = MagicMock()
        stack, _, _ = _mocks_simples()
        with stack:
            with patch("main.parse_args", return_value=cli_args), \
                 patch("main.CnesNacionalAdapter", mock_nacional_cls):
                from main import main
                main()

        mock_nacional_cls.assert_not_called()

    def test_skip_hr_pula_carregar_folha(self):
        cli_args = CliArgs(
            competencia=None, output_dir=None,
            skip_nacional=False, skip_hr=True, verbose=False,
        )
        stack, _, mock_carregar = _mocks_simples(folha_hr_path=Path("folha.xlsx"))
        with stack:
            with patch("main.parse_args", return_value=cli_args):
                from main import main
                main()

        mock_carregar.assert_not_called()

    def test_competencia_cli_sobrescreve_env(self):
        cli_args = CliArgs(
            competencia=(2025, 6), output_dir=None,
            skip_nacional=False, skip_hr=False, verbose=False,
        )
        adapter_nacional_inst = _mock_adapter_nacional()
        stack, _, _ = _mocks_simples(adapter_nacional=adapter_nacional_inst)
        with stack:
            with patch("main.parse_args", return_value=cli_args):
                from main import main
                main()

        adapter_nacional_inst.listar_profissionais.assert_called_once_with((2025, 6))
        adapter_nacional_inst.listar_estabelecimentos.assert_called_once_with((2025, 6))

    def test_output_dir_cli_sobrescreve_env(self):
        cli_args = CliArgs(
            competencia=None, output_dir="/tmp/teste",
            skip_nacional=False, skip_hr=False, verbose=False,
        )
        stack, mock_exportar, _ = _mocks_simples()
        with stack:
            with patch("main.parse_args", return_value=cli_args):
                from main import main
                main()

        caminhos = [c.args[1] for c in mock_exportar.call_args_list]
        expected_dir = Path("/tmp/teste").resolve()
        assert any(expected_dir in p.parents for p in caminhos)
