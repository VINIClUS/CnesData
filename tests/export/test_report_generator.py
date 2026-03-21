"""
test_report_generator.py — Testes Unitários do Gerador de Relatórios Excel (WP-007)

Cobertura:
  - Estrutura do .xlsx: abas criadas/omitidas conforme DataFrames não-vazios.
  - Coluna RECOMENDACAO: presente e sem nulos em todas as abas de auditoria.
  - Aba Principal: criada sempre, sem coluna RECOMENDACAO.
  - Qualidade: contagem de linhas preservada, diretório pai criado.
"""

from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from export.report_generator import gerar_relatorio

_NOME_ABAS_AUDITORIA = [
    "Ghost_Payroll",
    "Missing_Registro",
    "Multi_Unidades",
    "ACS_TACS_Incorretos",
    "ACE_TACE_Incorretos",
]


def _df_vinculos(n: int = 3) -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": [str(i) * 11 for i in range(n)],
        "NOME_PROFISSIONAL": [f"PROF {i}" for i in range(n)],
        "CBO": ["515105"] * n,
        "COD_CNES": ["0985333"] * n,
        "ESTABELECIMENTO": ["UBS TESTE"] * n,
    })


def _df_ghost(n: int = 2) -> pd.DataFrame:
    df = _df_vinculos(n)
    df["MOTIVO_GHOST"] = ["AUSENTE_NO_RH"] * n
    return df


def _df_missing(n: int = 2) -> pd.DataFrame:
    return pd.DataFrame({
        "CPF": [str(i) * 11 for i in range(n)],
        "NOME": [f"PROF {i}" for i in range(n)],
        "STATUS": ["ATIVO"] * n,
    })


def _df_auditoria(n: int = 2) -> pd.DataFrame:
    return _df_vinculos(n)


def _gerar(caminho: Path, **kwargs) -> None:
    """Chama gerar_relatorio com todos os DataFrames opcionais."""
    defaults: dict = {
        "df_principal": _df_vinculos(),
        "df_ghost": pd.DataFrame(),
        "df_missing": pd.DataFrame(),
        "df_multi_unidades": pd.DataFrame(),
        "df_acs": pd.DataFrame(),
        "df_ace": pd.DataFrame(),
    }
    defaults.update(kwargs)
    gerar_relatorio(caminho, **defaults)


def _abas(caminho: Path) -> list[str]:
    wb = openpyxl.load_workbook(caminho, read_only=True)
    names = wb.sheetnames
    wb.close()
    return names


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 1: Estrutura do arquivo
# ─────────────────────────────────────────────────────────────────────────────

class TestEstruturaArquivo:

    def test_cria_arquivo_xlsx(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho)
        assert caminho.exists()
        assert caminho.suffix == ".xlsx"

    def test_arquivo_abre_sem_erro(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho)
        wb = openpyxl.load_workbook(caminho)
        assert wb is not None
        wb.close()

    def test_aba_principal_sempre_criada(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho)
        assert "Principal" in _abas(caminho)

    def test_cria_diretorio_pai_se_inexistente(self, tmp_path: Path):
        caminho = tmp_path / "subdir" / "relatorio.xlsx"
        _gerar(caminho)
        assert caminho.exists()

    def test_aba_ghost_criada_quando_nao_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=_df_ghost())
        assert "Ghost_Payroll" in _abas(caminho)

    def test_aba_ghost_omitida_quando_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=pd.DataFrame())
        assert "Ghost_Payroll" not in _abas(caminho)

    def test_aba_missing_criada_quando_nao_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_missing=_df_missing())
        assert "Missing_Registro" in _abas(caminho)

    def test_aba_missing_omitida_quando_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_missing=pd.DataFrame())
        assert "Missing_Registro" not in _abas(caminho)

    def test_aba_multi_unidades_criada_quando_nao_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_multi_unidades=_df_auditoria())
        assert "Multi_Unidades" in _abas(caminho)

    def test_aba_acs_criada_quando_nao_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_acs=_df_auditoria())
        assert "ACS_TACS_Incorretos" in _abas(caminho)

    def test_aba_ace_criada_quando_nao_vazio(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ace=_df_auditoria())
        assert "ACE_TACE_Incorretos" in _abas(caminho)

    def test_somente_principal_quando_todos_vazios(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho)
        assert _abas(caminho) == ["Principal"]


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 2: Coluna RECOMENDACAO
# ─────────────────────────────────────────────────────────────────────────────

class TestRecomendacao:

    def test_aba_ghost_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=_df_ghost())
        df = pd.read_excel(caminho, sheet_name="Ghost_Payroll")
        assert "RECOMENDACAO" in df.columns

    def test_aba_missing_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_missing=_df_missing())
        df = pd.read_excel(caminho, sheet_name="Missing_Registro")
        assert "RECOMENDACAO" in df.columns

    def test_aba_multi_unidades_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_multi_unidades=_df_auditoria())
        df = pd.read_excel(caminho, sheet_name="Multi_Unidades")
        assert "RECOMENDACAO" in df.columns

    def test_aba_acs_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_acs=_df_auditoria())
        df = pd.read_excel(caminho, sheet_name="ACS_TACS_Incorretos")
        assert "RECOMENDACAO" in df.columns

    def test_aba_ace_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ace=_df_auditoria())
        df = pd.read_excel(caminho, sheet_name="ACE_TACE_Incorretos")
        assert "RECOMENDACAO" in df.columns

    def test_recomendacao_sem_nulos_em_ghost(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=_df_ghost(5))
        df = pd.read_excel(caminho, sheet_name="Ghost_Payroll")
        assert df["RECOMENDACAO"].isna().sum() == 0

    def test_recomendacao_sem_nulos_em_missing(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_missing=_df_missing(5))
        df = pd.read_excel(caminho, sheet_name="Missing_Registro")
        assert df["RECOMENDACAO"].isna().sum() == 0

    def test_recomendacao_nao_vazia_em_acs(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_acs=_df_auditoria(3))
        df = pd.read_excel(caminho, sheet_name="ACS_TACS_Incorretos")
        assert (df["RECOMENDACAO"].str.len() > 0).all()

    def test_principal_nao_tem_coluna_recomendacao(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho)
        df = pd.read_excel(caminho, sheet_name="Principal")
        assert "RECOMENDACAO" not in df.columns


# ─────────────────────────────────────────────────────────────────────────────
# Grupo 3: Qualidade dos dados
# ─────────────────────────────────────────────────────────────────────────────

class TestQualidadeDados:

    def test_contagem_linhas_principal_preservada(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_principal=_df_vinculos(7))
        df = pd.read_excel(caminho, sheet_name="Principal")
        assert len(df) == 7

    def test_contagem_linhas_ghost_preservada(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=_df_ghost(4))
        df = pd.read_excel(caminho, sheet_name="Ghost_Payroll")
        assert len(df) == 4

    def test_colunas_originais_preservadas_em_ghost(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_ghost=_df_ghost())
        df = pd.read_excel(caminho, sheet_name="Ghost_Payroll")
        assert "CPF" in df.columns
        assert "MOTIVO_GHOST" in df.columns

    def test_colunas_originais_preservadas_em_missing(self, tmp_path: Path):
        caminho = tmp_path / "relatorio.xlsx"
        _gerar(caminho, df_missing=_df_missing())
        df = pd.read_excel(caminho, sheet_name="Missing_Registro")
        assert "CPF" in df.columns
        assert "STATUS" in df.columns
