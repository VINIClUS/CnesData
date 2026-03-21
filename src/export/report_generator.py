"""Gerador de relatórios Excel multi-aba segmentados por tipo de anomalia — WP-007."""

import logging
from pathlib import Path

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
import pandas as pd

logger = logging.getLogger(__name__)

_RECOMENDACOES: dict[str, str] = {
    "ghost": (
        "Verificar situação no RH e regularizar vínculo CNES ou desligar profissional."
    ),
    "missing": (
        "Cadastrar profissional no CNES local para a competência vigente."
    ),
    "multi_unidades": (
        "Validar se a dupla lotação é estrutural ou erro de cadastro."
    ),
    "acs_tacs": (
        "Revisar lotação: ACS/TACS deve estar em UBS (01), CS II (02) ou equivalente (15)."
    ),
    "ace_tace": (
        "Revisar lotação: ACE/TACE deve estar em CS II (02), CCZ (69), Distrito (22) ou (15)."
    ),
}

_COR_CABECALHO = "1F4E79"
_COR_FONTE_CABECALHO = "FFFFFF"


def gerar_relatorio(
    caminho: Path,
    df_principal: pd.DataFrame,
    df_ghost: pd.DataFrame,
    df_missing: pd.DataFrame,
    df_multi_unidades: pd.DataFrame,
    df_acs: pd.DataFrame,
    df_ace: pd.DataFrame,
) -> None:
    """Gera relatório Excel multi-aba com DataFrames de auditoria segmentados.

    Abas criadas apenas quando o DataFrame correspondente não é vazio.
    Todas as abas de auditoria recebem coluna RECOMENDACAO preenchida.

    Args:
        caminho: Caminho do arquivo .xlsx de saída (diretório criado se inexistente).
        df_principal: Todos os vínculos processados.
        df_ghost: Anomalias Ghost Payroll.
        df_missing: Anomalias Missing Registration.
        df_multi_unidades: Anomalias RQ-003-B.
        df_acs: Anomalias RQ-005 ACS/TACS.
        df_ace: Anomalias RQ-005 ACE/TACE.
    """
    caminho.parent.mkdir(parents=True, exist_ok=True)

    abas_auditoria: list[tuple[str, pd.DataFrame, str]] = [
        ("Ghost_Payroll",       df_ghost,          "ghost"),
        ("Missing_Registro",    df_missing,         "missing"),
        ("Multi_Unidades",      df_multi_unidades,  "multi_unidades"),
        ("ACS_TACS_Incorretos", df_acs,             "acs_tacs"),
        ("ACE_TACE_Incorretos", df_ace,             "ace_tace"),
    ]

    with pd.ExcelWriter(caminho, engine="openpyxl") as writer:
        df_principal.to_excel(writer, sheet_name="Principal", index=False)
        _formatar_cabecalho(writer.sheets["Principal"])

        for nome_aba, df, chave_rec in abas_auditoria:
            if df.empty:
                continue
            df_com_rec = _adicionar_recomendacao(df, _RECOMENDACOES[chave_rec])
            df_com_rec.to_excel(writer, sheet_name=nome_aba, index=False)
            _formatar_cabecalho(writer.sheets[nome_aba])

    total_abas = 1 + sum(1 for _, df, _ in abas_auditoria if not df.empty)
    logger.info(
        "relatorio gerado caminho=%s abas=%d vinculos=%d",
        caminho, total_abas, len(df_principal),
    )


def _adicionar_recomendacao(df: pd.DataFrame, texto: str) -> pd.DataFrame:
    resultado = df.copy()
    resultado["RECOMENDACAO"] = texto
    return resultado


def _formatar_cabecalho(ws) -> None:
    fonte = Font(bold=True, color=_COR_FONTE_CABECALHO)
    preenchimento = PatternFill(fill_type="solid", fgColor=_COR_CABECALHO)
    alinhamento = Alignment(horizontal="center", vertical="center", wrap_text=True)

    for celula in ws[1]:
        celula.font = fonte
        celula.fill = preenchimento
        celula.alignment = alinhamento

    for coluna in ws.columns:
        largura = max(
            len(str(celula.value)) if celula.value is not None else 0
            for celula in coluna
        )
        ws.column_dimensions[coluna[0].column_letter].width = min(largura + 4, 60)
