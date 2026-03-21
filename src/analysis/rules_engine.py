"""
rules_engine.py — Camada de Análise: Motor de Regras de Auditoria

Implementa as regras de reconciliação definidas no data_dictionary.md.
Cada função recebe um DataFrame já transformado e retorna um subconjunto
com os registros que violam a regra, prontos para exportação como relatório.

Regras implementadas:
  - RQ-003-B: Profissionais com vínculos em múltiplas unidades.
  - RQ-005 (ACS/TACS): Agentes Comunitários de Saúde lotados em unidade incorreta.
  - RQ-005 (ACE/TACE): Agentes de Combate às Endemias lotados em unidade incorreta.

Regras pendentes (dependem de hr_client.py — WP-001):
  - Ghost Payroll: ativo no CNES, inativo ou ausente no RH.
  - Missing Registration: no RH mas ausente/desatualizado no CNES.

Fonte de verdade dos domínios: data_dictionary.md (seção RQ-005).
"""

import logging
from typing import Final

import pandas as pd

logger = logging.getLogger(__name__)

# ── Domínios de CBO por grupo (fonte: data_dictionary.md — RQ-005) ───────────

CBOS_ACS_TACS: Final[frozenset[str]] = frozenset({
    "515105",  # Agente Comunitário de Saúde (ACS)
    "322255",  # Técnico em Agente Comunitário de Saúde (TACS)
})

CBOS_ACE_TACE: Final[frozenset[str]] = frozenset({
    "515140",  # Agente de Combate às Endemias — CBO legado (em transição)
    "322210",  # Técnico Agente de Combate — CBO legado (em transição)
    "322260",  # Técnico em Agente de Combate às Endemias (TACE) — CBO atual
})

# ── TP_UNID_ID válidos por grupo (fonte: data_dictionary.md — RQ-005) ────────

# ACS/TACS devem estar em: UBS (01), CS II (02) ou equivalente (15).
TP_UNID_VALIDOS_ACS_TACS: Final[frozenset[str]] = frozenset({"01", "02", "15"})

# ACE/TACE devem estar em: CS II (02), CCZ/COVEPE (69), Distrito (22) ou (15).
TP_UNID_VALIDOS_ACE_TACE: Final[frozenset[str]] = frozenset({"02", "69", "22", "15"})


def detectar_multiplas_unidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-003-B: Identifica profissionais com vínculos em mais de uma unidade.

    Padrão esperado no município 354130: profissionais com carga mínima (2h)
    na Residência Terapêutica + carga principal no CAPS. O relatório gerado
    permite à equipe de RH validar se a dupla lotação é estrutural ou erro.

    Args:
        df: DataFrame transformado com colunas CPF e COD_CNES.

    Returns:
        pd.DataFrame: Subconjunto dos profissionais multi-unidade, acrescido
            da coluna QTD_UNIDADES (contagem de unidades distintas por CPF).
    """
    contagem_unidades: pd.DataFrame = (
        df.groupby("CPF")["COD_CNES"]
        .nunique()
        .rename("QTD_UNIDADES")
        .reset_index()
    )

    multi_unidades = contagem_unidades[contagem_unidades["QTD_UNIDADES"] > 1]
    resultado = df.merge(multi_unidades[["CPF", "QTD_UNIDADES"]], on="CPF", how="inner")

    logger.info(
        "RQ-003-B: %d profissional(is) com vínculos em múltiplas unidades (%d registros).",
        multi_unidades["CPF"].nunique(),
        len(resultado),
    )
    return resultado


def auditar_lotacao_acs_tacs(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-005 (Grupo ACS/TACS): Detecta ACS e TACS lotados em unidade incorreta.

    CBOs auditados (fonte: data_dictionary.md — RQ-005):
      - 515105: Agente Comunitário de Saúde (ACS)
      - 322255: Técnico em Agente Comunitário de Saúde (TACS)

    Lotação correta (COD_TIPO_UNIDADE): '01', '02', '15'.
    Profissionais em qualquer outro tipo são retornados para revisão.

    Args:
        df: DataFrame transformado com colunas CBO e COD_TIPO_UNIDADE.

    Returns:
        pd.DataFrame: Registros de ACS/TACS fora da lotação correta.
    """
    mascara_cbo = df["CBO"].isin(CBOS_ACS_TACS)
    mascara_unidade_incorreta = ~df["COD_TIPO_UNIDADE"].isin(TP_UNID_VALIDOS_ACS_TACS)

    resultado = df[mascara_cbo & mascara_unidade_incorreta].copy()

    logger.info(
        "RQ-005 (ACS/TACS): %d anomalia(s) de lotação encontrada(s).",
        len(resultado),
    )
    return resultado


def auditar_lotacao_ace_tace(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-005 (Grupo ACE/TACE): Detecta ACE e TACE lotados em unidade incorreta.

    CBOs auditados (fonte: data_dictionary.md — RQ-005):
      - 515140: Agente de Combate às Endemias — CBO legado (em transição)
      - 322210: Técnico Agente de Combate — CBO legado (em transição)
      - 322260: Técnico em Agente de Combate às Endemias (TACE) — CBO atual

    Lotação correta (COD_TIPO_UNIDADE): '02', '69', '22', '15'.
    Profissionais em qualquer outro tipo são retornados para revisão.

    Args:
        df: DataFrame transformado com colunas CBO e COD_TIPO_UNIDADE.

    Returns:
        pd.DataFrame: Registros de ACE/TACE fora da lotação correta.
    """
    mascara_cbo = df["CBO"].isin(CBOS_ACE_TACE)
    mascara_unidade_incorreta = ~df["COD_TIPO_UNIDADE"].isin(TP_UNID_VALIDOS_ACE_TACE)

    resultado = df[mascara_cbo & mascara_unidade_incorreta].copy()

    logger.info(
        "RQ-005 (ACE/TACE): %d anomalia(s) de lotação encontrada(s).",
        len(resultado),
    )
    return resultado
