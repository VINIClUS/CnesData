"""
rules_engine.py — Camada de Análise: Motor de Regras de Auditoria

Implementa as regras de reconciliação definidas no data_dictionary.md.
Cada função recebe DataFrames já transformados e retorna os registros
que violam a regra, prontos para exportação como relatório.

Regras implementadas:
  - RQ-003-B: Profissionais com vínculos em múltiplas unidades.
  - RQ-005 (ACS/TACS): Agentes Comunitários de Saúde lotados em unidade incorreta.
  - RQ-005 (ACE/TACE): Agentes de Combate às Endemias lotados em unidade incorreta.
  - Ghost Payroll (WP-003): ativo no CNES, ausente ou inativo no RH.
  - Missing Registration (WP-004): ativo no RH, ausente no CNES.
  - RQ-006: Estabelecimentos presentes no local mas ausentes na base nacional.
  - RQ-007: Estabelecimentos presentes na base nacional mas ausentes no local.
  - RQ-008: Profissionais (por CNS) presentes no local mas ausentes na base nacional.
  - RQ-009: Profissionais (por CNS) presentes na base nacional mas ausentes no local.
  - RQ-010: Divergência de CBO entre local e nacional (mesmo CNS+CNES).
  - RQ-011: Divergência de carga horária entre local e nacional (mesmo CNS+CNES).

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

# ACE/TACE devem estar em: CS II (02), CCZ/COVEPE (69), Distrito (22), (15) ou
# Secretaria/Orgao Gestor (50) — lotacao administrativa valida no municipio 354130.
TP_UNID_VALIDOS_ACE_TACE: Final[frozenset[str]] = frozenset({"02", "69", "22", "15", "50"})


def detectar_multiplas_unidades(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-003-B: Identifica profissionais com vínculos em mais de uma unidade.

    Padrão esperado no município 354130: profissionais com carga mínima (2h)
    na Residência Terapêutica + carga principal no CAPS. O relatório gerado
    permite à equipe de RH validar se a dupla lotação é estrutural ou erro.

    Args:
        df: DataFrame transformado com colunas CPF e CNES.

    Returns:
        pd.DataFrame: Subconjunto dos profissionais multi-unidade, acrescido
            da coluna QTD_UNIDADES (contagem de unidades distintas por CPF).
    """
    contagem_unidades: pd.DataFrame = (
        df.groupby("CPF")["CNES"]
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

    Lotação correta (TIPO_UNIDADE): '01', '02', '15'.
    Profissionais em qualquer outro tipo são retornados para revisão.

    Args:
        df: DataFrame transformado com colunas CBO e TIPO_UNIDADE.

    Returns:
        pd.DataFrame: Registros de ACS/TACS fora da lotação correta.
    """
    mascara_cbo = df["CBO"].isin(CBOS_ACS_TACS)
    mascara_unidade_incorreta = ~df["TIPO_UNIDADE"].isin(TP_UNID_VALIDOS_ACS_TACS)

    resultado = df[mascara_cbo & mascara_unidade_incorreta].copy()

    logger.info(
        "RQ-005 (ACS/TACS): %d anomalia(s) de lotação encontrada(s).",
        len(resultado),
    )
    return resultado


_STATUS_ATIVO: str = "ATIVO"


def auditar_lotacao_ace_tace(df: pd.DataFrame) -> pd.DataFrame:
    """
    RQ-005 (Grupo ACE/TACE): Detecta ACE e TACE lotados em unidade incorreta.

    CBOs auditados (fonte: data_dictionary.md — RQ-005):
      - 515140: Agente de Combate às Endemias — CBO legado (em transição)
      - 322210: Técnico Agente de Combate — CBO legado (em transição)
      - 322260: Técnico em Agente de Combate às Endemias (TACE) — CBO atual

    Lotação correta (TIPO_UNIDADE): '02', '69', '22', '15'.
    Profissionais em qualquer outro tipo são retornados para revisão.

    Args:
        df: DataFrame transformado com colunas CBO e TIPO_UNIDADE.

    Returns:
        pd.DataFrame: Registros de ACE/TACE fora da lotação correta.
    """
    mascara_cbo = df["CBO"].isin(CBOS_ACE_TACE)
    mascara_unidade_incorreta = ~df["TIPO_UNIDADE"].isin(TP_UNID_VALIDOS_ACE_TACE)

    resultado = df[mascara_cbo & mascara_unidade_incorreta].copy()

    logger.info(
        "RQ-005 (ACE/TACE): %d anomalia(s) de lotação encontrada(s).",
        len(resultado),
    )
    return resultado


def detectar_folha_fantasma(
    df_cnes: pd.DataFrame,
    df_rh: pd.DataFrame,
) -> pd.DataFrame:
    """
    WP-003: Identifica profissionais ativos no CNES mas ausentes ou inativos no RH.

    Dois motivos possíveis:
      - AUSENTE_NO_RH: CPF não encontrado no sistema de RH.
      - INATIVO_NO_RH: CPF no RH mas STATUS ≠ 'ATIVO'.

    Args:
        df_cnes: DataFrame do Firebird (CPF já normalizado).
        df_rh: DataFrame do sistema de RH (CPF já normalizado, coluna STATUS).

    Returns:
        Subconjunto de df_cnes com coluna MOTIVO_GHOST adicionada.
    """
    if df_cnes.empty:
        return df_cnes.copy().assign(MOTIVO_GHOST=pd.Series(dtype=str))

    cpfs_rh_ativos: frozenset[str] = frozenset(
        df_rh.loc[df_rh["STATUS"] == _STATUS_ATIVO, "CPF"]
    )
    cpfs_rh_todos: frozenset[str] = frozenset(df_rh["CPF"])

    resultado = df_cnes.copy()
    mascara_ausente = ~resultado["CPF"].isin(cpfs_rh_todos)
    mascara_inativo = resultado["CPF"].isin(cpfs_rh_todos) & ~resultado["CPF"].isin(cpfs_rh_ativos)

    resultado["MOTIVO_GHOST"] = None
    resultado.loc[mascara_ausente, "MOTIVO_GHOST"] = "AUSENTE_NO_RH"
    resultado.loc[mascara_inativo, "MOTIVO_GHOST"] = "INATIVO_NO_RH"
    resultado = resultado[resultado["MOTIVO_GHOST"].notna()].reset_index(drop=True)

    logger.info(
        "ghost_payroll total=%d ausentes=%d inativos=%d",
        len(resultado),
        (resultado["MOTIVO_GHOST"] == "AUSENTE_NO_RH").sum(),
        (resultado["MOTIVO_GHOST"] == "INATIVO_NO_RH").sum(),
    )
    return resultado


def detectar_estabelecimentos_fantasma(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
) -> pd.DataFrame:
    """RQ-006: CNES presente no local mas AUSENTE na base nacional.

    Args:
        df_local: Estabelecimentos locais (schema padronizado, coluna CNES).
        df_nacional: Estabelecimentos nacionais (schema padronizado, coluna CNES).

    Returns:
        Subconjunto de df_local com estabelecimentos sem correspondência nacional.
    """
    cnes_locais_norm = df_local["CNES"].astype(str).str.strip()
    cnes_nacionais: frozenset[str] = frozenset(
        df_nacional["CNES"].dropna().astype(str).str.strip()
    )
    resultado = df_local[~cnes_locais_norm.isin(cnes_nacionais)].copy()
    logger.info("RQ-006: %d estabelecimento(s) fantasma detectado(s).", len(resultado))
    return resultado


def detectar_estabelecimentos_ausentes_local(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
    tipos_excluir: frozenset[str] | None = None,
) -> pd.DataFrame:
    """RQ-007: CNES presente na base nacional mas AUSENTE no local.

    Args:
        df_local: Estabelecimentos locais (schema padronizado, coluna CNES).
        df_nacional: Estabelecimentos nacionais (schema padronizado, coluna CNES).
        tipos_excluir: Tipos de unidade a excluir do escopo (ex: consultórios
            de outros mantenedores). Requer coluna TIPO_UNIDADE no df_nacional.

    Returns:
        Subconjunto de df_nacional com estabelecimentos sem correspondência local.
    """
    cnes_locais: frozenset[str] = frozenset(
        df_local["CNES"].dropna().astype(str).str.strip()
    )
    df_escopo = df_nacional
    if tipos_excluir and "TIPO_UNIDADE" in df_nacional.columns:
        df_escopo = df_nacional[
            ~df_nacional["TIPO_UNIDADE"].astype(str).isin(tipos_excluir)
        ]
    cnes_escopo_norm = df_escopo["CNES"].astype(str).str.strip()
    resultado = df_escopo[~cnes_escopo_norm.isin(cnes_locais)].copy()
    logger.info("RQ-007: %d estabelecimento(s) ausente(s) no local.", len(resultado))
    return resultado


def detectar_profissionais_fantasma(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
) -> pd.DataFrame:
    """RQ-008: CNS presente no local mas AUSENTE na base nacional.

    Args:
        df_local: Profissionais locais (schema padronizado, coluna CNS).
        df_nacional: Profissionais nacionais (schema padronizado, coluna CNS).

    Returns:
        Subconjunto de df_local com vínculos sem correspondência nacional por CNS.
    """
    df_local_com_cns = df_local[df_local["CNS"].notna()]
    cns_locais_norm = df_local_com_cns["CNS"].astype(str).str.strip()
    cns_nacionais: frozenset[str] = frozenset(
        df_nacional["CNS"].dropna().astype(str).str.strip()
    )
    resultado = df_local_com_cns[~cns_locais_norm.isin(cns_nacionais)].copy()
    logger.info(
        "RQ-008: %d vínculo(s) fantasma, %d CNS único(s).",
        len(resultado),
        resultado["CNS"].nunique(),
    )
    return resultado


def detectar_profissionais_ausentes_local(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
    cnes_excluir: frozenset[str] | None = None,
) -> pd.DataFrame:
    """RQ-009: CNS presente na base nacional mas AUSENTE no local.

    Args:
        df_local: Profissionais locais (schema padronizado, coluna CNS).
        df_nacional: Profissionais nacionais (schema padronizado, coluna CNS).
        cnes_excluir: CNES cujos profissionais devem ser ignorados — tipicamente
            os CNES já detectados pelo RQ-007, evitando falsos positivos em cascata.

    Returns:
        Subconjunto de df_nacional com vínculos sem correspondência local por CNS.
    """
    cns_locais: frozenset[str] = frozenset(
        df_local["CNS"].dropna().astype(str).str.strip()
    )
    df_escopo = df_nacional[df_nacional["CNS"].notna()]
    if cnes_excluir:
        df_escopo = df_escopo[~df_escopo["CNES"].astype(str).str.strip().isin(cnes_excluir)]
    cns_escopo_norm = df_escopo["CNS"].astype(str).str.strip()
    resultado = df_escopo[~cns_escopo_norm.isin(cns_locais)].copy()
    logger.info("RQ-009: %d profissional(is) ausente(s) no local.", len(resultado))
    return resultado


def detectar_divergencia_cbo(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
    cbo_lookup: dict[str, str] | None = None,
) -> pd.DataFrame:
    """RQ-010: Mesmo profissional+estabelecimento com CBO diferente entre fontes.

    Args:
        df_local: Profissionais locais (colunas CNS, CNES, CBO).
        df_nacional: Profissionais nacionais (colunas CNS, CNES, CBO).
        cbo_lookup: Dict CBO → descrição para enriquecimento visual opcional.

    Returns:
        DataFrame com pares (CNS, CNES) divergentes e colunas CBO_LOCAL, CBO_NACIONAL.
    """
    df_l = df_local[["CNS", "CNES", "CBO"]].rename(columns={"CBO": "CBO_LOCAL"})
    df_n = df_nacional[["CNS", "CNES", "CBO"]].rename(columns={"CBO": "CBO_NACIONAL"})
    merged = df_l.merge(df_n, on=["CNS", "CNES"], how="inner")
    resultado = merged[merged["CBO_LOCAL"] != merged["CBO_NACIONAL"]].copy()
    if cbo_lookup is not None and not resultado.empty:
        resultado["DESCRICAO_CBO_LOCAL"] = (
            resultado["CBO_LOCAL"].map(cbo_lookup).fillna("CBO NAO CATALOGADO")
        )
        resultado["DESCRICAO_CBO_NACIONAL"] = (
            resultado["CBO_NACIONAL"].map(cbo_lookup).fillna("CBO NAO CATALOGADO")
        )
    logger.info("RQ-010: %d divergência(s) de CBO.", len(resultado))
    return resultado


def detectar_divergencia_carga_horaria(
    df_local: pd.DataFrame,
    df_nacional: pd.DataFrame,
    tolerancia: int = 2,
) -> pd.DataFrame:
    """RQ-011: Mesmo profissional+estabelecimento com carga horária divergente.

    Args:
        df_local: Profissionais locais (colunas CNS, CNES, CH_TOTAL).
        df_nacional: Profissionais nacionais (colunas CNS, CNES, CH_TOTAL).
        tolerancia: Diferença mínima em horas para ser considerada divergência.

    Returns:
        DataFrame com pares divergentes e colunas CH_LOCAL, CH_NACIONAL, DELTA_CH.
    """
    df_l = df_local[["CNS", "CNES", "CH_TOTAL"]].rename(columns={"CH_TOTAL": "CH_LOCAL"})
    df_n = df_nacional[["CNS", "CNES", "CH_TOTAL"]].rename(columns={"CH_TOTAL": "CH_NACIONAL"})
    merged = df_l.merge(df_n, on=["CNS", "CNES"], how="inner")
    merged["DELTA_CH"] = (merged["CH_LOCAL"] - merged["CH_NACIONAL"]).abs()
    resultado = merged[merged["DELTA_CH"] > tolerancia].copy()
    logger.info("RQ-011: %d divergência(s) de carga horária.", len(resultado))
    return resultado


def detectar_registro_ausente(
    df_cnes: pd.DataFrame,
    df_rh: pd.DataFrame,
) -> pd.DataFrame:
    """
    WP-004: Identifica profissionais ativos no RH mas ausentes no CNES local.

    Apenas STATUS='ATIVO' no RH é considerado anomalia. STATUS='INATIVO' ou
    'AFASTADO' são desconsiderados — profissional desligado ou em licença
    não precisa de vínculo CNES ativo.

    Args:
        df_cnes: DataFrame do Firebird (CPF já normalizado).
        df_rh: DataFrame do sistema de RH (CPF já normalizado, coluna STATUS).

    Returns:
        Subconjunto de df_rh (STATUS=ATIVO) com CPF ausente no CNES.
    """
    if df_rh.empty:
        return df_rh.copy()

    cpfs_cnes: frozenset[str] = frozenset(df_cnes["CPF"])
    rh_ativos = df_rh[df_rh["STATUS"] == _STATUS_ATIVO]
    resultado = rh_ativos[~rh_ativos["CPF"].isin(cpfs_cnes)].copy().reset_index(drop=True)

    logger.info(
        "missing_registration total=%d",
        len(resultado),
    )
    return resultado
