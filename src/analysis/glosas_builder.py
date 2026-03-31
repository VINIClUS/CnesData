"""Converte DataFrames de auditoria em schema gold.glosas_profissional."""
from datetime import datetime
from typing import Optional

import pandas as pd

from pipeline.state import PipelineState

_CANONICAL_COLS = ["cpf", "cns", "nome_profissional", "sexo", "cnes_estabelecimento", "motivo"]

_OUTPUT_COLS = [
    "competencia",
    "regra",
    "cpf",
    "cns",
    "nome_profissional",
    "sexo",
    "cnes_estabelecimento",
    "motivo",
    "criado_em_firebird",
    "criado_em_pipeline",
    "atualizado_em_pipeline",
]


def _empty_canonical() -> pd.DataFrame:
    return pd.DataFrame(columns=_CANONICAL_COLS)


def _extrair_generico(
    df: pd.DataFrame,
    cpf_col: Optional[str],
    cns_col: Optional[str],
    nome_col: Optional[str],
    sexo_col: Optional[str],
    cnes_col: Optional[str],
    motivo_col: Optional[str],
    motivo_literal: Optional[str] = None,
) -> pd.DataFrame:
    if df.empty:
        return _empty_canonical()

    result = pd.DataFrame(index=df.index)
    result["cpf"] = df[cpf_col] if cpf_col and cpf_col in df.columns else None
    result["cns"] = df[cns_col] if cns_col and cns_col in df.columns else None
    result["nome_profissional"] = df[nome_col] if nome_col and nome_col in df.columns else None
    result["sexo"] = df[sexo_col] if sexo_col and sexo_col in df.columns else None
    result["cnes_estabelecimento"] = df[cnes_col] if cnes_col and cnes_col in df.columns else None

    if motivo_literal is not None:
        result["motivo"] = motivo_literal
    elif motivo_col and motivo_col in df.columns:
        result["motivo"] = df[motivo_col]
    else:
        result["motivo"] = None

    return result[_CANONICAL_COLS]


def _extrair_rq003b(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, "CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "CNES", None)


def _extrair_rq005_acs(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, "CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "CNES", None)


def _extrair_rq005_ace(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, "CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "CNES", None)


def _extrair_ghost(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, "CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "CNES", "MOTIVO_GHOST")


def _extrair_missing(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, "CPF", None, "NOME_PROFISSIONAL", None, None, None)


def _extrair_rq008(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, None, "CNS", "NOME_PROFISSIONAL", None, "CNES", None)


def _extrair_rq009(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(df, None, "CNS", "NOME_PROFISSIONAL", None, "CNES", None)


def _extrair_rq010(df: pd.DataFrame) -> pd.DataFrame:
    return _extrair_generico(
        df, None, "CNS", None, None, "CNES", None, motivo_literal="CBO_LOCAL != CBO_NACIONAL"
    )


def _extrair_rq011(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return _empty_canonical()

    base = _extrair_generico(df, None, "CNS", None, None, "CNES", None)

    if "DELTA_CH" in df.columns:
        base["motivo"] = df["DELTA_CH"].apply(lambda v: f"DELTA_CH={v}")
    else:
        base["motivo"] = None

    return base[_CANONICAL_COLS]


def construir_glosas(
    competencia: str,
    state: PipelineState,
    criado_em_pipeline: datetime,
) -> pd.DataFrame:
    """Retorna DataFrame com schema gold.glosas_profissional para INSERT.

    Args:
        competencia: Competência no formato 'YYYY-MM'.
        state: Estado do pipeline com DataFrames de auditoria preenchidos.
        criado_em_pipeline: Timestamp da execução do pipeline.

    Returns:
        DataFrame com colunas: competencia, regra, cpf, cns, nome_profissional,
        sexo, cnes_estabelecimento, motivo, criado_em_firebird, criado_em_pipeline,
        atualizado_em_pipeline.
    """
    regras = [
        ("RQ003B", _extrair_rq003b(state.df_multi_unidades)),
        ("RQ005_ACS", _extrair_rq005_acs(state.df_acs_incorretos)),
        ("RQ005_ACE", _extrair_rq005_ace(state.df_ace_incorretos)),
        ("GHOST", _extrair_ghost(state.df_ghost)),
        ("MISSING", _extrair_missing(state.df_missing)),
        ("RQ008", _extrair_rq008(state.df_prof_fantasma)),
        ("RQ009", _extrair_rq009(state.df_prof_ausente)),
        ("RQ010", _extrair_rq010(state.df_cbo_diverg)),
        ("RQ011", _extrair_rq011(state.df_ch_diverg)),
    ]

    frames = []
    for regra, df in regras:
        if df.empty:
            continue
        df = df.copy()
        df.insert(0, "regra", regra)
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=_OUTPUT_COLS)

    result = pd.concat(frames, ignore_index=True)
    result.insert(0, "competencia", competencia)
    result["criado_em_firebird"] = None
    result["criado_em_pipeline"] = criado_em_pipeline
    result["atualizado_em_pipeline"] = criado_em_pipeline

    return result[_OUTPUT_COLS]
