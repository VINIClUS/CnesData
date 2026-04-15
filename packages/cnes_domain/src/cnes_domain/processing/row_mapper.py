"""Row mapper -- converte DataFrames canonicos em dicts prontos para persistencia."""

import math

import polars as pl

from cnes_domain.tenant import get_tenant_id


def _nan_to_none(rows: list[dict]) -> list[dict]:
    for row in rows:
        for k, v in row.items():
            if v is not None and isinstance(v, float) and math.isnan(v):
                row[k] = None
    return rows


def _fonte_jsonb(fonte: str) -> dict:
    return {fonte: True}


def mapear_profissionais(df: pl.DataFrame) -> list[dict]:
    tid = get_tenant_id()
    dedup = df.unique(subset=["CPF"])
    out = dedup.select("CPF", "CNS", "NOME_PROFISSIONAL", "SEXO", "FONTE")
    out = out.rename({
        "CPF": "cpf",
        "CNS": "cns",
        "NOME_PROFISSIONAL": "nome_profissional",
        "SEXO": "sexo",
        "FONTE": "fonte",
    })
    out = out.with_columns(
        pl.lit(tid).alias("tenant_id"),
        pl.col("fonte")
        .map_elements(_fonte_jsonb, return_dtype=pl.Object)
        .alias("fontes"),
    )
    return _nan_to_none(
        out.select(
            "tenant_id", "cpf", "cns",
            "nome_profissional", "sexo", "fontes",
        ).to_dicts()
    )


def mapear_vinculos(
    competencia: str,
    df: pl.DataFrame,
) -> list[dict]:
    tid = get_tenant_id()
    out = df.select(
        "CPF", "CNES", "CBO", "TIPO_VINCULO", "SUS",
        "CH_TOTAL", "CH_AMBULATORIAL", "CH_OUTRAS",
        "CH_HOSPITALAR", "FONTE",
    ).rename({
        "CPF": "cpf",
        "CNES": "cnes",
        "CBO": "cbo",
        "TIPO_VINCULO": "tipo_vinculo",
        "SUS": "sus",
        "CH_TOTAL": "ch_total",
        "CH_AMBULATORIAL": "ch_ambulatorial",
        "CH_OUTRAS": "ch_outras",
        "CH_HOSPITALAR": "ch_hospitalar",
        "FONTE": "fonte",
    })
    out = out.with_columns(
        pl.lit(tid).alias("tenant_id"),
        pl.lit(competencia).alias("competencia"),
        pl.when(pl.col("sus") == "S")
        .then(True)
        .when(pl.col("sus") == "N")
        .then(False)
        .otherwise(None)
        .alias("sus"),
        pl.col("fonte")
        .map_elements(_fonte_jsonb, return_dtype=pl.Object)
        .alias("fontes"),
    )
    return _nan_to_none(
        out.select(
            "tenant_id", "competencia", "cpf", "cnes", "cbo",
            "tipo_vinculo", "sus", "ch_total", "ch_ambulatorial",
            "ch_outras", "ch_hospitalar", "fontes",
        ).to_dicts()
    )


def mapear_estabelecimentos(df: pl.DataFrame) -> list[dict]:
    tid = get_tenant_id()
    out = df.select(
        "CNES", "NOME_FANTASIA", "TIPO_UNIDADE",
        "CNPJ_MANTENEDORA", "NATUREZA_JURIDICA",
        "VINCULO_SUS", "FONTE",
    ).rename({
        "CNES": "cnes",
        "NOME_FANTASIA": "nome_fantasia",
        "TIPO_UNIDADE": "tipo_unidade",
        "CNPJ_MANTENEDORA": "cnpj_mantenedora",
        "NATUREZA_JURIDICA": "natureza_juridica",
        "VINCULO_SUS": "vinculo_sus",
        "FONTE": "fonte",
    })
    out = out.with_columns(
        pl.lit(tid).alias("tenant_id"),
        pl.when(pl.col("vinculo_sus") == "S")
        .then(True)
        .when(pl.col("vinculo_sus") == "N")
        .then(False)
        .otherwise(None)
        .alias("vinculo_sus"),
        pl.col("fonte")
        .map_elements(_fonte_jsonb, return_dtype=pl.Object)
        .alias("fontes"),
    )
    return _nan_to_none(
        out.select(
            "tenant_id", "cnes", "nome_fantasia", "tipo_unidade",
            "cnpj_mantenedora", "natureza_juridica",
            "vinculo_sus", "fontes",
        ).to_dicts()
    )


def extrair_fonte(df: pl.DataFrame) -> str:
    if df.is_empty():
        raise ValueError("dataframe_vazio")
    fontes = df["FONTE"].unique()
    if len(fontes) != 1:
        raise ValueError(f"fonte_mista count={len(fontes)}")
    return fontes[0]
