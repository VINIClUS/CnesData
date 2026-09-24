"""Referências SIA (SIGTAP, CADMUN) -> Parquet versionado com qualidade explícita, sem SQL."""

from __future__ import annotations

import polars as pl

from data_processor.adapters.sia_dim_sync import build_reference_municipio, build_reference_sigtap
from data_processor.sources.sia.contract import (
    QualityCheck,
    SiaContractError,
    split_quality,
    with_source_row,
)

REFERENCE_SUBTYPES = frozenset({"DIM_SIGTAP", "DIM_MUNICIPIO"})


def normalize_reference(subtype: str, frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Canonicaliza uma referência SIA e separa as linhas retiradas.

    Returns:
        (referência canônica com `_source_row`, linhas de qualidade).

    Raises:
        SiaContractError: subtipo fora de REFERENCE_SUBTYPES.
    """
    if subtype == "DIM_SIGTAP":
        return _sigtap(frame)
    if subtype == "DIM_MUNICIPIO":
        return _municipio(frame)
    raise SiaContractError(f"sia_reference_unknown subtype={subtype}")


def _sigtap(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    canonical = with_source_row(build_reference_sigtap(frame))
    detail = pl.lit("co_procedimento=") + pl.col("codigo_origem").fill_null("")
    checks = (
        QualityCheck("codigo_procedimento_invalido", pl.col("cod_procedimento").is_null(), detail),
    )
    data, quality = split_quality(canonical, checks, ("cod_procedimento",))
    return data.drop("codigo_origem"), quality


def _municipio(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    canonical = with_source_row(build_reference_municipio(frame))
    detail = pl.lit("codmunic=") + pl.col("codmunic_origem").fill_null("")
    checks = (QualityCheck("codigo_municipio_invalido", pl.col("ibge6").is_null(), detail),)
    data, quality = split_quality(canonical, checks, ("ibge6",))
    return data.drop("codmunic_origem"), quality
