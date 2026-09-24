"""SIA referências: SIGTAP tb_procedimento e CADMUN -> frames canônicos (sem SQL)."""
from __future__ import annotations

import polars as pl

from data_processor.adapters.sia_adapter import clean_text, require_schema, text_schema

# SIGTAP Tabela Unificada tb_procedimento layout (DATASUS), lower-cased; S_CDN is a
# generic 2-char/8-char code domain and carries no 10-digit SIGTAP procedure.
_SIGTAP_SCHEMA = text_schema(
    "co_procedimento", "no_procedimento", "tp_complexidade", "co_financiamento", "dt_competencia",
)
_CADMUN_SCHEMA = text_schema("coduf", "codmunic", "nome")


def _ibge7_check_digit(ibge6: pl.Expr) -> pl.Expr:
    total = pl.lit(0, dtype=pl.Int64)
    for position in range(6):
        digit = ibge6.str.slice(position, 1).cast(pl.Int64)
        if position % 2 == 1:
            doubled = digit * 2
            digit = doubled // 10 + doubled % 10
        total = total + digit
    return ((10 - total % 10) % 10).cast(pl.String)


def build_reference_sigtap(frame: pl.DataFrame) -> pl.DataFrame:
    """Mapeia tb_procedimento do SIGTAP; código fora de 10 dígitos vira null.

    Raises:
        ValueError: schema raw fora do contrato SIGTAP.
    """
    require_schema(frame, "DIM_SIGTAP", _SIGTAP_SCHEMA)
    code = clean_text("co_procedimento")
    competencia = clean_text("dt_competencia")
    return frame.select(
        pl.when(code.str.contains(r"^\d{10}$")).then(code).alias("cod_procedimento"),
        clean_text("no_procedimento").alias("descricao"),
        clean_text("tp_complexidade").alias("complexidade"),
        clean_text("co_financiamento").alias("financiamento"),
        pl.when(competencia.str.contains(r"^\d{6}$"))
        .then(competencia.str.slice(0, 4) + "-" + competencia.str.slice(4, 2))
        .alias("competencia_sigtap"),
        code.alias("codigo_origem"),
    )


def build_reference_municipio(frame: pl.DataFrame) -> pl.DataFrame:
    """Mapeia CADMUN para `ibge6, ibge7, uf, nome`; código inválido vira ibge6 null.

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, "DIM_MUNICIPIO", _CADMUN_SCHEMA)
    code = clean_text("codmunic")
    uf = clean_text("coduf")
    numeric = code.str.contains(r"^\d+$")
    length = code.str.len_chars()
    candidate = (
        pl.when(numeric & (length == 4) & uf.str.contains(r"^\d{2}$")).then(uf + code)
        .when(numeric & (length == 6)).then(code)
        .when(numeric & (length == 7)).then(code.str.slice(0, 6))
    )
    ibge7 = candidate + _ibge7_check_digit(candidate)
    valid = candidate.is_not_null() & ((length != 7) | (ibge7 == code))
    return frame.select(
        pl.when(valid).then(candidate).alias("ibge6"),
        pl.when(valid).then(ibge7).alias("ibge7"),
        uf.alias("uf"),
        clean_text("nome").alias("nome"),
        code.alias("codmunic_origem"),
    )
