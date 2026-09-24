"""SIA referências: S_CDN -> SIGTAP canônico; CADMUN -> município canônico (sem SQL)."""
from __future__ import annotations

import polars as pl

from data_processor.adapters.sia_adapter import clean_text, require_schema, text_schema

_CDN_SCHEMA = text_schema("cdn_tb", "cdn_it", "cdn_dscr", "cdn_chksm")
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
    """Mapeia S_CDN para `tabela, item, descricao, checksum`, preservando todas as tabelas.

    Raises:
        ValueError: schema raw fora do contrato Edge.
    """
    require_schema(frame, "DIM_SIGTAP", _CDN_SCHEMA)
    return frame.select(
        clean_text("cdn_tb").alias("tabela"),
        clean_text("cdn_it").alias("item"),
        clean_text("cdn_dscr").alias("descricao"),
        clean_text("cdn_chksm").alias("checksum"),
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
