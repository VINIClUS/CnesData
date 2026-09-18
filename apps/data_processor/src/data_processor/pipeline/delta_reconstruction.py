"""Reconstroi o estado atual de um snapshot a partir de uma cadeia FULL+DELTA."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Sequence

_KEY = "__nk__"


def _key_expr(natural_key: tuple[str, ...]) -> pl.Expr:
    return pl.concat_str(
        [pl.col(column).cast(pl.Utf8).fill_null("") for column in natural_key],
        separator="\x1f",
    ).alias(_KEY)


def _split_delta(
    delta: pl.DataFrame, natural_key: tuple[str, ...]
) -> tuple[list[str], pl.DataFrame]:
    tagged = delta.with_columns(_key_expr(natural_key))
    delete_keys = tagged.filter(pl.col("_op") == "D").get_column(_KEY).to_list()
    upserts = (
        tagged.filter(pl.col("_op") != "D")
        .unique(subset=[_KEY], keep="last", maintain_order=True)
        .filter(~pl.col(_KEY).is_in(delete_keys))
    )
    return tagged.get_column(_KEY).to_list(), upserts.drop("_op")


def _apply_delta(
    current: pl.DataFrame, delta: pl.DataFrame, natural_key: tuple[str, ...]
) -> pl.DataFrame:
    touched_keys, upserts = _split_delta(delta, natural_key)
    remaining = current.filter(~pl.col(_KEY).is_in(touched_keys))
    return pl.concat([remaining, upserts], how="diagonal_relaxed")


def reconstruct_from_deltas(
    base: pl.DataFrame,
    deltas: Sequence[pl.DataFrame],
    natural_key: tuple[str, ...],
) -> pl.DataFrame:
    """Aplica deltas CDC (I/U/D) sobre a base FULL, na ordem recebida.

    Dentro de um mesmo delta, um `D` sobre uma chave vence qualquer `I`/`U`
    sobre a mesma chave; `I`/`U` repetidos na mesma chave colapsam no último.

    Args:
        base: snapshot FULL já canonicalizado.
        deltas: deltas em ordem de `sequence`, cada um com coluna `_op`.
        natural_key: colunas que compõem a chave natural (aceita nulos).

    Returns:
        DataFrame com o estado atual, sem a coluna de chave auxiliar.
    """
    current = base.with_columns(_key_expr(natural_key))
    for delta in deltas:
        current = _apply_delta(current, delta, natural_key)
    return current.drop(_KEY)
