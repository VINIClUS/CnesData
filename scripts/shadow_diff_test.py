"""Teste do shadow diff."""
from pathlib import Path

import polars as pl

from scripts.shadow_diff import compare_parquets, normalize_df


def test_normalize_df_ordena_canonicamente() -> None:
    df = pl.DataFrame({"b": [2, 1], "a": ["y", "x"]})
    norm = normalize_df(df)
    assert norm.columns == sorted(df.columns)
    # sorted por todas colunas ascending
    assert norm.row(0) == ("x", 1)


def test_compare_parquets_identicos(tmp_path: Path) -> None:
    df = pl.DataFrame({"cnes": ["0001", "0002"], "nome": ["A", "B"]})
    p1 = tmp_path / "a.parquet"
    p2 = tmp_path / "b.parquet"
    df.write_parquet(p1)
    df.write_parquet(p2)
    result = compare_parquets(p1, p2)
    assert result.identical is True
    assert result.diff_rows == 0


def test_compare_parquets_difere(tmp_path: Path) -> None:
    a = pl.DataFrame({"cnes": ["0001"], "nome": ["A"]})
    b = pl.DataFrame({"cnes": ["0001"], "nome": ["B"]})
    pa = tmp_path / "a.parquet"
    pb = tmp_path / "b.parquet"
    a.write_parquet(pa)
    b.write_parquet(pb)
    result = compare_parquets(pa, pb)
    assert result.identical is False
    assert result.diff_rows == 1
