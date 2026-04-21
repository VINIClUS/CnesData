"""Compara outputs Parquet Python↔Go row-a-row com normalização canônica.

Uso:
    python scripts/shadow_diff.py \\
        --python docs/fixtures/golden/cnes_profissionais.parquet \\
        --go /path/to/shadow/<job_id>.parquet.gz
"""
from __future__ import annotations

import argparse
import gzip
import io
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


@dataclass
class DiffResult:
    identical: bool
    diff_rows: int
    summary: str


def normalize_df(df: pl.DataFrame) -> pl.DataFrame:
    """Ordena colunas alfabeticamente e rows por todas as colunas ASC."""
    sorted_cols = sorted(df.columns)
    return df.select(sorted_cols).sort(by=sorted_cols)


def _load(path: Path) -> pl.DataFrame:
    data = path.read_bytes()
    if path.suffix == ".gz":
        data = gzip.decompress(data)
    return pl.read_parquet(io.BytesIO(data))


def compare_parquets(a: Path, b: Path) -> DiffResult:
    """Compara dois Parquets normalizados."""
    df_a = normalize_df(_load(a))
    df_b = normalize_df(_load(b))

    if df_a.columns != df_b.columns:
        return DiffResult(
            identical=False, diff_rows=-1,
            summary=f"column mismatch a={df_a.columns} b={df_b.columns}",
        )
    if df_a.height != df_b.height:
        return DiffResult(
            identical=False, diff_rows=abs(df_a.height - df_b.height),
            summary=f"row count mismatch a={df_a.height} b={df_b.height}",
        )

    differ = df_a.with_row_index("__idx").join(
        df_b.with_row_index("__idx"), on="__idx", suffix="_b", how="inner",
    )
    diff_count = 0
    for col in df_a.columns:
        diff_mask = differ[col] != differ[f"{col}_b"]
        diff_count += int(diff_mask.sum() or 0)

    if diff_count == 0:
        return DiffResult(identical=True, diff_rows=0, summary="identical")
    return DiffResult(
        identical=False, diff_rows=diff_count,
        summary=f"{diff_count} cell diffs",
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--go", type=Path, required=True)
    args = parser.parse_args()

    result = compare_parquets(args.python, args.go)
    logger.info(
        "diff_result identical=%s diff=%d summary=%s",
        result.identical, result.diff_rows, result.summary,
    )
    return 0 if result.identical else 1


if __name__ == "__main__":
    sys.exit(main())
