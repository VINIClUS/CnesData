"""Compara outputs Parquet Python↔Go row-a-row com normalização canônica.

Uso:
    python scripts/shadow_diff.py \\
        --python docs/fixtures/golden/cnes_profissionais.parquet \\
        --go /path/to/shadow/<job_id>.parquet.gz [--key CNES --key CBO] [--show-values]

Por padrao o log traz so tipo, coluna e um token da chave (HMAC com segredo aleatorio por
execucao: agrupa as diferencas da mesma linha, mas nao e reversivel por dicionario nem
correlacionavel entre execucoes); `--show-values` expoe chaves e valores brutos (dados
pessoais em saidas de producao).
"""
from __future__ import annotations

import argparse
import gzip
import hmac
import io
import logging
import math
import secrets
import sys
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

_MAX_LOGGED_DIFFERENCES = 100
_DIGEST_CHARS = 12

type DifferenceKind = Literal["cell", "left_only", "right_only", "duplicate_key"]
type _Row = dict[str, object]


@dataclass(frozen=True)
class Difference:
    kind: DifferenceKind
    key: tuple[str, ...]
    column: str | None
    left: object
    right: object


@dataclass
class DiffResult:
    identical: bool
    diff_rows: int
    summary: str
    differences: tuple[Difference, ...] = ()


def normalize_df(df: pl.DataFrame) -> pl.DataFrame:
    """Ordena colunas alfabeticamente e rows por todas as colunas ASC."""
    sorted_cols = sorted(df.columns)
    return df.select(sorted_cols).sort(by=sorted_cols)


def _load(path: Path) -> pl.DataFrame:
    data = path.read_bytes()
    if path.suffix == ".gz":
        data = gzip.decompress(data)
    return pl.read_parquet(io.BytesIO(data))


def _extra_rows(a: pl.DataFrame, b: pl.DataFrame) -> list[Difference]:
    shared = min(a.height, b.height)
    left = [Difference("left_only", (str(i),), None, None, None) for i in range(shared, a.height)]
    right = [Difference("right_only", (str(i),), None, None, None) for i in range(shared, b.height)]
    return left + right


def _positional_differences(a: pl.DataFrame, b: pl.DataFrame) -> list[Difference]:
    shared = min(a.height, b.height)
    found: list[Difference] = []
    for column in a.columns:
        indices = a[column].head(shared).ne_missing(b[column].head(shared)).arg_true()
        pairs = zip(
            indices.to_list(), a[column].gather(indices).to_list(),
            b[column].gather(indices).to_list(), strict=True,
        )
        found.extend(Difference("cell", (str(i),), column, left, right) for i, left, right in pairs)
    return found + _extra_rows(a, b)


def _same_items(left: list[object], right: list[object]) -> bool:
    return len(left) == len(right) and all(_same(a, b) for a, b in zip(left, right, strict=True))


def _same_fields(left: dict[str, object], right: dict[str, object]) -> bool:
    return left.keys() == right.keys() and all(_same(left[name], right[name]) for name in left)


def _same(left: object, right: object) -> bool:
    if type(left) is not type(right):
        return False
    if isinstance(left, float):
        return left == right or (math.isnan(left) and math.isnan(cast("float", right)))
    if isinstance(left, list):
        return _same_items(cast("list[object]", left), cast("list[object]", right))
    if isinstance(left, dict):
        return _same_fields(cast("dict[str, object]", left), cast("dict[str, object]", right))
    return left == right


def _group(frame: pl.DataFrame, key: Sequence[str]) -> dict[tuple[object, ...], list[_Row]]:
    groups: dict[tuple[object, ...], list[_Row]] = {}
    for row in frame.to_dicts():
        groups.setdefault(tuple(row[name] for name in key), []).append(row)
    return groups


def _key_differences(
    label: tuple[str, ...], left: list[_Row], right: list[_Row]
) -> list[Difference]:
    if len(left) > 1 or len(right) > 1:
        return [Difference("duplicate_key", label, None, len(left), len(right))]
    if not right:
        return [Difference("left_only", label, None, None, None)]
    if not left:
        return [Difference("right_only", label, None, None, None)]
    return [
        Difference("cell", label, column, left[0][column], right[0][column])
        for column in sorted(left[0])
        if not _same(left[0][column], right[0][column])
    ]


def _keyed_differences(a: pl.DataFrame, b: pl.DataFrame, key: Sequence[str]) -> list[Difference]:
    left, right = _group(a, key), _group(b, key)
    labelled = {tuple(str(part) for part in name): name for name in left.keys() | right.keys()}
    found: list[Difference] = []
    for label in sorted(labelled):
        name = labelled[label]
        found.extend(_key_differences(label, left.get(name, []), right.get(name, [])))
    return found


def diff_frames(
    a: pl.DataFrame, b: pl.DataFrame, key: Sequence[str] | None = None
) -> tuple[Difference, ...]:
    """Diferenças estruturadas por célula, linha ausente por lado e chave duplicada.

    Args: a/b: frames com as mesmas colunas. key: colunas da chave; None compara por posição.
    Returns: diferenças ordenadas por chave; nulo contra valor conta como diferença.
    Raises: ValueError: colunas distintas ou coluna da chave ausente.
    """
    if set(a.columns) != set(b.columns):
        raise ValueError(f"column_mismatch left={sorted(a.columns)} right={sorted(b.columns)}")
    if key is None:
        return tuple(_positional_differences(a, b))
    missing = [name for name in key if name not in a.columns]
    if missing:
        raise ValueError(f"key_missing column={missing[0]}")
    return tuple(_keyed_differences(a, b, key))


def compare_parquets(a: Path, b: Path, key: Sequence[str] | None = None) -> DiffResult:
    """Compara dois Parquets normalizados."""
    df_a = normalize_df(_load(a))
    df_b = normalize_df(_load(b))

    if df_a.columns != df_b.columns:
        return DiffResult(
            identical=False, diff_rows=-1,
            summary=f"column mismatch a={df_a.columns} b={df_b.columns}",
        )
    if key is None and df_a.height != df_b.height:
        return DiffResult(
            identical=False, diff_rows=abs(df_a.height - df_b.height),
            summary=f"row count mismatch a={df_a.height} b={df_b.height}",
        )

    differences = diff_frames(df_a, df_b, key)
    if not differences:
        return DiffResult(identical=True, diff_rows=0, summary="identical")
    count = len(differences) if key is None else len({item.key for item in differences})
    unit = "cell" if key is None else "row"
    return DiffResult(
        identical=False, diff_rows=count, summary=f"{count} {unit} diffs", differences=differences,
    )


def _key_token(key: tuple[str, ...], secret: bytes) -> str:
    return hmac.new(secret, "\x1f".join(key).encode(), sha256).hexdigest()[:_DIGEST_CHARS]


def _log_differences(
    differences: tuple[Difference, ...], show_values: bool, secret: bytes
) -> None:
    for item in differences[:_MAX_LOGGED_DIFFERENCES]:
        if show_values:
            logger.info(
                "difference kind=%s key=%s column=%s left=%r right=%r",
                item.kind, item.key, item.column, item.left, item.right,
            )
        else:
            logger.info(
                "difference kind=%s column=%s key_token=%s",
                item.kind, item.column, _key_token(item.key, secret),
            )
    if len(differences) > _MAX_LOGGED_DIFFERENCES:
        logger.info("differences_truncated shown=%d", _MAX_LOGGED_DIFFERENCES)


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python", type=Path, required=True)
    parser.add_argument("--go", type=Path, required=True)
    parser.add_argument("--key", action="append", default=None)
    parser.add_argument("--show-values", action="store_true")
    args = parser.parse_args(argv)

    try:
        result = compare_parquets(args.python, args.go, args.key)
    except ValueError as error:
        logger.error("diff_error error=%s", error)
        return 1
    logger.info(
        "diff_result identical=%s diff=%d summary=%s",
        result.identical, result.diff_rows, result.summary,
    )
    _log_differences(result.differences, args.show_values, secrets.token_bytes(32))
    return 0 if result.identical else 1


if __name__ == "__main__":
    sys.exit(main())
