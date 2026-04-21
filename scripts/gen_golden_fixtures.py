"""Gera fixtures golden capturando output atual do dump_agent Python.

Uso:
    python scripts/gen_golden_fixtures.py --cod-municipio 354130 --output docs/fixtures/golden/

Cada intent (profissionais, estabelecimentos, equipes, sihd_producao) produz
par <intent>.parquet + <intent>.meta.json com metadata de query/params.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


def _resolve_extractor(intent: str) -> Callable[..., pl.DataFrame]:
    from dump_agent.extractors.cnes_extractor import CnesExtractor
    from dump_agent.extractors.sihd_extractor import SihdExtractor

    if intent.startswith("cnes_"):
        return CnesExtractor()
    if intent.startswith("sihd_"):
        return SihdExtractor()
    raise ValueError(f"unknown intent={intent}")


def capture_intent(intent: str, *, cod_municipio: str = "354130") -> pl.DataFrame:
    extractor = _resolve_extractor(intent)
    return extractor(cod_municipio=cod_municipio)  # type: ignore[operator]


def write_fixture(
    df: pl.DataFrame,
    output_dir: Path,
    intent: str,
    *,
    query: str,
    params: tuple,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{intent}.parquet"
    meta_path = output_dir / f"{intent}.meta.json"

    df.write_parquet(parquet_path, compression="snappy")

    sha = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    meta = {
        "intent": intent,
        "row_count": df.height,
        "columns": df.columns,
        "sql_query": query,
        "sql_params": list(params),
        "sha256": sha,
        "polars_version": pl.__version__,
    }
    meta_path.write_text(json.dumps(meta, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info("fixture_written intent=%s rows=%d sha256=%s", intent, df.height, sha[:12])


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cod-municipio", default="354130")
    parser.add_argument("--output", type=Path, default=Path("docs/fixtures/golden/"))
    parser.add_argument(
        "--intents",
        nargs="+",
        default=["cnes_profissionais", "cnes_estabelecimentos", "cnes_equipes", "sihd_producao"],
    )
    args = parser.parse_args()

    for intent in args.intents:
        try:
            df = capture_intent(intent, cod_municipio=args.cod_municipio)
        except Exception:
            logger.exception("capture_failed intent=%s", intent)
            return 1
        write_fixture(df, args.output, intent, query="<captured>", params=(args.cod_municipio,))
    return 0


if __name__ == "__main__":
    sys.exit(main())
