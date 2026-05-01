"""Seed dim_cid10 from CID10 CSV (idempotent upsert)."""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def seed_cid10(engine: Engine, csv_path: Path) -> int:
    with csv_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
                INSERT INTO gold.dim_cid10 (cod_cid, descricao, capitulo)
                VALUES (:cod, :desc, :cap)
                ON CONFLICT (cod_cid) DO UPDATE SET
                    descricao = EXCLUDED.descricao,
                    capitulo = EXCLUDED.capitulo
            """), {
                "cod": row["cod_cid"],
                "desc": row["descricao"],
                "cap": int(row["capitulo"]),
            })

    logger.info("cid10_seed_ok count=%d", len(rows))
    return len(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--db-url", required=True)
    args = parser.parse_args()

    engine = create_engine(args.db_url)
    try:
        seed_cid10(engine, args.csv)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
