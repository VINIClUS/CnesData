"""Seed dim_procedimento_sus from SIGTAP CSV (idempotent upsert)."""
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


def seed_sigtap(engine: Engine, csv_path: Path) -> int:
    with csv_path.open(encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    with engine.begin() as conn:
        for row in rows:
            conn.execute(text("""
                INSERT INTO gold.dim_procedimento_sus (
                    cod_sigtap, descricao, complexidade,
                    financiamento, modalidade,
                    competencia_vigencia_ini, competencia_vigencia_fim
                ) VALUES (:cod, :desc, :complex, :fin, :mod, :ini, :fim)
                ON CONFLICT (cod_sigtap) DO UPDATE SET
                    descricao = EXCLUDED.descricao,
                    complexidade = EXCLUDED.complexidade,
                    financiamento = EXCLUDED.financiamento,
                    modalidade = EXCLUDED.modalidade,
                    competencia_vigencia_ini = EXCLUDED.competencia_vigencia_ini,
                    competencia_vigencia_fim = EXCLUDED.competencia_vigencia_fim
            """), {
                "cod": row["cod_sigtap"],
                "desc": row["descricao"],
                "complex": int(row["complexidade"]),
                "fin": row["financiamento"],
                "mod": row["modalidade"],
                "ini": int(row["competencia_vigencia_ini"]),
                "fim": int(row["competencia_vigencia_fim"]),
            })

    logger.info("sigtap_seed_ok count=%d csv=%s", len(rows), csv_path)
    return len(rows)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--db-url", required=True)
    args = parser.parse_args()

    engine = create_engine(args.db_url)
    try:
        seed_sigtap(engine, args.csv)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    sys.exit(main())
