"""Seed gold.dim_cbo, dim_cid10, dim_municipio, dim_procedimento_sus from CSVs.

Idempotent: re-running preserves existing data (INSERT ... ON CONFLICT DO NOTHING).
"""
from __future__ import annotations

import argparse
import csv
import logging
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine, text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


def _seed_cbo(conn: Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                text("""
                    INSERT INTO gold.dim_cbo (cod_cbo, descricao)
                    VALUES (:c, :d)
                    ON CONFLICT (cod_cbo) DO NOTHING
                """),
                {"c": row["cod_cbo"], "d": row["descricao"]},
            )
            count += 1
    return count


def _seed_cid(conn: Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            conn.execute(
                text("""
                    INSERT INTO gold.dim_cid10 (cod_cid, descricao, capitulo)
                    VALUES (:c, :d, :cap)
                    ON CONFLICT (cod_cid) DO NOTHING
                """),
                {
                    "c": row["cod_cid"],
                    "d": row["descricao"],
                    "cap": int(row["capitulo"]),
                },
            )
            count += 1
    return count


def _seed_municipio(conn: Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pop_val = row.get("populacao_estimada")
            teto_val = row.get("teto_pab_cents")
            conn.execute(
                text("""
                    INSERT INTO gold.dim_municipio
                    (ibge6, ibge7, nome, uf, populacao_estimada, teto_pab_cents)
                    VALUES (:i6, :i7, :n, :uf, :pop, :teto)
                    ON CONFLICT (ibge6) DO NOTHING
                """),
                {
                    "i6": row["ibge6"],
                    "i7": row["ibge7"],
                    "n": row["nome"],
                    "uf": row["uf"],
                    "pop": int(pop_val) if pop_val else None,
                    "teto": int(teto_val) if teto_val else None,
                },
            )
            count += 1
    return count


def _seed_sigtap(conn: Connection, csv_path: Path) -> int:
    count = 0
    with csv_path.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cpx_val = row.get("complexidade")
            fin_val = row.get("financiamento") or None
            mod_val = row.get("modalidade") or None
            conn.execute(
                text("""
                    INSERT INTO gold.dim_procedimento_sus
                    (cod_sigtap, descricao, complexidade, financiamento, modalidade)
                    VALUES (:c, :d, :cpx, :fin, :mod)
                    ON CONFLICT (cod_sigtap) DO NOTHING
                """),
                {
                    "c": row["cod_sigtap"],
                    "d": row["descricao"],
                    "cpx": int(cpx_val) if cpx_val else None,
                    "fin": fin_val,
                    "mod": mod_val,
                },
            )
            count += 1
    return count


def seed_all(conn: Connection, fixtures_dir: Path) -> dict[str, int]:
    return {
        "cbo": _seed_cbo(conn, fixtures_dir / "cbo2002.csv"),
        "cid": _seed_cid(conn, fixtures_dir / "cid10.csv"),
        "municipio": _seed_municipio(conn, fixtures_dir / "ibge_municipios.csv"),
        "sigtap": _seed_sigtap(conn, fixtures_dir / "sigtap_2026.csv"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-url", required=True)
    parser.add_argument("--fixtures-dir", default="docs/fixtures/reference-data/")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    engine = create_engine(args.db_url)
    fixtures = Path(args.fixtures_dir)

    with engine.begin() as conn:
        counts = seed_all(conn, fixtures)

    logger.info(
        "seed_done cbo=%d cid=%d municipio=%d sigtap=%d",
        counts["cbo"], counts["cid"], counts["municipio"], counts["sigtap"],
    )


if __name__ == "__main__":
    main()
