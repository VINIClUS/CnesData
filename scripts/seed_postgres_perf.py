"""Popula Postgres perf com volumes grandes sintéticos."""
import argparse
import logging
import random
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert

from cnes_infra.storage.schema import dim_estabelecimento

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--url",
        default="postgresql+psycopg://cnesdata:cnesdata_perf@localhost:5434/cnesdata_perf",
    )
    parser.add_argument("--n", type=int, default=100_000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    random.seed(42)
    engine = create_engine(args.url)

    with engine.begin() as con:
        con.execute(text("CREATE SCHEMA IF NOT EXISTS gold"))

    batch_size = 5000
    rows_inseridas = 0
    with engine.begin() as con:
        for start in range(0, args.n, batch_size):
            chunk = [
                {
                    "tenant_id": "355030",
                    "cnes": f"{i:07d}",
                    "fontes": {"PERF": True},
                }
                for i in range(start, min(start + batch_size, args.n))
            ]
            con.execute(
                insert(dim_estabelecimento)
                .values(chunk)
                .on_conflict_do_nothing(index_elements=["tenant_id", "cnes"])
            )
            rows_inseridas += len(chunk)
    logger.info("seed_pg_done rows=%d", rows_inseridas)
    return 0


if __name__ == "__main__":
    sys.exit(main())
