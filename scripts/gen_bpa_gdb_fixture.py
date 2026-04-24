"""Gera BPAMAG.GDB sintético via fdb + fbclient.dll 1.5.6."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA_DDL = """
CREATE TABLE BPA_CAB (
    NU_COMPETENCIA VARCHAR(6) NOT NULL,
    CO_CNES VARCHAR(7) NOT NULL,
    PRIMARY KEY (NU_COMPETENCIA, CO_CNES)
);

CREATE TABLE BPA_C_LINHAS (
    NU_COMPETENCIA VARCHAR(6) NOT NULL,
    CO_CNES VARCHAR(7) NOT NULL,
    CO_PROCEDIMENTO VARCHAR(10) NOT NULL,
    QT_APROVADA INTEGER NOT NULL,
    CO_CBO VARCHAR(6),
    TP_IDADE SMALLINT,
    NU_IDADE SMALLINT
);

CREATE TABLE BPA_I_LINHAS (
    NU_COMPETENCIA VARCHAR(6) NOT NULL,
    CO_CNES VARCHAR(7) NOT NULL,
    NU_CNS_PAC VARCHAR(15) NOT NULL,
    NU_CPF_PAC VARCHAR(11),
    CO_PROCEDIMENTO VARCHAR(10) NOT NULL,
    CO_CBO VARCHAR(6) NOT NULL,
    CO_CID10 VARCHAR(4),
    DT_ATENDIMENTO DATE NOT NULL,
    QT_APROVADA INTEGER NOT NULL,
    NU_CNS_PROF VARCHAR(15) NOT NULL
);
"""

_SEED_ROWS = [
    ("BPA_CAB", "('202601', '2269481')"),
    ("BPA_C_LINHAS", "('202601', '2269481', '0301010056', 10, '225125', 3, 45)"),
    ("BPA_I_LINHAS", """('202601', '2269481', '700123456789012', '12345678901',
                        '0301010064', '225125', 'J00', DATE '2026-01-15',
                        1, '700987654321098')"""),
]


def create_gdb(gdb_path: Path, dll_path: Path) -> None:
    import fdb  # pyright: ignore[reportMissingImports]  # runtime-only, x86 Python

    fdb.load_api(str(dll_path))

    if gdb_path.exists():
        gdb_path.unlink()

    con = fdb.create_database(
        dsn=str(gdb_path), user="SYSDBA", password="masterkey",
        charset="WIN1252", page_size=4096,
    )
    try:
        cur = con.cursor()
        for ddl in _SCHEMA_DDL.strip().split(";"):
            stmt = ddl.strip()
            if stmt:
                cur.execute(stmt)
        con.commit()

        for table, values in _SEED_ROWS:
            cur.execute(f"INSERT INTO {table} VALUES {values}")
        con.commit()
    finally:
        con.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gdb", type=Path, required=True)
    parser.add_argument("--dll", type=Path, required=True)
    args = parser.parse_args()

    create_gdb(args.gdb, args.dll)
    logger.info("gdb_created path=%s size=%d", args.gdb, args.gdb.stat().st_size)
    return 0


if __name__ == "__main__":
    sys.exit(main())
