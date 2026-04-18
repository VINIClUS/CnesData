"""Popula fixture Firebird com schema CNES mínimo e N rows sintéticas."""
import argparse
import logging
import random
import sys

import fdb

logger = logging.getLogger(__name__)

_COD_MUN_FIXTURE: str = "355030"

_SCHEMA_SQL = [
    """CREATE TABLE LFCES004 (
        UNIDADE_ID INTEGER NOT NULL PRIMARY KEY,
        CNES VARCHAR(7),
        NOME_FANTA VARCHAR(60),
        TP_UNID_ID VARCHAR(2),
        CODMUNGEST VARCHAR(6),
        CNPJ_MANT VARCHAR(14)
    )""",
    """CREATE TABLE LFCES018 (
        PROF_ID INTEGER NOT NULL PRIMARY KEY,
        CPF_PROF VARCHAR(11),
        COD_CNS VARCHAR(15),
        NOME_PROF VARCHAR(60),
        NO_SOCIAL VARCHAR(60),
        SEXO VARCHAR(1),
        DATA_NASC DATE
    )""",
    """CREATE TABLE LFCES021 (
        UNIDADE_ID INTEGER,
        PROF_ID INTEGER,
        COD_CBO VARCHAR(6),
        IND_VINC VARCHAR(6),
        TP_SUS_NAO_SUS VARCHAR(1),
        CG_HORAAMB INTEGER,
        CGHORAOUTR INTEGER,
        CGHORAHOSP INTEGER
    )""",
    """CREATE TABLE LFCES060 (
        SEQ_EQUIPE VARCHAR(7),
        INE VARCHAR(10),
        DS_AREA VARCHAR(60),
        TP_EQUIPE VARCHAR(2),
        COD_MUN VARCHAR(6)
    )""",
]


def _popular(con, n_profs: int) -> None:
    random.seed(42)
    cur = con.cursor()
    for sql in _SCHEMA_SQL:
        cur.execute(sql)
    con.commit()

    n_est = max(1, n_profs // 100)
    cur.executemany(
        "INSERT INTO LFCES004 VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                i, f"{i:07d}", f"UBS_{i}"[:60], "02",
                _COD_MUN_FIXTURE,
                f"{random.randint(10**13, 10**14 - 1):014d}",
            )
            for i in range(n_est)
        ],
    )
    cur.executemany(
        "INSERT INTO LFCES018 VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (
                i, f"{i:011d}", f"7{i:014d}"[:15],
                f"PROF_{i}"[:60], None,
                random.choice(("F", "M")), "1990-01-01",
            )
            for i in range(n_profs)
        ],
    )
    cur.executemany(
        "INSERT INTO LFCES021 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                i % n_est, i,
                f"{random.randint(100000, 999999):06d}",
                "010101", "S",
                random.randint(0, 40), 0, 0,
            )
            for i in range(n_profs)
        ],
    )
    con.commit()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3051)
    parser.add_argument("--db", default="CNES.FDB")
    parser.add_argument("--password", default="masterkey")
    parser.add_argument("--n-profs", type=int, default=100_000)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    con = fdb.create_database(
        dsn=f"{args.host}/{args.port}:{args.db}",
        user="SYSDBA", password=args.password,
    )
    try:
        _popular(con, args.n_profs)
        logger.info("seed_fb_done n_profs=%d", args.n_profs)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
