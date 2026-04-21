"""Gera SQL seed para Firebird docker shadow E2E.

Cria tabelas minimas LFCES018/LFCES004/LFCES021 + popula com faker data
reproduzivel. SQL compativel com Firebird 2.5 (ISQL syntax).
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from faker import Faker

logger = logging.getLogger(__name__)


_DDL = """SET SQL DIALECT 3;

CREATE TABLE LFCES004 (
    UNIDADE_ID   INTEGER NOT NULL,
    CNES         CHAR(7) NOT NULL,
    NOME_FANTA   VARCHAR(100),
    CNPJ_MANT    CHAR(14),
    TP_UNID_ID   INTEGER,
    CODMUNGEST   CHAR(6),
    PRIMARY KEY (UNIDADE_ID)
);

CREATE TABLE LFCES018 (
    PROF_ID      INTEGER NOT NULL,
    CPF_PROF     CHAR(11),
    COD_CNS      CHAR(15),
    NOME_PROF    VARCHAR(100),
    NO_SOCIAL    VARCHAR(100),
    SEXO         CHAR(1),
    DATA_NASC    DATE,
    PRIMARY KEY (PROF_ID)
);

CREATE TABLE LFCES021 (
    PROF_ID         INTEGER NOT NULL,
    UNIDADE_ID      INTEGER NOT NULL,
    COD_CBO         CHAR(6),
    IND_VINC        CHAR(6),
    TP_SUS_NAO_SUS  CHAR(1),
    CG_HORAAMB      INTEGER,
    CGHORAOUTR      INTEGER,
    CGHORAHOSP      INTEGER,
    PRIMARY KEY (PROF_ID, UNIDADE_ID)
);
"""


def _escape(val: object) -> str:
    if val is None:
        return "NULL"
    if isinstance(val, int):
        return str(val)
    s = str(val).replace("'", "''")
    return f"'{s}'"


def render_insert_stmts(table: str, rows: list[dict]) -> str:
    if not rows:
        return ""
    cols = list(rows[0].keys())
    out: list[str] = []
    for row in rows:
        vals = ", ".join(_escape(row[c]) for c in cols)
        out.append(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({vals});")
    return "\n".join(out)


def _build_estabelecimentos(fake: Faker, n: int) -> list[dict]:
    return [
        {
            "UNIDADE_ID": i + 1,
            "CNES": f"{(2000000 + i):07d}",
            "NOME_FANTA": fake.company()[:100].replace("'", ""),
            "CNPJ_MANT": "55293427000117",
            "TP_UNID_ID": fake.random_int(1, 99),
            "CODMUNGEST": "354130",
        }
        for i in range(n)
    ]


def _build_profissionais(fake: Faker, n: int) -> list[dict]:
    return [
        {
            "PROF_ID": i + 1,
            "CPF_PROF": f"{fake.random_number(digits=11, fix_len=True):011d}",
            "COD_CNS": f"{fake.random_number(digits=15, fix_len=True):015d}",
            "NOME_PROF": fake.name()[:100].replace("'", ""),
            "NO_SOCIAL": None,
            "SEXO": fake.random_element(["M", "F"]),
            "DATA_NASC": fake.date_between(start_date="-60y", end_date="-25y").isoformat(),
        }
        for i in range(n)
    ]


def _build_vinculos(fake: Faker, n_prof: int, n_estab: int) -> list[dict]:
    out = []
    used = set()
    for _ in range(min(n_prof * 2, 500)):
        pid = fake.random_int(1, n_prof)
        uid = fake.random_int(1, n_estab)
        if (pid, uid) in used:
            continue
        used.add((pid, uid))
        out.append({
            "PROF_ID": pid,
            "UNIDADE_ID": uid,
            "COD_CBO": f"{fake.random_int(222105, 515310):06d}",
            "IND_VINC": f"{fake.random_int(10, 99):02d}0100",
            "TP_SUS_NAO_SUS": "1",
            "CG_HORAAMB": fake.random_int(0, 40),
            "CGHORAOUTR": fake.random_int(0, 10),
            "CGHORAHOSP": 0,
        })
    return out


def generate_cnes_seed(output: Path, *, seed: int, rows_per_table: int) -> None:
    Faker.seed(seed)
    fake = Faker("pt_BR")

    estabs = _build_estabelecimentos(fake, rows_per_table)
    profs = _build_profissionais(fake, rows_per_table)
    vincs = _build_vinculos(fake, rows_per_table, rows_per_table)

    output.parent.mkdir(parents=True, exist_ok=True)
    parts = [
        _DDL,
        render_insert_stmts("LFCES004", estabs),
        render_insert_stmts("LFCES018", profs),
        render_insert_stmts("LFCES021", vincs),
        "COMMIT;",
        "",
    ]
    output.write_text("\n\n".join(parts), encoding="utf-8")
    logger.info(
        "seed_generated path=%s estabs=%d profs=%d vincs=%d",
        output, len(estabs), len(profs), len(vincs),
    )


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rows-per-table", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    generate_cnes_seed(args.output, seed=args.seed, rows_per_table=args.rows_per_table)
    return 0


if __name__ == "__main__":
    sys.exit(main())
