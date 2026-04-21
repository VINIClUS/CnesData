"""Extrai tabelas CNES direto do FB para Parquet baseline shadow E2E.

Uso:
    python scripts/shadow_baseline_py.py \\
        --host localhost --port 3051 --db /firebird/data/shadow.fdb \\
        --user SYSDBA --password masterkey \\
        --cod-mun 354130 --output /tmp/py_baseline/
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import fdb
import polars as pl

logger = logging.getLogger(__name__)

_SQL_PROFISSIONAIS = """
SELECT
    prof.CPF_PROF, prof.COD_CNS, prof.NOME_PROF,
    COALESCE(prof.NO_SOCIAL, '') AS NO_SOCIAL, prof.SEXO, prof.DATA_NASC,
    vinc.COD_CBO, vinc.IND_VINC, vinc.TP_SUS_NAO_SUS,
    COALESCE(vinc.CG_HORAAMB, 0) AS CG_HORAAMB,
    COALESCE(vinc.CGHORAOUTR, 0) AS CGHORAOUTR,
    COALESCE(vinc.CGHORAHOSP, 0) AS CGHORAHOSP,
    (COALESCE(vinc.CG_HORAAMB, 0)
     + COALESCE(vinc.CGHORAOUTR, 0)
     + COALESCE(vinc.CGHORAHOSP, 0)) AS CARGA_HORARIA_TOTAL,
    est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
    est.CODMUNGEST
FROM       LFCES021 vinc
INNER JOIN LFCES004 est  ON est.UNIDADE_ID = vinc.UNIDADE_ID
INNER JOIN LFCES018 prof ON prof.PROF_ID   = vinc.PROF_ID
WHERE est.CODMUNGEST = ?
ORDER BY prof.NOME_PROF, vinc.COD_CBO
"""

_SQL_ESTABELECIMENTOS = """
SELECT
    est.CNES, est.NOME_FANTA, est.TP_UNID_ID,
    est.CODMUNGEST, est.CNPJ_MANT
FROM LFCES004 est
WHERE est.CODMUNGEST = ?
ORDER BY est.CNES
"""


def _connect(host: str, port: int, db: str, user: str, pw: str):
    return fdb.connect(
        host=host, port=port, database=db,
        user=user, password=pw, charset="WIN1252",
    )


def extract_to_parquet(
    conn,
    sql: str,
    param: str,
    output: Path,
) -> int:
    cur = conn.cursor()
    cur.execute(sql, (param,))
    # Lowercase para paridade com Go (parquet tags são lowercase)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    df = pl.DataFrame(
        rows,
        schema=cols,
        orient="row",
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output)
    logger.info("extracted path=%s rows=%d cols=%d", output, df.height, len(cols))
    return df.height


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3051)
    parser.add_argument("--db", default="/firebird/data/shadow.fdb")
    parser.add_argument("--user", default="SYSDBA")
    parser.add_argument("--password", default="masterkey")
    parser.add_argument("--cod-mun", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    conn = _connect(args.host, args.port, args.db, args.user, args.password)
    try:
        args.output.mkdir(parents=True, exist_ok=True)
        extract_to_parquet(
            conn, _SQL_PROFISSIONAIS, args.cod_mun,
            args.output / "cnes_profissionais.parquet",
        )
        extract_to_parquet(
            conn, _SQL_ESTABELECIMENTOS, args.cod_mun,
            args.output / "cnes_estabelecimentos.parquet",
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
