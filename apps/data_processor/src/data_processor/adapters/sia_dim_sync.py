"""SIA dim sync: S_CDN -> dim_procedimento_sus; CADMUN -> dim_municipio."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


def _ibge7_check_digit(ibge6: str) -> int:
    total = 0
    for i, ch in enumerate(ibge6):
        d = int(ch)
        if (i + 1) % 2 == 0:
            d *= 2
            if d >= 10:
                d = d // 10 + d % 10
        total += d
    return (10 - total % 10) % 10


def sync_dim_procedimento(engine: Engine, df: pl.DataFrame) -> int:
    procs = df.filter(pl.col("cdn_tb") == "PROC")
    n = 0
    with engine.begin() as conn:
        for row in procs.iter_rows(named=True):
            conn.execute(text("""
                INSERT INTO gold.dim_procedimento_sus
                    (cod_sigtap, descricao, complexidade,
                     financiamento, modalidade,
                     competencia_vigencia_ini, competencia_vigencia_fim)
                VALUES (:cod, :desc, 1, 'PAB', 'AMB', 200001, 209912)
                ON CONFLICT (cod_sigtap) DO UPDATE SET
                    descricao = EXCLUDED.descricao
            """), {
                "cod": row["cdn_it"].strip(),
                "desc": row["cdn_dscr"].strip(),
            })
            n += 1
    logger.info("sia_dim_sync_proc count=%d", n)
    return n


def sync_dim_municipio(engine: Engine, df: pl.DataFrame) -> int:
    n = 0
    with engine.begin() as conn:
        for row in df.iter_rows(named=True):
            ibge6 = row["codmunic"].strip().zfill(6)
            ibge7 = ibge6 + str(_ibge7_check_digit(ibge6))
            conn.execute(text("""
                INSERT INTO gold.dim_municipio (ibge6, ibge7, nome, uf)
                VALUES (:i6, :i7, :no, :uf)
                ON CONFLICT (ibge6) DO UPDATE SET
                    nome = EXCLUDED.nome,
                    uf = EXCLUDED.uf
            """), {
                "i6": ibge6,
                "i7": ibge7,
                "no": row["nome"].strip(),
                "uf": row["coduf"].strip(),
            })
            n += 1
    logger.info("sia_dim_sync_mun count=%d", n)
    return n
