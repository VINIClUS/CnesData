"""CDC merger — branches incoming parquet rows on _op column.

Used when edge agent runs in delta mode (AGENT_DELTA_MODE=true) and emits
parquet with _op in {'I', 'U', 'D'} column. Inserts/Updates route to existing
adapter+upsert path; Deletes call inline SQL DELETE per (source, intent) PK.

DELTA_MODE env (default 'true'):
  - 'true': accept _op parquet; route through merge_delta
  - 'false': reject _op parquet with FatalError
"""
from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    import polars as pl
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)


class FatalError(RuntimeError):
    """Unrecoverable processor error; signals operator misconfig."""


_PK_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("cnes", "estabelecimentos"): ("CNES",),
    ("cnes", "profissionais"): ("CPF_PROF", "CNES", "COD_CBO"),
    ("cnes", "equipes"): ("SEQ_EQUIPE",),
    ("sihd", "aih"): ("NUM_AIH",),
    ("bpa", "linhas"): ("CPF", "COMPETEN", "COD_PROC"),
}


_DELETE_SQL: dict[tuple[str, str], str] = {
    ("cnes", "estabelecimentos"): (
        "DELETE FROM gold.dim_estabelecimento WHERE cnes = :CNES"
    ),
    ("cnes", "profissionais"): (
        "DELETE FROM gold.fato_vinculo_cnes "
        "WHERE sk_profissional = ("
        " SELECT sk_profissional FROM gold.dim_profissional "
        " WHERE cpf = :CPF_PROF) "
        "AND sk_estabelecimento = ("
        " SELECT sk_estabelecimento FROM gold.dim_estabelecimento "
        " WHERE cnes = :CNES) "
        "AND sk_cbo = :COD_CBO"
    ),
    ("cnes", "equipes"): (
        "DELETE FROM gold.dim_equipe WHERE seq_equipe = :SEQ_EQUIPE"
    ),
    ("sihd", "aih"): (
        "DELETE FROM gold.fato_internacao WHERE num_aih = :NUM_AIH"
    ),
    ("bpa", "linhas"): (
        "DELETE FROM gold.fato_producao_ambulatorial "
        "WHERE cpf = :CPF AND competencia = :COMPETEN "
        "AND cod_procedimento = :COD_PROC"
    ),
}


def is_delta_mode_enabled() -> bool:
    """True if DELTA_MODE env is unset or any value other than 'false'."""
    return os.getenv("DELTA_MODE", "true").lower() != "false"


def has_op_column(df: pl.DataFrame) -> bool:
    """True if Parquet schema includes the _op CDC column."""
    return "_op" in df.columns


def merge_delta(
    df: pl.DataFrame, conn: Connection, source: str, intent: str,
) -> dict[str, int]:
    """Branch rows on _op; execute D inline; return I/U/D counts.

    Returns:
        Mapping {inserts, updates, deletes} with row counts.
    Raises:
        FatalError: when _op value or (source, intent) pair is unknown.
    """
    inserts, updates, deletes = _bucket_by_op(df)
    delete_count = _apply_deletes(conn, deletes, source, intent)
    counts = {
        "inserts": len(inserts),
        "updates": len(updates),
        "deletes": delete_count,
    }
    logger.info(
        "delta_merge source=%s intent=%s i=%d u=%d d=%d",
        source, intent,
        counts["inserts"], counts["updates"], counts["deletes"],
    )
    return counts


def _bucket_by_op(
    df: pl.DataFrame,
) -> tuple[list[dict], list[dict], list[dict]]:
    inserts: list[dict] = []
    updates: list[dict] = []
    deletes: list[dict] = []
    for row in df.to_dicts():
        op = row.pop("_op", None)
        if op == "I":
            inserts.append(row)
        elif op == "U":
            updates.append(row)
        elif op == "D":
            deletes.append(row)
        else:
            raise FatalError(f"unknown_op op={op!r}")
    return inserts, updates, deletes


def _apply_deletes(
    conn: Connection, deletes: list[dict], source: str, intent: str,
) -> int:
    if not deletes:
        return 0
    key = (source, intent)
    sql = _DELETE_SQL.get(key)
    pk_cols = _PK_COLUMNS.get(key)
    if sql is None or pk_cols is None:
        raise FatalError(
            f"unknown_source_intent source={source} intent={intent}",
        )
    deleted = 0
    for row in deletes:
        pk = {c: row.get(c) for c in pk_cols}
        result = conn.execute(text(sql), pk)
        if result.rowcount == 0:
            logger.info(
                "delete_no_op source=%s intent=%s pk=%s",
                source, intent, pk,
            )
        else:
            deleted += result.rowcount
    return deleted
