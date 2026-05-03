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
from collections.abc import Callable
from typing import TYPE_CHECKING

import polars as pl
from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

ApplyIU = Callable[[pl.DataFrame], int]


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
    df: pl.DataFrame,
    conn: Connection,
    source: str,
    intent: str,
    apply_iu_fn: ApplyIU | None = None,
) -> dict[str, int]:
    """Branch rows on _op; apply D inline; route I/U via apply_iu_fn callback.

    Args:
        df: Parquet rows with `_op` column.
        conn: SQLAlchemy connection used for inline DELETEs.
        source: Source key (e.g. "cnes").
        intent: Intent key (e.g. "estabelecimentos").
        apply_iu_fn: Optional callback(df_iu) -> rowcount. When None,
            I/U rows are bucketed and counted but NOT applied
            (legacy unit-test mode).
    Returns:
        Mapping {inserts, updates, deletes, applied} with row counts.
    Raises:
        FatalError: when _op value or (source, intent) pair is unknown.
    """
    inserts, updates, deletes = _bucket_by_op(df)
    delete_count = _apply_deletes(conn, deletes, source, intent)
    applied = _apply_iu(inserts, updates, apply_iu_fn)
    counts = {
        "inserts": len(inserts),
        "updates": len(updates),
        "deletes": delete_count,
        "applied": applied,
    }
    logger.info(
        "delta_merge source=%s intent=%s i=%d u=%d d=%d applied=%d",
        source, intent,
        counts["inserts"], counts["updates"], counts["deletes"],
        counts["applied"],
    )
    return counts


def _apply_iu(
    inserts: list[dict], updates: list[dict], apply_iu_fn: ApplyIU | None,
) -> int:
    if apply_iu_fn is None:
        return 0
    iu_rows = inserts + updates
    if not iu_rows:
        return 0
    df_iu = pl.DataFrame(iu_rows)
    return apply_iu_fn(df_iu)


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
