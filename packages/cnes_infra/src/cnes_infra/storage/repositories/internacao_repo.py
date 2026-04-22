"""internacao_repo: stub (pending SIHD v2 integration, see Gold v2 §10)."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from cnes_contracts.fatos import Internacao


def gravar(conn: Connection, i: Internacao) -> None:
    raise NotImplementedError(
        "internacao_repo: deferred pending SIHD v2 integration "
        "(docs/data-dictionary-gold-v2.md §10)",
    )
