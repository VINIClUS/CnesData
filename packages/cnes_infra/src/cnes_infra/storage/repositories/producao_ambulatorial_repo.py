"""producao_ambulatorial_repo: stub (pending SIA/BPA intents, see Gold v2 §10)."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection

    from cnes_contracts.fatos import ProducaoAmbulatorial


def gravar(conn: Connection, p: ProducaoAmbulatorial) -> None:
    raise NotImplementedError(
        "producao_ambulatorial_repo: deferred pending SIA/BPA intents "
        "(docs/data-dictionary-gold-v2.md §10)",
    )
