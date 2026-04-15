"""EstabelecimentoRepository — upsert dim_estabelecimento."""

import logging
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from cnes_infra.storage.schema import dim_estabelecimento

logger = logging.getLogger(__name__)

_CHUNK_SIZE: int = 1000


def _chunked(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


class EstabelecimentoRepository:
    def __init__(self, con) -> None:
        self._con = con

    def gravar(self, rows: Iterable[dict]) -> int:
        materialized = list(rows)
        if not materialized:
            return 0
        for chunk in _chunked(materialized, _CHUNK_SIZE):
            self._con.execute(
                insert(dim_estabelecimento)
                .values(chunk)
                .on_conflict_do_update(
                    index_elements=["tenant_id", "cnes"],
                    set_={
                        "fontes": text(
                            "dim_estabelecimento.fontes"
                            " || EXCLUDED.fontes"
                        ),
                        "atualizado_em": text("NOW()"),
                    },
                ),
            )
        return len(materialized)
