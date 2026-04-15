"""VinculoRepository — snapshot replace fato_vinculo."""

import logging
from collections.abc import Iterable

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert

from cnes_domain.tenant import get_tenant_id
from cnes_infra.storage.schema import fato_vinculo

logger = logging.getLogger(__name__)

_CHUNK_SIZE: int = 1000


def _chunked(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


class VinculoRepository:
    def __init__(self, con) -> None:
        self._con = con

    def snapshot_replace(
        self,
        competencia: str,
        fonte: str,
        rows: Iterable[dict],
    ) -> int:
        self._con.execute(
            text(
                "DELETE FROM gold.fato_vinculo "
                "WHERE tenant_id = :tid "
                "AND competencia = :comp "
                "AND fontes ? :fonte"
            ),
            {"tid": get_tenant_id(), "comp": competencia, "fonte": fonte},
        )
        materialized = list(rows)
        for chunk in _chunked(materialized, _CHUNK_SIZE):
            self._con.execute(insert(fato_vinculo).values(chunk))
        return len(materialized)
