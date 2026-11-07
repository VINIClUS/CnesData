"""Protocolo do repositório legado vinculado ao SQLAlchemy."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from datetime import date
    from uuid import UUID

    from sqlalchemy.engine import Engine

    from cnes_contracts.landing import ClaimedExtraction

class ExtractionRepoPort(Protocol):

    def enqueue(
        self, engine: Engine, *, tenant_id: str, source_type: str,
        competencia: date, files: list[dict[str, Any]],
        depends_on: list[UUID] | None = None,
    ) -> UUID: ...

    def claim_next(
        self, engine: Engine, *, lease_seconds: int = 300,
    ) -> ClaimedExtraction | None: ...

    def register(
        self, engine: Engine, *, job_id: UUID, files: list[dict[str, Any]],
        agent_version: str | None = None,
        machine_id: str | None = None,
    ) -> UUID | None: ...

    def mark_completed(self, engine: Engine, *, job_id: UUID) -> None: ...

    def mark_failed(
        self, engine: Engine, *, job_id: UUID, reason: str,
    ) -> None: ...

    def reap_expired(self, engine: Engine) -> int: ...
