"""PEP 544 Protocols for domain ports."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datetime import date
    from uuid import UUID

    from sqlalchemy.engine import Engine

    from cnes_contracts.fatos import VinculoCNES
    from cnes_contracts.landing import ClaimedExtraction, FileManifest


class DimLookupPort(Protocol):

    def sk_profissional_por_cpf_hash(self, cpf_hash: str) -> int | None: ...
    def sk_estabelecimento_por_cnes(self, cnes: str) -> int | None: ...
    def sk_cbo_por_codigo(self, cod_cbo: str) -> int | None: ...
    def sk_competencia_por_yyyymm(self, yyyymm: int) -> int | None: ...


class RowMapperPort(Protocol):

    def map_vinculo(self, row: dict) -> VinculoCNES: ...


class ExtractionRepoPort(Protocol):

    def enqueue(
        self, engine: Engine, *, tenant_id: str, source_type: str,
        competencia: date, files: list[dict],
        depends_on: list[UUID] | None = None,
    ) -> UUID: ...

    def claim_next(
        self, engine: Engine, *, lease_seconds: int = 300,
    ) -> ClaimedExtraction | None: ...

    def register(
        self, engine: Engine, *, job_id: UUID, files: list[dict],
        agent_version: str | None = None,
        machine_id: str | None = None,
    ) -> UUID | None: ...

    def mark_completed(self, engine: Engine, *, job_id: UUID) -> None: ...

    def mark_failed(
        self, engine: Engine, *, job_id: UUID, reason: str,
    ) -> None: ...

    def reap_expired(self, engine: Engine) -> int: ...


@runtime_checkable
class ExtractorPort(Protocol):

    def extract(
        self, source: str, competencia: date, tenant: str,
    ) -> list[FileManifest]: ...
