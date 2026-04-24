"""PEP 544 Protocols for domain ports."""
from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datetime import date
    from uuid import UUID

    from cnes_contracts.fatos import VinculoCNES
    from cnes_contracts.landing import (
        Extraction,
        ExtractionRegisterPayload,
        FileManifest,
    )


class DimLookupPort(Protocol):

    def sk_profissional_por_cpf_hash(self, cpf_hash: str) -> int | None: ...
    def sk_estabelecimento_por_cnes(self, cnes: str) -> int | None: ...
    def sk_cbo_por_codigo(self, cod_cbo: str) -> int | None: ...
    def sk_competencia_por_yyyymm(self, yyyymm: int) -> int | None: ...


class RowMapperPort(Protocol):

    def map_vinculo(self, row: dict) -> VinculoCNES: ...


class ExtractionRepoPort(Protocol):

    def register(
        self, payload: ExtractionRegisterPayload,
    ) -> tuple[UUID, str]: ...
    def claim_next(
        self, processor_id: str, lease_secs: int,
    ) -> Extraction | None: ...
    def complete(self, extraction_id: UUID) -> None: ...
    def fail(self, extraction_id: UUID, error: str) -> None: ...
    def heartbeat(
        self, extraction_id: UUID, processor_id: str,
    ) -> None: ...
    def reap_expired(self) -> int: ...
    def mark_uploaded(
        self, extraction_id: UUID, sha256: str, row_count: int,
    ) -> None: ...


@runtime_checkable
class ExtractorPort(Protocol):

    def extract(
        self, source: str, competencia: date, tenant: str,
    ) -> list[FileManifest]: ...
