"""Ports portáveis de extração e mapeamento."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from datetime import date

    from cnes_contracts.fatos import VinculoCNES
    from cnes_contracts.landing import FileManifest

class DimLookupPort(Protocol):

    def sk_profissional_por_cpf_hash(self, cpf_hash: str) -> int | None: ...
    def sk_estabelecimento_por_cnes(self, cnes: str) -> int | None: ...
    def sk_cbo_por_codigo(self, cod_cbo: str) -> int | None: ...
    def sk_competencia_por_yyyymm(self, yyyymm: int) -> int | None: ...

class RowMapperPort(Protocol):

    def map_vinculo(self, row: dict[str, Any]) -> VinculoCNES: ...

@runtime_checkable
class ExtractorPort(Protocol):

    def extract(
        self, source: str, competencia: date, tenant: str,
    ) -> list[FileManifest]: ...
