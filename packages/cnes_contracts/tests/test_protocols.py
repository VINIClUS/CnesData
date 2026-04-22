"""Protocol smoke — import + structural satisfaction."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

from cnes_contracts.fatos import VinculoCNES
from cnes_contracts.landing import (  # noqa: TC001 - runtime for test calls
    Extraction,
    ExtractionRegisterPayload,
)
from cnes_contracts.protocols import (  # noqa: TC001 - runtime for test calls
    DimLookupPort,
    ExtractionRepoPort,
    RowMapperPort,
)


class _FakeLookup:
    def sk_profissional_por_cpf_hash(self, cpf_hash: str) -> int | None:
        return 1

    def sk_estabelecimento_por_cnes(self, cnes: str) -> int | None:
        return 2

    def sk_cbo_por_codigo(self, cod_cbo: str) -> int | None:
        return 3

    def sk_competencia_por_yyyymm(self, yyyymm: int) -> int | None:
        return 4


class _FakeMapper:
    def map_vinculo(self, row: dict) -> VinculoCNES:
        return VinculoCNES(
            sk_profissional=1,
            sk_estabelecimento=1,
            sk_cbo=1,
            sk_competencia=1,
            job_id=uuid4(),
            fonte_sistema="CNES_LOCAL",
            extracao_ts=datetime.now(UTC),
        )


class _FakeRepo:
    def register(
        self, payload: ExtractionRegisterPayload,
    ) -> tuple[UUID, str]:
        return uuid4(), "url"

    def claim_next(
        self, processor_id: str, lease_secs: int,
    ) -> Extraction | None:
        return None

    def complete(self, extraction_id: UUID) -> None:
        pass

    def fail(self, extraction_id: UUID, error: str) -> None:
        pass

    def heartbeat(
        self, extraction_id: UUID, processor_id: str,
    ) -> None:
        pass

    def reap_expired(self) -> int:
        return 0

    def mark_uploaded(
        self, extraction_id: UUID, sha256: str, row_count: int,
    ) -> None:
        pass


def test_dim_lookup_protocol_satisfeito():
    lookup: DimLookupPort = _FakeLookup()
    assert lookup.sk_profissional_por_cpf_hash("abc") == 1


def test_row_mapper_protocol_satisfeito():
    mapper: RowMapperPort = _FakeMapper()
    result = mapper.map_vinculo({})
    assert result.sk_profissional == 1


def test_extraction_repo_protocol_satisfeito():
    repo: ExtractionRepoPort = _FakeRepo()
    assert repo.reap_expired() == 0
