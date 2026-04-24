"""Protocol smoke — import + structural satisfaction."""
from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import UUID, uuid4

from cnes_contracts.fatos import VinculoCNES
from cnes_contracts.landing import (
    Extraction,
    ExtractionRegisterPayload,
    FileManifest,
)
from cnes_contracts.protocols import (
    DimLookupPort,
    ExtractionRepoPort,
    ExtractorPort,
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


class _FakeExtractor:
    def extract(
        self, source: str, competencia: date, tenant: str,
    ) -> list[FileManifest]:
        return [
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="BPA_C",
                size_bytes=1, sha256="a" * 64,
            ),
        ]


def test_dim_lookup_protocol_satisfeito():
    lookup: DimLookupPort = _FakeLookup()
    assert lookup.sk_profissional_por_cpf_hash("abc") == 1
    assert lookup.sk_estabelecimento_por_cnes("x") == 2
    assert lookup.sk_cbo_por_codigo("y") == 3
    assert lookup.sk_competencia_por_yyyymm(202601) == 4


def test_row_mapper_protocol_satisfeito():
    mapper: RowMapperPort = _FakeMapper()
    result = mapper.map_vinculo({})
    assert result.sk_profissional == 1


def test_extraction_repo_protocol_satisfeito():
    repo: ExtractionRepoPort = _FakeRepo()
    payload = ExtractionRegisterPayload(
        job_id=uuid4(),
        files=[
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="BPA_C",
                size_bytes=1, sha256="a" * 64,
            ),
        ],
    )
    job_id, url = repo.register(payload)
    assert isinstance(job_id, UUID)
    assert url == "url"
    assert repo.claim_next("p", 30) is None
    repo.complete(uuid4())
    repo.fail(uuid4(), "err")
    repo.heartbeat(uuid4(), "p")
    repo.mark_uploaded(uuid4(), "a" * 64, 10)
    assert repo.reap_expired() == 0


def test_extractor_port_satisfeito():
    extractor: ExtractorPort = _FakeExtractor()
    files = extractor.extract("BPA_MAG", date(2026, 1, 1), "t")
    assert len(files) == 1
    assert isinstance(extractor, ExtractorPort)


def test_protocol_stubs_invocados_direto():
    lookup = _FakeLookup()
    assert DimLookupPort.sk_profissional_por_cpf_hash(lookup, "x") is None
    assert DimLookupPort.sk_estabelecimento_por_cnes(lookup, "x") is None
    assert DimLookupPort.sk_cbo_por_codigo(lookup, "x") is None
    assert DimLookupPort.sk_competencia_por_yyyymm(lookup, 1) is None
    mapper = _FakeMapper()
    assert RowMapperPort.map_vinculo(mapper, {}) is None
    repo = _FakeRepo()
    payload = ExtractionRegisterPayload(
        job_id=uuid4(),
        files=[
            FileManifest(
                minio_key="x.parquet.gz", fato_subtype="BPA_C",
                size_bytes=1, sha256="a" * 64,
            ),
        ],
    )
    assert ExtractionRepoPort.register(repo, payload) is None
    assert ExtractionRepoPort.claim_next(repo, "p", 30) is None
    assert ExtractionRepoPort.complete(repo, uuid4()) is None
    assert ExtractionRepoPort.fail(repo, uuid4(), "e") is None
    assert ExtractionRepoPort.heartbeat(repo, uuid4(), "p") is None
    assert ExtractionRepoPort.reap_expired(repo) is None
    assert ExtractionRepoPort.mark_uploaded(repo, uuid4(), "a" * 64, 1) is None
    extractor = _FakeExtractor()
    assert ExtractorPort.extract(
        extractor, "S", date(2026, 1, 1), "t",
    ) is None
