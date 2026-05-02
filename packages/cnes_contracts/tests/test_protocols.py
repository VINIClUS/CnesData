"""Protocol smoke — import + structural satisfaction."""
from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

from cnes_contracts.fatos import VinculoCNES
from cnes_contracts.landing import FileManifest
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
    def enqueue(
        self, engine, *, tenant_id, source_type, competencia,
        files, depends_on=None,
    ):
        return uuid4()

    def claim_next(
        self, engine, *, lease_seconds=300,
    ):
        return None

    def register(
        self, engine, *, job_id, files,
        agent_version=None, machine_id=None,
    ):
        return job_id

    def mark_completed(self, engine, *, job_id):
        return None

    def mark_failed(self, engine, *, job_id, reason):
        return None

    def reap_expired(self, engine):
        return 0


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
    engine = object()  # placeholder; Protocol is structural, runtime value irrelevant
    job_id = uuid4()
    assert repo.enqueue(
        engine, tenant_id="354130", source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
    ) is not None
    assert repo.claim_next(engine) is None
    assert repo.register(
        engine, job_id=job_id, files=[],
    ) == job_id
    assert repo.mark_completed(engine, job_id=job_id) is None
    assert repo.mark_failed(engine, job_id=job_id, reason="x") is None
    assert repo.reap_expired(engine) == 0


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
    engine = object()
    assert ExtractionRepoPort.enqueue(
        repo, engine, tenant_id="354130", source_type="BPA_MAG",
        competencia=date(2026, 1, 1),
        files=[{"minio_key": "x.parquet.gz"}],
    ) is None
    assert ExtractionRepoPort.claim_next(repo, engine) is None
    assert ExtractionRepoPort.register(
        repo, engine, job_id=uuid4(), files=[],
    ) is None
    assert ExtractionRepoPort.mark_completed(
        repo, engine, job_id=uuid4(),
    ) is None
    assert ExtractionRepoPort.mark_failed(
        repo, engine, job_id=uuid4(), reason="e",
    ) is None
    assert ExtractionRepoPort.reap_expired(repo, engine) is None
    extractor = _FakeExtractor()
    assert ExtractorPort.extract(
        extractor, "S", date(2026, 1, 1), "t",
    ) is None
