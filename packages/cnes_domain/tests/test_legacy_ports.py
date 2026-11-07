"""Protocol smoke — import + structural satisfaction."""
from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any, cast
from uuid import uuid4

from cnes_contracts.fatos import VinculoCNES
from cnes_contracts.landing import FileManifest
from cnes_domain.ports.extraction import (
    DimLookupPort,
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



def test_extractor_port_satisfeito():
    extractor: ExtractorPort = _FakeExtractor()
    files = extractor.extract("BPA_MAG", date(2026, 1, 1), "t")
    assert len(files) == 1
    assert isinstance(extractor, ExtractorPort)


def test_protocol_stubs_invocados_direto():
    dim_port = cast("Any", DimLookupPort)
    mapper_port = cast("Any", RowMapperPort)
    extractor_port = cast("Any", ExtractorPort)
    lookup = _FakeLookup()
    assert dim_port.sk_profissional_por_cpf_hash(lookup, "x") is None
    assert dim_port.sk_estabelecimento_por_cnes(lookup, "x") is None
    assert dim_port.sk_cbo_por_codigo(lookup, "x") is None
    assert dim_port.sk_competencia_por_yyyymm(lookup, 1) is None
    mapper = _FakeMapper()
    assert mapper_port.map_vinculo(mapper, {}) is None
    extractor = _FakeExtractor()
    assert extractor_port.extract(
        extractor, "S", date(2026, 1, 1), "t",
    ) is None
