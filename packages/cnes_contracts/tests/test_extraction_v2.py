from __future__ import annotations

from datetime import UTC, date, datetime
from typing import ClassVar
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.jobs import JobStatus
from cnes_contracts.landing import Extraction, FileManifest


def _file(subtype: str) -> FileManifest:
    return FileManifest(
        minio_key=f"x/y/{subtype.lower()}.parquet.gz",
        fato_subtype=subtype, size_bytes=1024, sha256="a" * 64,
    )


class TestExtraction:
    def test_bpa_mag_com_dois_files(self) -> None:
        e = Extraction(
            job_id=uuid4(), tenant_id="pilot-sp", source_type="BPA_MAG",
            competencia=date(2026, 1, 1),
            files=[_file("BPA_C"), _file("BPA_I")],
            status=JobStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        assert len(e.files) == 2

    def test_rejeita_files_vazio(self) -> None:
        with pytest.raises(ValidationError):
            Extraction(
                job_id=uuid4(), tenant_id="t", source_type="BPA_MAG",
                competencia=date(2026, 1, 1), files=[],
                status=JobStatus.PENDING,
                created_at=datetime.now(UTC),
            )

    def test_rejeita_source_type_desconhecido(self) -> None:
        with pytest.raises(ValidationError):
            Extraction(
                job_id=uuid4(), tenant_id="t", source_type="UNKNOWN",
                competencia=date(2026, 1, 1), files=[_file("BPA_C")],
                status=JobStatus.PENDING,
                created_at=datetime.now(UTC),
            )

    def test_depends_on_default_vazio(self) -> None:
        e = Extraction(
            job_id=uuid4(), tenant_id="t", source_type="CNES_LOCAL",
            competencia=date(2026, 1, 1), files=[_file("CNES_VINCULO")],
            status=JobStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        assert e.depends_on == []

    def test_depends_on_aceita_uuid_list(self) -> None:
        dep = uuid4()
        e = Extraction(
            job_id=uuid4(), tenant_id="t", source_type="SIA_LOCAL",
            competencia=date(2026, 1, 1), files=[_file("SIA_APA")],
            depends_on=[dep], status=JobStatus.PENDING,
            created_at=datetime.now(UTC),
        )
        assert e.depends_on == [dep]


class TestExtractionRegisterPayloadAgentMetadata:
    _BASE: ClassVar[dict] = {
        "job_id": UUID("00000000-0000-0000-0000-000000000001"),
        "files": [{
            "minio_key": "x/y.parquet.gz",
            "fato_subtype": "CNES_VINCULO",
            "size_bytes": 100,
            "sha256": "a" * 64,
        }],
    }

    def test_aceita_agent_version_e_machine_id(self) -> None:
        from cnes_contracts.landing import ExtractionRegisterPayload
        payload = ExtractionRegisterPayload.model_validate(
            {**self._BASE, "agent_version": "1.2.3", "machine_id": "edge-01"},
        )
        assert payload.agent_version == "1.2.3"
        assert payload.machine_id == "edge-01"

    def test_campos_opcionais_default_none(self) -> None:
        from cnes_contracts.landing import ExtractionRegisterPayload
        payload = ExtractionRegisterPayload.model_validate(self._BASE)
        assert payload.agent_version is None
        assert payload.machine_id is None

    def test_rejeita_agent_version_acima_de_64_chars(self) -> None:
        from cnes_contracts.landing import ExtractionRegisterPayload
        with pytest.raises(ValidationError):
            ExtractionRegisterPayload.model_validate(
                {**self._BASE, "agent_version": "x" * 65},
            )

    def test_rejeita_machine_id_acima_de_128_chars(self) -> None:
        from cnes_contracts.landing import ExtractionRegisterPayload
        with pytest.raises(ValidationError):
            ExtractionRegisterPayload.model_validate(
                {**self._BASE, "machine_id": "y" * 129},
            )
