from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

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
