"""Tests for UploadUrlRequest + UploadUrlResponse contracts."""
from datetime import date
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.landing import UploadUrlRequest, UploadUrlResponse


def test_upload_url_request_minimal_required_fields():
    req = UploadUrlRequest(
        job_id=uuid4(),
        tenant_id="354130",
        source_type="CNES_LOCAL",
        tipo_extracao="profissionais",
        competencia=date(2026, 1, 1),
        intent="cnes_profissionais",
    )
    assert req.agent_version is None
    assert req.machine_id is None


def test_upload_url_request_rejects_invalid_source_type():
    with pytest.raises(ValidationError):
        UploadUrlRequest(
            job_id=uuid4(),
            tenant_id="354130",
            source_type="UNKNOWN_SOURCE",
            tipo_extracao="profissionais",
            competencia=date(2026, 1, 1),
            intent="cnes_profissionais",
        )


def test_upload_url_request_max_length_machine_id():
    with pytest.raises(ValidationError):
        UploadUrlRequest(
            job_id=uuid4(),
            tenant_id="354130",
            source_type="CNES_LOCAL",
            tipo_extracao="profissionais",
            competencia=date(2026, 1, 1),
            intent="cnes_profissionais",
            machine_id="x" * 129,
        )


def test_upload_url_response_required_fields():
    extraction_id = uuid4()
    resp = UploadUrlResponse(
        extraction_id=extraction_id,
        upload_url="https://minio.example/bucket/key?sig=abc",
        minio_key="354130/CNES_VINCULO/2026-01-01/foo.parquet.gz",
    )
    assert resp.extraction_id == extraction_id


def test_upload_url_response_rejects_invalid_minio_key():
    with pytest.raises(ValidationError):
        UploadUrlResponse(
            extraction_id=uuid4(),
            upload_url="https://minio.example/key",
            minio_key="not_a_parquet",
        )
