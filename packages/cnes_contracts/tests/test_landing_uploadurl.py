"""Tests for UploadUrlRequest + UploadUrlResponse contracts."""
from __future__ import annotations

from datetime import date
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.landing import UploadUrlRequest, UploadUrlResponse


def test_aceita_request_minimo():
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


def test_rejeita_source_type_desconhecido():
    with pytest.raises(ValidationError):
        UploadUrlRequest(
            job_id=uuid4(),
            tenant_id="354130",
            source_type="UNKNOWN_SOURCE",
            tipo_extracao="profissionais",
            competencia=date(2026, 1, 1),
            intent="cnes_profissionais",
        )


def test_rejeita_machine_id_acima_de_128_chars():
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


def test_aceita_response_valido():
    extraction_id = uuid4()
    resp = UploadUrlResponse(
        extraction_id=extraction_id,
        upload_url="https://minio.example/bucket/key?sig=abc",
        minio_key="354130/CNES_VINCULO/2026-01-01/foo.parquet.gz",
    )
    assert resp.extraction_id == extraction_id


def test_rejeita_minio_key_sem_extensao_parquet_gz():
    with pytest.raises(ValidationError):
        UploadUrlResponse(
            extraction_id=uuid4(),
            upload_url="https://minio.example/key",
            minio_key="not_a_parquet",
        )


def test_request_frozen_rejeita_mutacao():
    req = UploadUrlRequest(
        job_id=uuid4(),
        tenant_id="354130",
        source_type="CNES_LOCAL",
        tipo_extracao="profissionais",
        competencia=date(2026, 1, 1),
        intent="cnes_profissionais",
    )
    with pytest.raises(ValidationError):
        req.tenant_id = "999999"


def test_response_frozen_rejeita_mutacao():
    resp = UploadUrlResponse(
        extraction_id=uuid4(),
        upload_url="https://minio.example/key?sig=abc",
        minio_key="354130/CNES_VINCULO/2026-01-01/foo.parquet.gz",
    )
    with pytest.raises(ValidationError):
        resp.minio_key = "other.parquet.gz"
