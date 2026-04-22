"""Testes de contratos de landing."""
from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from cnes_contracts.jobs import JobStatus
from cnes_contracts.landing import Extraction, ExtractionRegisterPayload


def _valid_extraction_kwargs():
    return {
        "id": uuid4(),
        "job_id": uuid4(),
        "tenant_id": "354130",
        "fonte_sistema": "CNES_LOCAL",
        "tipo_extracao": "mensal",
        "competencia": 202601,
        "status": JobStatus.PENDING,
        "created_at": datetime(2026, 1, 1, tzinfo=UTC),
    }


def test_extraction_valida():
    kwargs = _valid_extraction_kwargs()
    kwargs.update(
        object_key="raw/cnes/2026/01/ext.parquet",
        row_count=1000,
        sha256="a" * 64,
        schema_version=2,
        agent_version="1.0.0",
        machine_id="host-01",
        lease_owner="worker-1",
        lease_until=datetime(2026, 1, 2, tzinfo=UTC),
        retry_count=1,
        error_detail="none",
        uploaded_at=datetime(2026, 1, 1, 1, tzinfo=UTC),
        started_at=datetime(2026, 1, 1, 2, tzinfo=UTC),
        completed_at=datetime(2026, 1, 1, 3, tzinfo=UTC),
    )
    ext = Extraction(**kwargs)
    assert ext.tenant_id == "354130"
    assert ext.row_count == 1000
    assert ext.sha256 == "a" * 64


def test_extraction_defaults_minimos():
    ext = Extraction(**_valid_extraction_kwargs())
    assert ext.schema_version == 1
    assert ext.retry_count == 0
    assert ext.row_count is None
    assert ext.sha256 is None


def test_extraction_rejeita_tenant_id_invalido():
    kwargs = _valid_extraction_kwargs()
    kwargs["tenant_id"] = "abc"
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_competencia_fora_range_inferior():
    kwargs = _valid_extraction_kwargs()
    kwargs["competencia"] = 199912
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_competencia_fora_range_superior():
    kwargs = _valid_extraction_kwargs()
    kwargs["competencia"] = 210001
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_row_count_negativo():
    kwargs = _valid_extraction_kwargs()
    kwargs["row_count"] = -1
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_sha256_invalido():
    kwargs = _valid_extraction_kwargs()
    kwargs["sha256"] = "XYZ"
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_schema_version_zero():
    kwargs = _valid_extraction_kwargs()
    kwargs["schema_version"] = 0
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_retry_count_negativo():
    kwargs = _valid_extraction_kwargs()
    kwargs["retry_count"] = -1
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_rejeita_fonte_sistema_invalida():
    kwargs = _valid_extraction_kwargs()
    kwargs["fonte_sistema"] = "DESCONHECIDO"
    with pytest.raises(ValidationError):
        Extraction(**kwargs)


def test_extraction_register_payload_valido():
    p = ExtractionRegisterPayload(
        tenant_id="354130",
        fonte_sistema="SIHD",
        tipo_extracao="mensal",
        competencia=202601,
        job_id=uuid4(),
        agent_version="1.0.0",
        machine_id="host-01",
    )
    assert p.fonte_sistema == "SIHD"


def test_extraction_register_payload_rejeita_tenant_invalido():
    with pytest.raises(ValidationError):
        ExtractionRegisterPayload(
            tenant_id="1",
            fonte_sistema="SIHD",
            tipo_extracao="mensal",
            competencia=202601,
            job_id=uuid4(),
            agent_version="1.0.0",
            machine_id="host-01",
        )


def test_extraction_register_payload_rejeita_competencia_invalida():
    with pytest.raises(ValidationError):
        ExtractionRegisterPayload(
            tenant_id="354130",
            fonte_sistema="SIHD",
            tipo_extracao="mensal",
            competencia=210001,
            job_id=uuid4(),
            agent_version="1.0.0",
            machine_id="host-01",
        )
