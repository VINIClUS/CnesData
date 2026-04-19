"""Testes dos modelos Pydantic da API."""

import uuid
from datetime import datetime

import pytest
from pydantic import ValidationError

from cnes_domain.models.api import (
    AcquireJobRequest,
    AcquireJobResponse,
    CompleteUploadRequest,
    HealthResponse,
    HeartbeatRequest,
    HeartbeatResponse,
    JobStatusResponse,
)

_NOW = datetime(2025, 1, 15, 12, 0, 0, tzinfo=None)  # noqa: DTZ001
_UUID = uuid.uuid4()


class TestJobStatusResponse:

    def test_campos_obrigatorios(self):
        m = JobStatusResponse(
            job_id="abc123",
            status="pending",
            source_system="FIREBIRD",
            tenant_id="355030",
        )
        assert m.job_id == "abc123"
        assert m.created_at is None

    def test_campos_opcionais_aceitos(self):
        m = JobStatusResponse(
            job_id="x",
            status="done",
            source_system="HR",
            tenant_id="t",
            error_detail="falha",
        )
        assert m.error_detail == "falha"


class TestAcquireJobRequest:

    def test_machine_id_valido(self):
        m = AcquireJobRequest(machine_id="worker-01")
        assert m.machine_id == "worker-01"

    def test_machine_id_vazio_levanta_erro(self):
        with pytest.raises(ValidationError):
            AcquireJobRequest(machine_id="")

    def test_source_system_opcional_none(self):
        m = AcquireJobRequest(machine_id="w")
        assert m.source_system is None


class TestAcquireJobResponse:

    def test_campos_obrigatorios(self):
        m = AcquireJobResponse(
            job_id=_UUID,
            source_system="FIREBIRD",
            tenant_id="t",
            upload_url="https://s3.example.com/upload",
            object_key="bucket/obj.parquet",
            lease_expires_at=_NOW,
        )
        assert m.job_id == _UUID
        assert m.source_system == "FIREBIRD"


class TestHeartbeatRequest:

    def test_machine_id_valido(self):
        m = HeartbeatRequest(machine_id="worker-02")
        assert m.machine_id == "worker-02"

    def test_machine_id_muito_longo_levanta_erro(self):
        with pytest.raises(ValidationError):
            HeartbeatRequest(machine_id="x" * 129)


class TestHeartbeatResponse:

    def test_renewed_true(self):
        m = HeartbeatResponse(renewed=True)
        assert m.renewed is True
        assert m.lease_expires_at is None

    def test_renewed_false_com_data(self):
        m = HeartbeatResponse(renewed=False, lease_expires_at=_NOW)
        assert m.lease_expires_at == _NOW


class TestCompleteUploadRequest:

    def test_campos_validos(self):
        m = CompleteUploadRequest(
            machine_id="m", object_key="k", size_bytes=1024,
        )
        assert m.object_key == "k"
        assert m.size_bytes == 1024

    def test_object_key_vazio_levanta_erro(self):
        with pytest.raises(ValidationError):
            CompleteUploadRequest(
                machine_id="m", object_key="", size_bytes=1,
            )

    def test_rejeita_size_bytes_negativo(self):
        with pytest.raises(ValidationError):
            CompleteUploadRequest(
                machine_id="m", object_key="k", size_bytes=-1,
            )

    def test_aceita_size_bytes_zero(self):
        m = CompleteUploadRequest(
            machine_id="m", object_key="k", size_bytes=0,
        )
        assert m.size_bytes == 0

    def test_size_bytes_obrigatorio(self):
        with pytest.raises(ValidationError):
            CompleteUploadRequest(machine_id="m", object_key="k")


class TestHealthResponse:

    def test_campos_validos(self):
        m = HealthResponse(status="ok", db_connected=True, timestamp=_NOW)
        assert m.status == "ok"
        assert m.db_connected is True
