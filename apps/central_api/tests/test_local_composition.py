"""TDD da composition root local do central_api: zero Postgres/MinIO/AWS."""
from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO

import pytest

from central_api.composition import LocalRuntime, build_local_runtime
from central_api.services.raw_ingestion import RawIngestionService
from central_api.services.run_planning import RunPlanningService
from cnes_domain.profiles import ProfileNotImplemented, parse_profile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store import FilesystemObjectStore


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _settings(tmp_path):
    return parse_profile({"TENANT_ID": "354130", "DATA_DIR": str(tmp_path)})


def test_local_runtime_nao_constroi_postgres_minio_aws(tmp_path, monkeypatch):
    for name in ("sqlalchemy.create_engine", "boto3.client"):
        monkeypatch.setattr(name, lambda *a, name=name, **k: pytest.fail(name), raising=False)

    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    assert isinstance(runtime, LocalRuntime)
    assert isinstance(runtime.control_plane, SQLiteControlPlane)
    assert isinstance(runtime.object_store, FilesystemObjectStore)


def test_local_runtime_compoe_grafo_completo(tmp_path):
    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    assert isinstance(runtime.audit_sink, LocalAuditSink)
    assert isinstance(runtime.raw_ingestion, RawIngestionService)
    assert isinstance(runtime.run_planning, RunPlanningService)
    assert runtime.source_catalog.for_pipeline("cnes") is not None
    assert runtime.control_plane.get_tenant("354130") is not None


def test_local_runtime_expoe_o_mesmo_par_control_plane_object_store(tmp_path):
    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    assert runtime.run_planning is not None
    key = "raw/354130/CNES_LOCAL/2026-01/snap/data.parquet"
    body = b"payload"
    digest = sha256(body).hexdigest()
    runtime.object_store.put(key, BytesIO(body), digest)
    assert runtime.object_store.stat(key) is not None


def test_raw_ingestion_hook_e_run_planning_on_raw_manifest_accepted(tmp_path):
    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    assert runtime.raw_ingestion._accepted_manifest == runtime.run_planning.on_raw_manifest_accepted


def test_profile_aws_nao_implementado(tmp_path):
    settings = parse_profile({
        "PROFILE": "aws", "TENANT_ID": "354130", "DATA_DIR": str(tmp_path),
        "AUTH_MODE": "oidc", "OIDC_ISSUER": "https://issuer.example",
    })

    with pytest.raises(ProfileNotImplemented, match="aws_runtime_plan_required"):
        build_local_runtime(settings, _utc_now)


def test_executor_local_rejeita_execucao_de_unit_no_processo_da_api(tmp_path):
    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    with pytest.raises(NotImplementedError, match="processor_owns_unit_execution"):
        runtime.executor._handler(object())


def test_local_runtime_deixa_dispatch_para_o_processor(tmp_path):
    runtime = build_local_runtime(_settings(tmp_path), _utc_now)

    assert runtime.run_planning._dispatch_enabled is False
