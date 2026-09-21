"""TDD da composition root local do data_processor: zero Postgres/MinIO/AWS."""
from __future__ import annotations

from datetime import UTC, datetime

import pytest

from cnes_contracts.manifests.processing import NormalizeRequest
from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineLayout,
    SubtypeLayout,
    build_source_catalog,
)
from cnes_domain.profiles import ProfileNotImplemented, parse_profile
from cnes_infra.audit.local_sink import LocalAuditSink
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from cnes_infra.object_store import FilesystemObjectStore
from data_processor.composition import (
    LocalProcessorRuntime,
    UnsupportedSourceType,
    build_local_processor_runtime,
    build_source_registry,
    normalize_cnes,
)
from data_processor.orchestration.coordinator import PipelineCoordinator
from data_processor.orchestration.unit_handler import RunUnitCommandHandler
from data_processor.orchestration.unit_worker import UnitWorker
from data_processor.pipeline.source_registry import SourceRegistry
from data_processor.pipeline.stage_processor import StageProcessor


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _settings(tmp_path):
    return parse_profile({"TENANT_ID": "354130", "DATA_DIR": str(tmp_path)})


def test_runtime_local_nao_constroi_postgres_minio_aws(tmp_path, monkeypatch):
    for name in ("sqlalchemy.create_engine", "boto3.client"):
        monkeypatch.setattr(name, lambda *a, name=name, **k: pytest.fail(name), raising=False)

    runtime = build_local_processor_runtime(_settings(tmp_path), _utc_now)

    assert isinstance(runtime, LocalProcessorRuntime)
    assert isinstance(runtime.control_plane, SQLiteControlPlane)
    assert isinstance(runtime.object_store, FilesystemObjectStore)


def test_runtime_local_compoe_grafo_completo(tmp_path):
    runtime = build_local_processor_runtime(_settings(tmp_path), _utc_now)

    assert isinstance(runtime.source_registry, SourceRegistry)
    assert isinstance(runtime.stage_processor, StageProcessor)
    assert isinstance(runtime.audit_sink, LocalAuditSink)
    assert isinstance(runtime.coordinator, PipelineCoordinator)
    assert isinstance(runtime.unit_worker, UnitWorker)
    assert isinstance(runtime.unit_handler, RunUnitCommandHandler)
    assert runtime.control_plane.get_tenant("354130") is not None


def test_runtime_local_semeia_apenas_o_tenant_configurado(tmp_path):
    runtime = build_local_processor_runtime(_settings(tmp_path), _utc_now)

    tenant = runtime.control_plane.get_tenant("354130")

    assert tenant is not None
    assert tenant.tenant_id == "354130"


def test_profile_aws_nao_implementado(tmp_path):
    settings = parse_profile({
        "PROFILE": "aws", "TENANT_ID": "354130", "DATA_DIR": str(tmp_path),
        "AUTH_MODE": "oidc", "OIDC_ISSUER": "https://issuer.example",
    })

    with pytest.raises(ProfileNotImplemented, match="aws_runtime_plan_required"):
        build_local_processor_runtime(settings, _utc_now)


def test_registry_cnes_expoe_um_bundle_para_as_duas_fontes():
    catalog = build_source_catalog()
    registry = build_source_registry(catalog)

    local = registry.for_source(SourceType.CNES_LOCAL)
    nacional = registry.for_source(SourceType.CNES_NACIONAL)

    assert local is nacional
    assert registry.for_pipeline("cnes") is local
    assert local.pipeline_id == "cnes"
    assert local.definition is catalog.for_pipeline("cnes")
    assert local.source_types == ("CNES_LOCAL", "CNES_NACIONAL")
    assert local.dependencies == (
        RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
        RunDependency(source_type="CNES_NACIONAL", file_subtype="CNES_VINCULO", required=False),
    )
    assert local.layout == PipelineLayout(
        normalized=(
            SubtypeLayout("CNES_LOCAL", "CNES_VINCULO", ("cnes_local.parquet",)),
            SubtypeLayout("CNES_NACIONAL", "CNES_VINCULO", ("cnes_nacional.parquet",)),
        ),
        reconciliation_filename="cnes.parquet",
        divergence_filename="cnes_divergences.parquet",
        serving_documents=("overview",),
    )


def test_build_source_registry_usa_catalogo_default_quando_none():
    registry = build_source_registry()

    assert registry.for_pipeline("cnes") is not None


def test_normalize_cnes_rejeita_source_type_desconhecido():
    request = NormalizeRequest.model_construct(source_type="SIHD")

    with pytest.raises(UnsupportedSourceType):
        normalize_cnes(request, store=None)
