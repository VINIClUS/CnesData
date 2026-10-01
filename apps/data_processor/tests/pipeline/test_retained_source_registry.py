"""Registry das fontes retidas: bundles, layout por dependencia e chaves do StageProcessor."""
from __future__ import annotations

import hashlib
import inspect
import sys
import zlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING

import polars as pl
import pytest

from apps.data_processor.tests.sources.sihd import (
    FakeObjectStore,
    normalize_request,
    put_raw,
    raw_spec,
    reconcile_request,
)
from cnes_contracts.manifests.outputs import OutputManifest, ServingDocument
from cnes_contracts.manifests.processing import (
    MaterializeResult,
    NormalizeResult,
    ReconcileResult,
)
from cnes_contracts.manifests.raw import RawManifest, SnapshotMode, SourceType
from cnes_domain.control_plane.entities import ManifestRef, Run, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.orchestration.source_catalog import build_source_catalog
from cnes_domain.orchestration.source_definitions.bpa import BPA_DEPENDENCIES, BPA_LAYOUT
from cnes_domain.orchestration.source_definitions.sia import SIA_DEPENDENCIES, SIA_LAYOUT
from cnes_domain.orchestration.source_definitions.sihd import SIHD_DEPENDENCIES, SIHD_LAYOUT
from cnes_domain.ports.object_store import ObjectStat
from data_processor.composition import build_source_registry
from data_processor.orchestration.attempt_store import AttemptObjectStore
from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry
from data_processor.pipeline.stage_processor import StageProcessor
from data_processor.sources.bpa.normalize import normalize_bpa
from data_processor.sources.bpa.reconcile import reconcile_bpa
from data_processor.sources.bpa.serving import materialize_bpa
from data_processor.sources.sia.normalize import normalize_sia
from data_processor.sources.sia.reconcile import reconcile_sia
from data_processor.sources.sia.serving import materialize_sia
from data_processor.sources.sihd.normalize import normalize_sihd
from data_processor.sources.sihd.reconcile import reconcile_sihd
from data_processor.sources.sihd.serving import materialize_sihd

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

    from cnes_domain.orchestration.source_catalog import PipelineDefinition

_TENANT = "354130"
_RUN_ID = "run-1"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)

_RETAINED = {
    "sihd": (
        normalize_sihd, reconcile_sihd, materialize_sihd, SIHD_LAYOUT, SIHD_DEPENDENCIES,
    ),
    "bpa": (normalize_bpa, reconcile_bpa, materialize_bpa, BPA_LAYOUT, BPA_DEPENDENCIES),
    "sia": (normalize_sia, reconcile_sia, materialize_sia, SIA_LAYOUT, SIA_DEPENDENCIES),
}
_SOURCE_PIPELINES = (
    (SourceType.CNES_LOCAL, "cnes"),
    (SourceType.CNES_NACIONAL, "cnes"),
    (SourceType.SIHD, "sihd"),
    (SourceType.BPA_MAG, "bpa"),
    (SourceType.SIA_LOCAL, "sia"),
)
_EXPECTED_SUBTYPES = {
    "sihd": ("SIHD_INTERNACAO", "SIHD_PROC_AIH"),
    "bpa": ("BPA_C", "BPA_I"),
    "sia": ("SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO"),
}


@dataclass
class _Store:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def open(self, key: str) -> ContextManager[BinaryIO]:
        return nullcontext(BytesIO(self.objects[key]))

    def stat(self, key: str) -> ObjectStat | None:
        data = self.objects.get(key)
        if data is None:
            return None
        return ObjectStat(key=key, size_bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def delete(self, key: str) -> None:
        self.objects.pop(key, None)

    def promote(self, source_key: str, destination_key: str, expected_sha256: str) -> ObjectStat:
        raise NotImplementedError


@dataclass
class _ControlPlane:
    run: Run
    units: tuple[RunUnit, ...] = ()

    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        return self.run

    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        return self.units


@dataclass
class _Recorder:
    requests: list[object] = field(default_factory=list)

    def normalize(self, request: object, store: object) -> NormalizeResult:
        self.requests.append(request)
        return NormalizeResult(manifests=tuple(
            _output(key, "normalized", request.source_type, request.unit_id)
            for key in request.target_keys
        ))

    def reconcile(self, request: object, store: object) -> ReconcileResult:
        self.requests.append(request)
        keys = (request.reconciliation_key, request.divergence_key)
        first, second = (_output(key, "reconciliation", None, request.unit_id) for key in keys)
        return ReconcileResult(
            reconciliation_manifest=first, divergence_manifest=second, kpis={}
        )

    def materialize(self, request: object, store: object) -> MaterializeResult:
        self.requests.append(request)
        manifests = tuple(
            _output(key, "serving", None, request.unit_id) for key in request.target_keys
        )
        documents = tuple(
            ServingDocument(
                schema_version="serving-v1", document_name=key.rsplit("/", 1)[-1][:-5],
                tenant_id=_TENANT, run_id=_RUN_ID, generated_at=_NOW, payload={},
            )
            for key in request.target_keys
        )
        return MaterializeResult(manifests=manifests, documents=documents)


def _digest(value: str) -> str:
    return f"{zlib.crc32(value.encode()):08x}"


def _output(
    key: str, layer: str, source_type: SourceType | None, unit_id: str, manifest_id: str = "",
) -> OutputManifest:
    return OutputManifest(
        manifest_version=1, manifest_id=manifest_id or f"out-{_digest(key)}",
        tenant_id=_TENANT, layer=layer, source_type=source_type, competencia=_COMPETENCIA,
        run_id=_RUN_ID, unit_id=unit_id, attempt=1, schema_version="v1", object_key=key,
        object_sha256="1" * 64, row_count=1, created_at=_NOW,
    )


def _recording_registry(recorder: _Recorder) -> SourceRegistry:
    catalog = build_source_catalog()
    bundles = tuple(
        SourcePipeline(
            definition=definition, normalize=recorder.normalize,
            reconcile=recorder.reconcile, materialize=recorder.materialize,
        )
        for definition in catalog.definitions
    )
    return SourceRegistry(catalog, bundles)


def _run(definition: PipelineDefinition) -> Run:
    return Run.model_validate({
        "tenant_id": _TENANT, "run_id": _RUN_ID, "competencia": _COMPETENCIA,
        "dataset_name": definition.pipeline_id, "state": RunState.PROCESSING,
        "dependencies": definition.dependencies, "missing_sources": (), "created_at": _NOW,
    })


def _unit(
    stage: RunStage, unit_id: str, *, depends_on: tuple[str, ...] = (),
    inputs: tuple[ManifestRef, ...] = (), subject: tuple[str, str] | None = None,
) -> RunUnit:
    source_type, file_subtype = subject if subject else (None, None)
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=stage,
        source_type=source_type, file_subtype=file_subtype, partition="all",
        depends_on_unit_ids=depends_on, input_manifests=inputs, state=RunUnitState.LEASED,
        attempt=1, fencing_token=1, lease_owner="worker-1", lease_until=_NOW,
        dispatch_id="a" * 16, output_manifests=(), error_code=None,
    )


def _succeeded(unit_id: str, stage: RunStage, refs: tuple[ManifestRef, ...]) -> RunUnit:
    if stage is RunStage.NORMALIZE:
        dummy = ManifestRef(manifest_id="raw-dummy", manifest_key="raw/dummy/manifest.json")
        base = _unit(stage, unit_id, inputs=(dummy,), subject=("SIHD", "SIHD_INTERNACAO"))
    else:
        base = _unit(stage, unit_id, depends_on=("upstream",))
    return base.model_copy(update={
        "state": RunUnitState.SUCCEEDED, "output_manifests": refs,
        "lease_owner": None, "lease_until": None, "dispatch_id": None,
    })


def _raw_ref(store: _Store, source_type: str, file_subtype: str) -> ManifestRef:
    base = f"raw/{_TENANT}/{source_type}/{_COMPETENCIA}/snap-{file_subtype}"
    manifest = RawManifest(
        manifest_version=1, manifest_id=f"raw-{file_subtype}", tenant_id=_TENANT,
        source_type=SourceType(source_type), file_subtype=file_subtype,
        competencia=_COMPETENCIA, agent_id="agent-1", agent_version="1.0.0",
        schema_version="raw-v1", snapshot_mode=SnapshotMode.FULL,
        snapshot_id=f"snap-{file_subtype}", base_snapshot_id=None, sequence=1,
        previous_manifest_sha256=None, object_sha256="0" * 64, row_count=1, size_bytes=10,
        object_key=f"{base}/data.parquet", created_at=_NOW,
    )
    key = f"{base}/manifest.json"
    store.objects[key] = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    return ManifestRef(manifest_id=manifest.manifest_id, manifest_key=key)


def _output_ref(store: _Store, unit_id: str, layer: str, object_key: str) -> ManifestRef:
    manifest_id = f"m-{_digest(object_key)}"
    source_type = SourceType(object_key.split("/")[2]) if layer == "normalized" else None
    manifest = _output(object_key, layer, source_type, unit_id, manifest_id)
    key = f"tmp/{_TENANT}/{_RUN_ID}/{unit_id}/1/manifests/{manifest_id}/manifest.json"
    store.objects[key] = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    return ManifestRef(manifest_id=manifest_id, manifest_key=key)


def _processor(
    definition: PipelineDefinition, store: _Store, recorder: _Recorder,
    units: tuple[RunUnit, ...] = (),
) -> StageProcessor:
    return StageProcessor(
        _ControlPlane(_run(definition), units), store, _recording_registry(recorder),
        lambda: _NOW,
    )


def _attempt(store: _Store) -> AttemptObjectStore:
    return AttemptObjectStore(delegate=store, prefix="tmp/x")


@pytest.mark.parametrize(("source_type", "pipeline_id"), _SOURCE_PIPELINES)
def test_registry_resolve_as_cinco_fontes(source_type: SourceType, pipeline_id: str) -> None:
    registry = build_source_registry()

    assert registry.for_source(source_type).pipeline_id == pipeline_id
    assert registry.for_source(source_type) is registry.for_pipeline(pipeline_id)


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_bundle_retido_expoe_stages_reais_e_definicao_do_catalogo(pipeline_id: str) -> None:
    normalize, reconcile, materialize, layout, dependencies = _RETAINED[pipeline_id]
    catalog = build_source_catalog()

    bundle = build_source_registry(catalog).for_pipeline(pipeline_id)

    assert bundle.normalize is normalize
    assert bundle.reconcile is reconcile
    assert bundle.materialize is materialize
    assert bundle.definition is catalog.for_pipeline(pipeline_id)
    assert bundle.layout is layout
    assert bundle.dependencies is dependencies


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_stages_retidos_nao_dependem_de_engine_nem_sqlalchemy(pipeline_id: str) -> None:
    bundle = build_source_registry().for_pipeline(pipeline_id)

    for stage in (bundle.normalize, bundle.reconcile, bundle.materialize):
        parameters = inspect.signature(stage).parameters
        assert tuple(parameters) == ("request", "store")
        assert all(p.kind is p.POSITIONAL_OR_KEYWORD for p in parameters.values())
        assert not [p for p in parameters.values() if "Engine" in str(p.annotation)]
        assert "sqlalchemy" not in vars(sys.modules[stage.__module__])


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_um_layout_por_dependencia_na_mesma_ordem(pipeline_id: str) -> None:
    _, _, _, layout, dependencies = _RETAINED[pipeline_id]

    layout_pairs = [(item.source_type, item.file_subtype) for item in layout.normalized]
    dependency_pairs = [(dep.source_type, dep.file_subtype) for dep in dependencies]

    assert layout_pairs == dependency_pairs
    assert tuple(subtype for _, subtype in layout_pairs) == _EXPECTED_SUBTYPES[pipeline_id]


@pytest.mark.parametrize(("pipeline_id", "total"), [("sihd", 2), ("bpa", 2), ("sia", 5)])
def test_dependencias_retidas_sao_todas_obrigatorias(pipeline_id: str, total: int) -> None:
    dependencies = _RETAINED[pipeline_id][4]

    assert len(dependencies) == total
    assert sum(dep.required for dep in dependencies) == total


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_stage_processor_deriva_chaves_normalize_sem_condicional_de_fonte(
    pipeline_id: str,
) -> None:
    layout = _RETAINED[pipeline_id][3]
    definition = build_source_catalog().for_pipeline(pipeline_id)
    store, recorder = _Store(), _Recorder()
    processor = _processor(definition, store, recorder)

    for item in layout.normalized:
        ref = _raw_ref(store, item.source_type, item.file_subtype)
        unit = _unit(
            RunStage.NORMALIZE, f"u-{item.file_subtype}", inputs=(ref,),
            subject=(item.source_type, item.file_subtype),
        )
        result = processor(unit, _attempt(store))
        expected = tuple(sorted(
            f"normalized/{_TENANT}/{item.source_type}/{_COMPETENCIA}/{_RUN_ID}/{name}"
            for name in item.normalized_filenames
        ))
        assert recorder.requests[-1].target_keys == expected
        assert tuple(m.object_key for m in result) == expected

    assert len(recorder.requests) == len(layout.normalized)


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_stage_processor_deriva_chaves_reconcile_e_divergencia(pipeline_id: str) -> None:
    layout = _RETAINED[pipeline_id][3]
    definition = build_source_catalog().for_pipeline(pipeline_id)
    store, recorder = _Store(), _Recorder()
    predecessors = tuple(
        _succeeded(
            f"u-{item.file_subtype}", RunStage.NORMALIZE,
            tuple(
                _output_ref(
                    store, f"u-{item.file_subtype}", "normalized",
                    f"normalized/{_TENANT}/{item.source_type}/{_COMPETENCIA}/{_RUN_ID}/{name}",
                )
                for name in item.normalized_filenames
            ),
        )
        for item in layout.normalized
    )
    unit = _unit(
        RunStage.RECONCILE, "u-reconcile", depends_on=tuple(p.unit_id for p in predecessors)
    )

    result = _processor(definition, store, recorder, predecessors)(unit, _attempt(store))

    prefix = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}"
    request = recorder.requests[0]
    assert request.reconciliation_key == f"{prefix}/{layout.reconciliation_filename}"
    assert request.divergence_key == f"{prefix}/{layout.divergence_filename}"
    assert len(request.normalized_manifests) == sum(
        len(item.normalized_filenames) for item in layout.normalized
    )
    assert tuple(m.object_key for m in result) == (
        request.reconciliation_key, request.divergence_key
    )


@pytest.mark.parametrize("pipeline_id", sorted(_RETAINED))
def test_stage_processor_deriva_chaves_serving_por_documento(pipeline_id: str) -> None:
    layout = _RETAINED[pipeline_id][3]
    definition = build_source_catalog().for_pipeline(pipeline_id)
    store, recorder = _Store(), _Recorder()
    prefix = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}"
    refs = tuple(
        _output_ref(store, "u-reconcile", "reconciliation", f"{prefix}/{name}")
        for name in (layout.reconciliation_filename, layout.divergence_filename)
    )
    predecessor = _succeeded("u-reconcile", RunStage.RECONCILE, refs)
    unit = _unit(RunStage.MATERIALIZE, "u-materialize", depends_on=("u-reconcile",))

    result = _processor(definition, store, recorder, (predecessor,))(unit, _attempt(store))

    expected = tuple(sorted(
        f"serving/{_TENANT}/{_RUN_ID}/{document}.json" for document in layout.serving_documents
    ))
    request = recorder.requests[0]
    assert request.target_keys == expected
    assert request.reconciliation_manifest.object_key.endswith(layout.reconciliation_filename)
    assert request.divergence_manifest.object_key.endswith(layout.divergence_filename)
    assert tuple(m.object_key for m in result) == expected


def test_subtipo_sem_linhas_e_presente_e_reconcilia_sem_erro_de_ausencia() -> None:
    store = FakeObjectStore()
    manifests = []
    for subtype in ("SIHD_INTERNACAO", "SIHD_PROC_AIH"):
        raw = put_raw(store, raw_spec(subtype), pl.DataFrame())
        manifests.extend(normalize_sihd(normalize_request((raw,), subtype), store).manifests)

    result = reconcile_sihd(reconcile_request(tuple(manifests)), store)

    assert [m.row_count for m in manifests] == [0, 0, 0, 0]
    assert result.kpis["internacao_count"] == 0
    assert result.kpis["procedimento_count"] == 0
    assert result.reconciliation_manifest.row_count == 0
    assert result.divergence_manifest.row_count == 0


def test_fonte_retida_nao_registrada_e_erro_de_aceitacao() -> None:
    catalog = build_source_catalog()
    cnes_only = (build_source_registry(catalog).for_pipeline("cnes"),)

    with pytest.raises(ValueError, match="missing_pipeline_bundle:bpa"):
        SourceRegistry(catalog, cnes_only)
