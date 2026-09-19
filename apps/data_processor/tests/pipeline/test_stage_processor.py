"""TDD do StageProcessor: RunUnit -> request tipado -> stage callable exata."""
from __future__ import annotations

import hashlib
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import BytesIO
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

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
from cnes_domain.ports.object_store import ObjectStat
from data_processor.orchestration.attempt_store import AttemptObjectStore
from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry
from data_processor.pipeline.stage_processor import (
    StageProcessor,
    UnsupportedUnitSource,
    attempt_prefix_from_manifest_key,
)

if TYPE_CHECKING:
    from collections.abc import BinaryIO
    from contextlib import AbstractContextManager as ContextManager

_TENANT = "354130"
_RUN_ID = "run-1"
_COMPETENCIA = "2026-01"
_NOW = datetime(2026, 1, 15, 12, tzinfo=UTC)


@dataclass
class _FakeObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)

    def put(self, key: str, body: BinaryIO, expected_sha256: str) -> ObjectStat:
        data = body.read()
        digest = hashlib.sha256(data).hexdigest()
        self.objects[key] = data
        return ObjectStat(key=key, size_bytes=len(data), sha256=digest)

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
class _FakeControlPlane:
    run: Run
    units: tuple[RunUnit, ...] = ()

    def get_run(self, tenant_id: str, run_id: str) -> Run | None:
        if (tenant_id, run_id) == (self.run.tenant_id, self.run.run_id):
            return self.run
        return None

    def list_run_units(self, tenant_id: str, run_id: str) -> tuple[RunUnit, ...]:
        return self.units


def _run(**updates: object) -> Run:
    catalog = build_source_catalog()
    definition = catalog.for_pipeline("cnes")
    values: dict[str, object] = {
        "tenant_id": _TENANT, "run_id": _RUN_ID, "competencia": _COMPETENCIA,
        "dataset_name": "cnes", "state": RunState.PROCESSING,
        "dependencies": definition.dependencies, "missing_sources": (), "created_at": _NOW,
    }
    return Run.model_validate(values | updates)


def _registry(
    normalize: Mock | None = None, reconcile: Mock | None = None, materialize: Mock | None = None
) -> SourceRegistry:
    catalog = build_source_catalog()
    definition = catalog.for_pipeline("cnes")
    bundle = SourcePipeline(
        definition=definition,
        normalize=normalize or Mock(),
        reconcile=reconcile or Mock(),
        materialize=materialize or Mock(),
    )
    return SourceRegistry(catalog, (bundle,))


def _raw_manifest_ref(
    store: _FakeObjectStore, *, source_type: SourceType, file_subtype: str = "CNES_VINCULO",
    snapshot_id: str = "snap-1", manifest_id: str = "raw-manifest-1",
    competencia: str = _COMPETENCIA,
) -> ManifestRef:
    object_key = f"raw/{_TENANT}/{source_type.value}/{competencia}/{snapshot_id}/data.parquet"
    manifest_key = f"raw/{_TENANT}/{source_type.value}/{competencia}/{snapshot_id}/manifest.json"
    manifest = RawManifest(
        manifest_version=1, manifest_id=manifest_id, tenant_id=_TENANT,
        source_type=source_type, file_subtype=file_subtype, competencia=competencia,
        agent_id="agent-1", agent_version="1.0.0", schema_version="cnes-raw-v1",
        snapshot_mode=SnapshotMode.FULL, snapshot_id=snapshot_id, base_snapshot_id=None,
        sequence=1, previous_manifest_sha256=None,
        object_sha256="0" * 64, row_count=1, size_bytes=10, object_key=object_key,
        created_at=_NOW,
    )
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.objects[manifest_key] = payload
    return ManifestRef(manifest_id=manifest_id, manifest_key=manifest_key)


def _normalize_unit(
    input_manifests: tuple[ManifestRef, ...], *, source_type: str = "CNES_LOCAL",
    file_subtype: str = "CNES_VINCULO", unit_id: str = "unit-normalize", attempt: int = 1,
) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=RunStage.NORMALIZE,
        source_type=source_type, file_subtype=file_subtype, partition="all",
        depends_on_unit_ids=(), input_manifests=input_manifests, state=RunUnitState.LEASED,
        attempt=attempt, fencing_token=1, lease_owner="worker-1", lease_until=_NOW,
        dispatch_id="a" * 16, output_manifests=(), error_code=None,
    )


def _downstream_unit(
    stage: RunStage, depends_on: tuple[str, ...], *, unit_id: str, attempt: int = 1,
) -> RunUnit:
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=stage,
        source_type=None, file_subtype=None, partition="all",
        depends_on_unit_ids=depends_on, input_manifests=(), state=RunUnitState.LEASED,
        attempt=attempt, fencing_token=1, lease_owner="worker-1", lease_until=_NOW,
        dispatch_id="b" * 16, output_manifests=(), error_code=None,
    )


def _predecessor_unit(
    unit_id: str, refs: tuple[ManifestRef, ...], stage: RunStage = RunStage.NORMALIZE,
    *, state: RunUnitState = RunUnitState.SUCCEEDED,
) -> RunUnit:
    source_type = "CNES_LOCAL" if stage is RunStage.NORMALIZE else None
    file_subtype = "CNES_VINCULO" if stage is RunStage.NORMALIZE else None
    depends_on = () if stage is RunStage.NORMALIZE else ("upstream",)
    input_manifests = (
        (ManifestRef(
            manifest_id="raw-dummy",
            manifest_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-dummy/manifest.json",
        ),)
        if stage is RunStage.NORMALIZE else ()
    )
    return RunUnit(
        tenant_id=_TENANT, run_id=_RUN_ID, unit_id=unit_id, stage=stage,
        source_type=source_type, file_subtype=file_subtype, partition="all",
        depends_on_unit_ids=depends_on, input_manifests=input_manifests, state=state,
        attempt=1, fencing_token=1, lease_owner=None, lease_until=None,
        dispatch_id=None, output_manifests=refs, error_code="boom" if state.name.startswith(
            "SUCCEEDED_DEGRADED") else None,
    )


def _output_ref(
    store: _FakeObjectStore, *, unit_id: str, layer: str, object_key: str,
    manifest_id: str = "out-1", attempt: int = 1, source_type: SourceType | None = None,
) -> ManifestRef:
    manifest_key = (
        f"tmp/{_TENANT}/{_RUN_ID}/{unit_id}/{attempt}/manifests/{manifest_id}/manifest.json"
    )
    manifest = OutputManifest(
        manifest_version=1, manifest_id=manifest_id, tenant_id=_TENANT, layer=layer,
        source_type=source_type, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id=unit_id,
        attempt=attempt, schema_version="v1", object_key=object_key,
        object_sha256="1" * 64, row_count=1, created_at=_NOW,
    )
    payload = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    store.objects[manifest_key] = payload
    return ManifestRef(manifest_id=manifest_id, manifest_key=manifest_key)


def test_normalize_dispatcha_exatamente_a_funcao_de_normalize() -> None:
    store = _FakeObjectStore()
    ref = _raw_manifest_ref(store, source_type=SourceType.CNES_LOCAL)
    unit = _normalize_unit((ref,))
    run = _run()
    captured: dict[str, object] = {}

    def normalize(request: object, scoped_store: AttemptObjectStore) -> NormalizeResult:
        captured["request"] = request
        captured["inputs"] = dict(scoped_store.inputs)
        key = request.target_keys[0]
        return NormalizeResult(manifests=(OutputManifest(
            manifest_version=1, manifest_id="norm-1", tenant_id=_TENANT, layer="normalized",
            source_type=SourceType.CNES_LOCAL, competencia=_COMPETENCIA, run_id=_RUN_ID,
            unit_id=unit.unit_id, attempt=unit.attempt, schema_version="cnes-normalized-v1",
            object_key=key, object_sha256="2" * 64, row_count=1, created_at=_NOW,
        ),))

    reconcile, materialize = Mock(), Mock()
    registry = _registry(normalize=normalize, reconcile=reconcile, materialize=materialize)
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")

    result = processor(unit, attempt_store)

    assert len(result) == 1
    assert result[0].object_key == (
        f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    )
    reconcile.assert_not_called()
    materialize.assert_not_called()
    raw_key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-1/data.parquet"
    assert captured["inputs"] == {raw_key: raw_key}


@pytest.mark.parametrize(
    "source_type", ["NOT_A_SOURCE_TYPE", SourceType.SIHD.value],
)
def test_normalize_rejeita_source_type_fora_do_catalogo(source_type: str) -> None:
    store = _FakeObjectStore()
    dummy_ref = ManifestRef(
        manifest_id="raw-dummy",
        manifest_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-dummy/manifest.json",
    )
    unit = _normalize_unit((dummy_ref,), source_type=source_type)
    run = _run()
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(UnsupportedUnitSource):
        processor(unit, attempt_store)


def test_normalize_rejeita_raw_manifest_com_subtype_divergente() -> None:
    store = _FakeObjectStore()
    ref = _raw_manifest_ref(store, source_type=SourceType.CNES_LOCAL, file_subtype="OUTRO")
    unit = _normalize_unit((ref,))
    run = _run()
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="raw_manifest_identity_mismatch"):
        processor(unit, attempt_store)


def test_normalize_rejeita_resultado_fora_do_target_key() -> None:
    store = _FakeObjectStore()
    ref = _raw_manifest_ref(store, source_type=SourceType.CNES_LOCAL)
    unit = _normalize_unit((ref,))
    run = _run()

    def normalize(request: object, scoped_store: AttemptObjectStore) -> NormalizeResult:
        return NormalizeResult(manifests=(OutputManifest(
            manifest_version=1, manifest_id="norm-1", tenant_id=_TENANT, layer="normalized",
            source_type=SourceType.CNES_LOCAL, competencia=_COMPETENCIA, run_id=_RUN_ID,
            unit_id=unit.unit_id, attempt=unit.attempt, schema_version="cnes-normalized-v1",
            object_key=f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/wrong.parquet",
            object_sha256="2" * 64, row_count=1, created_at=_NOW,
        ),))

    registry = _registry(normalize=normalize)
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="result_target_mismatch"):
        processor(unit, attempt_store)


def test_reconcile_dispatcha_exatamente_a_funcao_de_reconcile_e_ignora_degradado() -> None:
    store = _FakeObjectStore()
    local_key = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    local_ref = _output_ref(
        store, unit_id="unit-local", layer="normalized", object_key=local_key,
        manifest_id="norm-local", source_type=SourceType.CNES_LOCAL,
    )
    local_unit = _predecessor_unit("unit-local", (local_ref,))
    degraded_unit = _predecessor_unit(
        "unit-nacional", (), state=RunUnitState.SUCCEEDED_DEGRADED,
    )
    unit = _downstream_unit(
        RunStage.RECONCILE, (local_unit.unit_id, degraded_unit.unit_id), unit_id="unit-reconcile",
    )
    run = _run()
    captured: dict[str, object] = {}

    def reconcile(request: object, scoped_store: AttemptObjectStore) -> ReconcileResult:
        captured["request"] = request
        captured["inputs"] = dict(scoped_store.inputs)
        return ReconcileResult(
            reconciliation_manifest=OutputManifest(
                manifest_version=1, manifest_id="rec-1", tenant_id=_TENANT,
                layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
                run_id=_RUN_ID, unit_id=unit.unit_id, attempt=unit.attempt,
                schema_version="v1", object_key=request.reconciliation_key,
                object_sha256="3" * 64, row_count=1, created_at=_NOW,
            ),
            divergence_manifest=OutputManifest(
                manifest_version=1, manifest_id="div-1", tenant_id=_TENANT,
                layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
                run_id=_RUN_ID, unit_id=unit.unit_id, attempt=unit.attempt,
                schema_version="v1", object_key=request.divergence_key,
                object_sha256="4" * 64, row_count=1, created_at=_NOW,
            ),
            kpis={},
        )

    normalize, materialize = Mock(), Mock()
    registry = _registry(normalize=normalize, reconcile=reconcile, materialize=materialize)
    control_plane = _FakeControlPlane(run, (local_unit, degraded_unit))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")

    result = processor(unit, attempt_store)

    assert len(result) == 2
    normalize.assert_not_called()
    materialize.assert_not_called()
    expected_physical = f"tmp/{_TENANT}/{_RUN_ID}/unit-local/1/{local_key}"
    assert captured["inputs"] == {local_key: expected_physical}


def test_reconcile_rejeita_predecessor_ausente() -> None:
    store = _FakeObjectStore()
    unit = _downstream_unit(RunStage.RECONCILE, ("missing-unit",), unit_id="unit-reconcile")
    run = _run()
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(run, ()), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="missing_predecessor_unit:missing-unit"):
        processor(unit, attempt_store)


def test_reconcile_rejeita_predecessor_com_layer_errado() -> None:
    store = _FakeObjectStore()
    bad_key = f"serving/{_TENANT}/{_RUN_ID}/overview.json"
    bad_ref = _output_ref(store, unit_id="unit-local", layer="serving", object_key=bad_key)
    local_unit = _predecessor_unit("unit-local", (bad_ref,))
    unit = _downstream_unit(RunStage.RECONCILE, (local_unit.unit_id,), unit_id="unit-reconcile")
    run = _run()
    registry = _registry()
    control_plane = _FakeControlPlane(run, (local_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="wrong_layer:serving"):
        processor(unit, attempt_store)


def test_materialize_dispatcha_exatamente_a_funcao_de_materialize() -> None:
    store = _FakeObjectStore()
    rec_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/cnes.parquet"
    div_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/cnes_divergences.parquet"
    rec_ref = _output_ref(
        store, unit_id="unit-reconcile", layer="reconciliation", object_key=rec_key,
        manifest_id="rec-out",
    )
    div_ref = _output_ref(
        store, unit_id="unit-reconcile", layer="reconciliation", object_key=div_key,
        manifest_id="div-out",
    )
    reconcile_unit = _predecessor_unit(
        "unit-reconcile", (rec_ref, div_ref), stage=RunStage.RECONCILE,
    )
    unit = _downstream_unit(
        RunStage.MATERIALIZE, (reconcile_unit.unit_id,), unit_id="unit-materialize",
    )
    run = _run()
    captured: dict[str, object] = {}

    def materialize(request: object, scoped_store: AttemptObjectStore) -> MaterializeResult:
        captured["request"] = request
        captured["inputs"] = dict(scoped_store.inputs)
        manifest = OutputManifest(
            manifest_version=1, manifest_id="serving-1", tenant_id=_TENANT, layer="serving",
            source_type=None, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id=unit.unit_id,
            attempt=unit.attempt, schema_version="v1", object_key=request.target_keys[0],
            object_sha256="5" * 64, row_count=1, created_at=_NOW,
        )
        document = ServingDocument(
            schema_version="cnes-serving-v1", document_name="overview", tenant_id=_TENANT,
            run_id=_RUN_ID, generated_at=_NOW, payload={},
        )
        return MaterializeResult(manifests=(manifest,), documents=(document,))

    normalize, reconcile = Mock(), Mock()
    registry = _registry(normalize=normalize, reconcile=reconcile, materialize=materialize)
    control_plane = _FakeControlPlane(run, (reconcile_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")

    result = processor(unit, attempt_store)

    assert len(result) == 1
    assert result[0].object_key == f"serving/{_TENANT}/{_RUN_ID}/overview.json"
    normalize.assert_not_called()
    reconcile.assert_not_called()
    assert captured["inputs"] == {
        rec_key: f"tmp/{_TENANT}/{_RUN_ID}/unit-reconcile/1/{rec_key}",
        div_key: f"tmp/{_TENANT}/{_RUN_ID}/unit-reconcile/1/{div_key}",
    }


def test_materialize_rejeita_predecessor_sem_par_reconciliation_divergence() -> None:
    store = _FakeObjectStore()
    only_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/only.parquet"
    only_ref = _output_ref(
        store, unit_id="unit-reconcile", layer="reconciliation", object_key=only_key,
        manifest_id="only-out",
    )
    reconcile_unit = _predecessor_unit(
        "unit-reconcile", (only_ref,), stage=RunStage.RECONCILE,
    )
    unit = _downstream_unit(
        RunStage.MATERIALIZE, (reconcile_unit.unit_id,), unit_id="unit-materialize",
    )
    run = _run()
    registry = _registry()
    control_plane = _FakeControlPlane(run, (reconcile_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="layout_mismatch:reconciliation_predecessor"):
        processor(unit, attempt_store)


def test_processor_rejeita_run_ausente() -> None:
    store = _FakeObjectStore()
    dummy_ref = ManifestRef(
        manifest_id="raw-dummy",
        manifest_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-dummy/manifest.json",
    )
    unit = _normalize_unit((dummy_ref,))
    absent_run = _run(run_id="other-run")
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(absent_run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="unit_run_mismatch"):
        processor(unit, attempt_store)


def test_attempt_prefix_from_manifest_key_extrai_prefixo() -> None:
    ref = ManifestRef(
        manifest_id="m1",
        manifest_key=f"tmp/{_TENANT}/{_RUN_ID}/unit-a/3/manifests/m1/manifest.json",
    )
    assert attempt_prefix_from_manifest_key(ref) == f"tmp/{_TENANT}/{_RUN_ID}/unit-a/3"


@pytest.mark.parametrize(
    "manifest_key",
    [
        "raw/354130/CNES_LOCAL/2026-01/snap/manifest.json",
        "tmp/354130/run-1/unit-a/3/wrong/m1/manifest.json",
    ],
)
def test_attempt_prefix_from_manifest_key_rejeita_shape_invalida(manifest_key: str) -> None:
    ref = ManifestRef(manifest_id="m1", manifest_key=manifest_key)
    with pytest.raises(ValueError, match="invalid_manifest_key"):
        attempt_prefix_from_manifest_key(ref)


def test_attempt_prefix_from_manifest_key_rejeita_leaf_forjado() -> None:
    # ManifestRef ja rejeita leafs fora de {manifest.json, run-manifest.json};
    # model_construct testa a defesa redundante de attempt_prefix_from_manifest_key.
    ref = ManifestRef.model_construct(
        manifest_id="m1",
        manifest_key="tmp/354130/run-1/unit-a/3/manifests/m1/wrong.json",
    )
    with pytest.raises(ValueError, match="invalid_manifest_key"):
        attempt_prefix_from_manifest_key(ref)


def test_normalize_seleciona_layout_do_segundo_source_da_lista() -> None:
    store = _FakeObjectStore()
    ref = _raw_manifest_ref(store, source_type=SourceType.CNES_NACIONAL)
    unit = _normalize_unit((ref,), source_type="CNES_NACIONAL")
    run = _run()

    def normalize(request: object, scoped_store: AttemptObjectStore) -> NormalizeResult:
        key = request.target_keys[0]
        return NormalizeResult(manifests=(OutputManifest(
            manifest_version=1, manifest_id="norm-1", tenant_id=_TENANT, layer="normalized",
            source_type=SourceType.CNES_NACIONAL, competencia=_COMPETENCIA, run_id=_RUN_ID,
            unit_id=unit.unit_id, attempt=unit.attempt, schema_version="cnes-normalized-v1",
            object_key=key, object_sha256="2" * 64, row_count=1, created_at=_NOW,
        ),))

    registry = _registry(normalize=normalize)
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    result = processor(unit, attempt_store)
    assert result[0].object_key == (
        f"normalized/{_TENANT}/CNES_NACIONAL/{_COMPETENCIA}/{_RUN_ID}/cnes_nacional.parquet"
    )


def test_normalize_rejeita_source_type_registrado_sem_layout_para_o_subtype() -> None:
    store = _FakeObjectStore()
    catalog = build_source_catalog()
    definition = catalog.for_pipeline("cnes")
    bundle = SourcePipeline(
        definition=definition, normalize=Mock(), reconcile=Mock(), materialize=Mock(),
    )
    registry = SourceRegistry(catalog, (bundle,))
    ref = _raw_manifest_ref(
        store, source_type=SourceType.CNES_LOCAL, file_subtype="OUTRO_SUBTYPE",
    )
    unit = _normalize_unit((ref,), source_type="CNES_LOCAL", file_subtype="OUTRO_SUBTYPE")
    run = _run()
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match=r"layout_mismatch:CNES_LOCAL/OUTRO_SUBTYPE"):
        processor(unit, attempt_store)


def test_normalize_rejeita_raw_sidecar_com_manifest_id_divergente() -> None:
    store = _FakeObjectStore()
    manifest_key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-1/manifest.json"
    manifest = RawManifest(
        manifest_version=1, manifest_id="stored-id", tenant_id=_TENANT,
        source_type=SourceType.CNES_LOCAL, file_subtype="CNES_VINCULO", competencia=_COMPETENCIA,
        agent_id="agent-1", agent_version="1.0.0", schema_version="cnes-raw-v1",
        snapshot_mode=SnapshotMode.FULL, snapshot_id="snap-1", base_snapshot_id=None,
        sequence=1, previous_manifest_sha256=None, object_sha256="0" * 64, row_count=1,
        size_bytes=10,
        object_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-1/data.parquet",
        created_at=_NOW,
    )
    store.objects[manifest_key] = manifest.model_dump_json(
        exclude_none=False, by_alias=False
    ).encode()
    ref = ManifestRef(manifest_id="declared-id", manifest_key=manifest_key)
    unit = _normalize_unit((ref,))
    run = _run()
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="raw_manifest_id_mismatch"):
        processor(unit, attempt_store)


def test_normalize_rejeita_raw_sidecar_nao_canonico() -> None:
    store = _FakeObjectStore()
    manifest_key = f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-1/manifest.json"
    manifest = RawManifest(
        manifest_version=1, manifest_id="raw-1", tenant_id=_TENANT,
        source_type=SourceType.CNES_LOCAL, file_subtype="CNES_VINCULO", competencia=_COMPETENCIA,
        agent_id="agent-1", agent_version="1.0.0", schema_version="cnes-raw-v1",
        snapshot_mode=SnapshotMode.FULL, snapshot_id="snap-1", base_snapshot_id=None,
        sequence=1, previous_manifest_sha256=None, object_sha256="0" * 64, row_count=1,
        size_bytes=10,
        object_key=f"raw/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/snap-1/data.parquet",
        created_at=_NOW,
    )
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False)
    store.objects[manifest_key] = (canonical + " ").encode()
    ref = ManifestRef(manifest_id="raw-1", manifest_key=manifest_key)
    unit = _normalize_unit((ref,))
    run = _run()
    registry = _registry()
    processor = StageProcessor(_FakeControlPlane(run), store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="raw_manifest_not_canonical"):
        processor(unit, attempt_store)


def test_reconcile_rejeita_sidecar_predecessor_com_manifest_id_divergente() -> None:
    store = _FakeObjectStore()
    local_key = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    manifest_key = f"tmp/{_TENANT}/{_RUN_ID}/unit-local/1/manifests/stored-out/manifest.json"
    manifest = OutputManifest(
        manifest_version=1, manifest_id="stored-out", tenant_id=_TENANT, layer="normalized",
        source_type=SourceType.CNES_LOCAL, competencia=_COMPETENCIA, run_id=_RUN_ID,
        unit_id="unit-local", attempt=1, schema_version="v1", object_key=local_key,
        object_sha256="1" * 64, row_count=1, created_at=_NOW,
    )
    store.objects[manifest_key] = manifest.model_dump_json(
        exclude_none=False, by_alias=False
    ).encode()
    bad_ref = ManifestRef(manifest_id="declared-out", manifest_key=manifest_key)
    local_unit = _predecessor_unit("unit-local", (bad_ref,))
    unit = _downstream_unit(RunStage.RECONCILE, (local_unit.unit_id,), unit_id="unit-reconcile")
    run = _run()
    registry = _registry()
    control_plane = _FakeControlPlane(run, (local_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="output_manifest_id_mismatch"):
        processor(unit, attempt_store)


def test_reconcile_rejeita_sidecar_predecessor_nao_canonico() -> None:
    store = _FakeObjectStore()
    local_key = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    manifest_key = f"tmp/{_TENANT}/{_RUN_ID}/unit-local/1/manifests/norm-local/manifest.json"
    manifest = OutputManifest(
        manifest_version=1, manifest_id="norm-local", tenant_id=_TENANT, layer="normalized",
        source_type=SourceType.CNES_LOCAL, competencia=_COMPETENCIA, run_id=_RUN_ID,
        unit_id="unit-local", attempt=1, schema_version="v1", object_key=local_key,
        object_sha256="1" * 64, row_count=1, created_at=_NOW,
    )
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False)
    store.objects[manifest_key] = (canonical + " ").encode()
    ref = ManifestRef(manifest_id="norm-local", manifest_key=manifest_key)
    local_unit = _predecessor_unit("unit-local", (ref,))
    unit = _downstream_unit(RunStage.RECONCILE, (local_unit.unit_id,), unit_id="unit-reconcile")
    run = _run()
    registry = _registry()
    control_plane = _FakeControlPlane(run, (local_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="output_manifest_not_canonical"):
        processor(unit, attempt_store)


def test_reconcile_rejeita_predecessores_com_object_key_duplicada() -> None:
    store = _FakeObjectStore()
    shared_key = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    ref_a = _output_ref(
        store, unit_id="unit-a", layer="normalized", object_key=shared_key,
        manifest_id="norm-a", source_type=SourceType.CNES_LOCAL,
    )
    ref_b = _output_ref(
        store, unit_id="unit-b", layer="normalized", object_key=shared_key,
        manifest_id="norm-b", source_type=SourceType.CNES_LOCAL,
    )
    unit_a = _predecessor_unit("unit-a", (ref_a,))
    unit_b = _predecessor_unit("unit-b", (ref_b,))
    unit = _downstream_unit(
        RunStage.RECONCILE, (unit_a.unit_id, unit_b.unit_id), unit_id="unit-reconcile",
    )
    run = _run()
    registry = _registry()
    control_plane = _FakeControlPlane(run, (unit_a, unit_b))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="duplicate_predecessor_object_key"):
        processor(unit, attempt_store)


def test_reconcile_rejeita_resultado_fora_das_target_keys() -> None:
    store = _FakeObjectStore()
    local_key = f"normalized/{_TENANT}/CNES_LOCAL/{_COMPETENCIA}/{_RUN_ID}/cnes_local.parquet"
    local_ref = _output_ref(
        store, unit_id="unit-local", layer="normalized", object_key=local_key,
        manifest_id="norm-local", source_type=SourceType.CNES_LOCAL,
    )
    local_unit = _predecessor_unit("unit-local", (local_ref,))
    unit = _downstream_unit(RunStage.RECONCILE, (local_unit.unit_id,), unit_id="unit-reconcile")
    run = _run()

    def reconcile(request: object, scoped_store: AttemptObjectStore) -> ReconcileResult:
        wrong_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/wrong.parquet"
        return ReconcileResult(
            reconciliation_manifest=OutputManifest(
                manifest_version=1, manifest_id="rec-1", tenant_id=_TENANT,
                layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
                run_id=_RUN_ID, unit_id=unit.unit_id, attempt=unit.attempt,
                schema_version="v1", object_key=wrong_key,
                object_sha256="3" * 64, row_count=1, created_at=_NOW,
            ),
            divergence_manifest=OutputManifest(
                manifest_version=1, manifest_id="div-1", tenant_id=_TENANT,
                layer="reconciliation", source_type=None, competencia=_COMPETENCIA,
                run_id=_RUN_ID, unit_id=unit.unit_id, attempt=unit.attempt,
                schema_version="v1", object_key=request.divergence_key,
                object_sha256="4" * 64, row_count=1, created_at=_NOW,
            ),
            kpis={},
        )

    registry = _registry(reconcile=reconcile)
    control_plane = _FakeControlPlane(run, (local_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="result_target_mismatch"):
        processor(unit, attempt_store)


def test_materialize_rejeita_resultado_fora_da_target_key() -> None:
    store = _FakeObjectStore()
    rec_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/cnes.parquet"
    div_key = f"reconciliation/{_TENANT}/{_COMPETENCIA}/{_RUN_ID}/cnes_divergences.parquet"
    rec_ref = _output_ref(
        store, unit_id="unit-reconcile", layer="reconciliation", object_key=rec_key,
        manifest_id="rec-out",
    )
    div_ref = _output_ref(
        store, unit_id="unit-reconcile", layer="reconciliation", object_key=div_key,
        manifest_id="div-out",
    )
    reconcile_unit = _predecessor_unit(
        "unit-reconcile", (rec_ref, div_ref), stage=RunStage.RECONCILE,
    )
    unit = _downstream_unit(
        RunStage.MATERIALIZE, (reconcile_unit.unit_id,), unit_id="unit-materialize",
    )
    run = _run()

    def materialize(request: object, scoped_store: AttemptObjectStore) -> MaterializeResult:
        manifest = OutputManifest(
            manifest_version=1, manifest_id="serving-1", tenant_id=_TENANT, layer="serving",
            source_type=None, competencia=_COMPETENCIA, run_id=_RUN_ID, unit_id=unit.unit_id,
            attempt=unit.attempt, schema_version="v1",
            object_key=f"serving/{_TENANT}/{_RUN_ID}/wrong.json",
            object_sha256="5" * 64, row_count=1, created_at=_NOW,
        )
        document = ServingDocument(
            schema_version="cnes-serving-v1", document_name="wrong", tenant_id=_TENANT,
            run_id=_RUN_ID, generated_at=_NOW, payload={},
        )
        return MaterializeResult(manifests=(manifest,), documents=(document,))

    registry = _registry(materialize=materialize)
    control_plane = _FakeControlPlane(run, (reconcile_unit,))
    processor = StageProcessor(control_plane, store, registry, lambda: _NOW)
    attempt_store = AttemptObjectStore(delegate=store, prefix="tmp/x")
    with pytest.raises(ValueError, match="result_target_mismatch"):
        processor(unit, attempt_store)
