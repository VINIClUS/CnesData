"""Translates a persisted RunUnit into a typed stage request and dispatches it."""

from __future__ import annotations

from typing import TYPE_CHECKING

from cnes_contracts.manifests.outputs import OutputManifest
from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    NormalizeRequest,
    ReconcileRequest,
)
from cnes_contracts.manifests.raw import RawManifest, SourceType
from cnes_domain.control_plane.enums import RunStage, RunUnitState

if TYPE_CHECKING:
    from collections.abc import Callable
    from datetime import datetime

    from cnes_domain.control_plane.entities import ManifestRef, Run, RunUnit
    from cnes_domain.orchestration.source_catalog import PipelineLayout, SubtypeLayout
    from cnes_domain.ports.control_plane import ControlPlanePort
    from cnes_domain.ports.object_store import ObjectStorePort
    from data_processor.orchestration.attempt_store import AttemptObjectStore
    from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry


class UnsupportedUnitSource(ValueError):
    pass


_ATTEMPT_KEY_SEGMENTS = 8


def attempt_prefix_from_manifest_key(ref: ManifestRef) -> str:
    parts = ref.manifest_key.split("/")
    valid = (
        len(parts) == _ATTEMPT_KEY_SEGMENTS
        and parts[0] == "tmp"
        and parts[5] == "manifests"
        and parts[7] == "manifest.json"
    )
    if not valid:
        raise ValueError(f"invalid_manifest_key key={ref.manifest_key}")
    return "/".join(parts[:5])


def _read_raw_manifest(store: ObjectStorePort, ref: ManifestRef) -> RawManifest:
    with store.open(ref.manifest_key) as stream:
        payload = stream.read()
    manifest = RawManifest.model_validate_json(payload)
    if manifest.manifest_id != ref.manifest_id:
        raise ValueError("raw_manifest_id_mismatch")
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    if canonical != payload:
        raise ValueError("raw_manifest_not_canonical")
    return manifest


def _read_output_manifest(store: ObjectStorePort, ref: ManifestRef) -> OutputManifest:
    with store.open(ref.manifest_key) as stream:
        payload = stream.read()
    manifest = OutputManifest.model_validate_json(payload)
    if manifest.manifest_id != ref.manifest_id:
        raise ValueError("output_manifest_id_mismatch")
    canonical = manifest.model_dump_json(exclude_none=False, by_alias=False).encode()
    if canonical != payload:
        raise ValueError("output_manifest_not_canonical")
    return manifest


def _result_keys_match(actual: frozenset[str], expected: tuple[str, ...]) -> bool:
    return actual == frozenset(expected)


class StageProcessor:
    def __init__(
        self,
        control_plane: ControlPlanePort,
        source_store: ObjectStorePort,
        source_registry: SourceRegistry,
        clock: Callable[[], datetime],
    ) -> None:
        self._control_plane = control_plane
        self._source_store = source_store
        self._source_registry = source_registry
        self._clock = clock

    def __call__(
        self, unit: RunUnit, attempt_store: AttemptObjectStore
    ) -> tuple[OutputManifest, ...]:
        run = self._load_run(unit)
        pipeline = self._source_registry.for_pipeline(run.dataset_name)
        if unit.stage is RunStage.NORMALIZE:
            return self._normalize(unit, run, pipeline, attempt_store)
        if unit.stage is RunStage.RECONCILE:
            return self._reconcile(unit, run, pipeline, attempt_store)
        return self._materialize(unit, run, pipeline, attempt_store)

    def _load_run(self, unit: RunUnit) -> Run:
        run = self._control_plane.get_run(unit.tenant_id, unit.run_id)
        if run is None or run.tenant_id != unit.tenant_id or run.run_id != unit.run_id:
            raise ValueError("unit_run_mismatch")
        return run

    def _source_type(self, unit: RunUnit, pipeline: SourcePipeline) -> SourceType:
        try:
            source_type = SourceType(unit.source_type)
        except (TypeError, ValueError) as exc:
            raise UnsupportedUnitSource(unit.source_type) from exc
        if source_type not in pipeline.source_types:
            raise UnsupportedUnitSource(unit.source_type)
        return source_type

    def _subtype_layout(
        self, pipeline: SourcePipeline, source_type: SourceType, unit: RunUnit
    ) -> SubtypeLayout:
        for layout in pipeline.layout.normalized:
            if layout.source_type == source_type and layout.file_subtype == unit.file_subtype:
                return layout
        raise ValueError(f"layout_mismatch:{source_type}/{unit.file_subtype}")

    def _raw_manifests(
        self, unit: RunUnit, source_type: SourceType
    ) -> tuple[RawManifest, ...]:
        manifests = tuple(
            _read_raw_manifest(self._source_store, ref) for ref in unit.input_manifests
        )
        for manifest in manifests:
            if manifest.source_type != source_type or manifest.file_subtype != unit.file_subtype:
                raise ValueError("raw_manifest_identity_mismatch")
        return manifests

    def _normalize(
        self, unit: RunUnit, run: Run, pipeline: SourcePipeline, attempt_store: AttemptObjectStore
    ) -> tuple[OutputManifest, ...]:
        source_type = self._source_type(unit, pipeline)
        layout = self._subtype_layout(pipeline, source_type, unit)
        raw_manifests = self._raw_manifests(unit, source_type)
        inputs = {manifest.object_key: manifest.object_key for manifest in raw_manifests}
        scoped_store = attempt_store.with_inputs(inputs)
        competencia = raw_manifests[0].competencia
        target_keys = tuple(
            f"normalized/{unit.tenant_id}/{source_type.value}/{competencia}/{unit.run_id}/{name}"
            for name in layout.normalized_filenames
        )
        request = NormalizeRequest(
            tenant_id=unit.tenant_id, run_id=unit.run_id, unit_id=unit.unit_id,
            attempt=unit.attempt, source_type=source_type, raw_manifests=raw_manifests,
            target_keys=target_keys, normalized_at=self._clock(),
        )
        result = pipeline.normalize(request, scoped_store)
        actual = frozenset(manifest.object_key for manifest in result.manifests)
        if not _result_keys_match(actual, target_keys):
            raise ValueError("result_target_mismatch")
        return result.manifests

    def _predecessor_units(self, unit: RunUnit, run: Run) -> tuple[RunUnit, ...]:
        all_units = self._control_plane.list_run_units(run.tenant_id, run.run_id)
        by_id = {candidate.unit_id: candidate for candidate in all_units}
        missing = [dep for dep in unit.depends_on_unit_ids if dep not in by_id]
        if missing:
            raise ValueError(f"missing_predecessor_unit:{missing[0]}")
        return tuple(by_id[dep] for dep in unit.depends_on_unit_ids)

    def _predecessor_outputs(
        self, units: tuple[RunUnit, ...], expected_layer: str
    ) -> tuple[list[OutputManifest], dict[str, str]]:
        manifests: list[OutputManifest] = []
        inputs: dict[str, str] = {}
        for predecessor in units:
            if predecessor.state != RunUnitState.SUCCEEDED:
                continue
            for ref in predecessor.output_manifests:
                prefix = attempt_prefix_from_manifest_key(ref)
                manifest = _read_output_manifest(self._source_store, ref)
                if manifest.layer != expected_layer:
                    raise ValueError(f"wrong_layer:{manifest.layer}")
                physical_key = f"{prefix}/{manifest.object_key}"
                if manifest.object_key in inputs:
                    raise ValueError("duplicate_predecessor_object_key")
                inputs[manifest.object_key] = physical_key
                manifests.append(manifest)
        return manifests, inputs

    def _reconcile(
        self, unit: RunUnit, run: Run, pipeline: SourcePipeline, attempt_store: AttemptObjectStore
    ) -> tuple[OutputManifest, ...]:
        predecessors = self._predecessor_units(unit, run)
        normalized_manifests, inputs = self._predecessor_outputs(predecessors, "normalized")
        scoped_store = attempt_store.with_inputs(inputs)
        layout = pipeline.layout
        reconciliation_key = (
            f"reconciliation/{unit.tenant_id}/{run.competencia}/{unit.run_id}/"
            f"{layout.reconciliation_filename}"
        )
        divergence_key = (
            f"reconciliation/{unit.tenant_id}/{run.competencia}/{unit.run_id}/"
            f"{layout.divergence_filename}"
        )
        request = ReconcileRequest(
            tenant_id=unit.tenant_id, competencia=run.competencia, run_id=unit.run_id,
            unit_id=unit.unit_id, attempt=unit.attempt,
            normalized_manifests=tuple(normalized_manifests),
            reconciliation_key=reconciliation_key, divergence_key=divergence_key,
            reconciled_at=self._clock(),
        )
        result = pipeline.reconcile(request, scoped_store)
        if (
            result.reconciliation_manifest.object_key != reconciliation_key
            or result.divergence_manifest.object_key != divergence_key
        ):
            raise ValueError("result_target_mismatch")
        return result.reconciliation_manifest, result.divergence_manifest

    def _split_reconciliation_outputs(
        self, manifests: list[OutputManifest], layout: PipelineLayout
    ) -> tuple[OutputManifest, OutputManifest]:
        reconciliation = [
            m for m in manifests if m.object_key.endswith(f"/{layout.reconciliation_filename}")
        ]
        divergence = [
            m for m in manifests if m.object_key.endswith(f"/{layout.divergence_filename}")
        ]
        if len(reconciliation) != 1 or len(divergence) != 1:
            raise ValueError("layout_mismatch:reconciliation_predecessor")
        return reconciliation[0], divergence[0]

    def _materialize(
        self, unit: RunUnit, run: Run, pipeline: SourcePipeline, attempt_store: AttemptObjectStore
    ) -> tuple[OutputManifest, ...]:
        predecessors = self._predecessor_units(unit, run)
        manifests, inputs = self._predecessor_outputs(predecessors, "reconciliation")
        reconciliation_manifest, divergence_manifest = self._split_reconciliation_outputs(
            manifests, pipeline.layout
        )
        scoped_store = attempt_store.with_inputs(inputs)
        target_keys = tuple(
            f"serving/{unit.tenant_id}/{unit.run_id}/{document}.json"
            for document in pipeline.layout.serving_documents
        )
        request = MaterializeRequest(
            tenant_id=unit.tenant_id, competencia=run.competencia, run_id=unit.run_id,
            unit_id=unit.unit_id, attempt=unit.attempt,
            reconciliation_manifest=reconciliation_manifest,
            divergence_manifest=divergence_manifest, missing_sources=run.missing_sources,
            target_keys=target_keys, generated_at=self._clock(),
        )
        result = pipeline.materialize(request, scoped_store)
        actual = frozenset(manifest.object_key for manifest in result.manifests)
        if not _result_keys_match(actual, target_keys):
            raise ValueError("result_target_mismatch")
        return result.manifests


__all__ = ["StageProcessor", "UnsupportedUnitSource", "attempt_prefix_from_manifest_key"]
