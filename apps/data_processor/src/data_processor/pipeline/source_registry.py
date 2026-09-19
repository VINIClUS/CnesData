"""Registry binding each catalog PipelineDefinition to its stage callables."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from cnes_contracts.manifests.processing import (
    MaterializeRequest,
    MaterializeResult,
    NormalizeRequest,
    NormalizeResult,
    ReconcileRequest,
    ReconcileResult,
)
from cnes_domain.ports.object_store import ObjectStorePort

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import RunDependency
    from cnes_domain.orchestration.source_catalog import (
        PipelineDefinition,
        PipelineLayout,
        SourceCatalog,
    )

type NormalizeStage = Callable[[NormalizeRequest, ObjectStorePort], NormalizeResult]
type ReconcileStage = Callable[[ReconcileRequest, ObjectStorePort], ReconcileResult]
type MaterializeStage = Callable[[MaterializeRequest, ObjectStorePort], MaterializeResult]


@dataclass(frozen=True, slots=True)
class SourcePipeline:
    definition: PipelineDefinition
    normalize: NormalizeStage
    reconcile: ReconcileStage
    materialize: MaterializeStage

    @property
    def pipeline_id(self) -> str:
        return self.definition.pipeline_id

    @property
    def source_types(self) -> tuple[str, ...]:
        return self.definition.source_types

    @property
    def dependencies(self) -> tuple[RunDependency, ...]:
        return self.definition.dependencies

    @property
    def layout(self) -> PipelineLayout:
        return self.definition.layout


def _validate_bundles(
    catalog: SourceCatalog, pipelines: tuple[SourcePipeline, ...]
) -> dict[str, SourcePipeline]:
    catalog_ids = {definition.pipeline_id for definition in catalog.definitions}
    by_pipeline: dict[str, SourcePipeline] = {}
    for bundle in pipelines:
        pipeline_id = bundle.pipeline_id
        if pipeline_id not in catalog_ids:
            raise ValueError(f"unregistered_pipeline_bundle:{pipeline_id}")
        if pipeline_id in by_pipeline:
            raise ValueError(f"duplicate_pipeline_bundle:{pipeline_id}")
        by_pipeline[pipeline_id] = bundle
    missing = catalog_ids - by_pipeline.keys()
    if missing:
        raise ValueError(f"missing_pipeline_bundle:{sorted(missing)[0]}")
    return by_pipeline


class SourceRegistry:
    def __init__(self, catalog: SourceCatalog, pipelines: tuple[SourcePipeline, ...]) -> None:
        self._by_pipeline = _validate_bundles(catalog, pipelines)
        self._by_source = {
            source_type: bundle
            for bundle in pipelines
            for source_type in bundle.source_types
        }

    def for_source(self, source_type: str) -> SourcePipeline:
        return self._by_source[source_type]

    def for_pipeline(self, pipeline_id: str) -> SourcePipeline:
        return self._by_pipeline[pipeline_id]


__all__ = [
    "MaterializeStage",
    "NormalizeStage",
    "ReconcileStage",
    "SourcePipeline",
    "SourceRegistry",
]
