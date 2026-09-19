"""Frozen catalog of pipeline definitions: source/subtype layout, one per dataset."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cnes_domain.control_plane.entities import RunDependency

_SAFE_LEAF = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


class CatalogConflict(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class SubtypeLayout:
    source_type: str
    file_subtype: str
    normalized_filenames: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PipelineLayout:
    normalized: tuple[SubtypeLayout, ...]
    reconciliation_filename: str
    divergence_filename: str
    serving_documents: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class PipelineDefinition:
    pipeline_id: str
    source_types: tuple[str, ...]
    dependencies: tuple[RunDependency, ...]
    layout: PipelineLayout


def _validate_shape(definition: PipelineDefinition) -> None:
    if not definition.pipeline_id.strip():
        raise CatalogConflict("blank_pipeline_id")
    if not definition.source_types:
        raise CatalogConflict(f"empty_source_types:{definition.pipeline_id}")
    if not definition.dependencies:
        raise CatalogConflict(f"empty_dependencies:{definition.pipeline_id}")
    if not definition.layout.normalized:
        raise CatalogConflict(f"empty_layout:{definition.pipeline_id}")


def _validate_pipeline_ids(definitions: tuple[PipelineDefinition, ...]) -> None:
    seen: set[str] = set()
    for definition in definitions:
        if definition.pipeline_id in seen:
            raise CatalogConflict(f"duplicate_pipeline_id:{definition.pipeline_id}")
        seen.add(definition.pipeline_id)


def _validate_source_ownership(definitions: tuple[PipelineDefinition, ...]) -> None:
    seen: set[str] = set()
    for definition in definitions:
        for source_type in definition.source_types:
            if source_type in seen:
                raise CatalogConflict(f"source_ownership_conflict:{source_type}")
            seen.add(source_type)


def _validate_dependency_layout_pairs(definition: PipelineDefinition) -> None:
    dependency_keys = {(dep.source_type, dep.file_subtype) for dep in definition.dependencies}
    layout_keys: set[tuple[str, str]] = set()
    for subtype_layout in definition.layout.normalized:
        key = (subtype_layout.source_type, subtype_layout.file_subtype)
        if key not in dependency_keys:
            raise CatalogConflict(f"layout_pair_outside_definition:{key[0]}/{key[1]}")
        if key in layout_keys:
            raise CatalogConflict(f"duplicate_subtype_layout:{key[0]}/{key[1]}")
        layout_keys.add(key)
    for dependency in definition.dependencies:
        key = (dependency.source_type, dependency.file_subtype)
        if dependency.required and key not in layout_keys:
            raise CatalogConflict(f"required_dependency_without_layout:{key[0]}/{key[1]}")


def _validate_filename(filename: str) -> None:
    if not _SAFE_LEAF.fullmatch(filename):
        raise CatalogConflict(f"unsafe_filename:{filename}")


def _validate_layout_names(definition: PipelineDefinition) -> None:
    layout = definition.layout
    for subtype_layout in layout.normalized:
        for filename in subtype_layout.normalized_filenames:
            _validate_filename(filename)
    _validate_filename(layout.reconciliation_filename)
    _validate_filename(layout.divergence_filename)
    for document in layout.serving_documents:
        _validate_filename(document)
    names = (layout.reconciliation_filename, layout.divergence_filename, *layout.serving_documents)
    seen: set[str] = set()
    for name in names:
        if name in seen:
            raise CatalogConflict(f"duplicate_layout_name:{name}")
        seen.add(name)


def _validate_global_normalized_filenames(definitions: tuple[PipelineDefinition, ...]) -> None:
    seen: set[str] = set()
    for definition in definitions:
        for subtype_layout in definition.layout.normalized:
            for filename in subtype_layout.normalized_filenames:
                if filename in seen:
                    raise CatalogConflict(f"normalized_filename_collision:{filename}")
                seen.add(filename)


class SourceCatalog:
    def __init__(self, definitions: tuple[PipelineDefinition, ...]) -> None:
        if not definitions:
            raise CatalogConflict("empty_catalog")
        for definition in definitions:
            _validate_shape(definition)
            _validate_dependency_layout_pairs(definition)
            _validate_layout_names(definition)
        _validate_pipeline_ids(definitions)
        _validate_source_ownership(definitions)
        _validate_global_normalized_filenames(definitions)
        self._definitions = definitions
        self._by_source = {
            source_type: definition
            for definition in definitions
            for source_type in definition.source_types
        }
        self._by_pipeline = {definition.pipeline_id: definition for definition in definitions}

    def for_source(self, source_type: str) -> PipelineDefinition:
        return self._by_source[source_type]

    def for_pipeline(self, pipeline_id: str) -> PipelineDefinition:
        return self._by_pipeline[pipeline_id]


def build_source_catalog() -> SourceCatalog:
    from cnes_domain.control_plane.entities import RunDependency

    definition = PipelineDefinition(
        pipeline_id="cnes",
        source_types=("CNES_LOCAL", "CNES_NACIONAL"),
        dependencies=(
            RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
            RunDependency(
                source_type="CNES_NACIONAL", file_subtype="CNES_VINCULO", required=False
            ),
        ),
        layout=PipelineLayout(
            normalized=(
                SubtypeLayout("CNES_LOCAL", "CNES_VINCULO", ("cnes_local.parquet",)),
                SubtypeLayout("CNES_NACIONAL", "CNES_VINCULO", ("cnes_nacional.parquet",)),
            ),
            reconciliation_filename="cnes.parquet",
            divergence_filename="cnes_divergences.parquet",
            serving_documents=("overview",),
        ),
    )
    return SourceCatalog((definition,))


__all__ = [
    "CatalogConflict",
    "PipelineDefinition",
    "PipelineLayout",
    "SourceCatalog",
    "SubtypeLayout",
    "build_source_catalog",
]
