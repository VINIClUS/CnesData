"""TDD do SourceRegistry: liga cada PipelineDefinition aos stage callables."""
from __future__ import annotations

import pytest

from cnes_contracts.manifests.raw import SourceType
from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineDefinition,
    PipelineLayout,
    SourceCatalog,
    SubtypeLayout,
)
from data_processor.pipeline.source_registry import SourcePipeline, SourceRegistry


def _normalize(request: object, store: object) -> str:
    return "normalized"


def _reconcile(request: object, store: object) -> str:
    return "reconciled"


def _materialize(request: object, store: object) -> str:
    return "materialized"


def _layout(normalized: tuple[SubtypeLayout, ...]) -> PipelineLayout:
    return PipelineLayout(
        normalized=normalized,
        reconciliation_filename="reconciliation.parquet",
        divergence_filename="divergences.parquet",
        serving_documents=("overview",),
    )


def _definition(pipeline_id: str, source_types: tuple[str, ...]) -> PipelineDefinition:
    dependencies = tuple(
        RunDependency(source_type=source, file_subtype="SUB", required=True)
        for source in source_types
    )
    normalized = tuple(
        SubtypeLayout(source, "SUB", (f"{source.lower()}.parquet",)) for source in source_types
    )
    return PipelineDefinition(
        pipeline_id=pipeline_id, source_types=source_types,
        dependencies=dependencies, layout=_layout(normalized),
    )


def _bundle(definition: PipelineDefinition) -> SourcePipeline:
    return SourcePipeline(
        definition=definition, normalize=_normalize, reconcile=_reconcile,
        materialize=_materialize,
    )


def test_registry_resolve_por_source_e_por_pipeline() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    bundle = _bundle(definition)
    registry = SourceRegistry(catalog, (bundle,))
    assert registry.for_source("SIHD") is bundle
    assert registry.for_pipeline("sihd") is bundle


def test_registry_expoe_um_bundle_para_multiplas_fontes() -> None:
    definition = _definition("cnes", ("CNES_LOCAL", "CNES_NACIONAL"))
    catalog = SourceCatalog((definition,))
    bundle = _bundle(definition)
    registry = SourceRegistry(catalog, (bundle,))
    local = registry.for_source("CNES_LOCAL")
    nacional = registry.for_source("CNES_NACIONAL")
    assert local is nacional
    assert registry.for_pipeline("cnes") is local
    assert local.pipeline_id == "cnes"
    assert local.definition is catalog.for_pipeline("cnes")
    assert local.source_types == ("CNES_LOCAL", "CNES_NACIONAL")
    assert local.dependencies == definition.dependencies
    assert local.layout == definition.layout


def test_registry_aceita_source_type_enum_como_chave() -> None:
    definition = _definition("cnes", ("CNES_LOCAL", "CNES_NACIONAL"))
    catalog = SourceCatalog((definition,))
    bundle = _bundle(definition)
    registry = SourceRegistry(catalog, (bundle,))
    assert registry.for_source(SourceType.CNES_LOCAL) is bundle


def test_registry_rejeita_bundle_faltante() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    with pytest.raises(ValueError, match="missing_pipeline_bundle:sihd"):
        SourceRegistry(catalog, ())


def test_registry_rejeita_bundle_extra() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    extra_definition = _definition("bpa", ("BPA_MAG",))
    bundles = (_bundle(definition), _bundle(extra_definition))
    with pytest.raises(ValueError, match="unregistered_pipeline_bundle:bpa"):
        SourceRegistry(catalog, bundles)


def test_registry_rejeita_bundle_duplicado_para_mesma_definicao() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    bundles = (_bundle(definition), _bundle(definition))
    with pytest.raises(ValueError, match="duplicate_pipeline_bundle:sihd"):
        SourceRegistry(catalog, bundles)


def test_registry_for_source_desconhecida_levanta_key_error() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    registry = SourceRegistry(catalog, (_bundle(definition),))
    with pytest.raises(KeyError):
        registry.for_source("UNKNOWN")


def test_registry_for_pipeline_desconhecido_levanta_key_error() -> None:
    definition = _definition("sihd", ("SIHD",))
    catalog = SourceCatalog((definition,))
    registry = SourceRegistry(catalog, (_bundle(definition),))
    with pytest.raises(KeyError):
        registry.for_pipeline("unknown")


def test_source_pipeline_proxies_sao_read_only() -> None:
    definition = _definition("sihd", ("SIHD",))
    bundle = _bundle(definition)
    with pytest.raises(AttributeError):
        bundle.pipeline_id = "other"  # type: ignore[misc]
