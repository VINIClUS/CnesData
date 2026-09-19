"""TDD do SourceCatalog: catalogo imutavel de pipelines congelado por CND-060."""
from __future__ import annotations

import re

import pytest

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    CatalogConflict,
    PipelineDefinition,
    PipelineLayout,
    SourceCatalog,
    SubtypeLayout,
    build_source_catalog,
)


def _dependency(source_type: str, file_subtype: str, *, required: bool) -> RunDependency:
    return RunDependency(source_type=source_type, file_subtype=file_subtype, required=required)


def _layout(
    normalized: tuple[SubtypeLayout, ...],
    *,
    reconciliation_filename: str = "reconciliation.parquet",
    divergence_filename: str = "divergences.parquet",
    serving_documents: tuple[str, ...] = ("overview",),
) -> PipelineLayout:
    return PipelineLayout(
        normalized=normalized,
        reconciliation_filename=reconciliation_filename,
        divergence_filename=divergence_filename,
        serving_documents=serving_documents,
    )


def _definition(
    pipeline_id: str = "sihd",
    source_types: tuple[str, ...] = ("SIHD",),
    dependencies: tuple[RunDependency, ...] | None = None,
    layout: PipelineLayout | None = None,
) -> PipelineDefinition:
    deps = dependencies
    if deps is None:
        deps = (_dependency("SIHD", "SIH", required=True),)
    lay = layout if layout is not None else _layout(
        (SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),)
    )
    return PipelineDefinition(
        pipeline_id=pipeline_id, source_types=source_types, dependencies=deps, layout=lay
    )


def test_catalogo_valido_resolve_por_source_e_pipeline() -> None:
    definition = _definition()
    catalog = SourceCatalog((definition,))
    assert catalog.for_pipeline("sihd") is definition
    assert catalog.for_source("SIHD") is definition


def test_rejeita_conjunto_vazio_de_definicoes() -> None:
    with pytest.raises(CatalogConflict, match="empty_catalog"):
        SourceCatalog(())


def test_rejeita_source_types_vazio() -> None:
    definition = _definition(source_types=())
    with pytest.raises(CatalogConflict, match="empty_source_types"):
        SourceCatalog((definition,))


def test_rejeita_dependencies_vazio() -> None:
    definition = _definition(dependencies=())
    with pytest.raises(CatalogConflict, match="empty_dependencies"):
        SourceCatalog((definition,))


def test_rejeita_layout_normalized_vazio() -> None:
    definition = _definition(layout=_layout(()))
    with pytest.raises(CatalogConflict, match="empty_layout"):
        SourceCatalog((definition,))


def test_rejeita_pipeline_id_em_branco() -> None:
    definition = _definition(pipeline_id="  ")
    with pytest.raises(CatalogConflict, match="blank_pipeline_id"):
        SourceCatalog((definition,))


def test_rejeita_pipeline_id_duplicado() -> None:
    first = _definition(pipeline_id="dup", source_types=("SIHD",))
    second = _definition(
        pipeline_id="dup",
        source_types=("BPA_MAG",),
        dependencies=(_dependency("BPA_MAG", "BPA_C", required=True),),
        layout=_layout((SubtypeLayout("BPA_MAG", "BPA_C", ("bpa.parquet",)),)),
    )
    with pytest.raises(CatalogConflict, match="duplicate_pipeline_id:dup"):
        SourceCatalog((first, second))


def test_rejeita_source_type_pertencente_a_duas_definicoes() -> None:
    first = _definition(pipeline_id="sihd", source_types=("SHARED",),
                         dependencies=(_dependency("SHARED", "SUB", required=True),),
                         layout=_layout((SubtypeLayout("SHARED", "SUB", ("a.parquet",)),)))
    second = _definition(pipeline_id="bpa", source_types=("SHARED",),
                          dependencies=(_dependency("SHARED", "SUB", required=True),),
                          layout=_layout((SubtypeLayout("SHARED", "SUB", ("b.parquet",)),)))
    with pytest.raises(CatalogConflict, match="source_ownership_conflict:SHARED"):
        SourceCatalog((first, second))


def test_rejeita_layout_fora_das_dependencies() -> None:
    definition = _definition(
        dependencies=(_dependency("SIHD", "SIH", required=True),),
        layout=_layout((SubtypeLayout("SIHD", "OUTRO", ("sihd.parquet",)),)),
    )
    with pytest.raises(CatalogConflict, match="layout_pair_outside_definition:SIHD/OUTRO"):
        SourceCatalog((definition,))


def test_rejeita_dependency_required_sem_layout() -> None:
    definition = _definition(
        dependencies=(
            _dependency("SIHD", "SIH", required=True),
            _dependency("SIHD", "OPT", required=True),
        ),
        layout=_layout((SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),)),
    )
    with pytest.raises(CatalogConflict, match="required_dependency_without_layout:SIHD/OPT"):
        SourceCatalog((definition,))


def test_aceita_dependency_opcional_sem_layout() -> None:
    definition = _definition(
        dependencies=(
            _dependency("SIHD", "SIH", required=True),
            _dependency("SIHD", "OPT", required=False),
        ),
        layout=_layout((SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),)),
    )
    catalog = SourceCatalog((definition,))
    assert catalog.for_pipeline("sihd") is definition


def test_rejeita_subtype_layout_duplicado() -> None:
    definition = _definition(
        dependencies=(_dependency("SIHD", "SIH", required=True),),
        layout=_layout((
            SubtypeLayout("SIHD", "SIH", ("a.parquet",)),
            SubtypeLayout("SIHD", "SIH", ("b.parquet",)),
        )),
    )
    with pytest.raises(CatalogConflict, match="duplicate_subtype_layout:SIHD/SIH"):
        SourceCatalog((definition,))


@pytest.mark.parametrize("filename", ["", "has/slash.parquet", "../escape.parquet", ".hidden"])
def test_rejeita_normalized_filename_inseguro(filename: str) -> None:
    definition = _definition(
        layout=_layout((SubtypeLayout("SIHD", "SIH", (filename,)),))
    )
    with pytest.raises(CatalogConflict, match="unsafe_filename"):
        SourceCatalog((definition,))


@pytest.mark.parametrize("field", ["reconciliation_filename", "divergence_filename"])
def test_rejeita_nome_de_reconciliacao_inseguro(field: str) -> None:
    definition = _definition(layout=_layout(
        (SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),), **{field: "bad/name.parquet"}
    ))
    with pytest.raises(CatalogConflict, match="unsafe_filename"):
        SourceCatalog((definition,))


def test_rejeita_serving_document_inseguro() -> None:
    definition = _definition(layout=_layout(
        (SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),), serving_documents=("bad name",)
    ))
    with pytest.raises(CatalogConflict, match="unsafe_filename"):
        SourceCatalog((definition,))


def test_rejeita_reconciliation_e_divergence_com_mesmo_nome() -> None:
    definition = _definition(layout=_layout(
        (SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),),
        reconciliation_filename="same.parquet",
        divergence_filename="same.parquet",
    ))
    with pytest.raises(CatalogConflict, match=re.escape("duplicate_layout_name:same.parquet")):
        SourceCatalog((definition,))


def test_rejeita_serving_documents_duplicados() -> None:
    definition = _definition(layout=_layout(
        (SubtypeLayout("SIHD", "SIH", ("sihd.parquet",)),),
        serving_documents=("overview", "overview"),
    ))
    with pytest.raises(CatalogConflict, match="duplicate_layout_name:overview"):
        SourceCatalog((definition,))


def test_rejeita_colisao_global_de_normalized_filename() -> None:
    first = PipelineDefinition(
        pipeline_id="sihd", source_types=("SIHD",),
        dependencies=(RunDependency(source_type="SIHD", file_subtype="SIH", required=True),),
        layout=PipelineLayout(
            normalized=(SubtypeLayout("SIHD", "SIH", ("shared.parquet",)),),
            reconciliation_filename="sihd.parquet",
            divergence_filename="sihd_divergences.parquet",
            serving_documents=("sihd-overview",)))
    second = PipelineDefinition(
        pipeline_id="bpa", source_types=("BPA_MAG",),
        dependencies=(RunDependency(source_type="BPA_MAG", file_subtype="BPA_C", required=True),),
        layout=PipelineLayout(
            normalized=(SubtypeLayout("BPA_MAG", "BPA_C", ("shared.parquet",)),),
            reconciliation_filename="bpa.parquet",
            divergence_filename="bpa_divergences.parquet",
            serving_documents=("bpa-overview",)))
    with pytest.raises(
        CatalogConflict, match=re.escape("normalized_filename_collision:shared.parquet")
    ):
        SourceCatalog((first, second))


def test_rejeita_colisao_de_normalized_filename_na_mesma_definicao() -> None:
    definition = _definition(
        dependencies=(
            _dependency("SIHD", "SIH", required=True),
            _dependency("SIHD", "OPT", required=True),
        ),
        layout=_layout((
            SubtypeLayout("SIHD", "SIH", ("shared.parquet",)),
            SubtypeLayout("SIHD", "OPT", ("shared.parquet",)),
        )),
    )
    with pytest.raises(
        CatalogConflict, match=re.escape("normalized_filename_collision:shared.parquet")
    ):
        SourceCatalog((definition,))


def test_definitions_expoe_a_tupla_congelada() -> None:
    definition = _definition()
    catalog = SourceCatalog((definition,))
    assert catalog.definitions == (definition,)


def test_for_source_desconhecido_levanta_key_error() -> None:
    catalog = SourceCatalog((_definition(),))
    with pytest.raises(KeyError):
        catalog.for_source("UNKNOWN")


def test_for_pipeline_desconhecido_levanta_key_error() -> None:
    catalog = SourceCatalog((_definition(),))
    with pytest.raises(KeyError):
        catalog.for_pipeline("unknown")


def test_build_source_catalog_expoe_definicao_cnes_congelada() -> None:
    catalog = build_source_catalog()
    definition = catalog.for_pipeline("cnes")
    assert catalog.for_source("CNES_LOCAL") is definition
    assert catalog.for_source("CNES_NACIONAL") is definition
    assert definition.pipeline_id == "cnes"
    assert definition.source_types == ("CNES_LOCAL", "CNES_NACIONAL")
    assert definition.dependencies == (
        RunDependency(source_type="CNES_LOCAL", file_subtype="CNES_VINCULO", required=True),
        RunDependency(source_type="CNES_NACIONAL", file_subtype="CNES_VINCULO", required=False),
    )
    assert definition.layout == PipelineLayout(
        normalized=(
            SubtypeLayout("CNES_LOCAL", "CNES_VINCULO", ("cnes_local.parquet",)),
            SubtypeLayout("CNES_NACIONAL", "CNES_VINCULO", ("cnes_nacional.parquet",)),
        ),
        reconciliation_filename="cnes.parquet",
        divergence_filename="cnes_divergences.parquet",
        serving_documents=("overview",),
    )


def test_build_source_catalog_e_deterministico() -> None:
    first = build_source_catalog().for_pipeline("cnes")
    second = build_source_catalog().for_pipeline("cnes")
    assert first == second
