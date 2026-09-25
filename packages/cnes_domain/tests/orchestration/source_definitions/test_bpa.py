"""Definição imutável do pipeline BPA-Mag (SRC-011)."""
from __future__ import annotations

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineDefinition,
    PipelineLayout,
    SourceCatalog,
    SubtypeLayout,
    build_source_catalog,
)
from cnes_domain.orchestration.source_definitions.bpa import (
    BPA_DEFINITION,
    BPA_DEPENDENCIES,
    BPA_LAYOUT,
)


def test_dependencias_bpa_exigem_bpa_c_e_bpa_i() -> None:
    assert (
        RunDependency(source_type="BPA_MAG", file_subtype="BPA_C", required=True),
        RunDependency(source_type="BPA_MAG", file_subtype="BPA_I", required=True),
    ) == BPA_DEPENDENCIES


def test_layout_bpa_bate_com_a_interface_produzida() -> None:
    assert PipelineLayout(
        normalized=(
            SubtypeLayout("BPA_MAG", "BPA_C", ("bpa_c.parquet", "quality_issues_bpa_c.parquet")),
            SubtypeLayout("BPA_MAG", "BPA_I", ("bpa_i.parquet", "quality_issues_bpa_i.parquet")),
        ),
        reconciliation_filename="bpa.parquet",
        divergence_filename="bpa_divergences.parquet",
        serving_documents=("overview", "by-establishment"),
    ) == BPA_LAYOUT


def test_layout_bpa_tem_uma_entrada_por_dependencia() -> None:
    layout_keys = [(item.source_type, item.file_subtype) for item in BPA_LAYOUT.normalized]
    dependency_keys = [(item.source_type, item.file_subtype) for item in BPA_DEPENDENCIES]

    assert layout_keys == dependency_keys


def test_definicao_bpa_agrega_dependencias_e_layout() -> None:
    assert PipelineDefinition(
        pipeline_id="bpa",
        source_types=("BPA_MAG",),
        dependencies=BPA_DEPENDENCIES,
        layout=BPA_LAYOUT,
    ) == BPA_DEFINITION


def test_catalogo_aceita_bpa_ao_lado_de_cnes() -> None:
    cnes = build_source_catalog().for_pipeline("cnes")

    catalog = SourceCatalog((cnes, BPA_DEFINITION))

    assert catalog.for_source("BPA_MAG") is BPA_DEFINITION
    assert catalog.for_pipeline("bpa") is BPA_DEFINITION
