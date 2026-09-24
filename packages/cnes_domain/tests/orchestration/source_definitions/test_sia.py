"""Definição imutável do pipeline SIA_LOCAL."""

from __future__ import annotations

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineLayout,
    SourceCatalog,
    SubtypeLayout,
    build_source_catalog,
)
from cnes_domain.orchestration.source_definitions.sia import (
    SIA_DEFINITION,
    SIA_DEPENDENCIES,
    SIA_LAYOUT,
)

_SUBTYPES = ("SIA_APA", "SIA_BPI", "SIA_BPIHST", "DIM_SIGTAP", "DIM_MUNICIPIO")


def test_dependencias_sia_sao_os_cinco_subtipos_obrigatorios() -> None:
    expected = tuple(
        RunDependency(source_type="SIA_LOCAL", file_subtype=subtype, required=True)
        for subtype in _SUBTYPES
    )
    assert expected == SIA_DEPENDENCIES


def test_layout_sia_e_exatamente_o_contrato_publicado() -> None:
    expected = PipelineLayout(
        normalized=(
            SubtypeLayout(
                "SIA_LOCAL", "SIA_APA", ("apa.parquet", "quality_issues_sia_apa.parquet")
            ),
            SubtypeLayout(
                "SIA_LOCAL", "SIA_BPI", ("bpi.parquet", "quality_issues_sia_bpi.parquet")
            ),
            SubtypeLayout(
                "SIA_LOCAL", "SIA_BPIHST", ("bpihst.parquet", "quality_issues_sia_bpihst.parquet")
            ),
            SubtypeLayout(
                "SIA_LOCAL",
                "DIM_SIGTAP",
                ("reference_sigtap.parquet", "quality_issues_dim_sigtap.parquet"),
            ),
            SubtypeLayout(
                "SIA_LOCAL",
                "DIM_MUNICIPIO",
                ("reference_municipio.parquet", "quality_issues_dim_municipio.parquet"),
            ),
        ),
        reconciliation_filename="sia.parquet",
        divergence_filename="sia_divergences.parquet",
        serving_documents=("overview", "by-establishment"),
    )
    assert expected == SIA_LAYOUT


def test_layout_tem_uma_entrada_por_dependencia() -> None:
    layout_keys = [(item.source_type, item.file_subtype) for item in SIA_LAYOUT.normalized]
    dependency_keys = [(item.source_type, item.file_subtype) for item in SIA_DEPENDENCIES]
    assert layout_keys == dependency_keys


def test_definicao_sia_amarra_pipeline_fonte_e_layout() -> None:
    assert SIA_DEFINITION.pipeline_id == "sia"
    assert SIA_DEFINITION.source_types == ("SIA_LOCAL",)
    assert SIA_DEFINITION.dependencies is SIA_DEPENDENCIES
    assert SIA_DEFINITION.layout is SIA_LAYOUT


def test_catalogo_aceita_sia_isolado_e_junto_do_cnes() -> None:
    alone = SourceCatalog((SIA_DEFINITION,))
    combined = SourceCatalog((*build_source_catalog().definitions, SIA_DEFINITION))

    assert alone.for_source("SIA_LOCAL") is SIA_DEFINITION
    assert combined.for_pipeline("sia") is SIA_DEFINITION
