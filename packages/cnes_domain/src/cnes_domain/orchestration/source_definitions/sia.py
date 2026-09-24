"""Definição imutável do pipeline SIA_LOCAL (APA, BPI, BPIHST e referências)."""

from __future__ import annotations

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineDefinition,
    PipelineLayout,
    SubtypeLayout,
)

SIA_DEPENDENCIES = (
    RunDependency(source_type="SIA_LOCAL", file_subtype="SIA_APA", required=True),
    RunDependency(source_type="SIA_LOCAL", file_subtype="SIA_BPI", required=True),
    RunDependency(source_type="SIA_LOCAL", file_subtype="SIA_BPIHST", required=True),
    RunDependency(source_type="SIA_LOCAL", file_subtype="DIM_SIGTAP", required=True),
    RunDependency(source_type="SIA_LOCAL", file_subtype="DIM_MUNICIPIO", required=True),
)
SIA_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout("SIA_LOCAL", "SIA_APA", ("apa.parquet", "quality_issues_sia_apa.parquet")),
        SubtypeLayout("SIA_LOCAL", "SIA_BPI", ("bpi.parquet", "quality_issues_sia_bpi.parquet")),
        SubtypeLayout(
            "SIA_LOCAL", "SIA_BPIHST", ("bpihst.parquet", "quality_issues_sia_bpihst.parquet")
        ),
        SubtypeLayout(
            "SIA_LOCAL", "DIM_SIGTAP",
            ("reference_sigtap.parquet", "quality_issues_dim_sigtap.parquet"),
        ),
        SubtypeLayout(
            "SIA_LOCAL", "DIM_MUNICIPIO",
            ("reference_municipio.parquet", "quality_issues_dim_municipio.parquet"),
        ),
    ),
    reconciliation_filename="sia.parquet",
    divergence_filename="sia_divergences.parquet",
    serving_documents=("overview", "by-establishment"),
)
SIA_DEFINITION = PipelineDefinition(
    pipeline_id="sia",
    source_types=("SIA_LOCAL",),
    dependencies=SIA_DEPENDENCIES,
    layout=SIA_LAYOUT,
)
