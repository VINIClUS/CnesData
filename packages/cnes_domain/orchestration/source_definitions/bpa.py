"""Definição imutável do pipeline BPA-Mag (BPA-C e BPA-I)."""

from __future__ import annotations

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineDefinition,
    PipelineLayout,
    SubtypeLayout,
)

BPA_DEPENDENCIES = (
    RunDependency(source_type="BPA_MAG", file_subtype="BPA_C", required=True),
    RunDependency(source_type="BPA_MAG", file_subtype="BPA_I", required=True),
)
BPA_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout("BPA_MAG", "BPA_C", ("bpa_c.parquet", "quality_issues_bpa_c.parquet")),
        SubtypeLayout("BPA_MAG", "BPA_I", ("bpa_i.parquet", "quality_issues_bpa_i.parquet")),
    ),
    reconciliation_filename="bpa.parquet",
    divergence_filename="bpa_divergences.parquet",
    serving_documents=("overview", "by-establishment"),
)
BPA_DEFINITION = PipelineDefinition(
    pipeline_id="bpa",
    source_types=("BPA_MAG",),
    dependencies=BPA_DEPENDENCIES,
    layout=BPA_LAYOUT,
)

__all__ = ["BPA_DEFINITION", "BPA_DEPENDENCIES", "BPA_LAYOUT"]
