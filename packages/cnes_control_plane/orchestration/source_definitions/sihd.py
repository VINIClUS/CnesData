"""Definicao do pipeline SIHD: dependencias, layout e PipelineDefinition."""

from __future__ import annotations

from cnes_control_plane.model.entities import RunDependency
from cnes_control_plane.orchestration.source_catalog import (
    PipelineDefinition,
    PipelineLayout,
    SubtypeLayout,
)

SIHD_DEPENDENCIES = (
    RunDependency(source_type="SIHD", file_subtype="SIHD_INTERNACAO", required=True),
    RunDependency(source_type="SIHD", file_subtype="SIHD_PROC_AIH", required=True),
)
SIHD_LAYOUT = PipelineLayout(
    normalized=(
        SubtypeLayout(
            "SIHD", "SIHD_INTERNACAO",
            ("internacoes.parquet", "quality_issues_internacao.parquet"),
        ),
        SubtypeLayout(
            "SIHD", "SIHD_PROC_AIH",
            ("procedimentos_aih.parquet", "quality_issues_proc_aih.parquet"),
        ),
    ),
    reconciliation_filename="sihd.parquet",
    divergence_filename="sihd_divergences.parquet",
    serving_documents=("overview",),
)
SIHD_DEFINITION = PipelineDefinition(
    pipeline_id="sihd",
    source_types=("SIHD",),
    dependencies=SIHD_DEPENDENCIES,
    layout=SIHD_LAYOUT,
)

__all__ = ["SIHD_DEFINITION", "SIHD_DEPENDENCIES", "SIHD_LAYOUT"]
