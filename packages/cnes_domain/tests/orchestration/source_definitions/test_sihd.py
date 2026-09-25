"""Definicao congelada do pipeline SIHD: dependencias, layout e catalogo."""

from __future__ import annotations

import subprocess
import sys

from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.orchestration.source_catalog import (
    PipelineLayout,
    SourceCatalog,
    SubtypeLayout,
    build_source_catalog,
)
from cnes_domain.orchestration.source_definitions.sihd import (
    SIHD_DEFINITION,
    SIHD_DEPENDENCIES,
    SIHD_LAYOUT,
)


def test_dependencias_sihd_exigem_internacao_e_procedimento() -> None:
    expected = (
        RunDependency(source_type="SIHD", file_subtype="SIHD_INTERNACAO", required=True),
        RunDependency(source_type="SIHD", file_subtype="SIHD_PROC_AIH", required=True),
    )
    assert expected == SIHD_DEPENDENCIES


def test_layout_sihd_e_exato() -> None:
    expected = PipelineLayout(
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
    assert expected == SIHD_LAYOUT


def test_layout_tem_uma_entrada_por_dependencia() -> None:
    layout_pairs = [(item.source_type, item.file_subtype) for item in SIHD_LAYOUT.normalized]
    dependency_pairs = [(item.source_type, item.file_subtype) for item in SIHD_DEPENDENCIES]
    assert layout_pairs == dependency_pairs


def test_definicao_sihd_aponta_para_constantes_do_modulo() -> None:
    assert SIHD_DEFINITION.pipeline_id == "sihd"
    assert SIHD_DEFINITION.source_types == ("SIHD",)
    assert SIHD_DEFINITION.dependencies is SIHD_DEPENDENCIES
    assert SIHD_DEFINITION.layout is SIHD_LAYOUT


def test_catalogo_aceita_sihd_isolado_e_junto_com_cnes() -> None:
    cnes = build_source_catalog().for_pipeline("cnes")
    assert SourceCatalog((SIHD_DEFINITION,)).for_source("SIHD") is SIHD_DEFINITION
    combined = SourceCatalog((cnes, SIHD_DEFINITION))
    assert combined.for_pipeline("sihd") is SIHD_DEFINITION


def test_importa_definicao_sem_ciclo_em_qualquer_ordem() -> None:
    orders = (
        "import cnes_domain.control_plane.entities\n"
        "import cnes_domain.orchestration.source_definitions.sihd",
        "import cnes_domain.orchestration.source_definitions.sihd\n"
        "import cnes_domain.control_plane.entities",
    )
    for code in orders:
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-c", code], capture_output=True, text=True, check=False
        )
        assert result.returncode == 0, result.stderr
