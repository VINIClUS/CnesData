"""Contrato do estágio de Dockerfile que o oci-build compila para o candidato."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[3]
WORKFLOW = ROOT / ".github/workflows/verify-production.yml"
MATRIX_REF = re.compile(r"\$\{\{ matrix\.([a-z_]+) \}\}")
BUILD_INPUTS = {
    "context": ".",
    "file": "apps/${{ matrix.app }}/Dockerfile",
    "platforms": "linux/amd64",
    "push": False,
    "load": True,
    "pull": True,
    "tags": "cnesdata/${{ matrix.app }}:production-verified",
    "build-args": "OS_PATCH_LEVEL=${{ github.run_id }}-${{ github.run_attempt }}",
}


def _oci_build() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))["jobs"]["oci-build"]


def _combinacoes(job: dict[str, Any]) -> dict[str, dict[str, str]]:
    matrix = job["strategy"]["matrix"]
    combinacoes = {app: {"app": app} for app in matrix["app"]}
    for extra in matrix.get("include", []):
        assert extra.get("app") in combinacoes, f"include_fora_da_matrix={extra}"
        combinacoes[extra["app"]].update(extra)
    return combinacoes


def _build(job: dict[str, Any]) -> dict[str, Any]:
    [step] = [
        step for step in job["steps"]
        if step.get("uses", "").startswith("docker/build-push-action@")
    ]
    return step["with"]


def _target(app: str) -> str | None:
    job = _oci_build()
    bruto = _build(job).get("target")
    if bruto is None:
        return None
    ref = MATRIX_REF.fullmatch(bruto)
    valor = _combinacoes(job)[app].get(ref.group(1), "") if ref else bruto
    return valor or None


def test_oci_build_do_central_api_resolve_para_o_estagio_production() -> None:
    assert _target("central_api") == "production"


def test_oci_build_do_data_processor_segue_sem_target() -> None:
    assert _target("data_processor") is None


def test_matrix_do_oci_build_segue_com_as_duas_imagens() -> None:
    assert set(_combinacoes(_oci_build())) == {"central_api", "data_processor"}


def test_oci_build_mantem_os_demais_inputs_do_build() -> None:
    inputs = dict(_build(_oci_build()))
    inputs.pop("target", None)
    assert inputs == BUILD_INPUTS


def test_dockerfile_do_central_api_declara_o_estagio_production() -> None:
    dockerfile = (ROOT / "apps/central_api/Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^FROM \S+ AS production$", dockerfile, re.MULTILINE)
