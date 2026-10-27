"""Contrato do job de CI que valida o runtime AWS contra os emuladores."""

from __future__ import annotations

from pathlib import Path
from shlex import split
from typing import Any

import yaml
from packaging.requirements import InvalidRequirement, Requirement
from packaging.specifiers import SpecifierSet

_WORKFLOW = Path(__file__).resolve().parents[2] / ".github/workflows/python-quality.yml"


def _job() -> dict[str, Any]:
    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    return workflow["jobs"]["aws-runtime-integration"]


def test_ci_executa_suite_aws_e_sempre_remove_emuladores() -> None:
    steps = _job()["steps"]
    commands = "\n".join(step.get("run", "") for step in steps)

    assert "uv sync --locked --all-packages" in commands
    assert "docker compose --profile aws-test up -d --wait dynamodb-local localstack" in commands
    assert 'pytest -m "dynamodb_local and s3_integration" tests/integration/aws' in commands
    teardown = next(step for step in steps if step.get("name") == "Stop AWS emulators")
    assert teardown["if"] == "always()"
    assert "docker compose --profile aws-test down -v" in teardown["run"]


def test_ci_aws_roda_em_runner_hospedado_pelo_github() -> None:
    assert _job()["runs-on"] == "ubuntu-latest"


def _pip_install_requirements(job: dict[str, Any]) -> list[Requirement]:
    lines = (line for step in job["steps"] for line in step.get("run", "").splitlines())
    requirements = []
    for line in lines:
        argv = split(line, comments=True)
        if argv[:2] != ["pip", "install"]:
            continue
        for argument in argv[2:]:
            try:
                requirements.append(Requirement(argument))
            except InvalidRequirement:
                continue
    return requirements


def test_ci_n_plus_1_declara_moto_para_coletar_testes_aws() -> None:
    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    requirements = _pip_install_requirements(workflow["jobs"]["n-plus-1"])
    moto = [requirement for requirement in requirements if requirement.name.lower() == "moto"]

    assert len(moto) == 1, "n_plus_1_moto_declarations=missing_or_duplicate"
    assert {"dynamodb", "s3"} <= moto[0].extras, "n_plus_1_moto_extras=incomplete"
    assert moto[0].specifier == SpecifierSet(">=5.2"), "n_plus_1_moto_version=incompatible"
    assert moto[0].marker is None, "n_plus_1_moto_requirement=conditional"
