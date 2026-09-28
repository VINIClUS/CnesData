"""Contrato do job de CI que valida o runtime AWS contra os emuladores."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

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
