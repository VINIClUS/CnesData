"""Workflow phase-a-prepare: roda a CLI Phase A só em runner hospedado, sob OIDC e sem S3."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import yaml

from tests.production.delivery.conftest import require_tool

ROOT = Path(__file__).resolve().parents[3]
PATH = ROOT / ".github/workflows/phase-a-prepare.yml"
ALLOWED_ACTIONS = {
    "actions/checkout",
    "actions/upload-artifact",
    "astral-sh/setup-uv",
    "actions/setup-python",
}


def _text() -> str:
    assert PATH.is_file(), "workflow_missing=phase-a-prepare.yml"
    return PATH.read_text(encoding="utf-8")


def _workflow() -> dict[Any, Any]:
    return yaml.safe_load(_text())


def _job() -> dict[str, Any]:
    jobs = _workflow()["jobs"]
    assert len(jobs) == 1
    return next(iter(jobs.values()))


def _steps() -> list[dict[str, Any]]:
    return _job()["steps"]


def _runs() -> str:
    return "\n".join(step.get("run", "") for step in _steps())


def test_dispara_so_por_workflow_dispatch_com_inputs_fechados():
    trigger = _workflow()["on"]
    assert list(trigger) == ["workflow_dispatch"]
    assert set(trigger["workflow_dispatch"]["inputs"]) == {
        "reference",
        "creator",
        "baseline",
        "account_id",
        "source_sha",
        "canary_trusted_principal_arn",
    }


def test_job_usa_environment_production_e_so_roda_em_main():
    assert _job()["environment"] == "production"
    assert _job()["if"] == "github.ref == 'refs/heads/main'"


def test_permissoes_do_job_sao_exatamente_leitura_oidc_e_packages_read():
    assert _workflow()["permissions"] == {}
    assert _job()["permissions"] == {
        "contents": "read",
        "id-token": "write",
        "packages": "read",
    }


def test_runner_e_hospedado_e_nunca_self_hosted():
    assert _job()["runs-on"] == "ubuntu-latest"
    assert "self-hosted" not in _text()


def test_actions_fixadas_por_sha_completo_e_sem_acao_de_credencial_de_terceiros():
    uses = [step["uses"] for step in _steps() if "uses" in step]
    assert uses
    for item in uses:
        assert re.fullmatch(r"[\w/-]+@[0-9a-f]{40}", item), item
        assert item.split("@")[0] in ALLOWED_ACTIONS


def test_inputs_chegam_aos_comandos_por_env_e_nunca_por_interpolacao():
    assert "${{ inputs." not in _runs()
    assert "${{ github." not in _runs()
    assert "secrets." not in _text()


def test_chama_as_duas_clis_pelo_modulo_com_ambiente_travado_por_lock():
    commands = _runs()
    assert "uv sync --locked --all-packages" in commands
    module = "uv run --no-sync python -m scripts.promotion.phase_a_cli"
    assert f"{module} promote-ecr" in commands
    assert f"{module} phase-inputs" in commands
    assert commands.index("promote-ecr") < commands.index("phase-inputs")


def test_assume_so_a_role_ecr_promotion_em_us_east_2_com_audience_sts():
    commands = _runs()
    assert "role/cnesdata-prod-ecr-promotion" in _text()
    assert len(re.findall(r"role/[\w-]+", _text())) == 1
    assert "audience=sts.amazonaws.com" in commands
    assert "AWS_WEB_IDENTITY_TOKEN_FILE" in _text()
    assert "AWS_REGION: us-east-2" in _text()


def test_token_do_ghcr_vem_do_job_e_o_token_oidc_e_mascarado():
    text = _text()
    assert "GITHUB_TOKEN: ${{ github.token }}" in text
    assert "::add-mask::" in _runs()


def test_saidas_ficam_em_artifact_do_job_e_nao_em_s3():
    uploads = [step for step in _steps() if "upload-artifact" in step.get("uses", "")]
    assert len(uploads) == 1
    paths = uploads[0]["with"]["path"]
    assert "promotion.json" in paths
    assert "phase-inputs.json" in paths
    assert uploads[0]["with"]["if-no-files-found"] == "error"
    assert not re.search(r"\baws s3\b|s3://|s3api", _text())


def test_actionlint_valida_o_workflow():
    done = subprocess.run(  # noqa: S603 -- Validates the checked-in workflow with fixed actionlint.
        [require_tool("actionlint"), str(PATH)],
        capture_output=True,
        text=True,
        check=False,
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 0, done.stdout
