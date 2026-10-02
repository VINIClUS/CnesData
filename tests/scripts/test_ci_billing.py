"""Contrato dos workflows de CI do billing (aceitacao e E2E Stripe sandbox)."""
from __future__ import annotations

import configparser
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_QUALITY = _ROOT / ".github/workflows/python-quality.yml"
_E2E = _ROOT / ".github/workflows/stripe-billing-e2e.yml"
_PYTEST_INI = _ROOT / "pytest.ini"
_OPENAPI = _ROOT / "docs/contracts/openapi.json"

_ACCEPTANCE_PATHS = (
    "packages/cnes_domain/tests/billing",
    "packages/cnes_infra/tests/billing",
    "apps/central_api/tests",
    "apps/billing_worker/tests",
    "tests/property/test_entitlement_projection_cas.py",
    "tests/property/test_quota_last_unit_race.py",
    "tests/property/test_stripe_event_reordering.py",
    "tests/chaos/test_quota_reservation_recovery.py",
    "tests/chaos/test_revocation_publish_fence.py",
    "tests/chaos/test_billing_reconciliation_resume.py",
    "tests/integration/billing",
    "tests/scripts/test_ci_billing.py",
)
_TRIGGER_PATHS = (
    "packages/**",
    "apps/central_api/**",
    "apps/billing_worker/**",
    "tests/**",
    "pytest.ini",
    "docs/contracts/openapi.json",
    ".github/workflows/stripe-billing-e2e.yml",
)
_BILLING_ROUTES = (
    ("/api/v1/billing/accounts", "post"),
    ("/api/v1/billing/accounts/{billing_account_id}/transfer", "post"),
    ("/api/v1/billing/accounts/{billing_account_id}/tenants", "post"),
    ("/api/v1/billing/checkout", "post"),
    ("/api/v1/billing/portal", "post"),
    ("/api/v1/billing/status", "get"),
    ("/api/v1/billing/webhooks/stripe", "post"),
    ("/api/v1/admin/billing/{billing_account_id}/revoke", "post"),
)
_STRIPE_SECRETS = (
    "STRIPE_TEST_SECRET_KEY",
    "STRIPE_TEST_PRICE_ID",
    "STRIPE_TEST_WEBHOOK_SECRET",
)
_FORBIDDEN_SNIPPETS = ("printenv", "env |", "set -x", "cat $GITHUB_ENV")
_ARTIFACT = "stripe-billing-e2e-junit.xml"
_REPO_GUARD = "github.repository == 'VINIClUS/CnesData'"


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    return workflow.get("on", workflow.get(True))


def _steps(job: dict[str, Any]) -> list[dict[str, Any]]:
    return job["steps"]


def _commands(job: dict[str, Any]) -> str:
    return "\n".join(step.get("run", "") for step in _steps(job))


def _step_named(job: dict[str, Any], name: str) -> dict[str, Any]:
    return next(step for step in _steps(job) if step.get("name") == name)


def _step_index(job: dict[str, Any], name: str) -> int:
    return [step.get("name") for step in _steps(job)].index(name)


def _e2e_job() -> dict[str, Any]:
    return next(iter(_load(_E2E)["jobs"].values()))


def _e2e_run_step() -> dict[str, Any]:
    return next(s for s in _steps(_e2e_job()) if "tests/e2e/billing" in s.get("run", ""))


def _runs_on_values(job: dict[str, Any]) -> list[str]:
    runs_on = job["runs-on"]
    return [runs_on] if isinstance(runs_on, str) else list(runs_on)


@pytest.mark.parametrize("path", _ACCEPTANCE_PATHS)
def test_billing_acceptance_cobre_o_caminho(path: str) -> None:
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]

    assert path in _commands(job)


def test_billing_acceptance_roda_em_runner_hospedado_pelo_github() -> None:
    assert _load(_QUALITY)["jobs"]["billing-acceptance"]["runs-on"] == "ubuntu-latest"


def test_billing_acceptance_exclui_stripe_da_matriz() -> None:
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]
    step = _step_named(job, "Billing acceptance matrix without Stripe network")

    assert "not stripe" in step["run"]


def test_billing_local_nao_depende_de_servico_remoto() -> None:
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]
    step = _step_named(job, "Local billing has no remote dependency")

    assert step["env"]["BILLING_MODE"] == "disabled"
    assert "tests/negative/test_local_billing_has_no_remote_dependency.py" in step["run"]


@pytest.mark.parametrize("path", _TRIGGER_PATHS)
def test_python_quality_dispara_para_caminho_de_billing(path: str) -> None:
    assert path in _triggers(_load(_QUALITY))["pull_request"]["paths"]


def test_aws_runtime_roda_billing_contra_dynamodb_local_antes_do_teardown() -> None:
    job = _load(_QUALITY)["jobs"]["aws-runtime-integration"]
    step = _step_named(job, "Run billing suites against DynamoDB Local")

    assert "-m dynamodb_local" in step["run"]
    assert "tests/integration/billing" in step["run"]
    assert "tests/chaos/test_stripe_projection_failures.py" in step["run"]
    assert "DYNAMODB_ENDPOINT_URL" in step["env"]
    teardown = _step_index(job, "Stop AWS emulators")
    assert _step_index(job, "Run billing suites against DynamoDB Local") < teardown


def test_aws_runtime_falha_quando_billing_dynamodb_local_e_pulado() -> None:
    job = _load(_QUALITY)["jobs"]["aws-runtime-integration"]
    run = _step_named(job, "Fail when billing DynamoDB Local tests were skipped")

    assert "billing-dynamodb-local.xml" in run["run"]
    assert _step_index(job, run["name"]) > _step_index(
        job, "Run billing suites against DynamoDB Local"
    )
    assert _step_index(job, run["name"]) < _step_index(job, "Stop AWS emulators")


@pytest.mark.parametrize("path", [_QUALITY, _E2E])
def test_workflows_de_billing_nao_usam_runner_self_hosted(path: Path) -> None:
    for job in _load(path)["jobs"].values():
        assert not any("self-hosted" in value for value in _runs_on_values(job))


def test_e2e_stripe_roda_so_manual_ou_agendado() -> None:
    assert set(_triggers(_load(_E2E))) == {"workflow_dispatch", "schedule"}


def test_e2e_stripe_usa_environment_sandbox() -> None:
    environment = _e2e_job()["environment"]
    name = environment["name"] if isinstance(environment, dict) else environment

    assert name == "stripe-sandbox"


def test_e2e_stripe_roda_em_runner_hospedado_e_so_no_repositorio_oficial() -> None:
    job = _e2e_job()

    assert job["runs-on"] == "ubuntu-latest"
    assert _REPO_GUARD in job["if"]


def test_e2e_stripe_tem_permissoes_minimas() -> None:
    assert _load(_E2E)["permissions"] == {"contents": "read"}


def test_e2e_stripe_executa_suite_com_segredos_do_environment() -> None:
    step = _e2e_run_step()

    assert "uv run pytest tests/e2e/billing -m stripe -v --maxfail=1" in step["run"]
    assert step["env"]["RUN_STRIPE_E2E"] == "1"
    for secret in _STRIPE_SECRETS:
        assert "${{ secrets." in step["env"][secret]


def test_e2e_stripe_publica_somente_o_junit() -> None:
    uploads = [
        step
        for step in _steps(_e2e_job())
        if str(step.get("uses", "")).startswith("actions/upload-artifact")
    ]

    assert uploads
    for step in uploads:
        assert step["with"]["path"] == _ARTIFACT


def test_e2e_stripe_nao_expoe_segredos_em_logs() -> None:
    commands = _commands(_e2e_job())

    for snippet in _FORBIDDEN_SNIPPETS:
        assert snippet not in commands


def test_pytest_ini_declara_marker_stripe() -> None:
    parser = configparser.ConfigParser()
    parser.read(_PYTEST_INI, encoding="utf-8")
    lines = parser["pytest"]["markers"].splitlines()

    assert any(line.strip().startswith("stripe:") for line in lines)


@pytest.mark.parametrize(("route", "method"), _BILLING_ROUTES)
def test_openapi_expoe_rota_de_billing(route: str, method: str) -> None:
    paths = json.loads(_OPENAPI.read_text(encoding="utf-8"))["paths"]

    assert method in paths[route]
