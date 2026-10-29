"""Contrato dos workflows de CI do billing (aceitacao e E2E Stripe sandbox)."""

from __future__ import annotations

import configparser
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from base64 import b64decode
from collections import Counter
from hashlib import sha256
from pathlib import Path
from shlex import split
from typing import Any, cast

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
    "tests/chaos/test_stripe_customer_orphan_recovery.py",
    "tests/chaos/test_billing_account_lost_key_recovery.py",
    "tests/chaos/test_shadow_observer_failures.py",
    "packages/cnes_infra/tests/control_plane/test_edge_registration_counting.py",
    "packages/cnes_infra/tests/control_plane/test_dynamodb_billed_tenant_counting.py",
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
)
_FORBIDDEN_SNIPPETS = ("printenv", "env |", "set -x", "cat $GITHUB_ENV")
_ARTIFACT = "stripe-billing-e2e-junit.xml"
_REPO_GUARD = "github.repository == 'VINIClUS/CnesData'"
_CLI_RELEASE = "https://github.com/stripe/stripe-cli/releases/download/v${STRIPE_CLI_VERSION}"
_CLI_STEP = "Install Stripe CLI"
_PIPE_TO_SHELL = re.compile(r"\|\s*(sudo\s+)?(ba)?sh\b")


def _load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _triggers(workflow: dict[Any, Any]) -> dict[str, Any]:
    return cast("dict[str, Any]", workflow.get("on", workflow.get(True)))


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


def test_e2e_stripe_instala_cli_fixa_do_release_oficial_com_checksum() -> None:
    step = _step_named(_e2e_job(), _CLI_STEP)

    assert re.fullmatch(r"\d+\.\d+\.\d+", str(step["env"]["STRIPE_CLI_VERSION"]))
    assert _CLI_RELEASE in step["run"]
    assert "stripe-linux-checksums.txt" in step["run"]
    assert "sha256sum --check --strict" in step["run"]
    assert not _PIPE_TO_SHELL.search(step["run"])
    assert "secrets." not in str(step)


def test_e2e_stripe_instala_cli_antes_da_suite_com_entrega_assinada() -> None:
    job = _e2e_job()
    run_step = _e2e_run_step()

    assert _step_index(job, _CLI_STEP) < _step_index(job, run_step["name"])
    assert run_step["env"]["STRIPE_E2E_DELIVERY"] == "cli"


def test_e2e_stripe_deriva_segredo_do_listener_com_device_fixo_e_mascarado() -> None:
    step = _e2e_run_step()
    lines = step["run"].splitlines()
    derive = next(i for i, line in enumerate(lines) if "stripe listen --print-secret" in line)
    mask = next(i for i, line in enumerate(lines) if "::add-mask::" in line)
    export = lines.index("export STRIPE_TEST_WEBHOOK_SECRET")
    suite = next(i for i, line in enumerate(lines) if "tests/e2e/billing" in line)

    assert re.fullmatch(r"[a-z0-9-]+", step["env"]["STRIPE_DEVICE_NAME"])
    assert "STRIPE_TEST_WEBHOOK_SECRET" not in step["env"]
    assert lines[derive].startswith('STRIPE_TEST_WEBHOOK_SECRET="$(')
    assert lines[0] == "set -euo pipefail"
    assert derive < mask < export < suite


_MATRIX_NAME = "Billing acceptance matrix without Stripe network"
_EVIDENCE_NAME = "Emit billing acceptance JUnit evidence"
_MATRIX_REPORT = "billing-acceptance-junit.xml"
_SKIP_CASES = Counter(
    {
        ("test_probe", "test_parametrizado[primeiro]", "motivo_parametro_<&>"): 1,
        ("test_probe", "test_parametrizado[segundo]", "motivo_parametro_<&>"): 1,
        ("test_probe", "test_fixture", "motivo_fixture"): 1,
    }
)
_PROBE_SOURCE = """
import pytest

@pytest.mark.parametrize("value", [1, 2], ids=["primeiro", "segundo"])
@pytest.mark.skip(reason="motivo_parametro_<&>")
def test_parametrizado(value):
    pass

@pytest.fixture
def indisponivel():
    pytest.skip("motivo_fixture")

def test_fixture(indisponivel):
    pass

def test_passou():
    pass

@pytest.mark.parametrize("value", [
    pytest.param(1, marks=pytest.mark.e2e),
    pytest.param(2, marks=pytest.mark.postgres),
    pytest.param(3, marks=pytest.mark.dynamodb_local),
    pytest.param(4, marks=pytest.mark.stripe),
])
def test_excluido(value):
    raise AssertionError("caso_excluido_executou")
"""


def _matrix_argv() -> list[str]:
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]
    argv = split(_step_named(job, _MATRIX_NAME)["run"])
    paths = {arg for arg in argv if arg.split("/")[0] in {"apps", "packages", "tests"}}
    assert argv[:4] == ["uv", "run", "--no-build", "pytest"]
    assert paths == set(_ACCEPTANCE_PATHS) - {"tests/scripts/test_ci_billing.py"}
    assert argv[argv.index("-m") + 1] == (
        "not e2e and not postgres and not dynamodb_local and not stripe"
    )
    return [arg for arg in argv[4:] if arg not in paths]


def _probe(tmp_path: Path, falha: bool) -> tuple[subprocess.CompletedProcess[str], Path]:
    suite = tmp_path / "test_probe.py"
    suite.write_text(_PROBE_SOURCE + ("\ndef test_falhou():\n    assert False\n" if falha else ""))
    config = tmp_path / "pytest.ini"
    config.write_text(_PYTEST_INI.read_text(encoding="utf-8"), encoding="utf-8")
    argv = [sys.executable, "-I", "-B", "-m", "pytest", *_matrix_argv()]
    argv += ["-p", "no:cacheprovider", "-c", str(config)]
    argv += ["--rootdir", str(tmp_path), "--confcutdir", str(tmp_path), str(suite)]
    result = subprocess.run(  # noqa: S603
        argv,
        cwd=tmp_path,
        env=_probe_env(tmp_path),
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    (tmp_path / "stdout.txt").write_text(result.stdout, encoding="utf-8")
    (tmp_path / "stderr.txt").write_text(result.stderr, encoding="utf-8")
    (tmp_path / "returncode.txt").write_text(str(result.returncode), encoding="ascii")
    assert result.returncode == int(falha), result.stdout + result.stderr
    for outcome, count in (("passed", 1), ("skipped", 3), ("deselected", 4)):
        assert re.search(rf"\b{count} {outcome}\b", result.stdout), result.stdout
    assert bool(re.search(r"\b1 failed\b", result.stdout)) == falha
    return result, tmp_path / _MATRIX_REPORT


def _probe_env(tmp_path: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/bin:/bin",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "BILLING_MODE": "disabled",
        "CI": "true",
        "TMPDIR": str(tmp_path),
        "XDG_CONFIG_HOME": str(tmp_path),
    }


def _assert_xml(report: Path, falha: bool) -> None:
    assert report.is_file(), "billing_junit=missing"
    root = ET.parse(report).getroot()  # noqa: S314 -- XML from the isolated synthetic suite.
    skipped = []
    for case in root.findall(".//testcase"):
        reason = case.find("skipped")
        if reason is not None:
            skipped.append(
                (
                    case.get("classname"),
                    case.get("name"),
                    reason.get("message", "").removeprefix("Skipped: "),
                )
            )
    assert Counter(skipped) == _SKIP_CASES
    assert (root.find(".//failure") is not None) == falha


def _emitter() -> dict[str, Any]:
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]
    steps = [step for step in _steps(job) if step.get("name") == _EVIDENCE_NAME]
    assert len(steps) == 1, "billing_junit_emitter=missing_or_duplicate"
    return steps[0]


def _emit(tmp_path: Path) -> subprocess.CompletedProcess[str]:
    environment = _probe_env(tmp_path) | {
        "GITHUB_RUN_ID": "123",
        "GITHUB_RUN_ATTEMPT": "2",
        "GITHUB_SHA": "e" * 40,
        "GITHUB_RETENTION_DAYS": "30",
    }
    return subprocess.run(  # noqa: S603
        ["/usr/bin/bash", "--noprofile", "--norc", "-c", _emitter()["run"]],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
        timeout=10,
    )


@pytest.mark.parametrize("falha", [False, True], ids=["sucesso", "falha"])
def test_billing_preserva_status_e_contagens_sinteticas(tmp_path: Path, falha: bool) -> None:
    _probe(tmp_path, falha)


@pytest.mark.parametrize("falha", [False, True], ids=["sucesso", "falha"])
def test_billing_exibe_motivos_dos_skips(tmp_path: Path, falha: bool) -> None:
    result, _ = _probe(tmp_path, falha)
    assert "motivo_parametro_<&>" in result.stdout
    assert "motivo_fixture" in result.stdout


@pytest.mark.parametrize("falha", [False, True], ids=["sucesso", "falha"])
def test_billing_relata_identidade_e_motivo_de_cada_skip(tmp_path: Path, falha: bool) -> None:
    _, report = _probe(tmp_path, falha)
    _assert_xml(report, falha)


def test_billing_mantem_selecao_permissoes_e_contratos() -> None:
    _matrix_argv()
    workflow = _load(_QUALITY)
    events = _triggers(workflow)
    assert set(events) == {"pull_request", "schedule", "workflow_dispatch"}
    assert set(events["pull_request"]["paths"]) == set(_TRIGGER_PATHS) | {
        "apps/data_processor/**",
        "scripts/flag_quality_violation.py",
        ".github/workflows/python-quality.yml",
    }
    assert events["schedule"] == [{"cron": "0 6 * * *"}]
    job = workflow["jobs"]["billing-acceptance"]
    assert job["permissions"] == {"contents": "read"}
    assert job["runs-on"] == "ubuntu-latest"
    assert _step_named(job, "Billing CI contract")["run"] == (
        "uv run --no-build pytest -q tests/scripts/test_ci_billing.py "
        "tests/scripts/test_ci_aws_runtime.py"
    )
    assert "continue-on-error" not in job
    assert all("continue-on-error" not in step for step in _steps(job))


def test_billing_preserva_canal_no_log_apos_falha() -> None:
    step = _emitter()
    job = _load(_QUALITY)["jobs"]["billing-acceptance"]
    assert step["if"] == "always()"
    assert "run" in step
    assert "uses" not in step
    assert _step_index(job, "Local billing has no remote dependency") < _step_index(
        job, _EVIDENCE_NAME
    )
    assert not any(
        str(item.get("uses", "")).startswith(("actions/upload-artifact", "actions/cache"))
        for item in _steps(job)
    )


@pytest.mark.parametrize("falha", [False, True], ids=["sucesso", "falha"])
def test_billing_emite_bytes_xml_com_atribuicao(tmp_path: Path, falha: bool) -> None:
    _emitter()
    _, report = _probe(tmp_path, falha)
    _assert_xml(report, falha)
    result = _emit(tmp_path)
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert len(lines) >= 3
    assert lines[0].startswith("billing_junit_begin ")
    assert lines[-1] == f"billing_junit_end v=1 run=123 attempt=2 sha={'e' * 40}"
    fields = lines[0].split()[1:]
    assert all("=" in field for field in fields)
    header = dict(field.split("=", 1) for field in fields)
    payload = report.read_bytes()
    assert header == {
        "v": "1",
        "run": "123",
        "attempt": "2",
        "sha": "e" * 40,
        "bytes": str(len(payload)),
        "sha256": sha256(payload).hexdigest(),
        "retention_days": "30",
    }
    assert all(len(line) == 76 for line in lines[1:-2])
    assert 0 < len(lines[-2]) <= 76
    assert b64decode("".join(lines[1:-1]), validate=True) == payload


@pytest.mark.parametrize("caso", ["ausente", "vazio", "excessivo"])
def test_billing_rejeita_relatorio_sem_evidencia_valida(tmp_path: Path, caso: str) -> None:
    _emitter()
    if caso != "ausente":
        (tmp_path / _MATRIX_REPORT).write_bytes(b"x" * (4194305 if caso == "excessivo" else 0))
    result = _emit(tmp_path)
    assert result.returncode != 0
    assert not any(line.startswith("billing_junit_") for line in result.stdout.splitlines())
