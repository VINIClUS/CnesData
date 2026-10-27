"""Contrato do job de CI que valida o runtime AWS contra os emuladores."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from shlex import split
from shutil import which
from typing import Any

import pytest
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


_ROOT = _WORKFLOW.parents[2]
_RUNTIME = _WORKFLOW.with_name("production-runtime.yml")
_PRODUCERS = ("runtime-python", "runtime-dashboard", "runtime-emulators")
_FINAL = "production-runtime-acceptance"
_PY = "apps/central_api/tests/"
_INFRA_TESTS = "packages/cnes_infra/tests/"
_PROCESSOR = "apps/data_processor/tests/"
_PYTHON_FILES = (
    _PY + "auth/test_aws_oidc.py",
    _PY + "test_production_cors.py",
    _PY + "routes/test_oauth_routes.py",
    _PY + "routes/test_production_serving.py",
    _INFRA_TESTS + "aws/test_production_settings.py",
    _INFRA_TESTS + "executor/test_production_network.py",
    _INFRA_TESTS + "control_plane/test_environment_gate.py",
    _INFRA_TESTS + "control_plane/test_edge_agent_fence.py",
    _INFRA_TESTS + "control_plane/test_execution_quota.py",
    "packages/cnes_domain/tests/orchestration/test_dispatch_admission.py",
    "packages/cnes_domain/tests/orchestration/test_dispatch_admission_quota.py",
    _INFRA_TESTS + "control_plane/test_outbox_cursor.py",
    _INFRA_TESTS + "control_plane/test_outbox_cursor_reads.py",
    _INFRA_TESTS + "control_plane/test_outbox_cursor_dynamodb_boundary.py",
    _INFRA_TESTS + "control_plane/test_outbox_cursor_sqlite_boundary.py",
    _PY + "test_tenant_discovery.py",
    _PY + "test_outbox_cursor_loop.py",
    _PY + "test_aws_no_outbox.py",
    _PROCESSOR + "test_aws_invocation_preflight.py",
    _PROCESSOR + "test_aws_mode_settings_preflight.py",
    _PROCESSOR + "test_aws_mode_composition.py",
    _PROCESSOR + "test_aws_entrypoint.py",
    _PROCESSOR + "test_recovery_mode.py",
    _PROCESSOR + "test_audit_dispatch_mode.py",
    _PROCESSOR + "test_audit_dispatch_deadline.py",
    _PROCESSOR + "test_aws_bootstrap_deadline.py",
    _PROCESSOR + "test_aws_main_exit_codes.py",
    _PROCESSOR + "test_deadline_watchdog.py",
)
_DASHBOARD_FILES = (
    "tests/unit/api/production-client.test.ts",
    "tests/unit/api/client.test.ts",
    "tests/unit/hooks/useActivate.test.tsx",
    "tests/unit/auth/oidc-production.test.ts",
    "tests/unit/lib/env.test.ts",
    "tests/unit/routes/tenant-discovery.test.tsx",
    "tests/unit/auth/tenant-discovery-cache.test.tsx",
    "tests/unit/auth/guard-session-race.test.tsx",
)
_EXCLUSIONS = (
    "not integration and not postgres and not bigquery and not e2e and not stress and not soak "
    "and not spike and not windows_only and not s3_integration and not dynamodb_local and not floci"
)
_REPORTS = {
    "python": ("runtime-python", "runtime-python.xml"),
    "dashboard": ("runtime-dashboard", "apps/web_dashboard/runtime-dashboard.xml"),
    "aws": ("runtime-emulators", "runtime-emulators.xml"),
    "floci": ("runtime-emulators", "runtime-floci.xml"),
}
_AWS_ENV = {
    "AWS_ACCESS_KEY_ID": "test",
    "AWS_SECRET_ACCESS_KEY": "test",
    "AWS_ENDPOINT_URL": "http://127.0.0.1:4566",
    "DYNAMODB_ENDPOINT_URL": "http://127.0.0.1:18000",
    "AWS_EC2_METADATA_DISABLED": "true",
}
_RESULTS = {
    "todos_success": ("success", "success", "success"),
    "python_failure": ("failure", "success", "success"),
    "python_cancelled": ("cancelled", "success", "success"),
    "python_skipped": ("skipped", "success", "success"),
    "python_vazio": ("", "success", "success"),
    "python_desconhecido": ("unknown", "success", "success"),
    "dashboard_failure": ("success", "failure", "success"),
    "dashboard_cancelled": ("success", "cancelled", "success"),
    "dashboard_skipped": ("success", "skipped", "success"),
    "dashboard_vazio": ("success", "", "success"),
    "dashboard_desconhecido": ("success", "unknown", "success"),
    "emuladores_failure": ("success", "success", "failure"),
    "emuladores_cancelled": ("success", "success", "cancelled"),
    "emuladores_skipped": ("success", "success", "skipped"),
    "emuladores_vazio": ("success", "success", ""),
    "emuladores_desconhecido": ("success", "success", "unknown"),
}


def _runtime() -> dict[Any, Any]:
    assert _RUNTIME.is_file(), "runtime_workflow_missing"
    return yaml.safe_load(_RUNTIME.read_text(encoding="utf-8"))


def _step(job: dict[str, Any], identifier: str) -> dict[str, Any]:
    matches = [step for step in job["steps"] if step.get("id") == identifier]
    assert len(matches) == 1
    return matches[0]


def _command(step: dict[str, Any]) -> str:
    return step["run"].replace("\\\n", " ")


def _directory(job: dict[str, Any], step: dict[str, Any]) -> str:
    default = job.get("defaults", {}).get("run", {}).get("working-directory", ".")
    return step.get("working-directory", default)


def _selections(argv: list[str]) -> set[str]:
    return {arg for arg in argv if arg.split("/")[0] in {"apps", "packages", "tests"}}


def _bounded(step: dict[str, Any], seconds: int) -> None:
    argv = split(_command(step))
    index = argv.index("timeout")
    assert argv[index + 1 : index + 3] == ["--signal=INT", "--kill-after=15s"]
    assert argv[index + 3] in (str(seconds), f"{seconds}s")


def _shell(body: str, cwd: Path, environment: dict[str, str]) -> subprocess.CompletedProcess[str]:
    bash = which("bash")
    assert bash is not None
    return subprocess.run(  # noqa: S603 -- Executes the checked-in workflow under test.
        [bash, "--noprofile", "--norc", "-eo", "pipefail", "-c", body],
        cwd=cwd,
        env=environment,
        capture_output=True,
        text=True,
        timeout=5,
        check=False,
    )


def _environment() -> dict[str, str]:
    return {
        "PATH": str(Path(sys.executable).parent) + os.pathsep + os.defpath,
        "BILLING_MODE": "disabled",
        "CI": "true",
        "PYTHONNOUSERSITE": "1",
    }


def test_runtime_dispara_sem_filtros() -> None:
    workflow = _runtime()
    events = workflow.get("on", workflow.get(True))
    assert isinstance(events, dict)
    assert set(events) == {"pull_request", "push", "workflow_dispatch"}
    assert events["pull_request"] in (None, {})
    assert events["push"] == {"branches": ["develop", "main"]}
    assert workflow["concurrency"] == {
        "group": "production-runtime-${{ github.ref }}",
        "cancel-in-progress": True,
    }
    assert set(workflow["jobs"]) == {*_PRODUCERS, _FINAL}
    assert all("if" not in workflow["jobs"][name] for name in _PRODUCERS)


@pytest.mark.parametrize("name", [*_PRODUCERS, _FINAL], ids=[*_PRODUCERS, _FINAL])
def test_runtime_job_tem_permissao_minima(name: str) -> None:
    workflow = _runtime()
    job = workflow["jobs"][name]
    assert workflow["permissions"] == {"contents": "read"}
    assert job["permissions"] == {"contents": "read"}
    assert job["runs-on"] == "ubuntu-latest"
    text = yaml.safe_dump(workflow)
    for forbidden in ("secrets", "id-token", "environment:", "self-hosted", "continue-on-error"):
        assert forbidden not in text
    if name == _FINAL:
        assert all("uses" not in step for step in job["steps"])


@pytest.mark.parametrize("name", _PRODUCERS, ids=_PRODUCERS)
def test_runtime_produtor_e_finito(name: str) -> None:
    workflow = _runtime()
    job = workflow["jobs"][name]
    assert "if" not in job
    assert all("if" not in step or step.get("id", "").startswith("stop-") for step in job["steps"])
    assert job["env"]["BILLING_MODE"] == "disabled"
    assert str(job["env"]["CI"]).lower() == "true"
    assert job["timeout-minutes"] == (20, 10, 35)[_PRODUCERS.index(name)]
    for env in (workflow.get("env", {}), job["env"]):
        assert not any(key.startswith(("AWS_", "DYNAMODB_", "FLOCI_")) for key in env)
    seconds: dict[str, tuple[str, int]] = {
        "runtime-python": ("python-contracts", 600),
        "runtime-dashboard": ("dashboard-contracts", 300),
        "runtime-emulators": ("aws-contracts", 420),
    }
    identifier, bound = seconds[name]
    _bounded(_step(job, identifier), bound)
    if name == "runtime-python":
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        assert "tests/scripts" in commands
        assert "--kill-after=15s 120" in commands
    if name == "runtime-emulators":
        _bounded(_step(job, "floci-contracts"), 600)
        starts = [step for step in job["steps"] if " up -d --wait" in step.get("run", "")]
        assert len(starts) == 2
        for step, bound in zip(starts, (180, 120), strict=True):
            _bounded(step, bound)


@pytest.mark.parametrize("name", _PRODUCERS, ids=_PRODUCERS)
def test_runtime_checkout_nao_persiste_credencial(name: str) -> None:
    job = _runtime()["jobs"][name]
    checkouts = [
        step for step in job["steps"] if step.get("uses", "").startswith("actions/checkout@")
    ]
    assert len(checkouts) == 1
    assert checkouts[0]["with"]["persist-credentials"] is False
    assert "ref" not in checkouts[0]["with"]
    assert "uses" not in job


def test_runtime_python_seleciona_contratos() -> None:
    step = _step(_runtime()["jobs"]["runtime-python"], "python-contracts")
    argv = split(_command(step))
    assert _selections(argv) == {*_PYTHON_FILES, "tests/production"}
    assert "--ignore=tests/production/infra" in argv
    assert _EXCLUSIONS in argv
    assert "--junitxml=runtime-python.xml" in argv
    assert "pytest" in argv
    assert "--collect-only" not in argv
    _bounded(step, 600)


def test_runtime_dashboard_seleciona_contratos() -> None:
    job = _runtime()["jobs"]["runtime-dashboard"]
    step = _step(job, "dashboard-contracts")
    argv = split(_command(step))
    assert _selections(argv) == set(_DASHBOARD_FILES)
    assert argv[argv.index("bun") : argv.index("bun") + 4] == ["bun", "run", "test", "--run"]
    assert {"--reporter=default", "--reporter=junit", "--outputFile=runtime-dashboard.xml"} <= set(
        argv
    )
    assert _directory(job, step) == "apps/web_dashboard"
    assert not {"--coverage", "build", "playwright"} & set(argv)
    _bounded(step, 300)


@pytest.mark.parametrize("suite", ["aws", "floci"], ids=["aws", "floci"])
def test_runtime_emulador_seleciona_suite(suite: str) -> None:
    step = _step(_runtime()["jobs"]["runtime-emulators"], suite + "-contracts")
    argv = split(_command(step))
    assert argv[argv.index("uv") : argv.index("uv") + 5] == [
        "uv",
        "run",
        "--no-build",
        "pytest",
        "-rs",
    ]
    assert (
        argv[argv.index("-m") + 1]
        == {"aws": "dynamodb_local and s3_integration", "floci": "floci"}[suite]
    )
    assert _selections(argv) == {"tests/integration/" + suite}
    assert "--junitxml=" + _REPORTS[suite][1] in argv
    assert not any(word in _command(step) for word in ("billing", "phase2", "production/infra"))
    _bounded(step, 420 if suite == "aws" else 600)


_JUNIT_CASES = (
    "python_verde",
    "python_skipped",
    "python_ausente",
    "dashboard_verde",
    "dashboard_skipped",
    "dashboard_ausente",
    "aws_verde",
    "aws_skipped",
    "aws_ausente",
    "floci_verde",
    "floci_skipped",
    "floci_ausente",
)


@pytest.mark.parametrize("case", _JUNIT_CASES, ids=_JUNIT_CASES)
def test_runtime_validador_junit_e_executado(tmp_path: Path, case: str) -> None:
    suite, state = case.split("_")
    job_name, report_name = _REPORTS[suite]
    job = _runtime()["jobs"][job_name]
    step = _step(job, "validate-" + suite)
    gate = _ROOT / "tests/production/test_runtime_acceptance.py"
    assert gate.is_file(), "runtime_acceptance_gate_missing"
    copied = tmp_path / gate.relative_to(_ROOT)
    copied.parent.mkdir(parents=True)
    copied.write_bytes(gate.read_bytes())
    report = tmp_path / report_name
    report.parent.mkdir(parents=True, exist_ok=True)
    if state != "ausente":
        child = "<skipped/>" if state == "skipped" else ""
        report.write_text('<testsuite tests="1"><testcase>' + child + "</testcase></testsuite>")
    assert _directory(job, step) == "."
    assert report_name in step["run"]
    assert "runpy" in step["run"]
    assert "require_complete_junit" in step["run"]
    result = _shell(step["run"], tmp_path, _environment())
    assert (result.returncode == 0) is (state == "verde"), result.stderr


@pytest.mark.parametrize("suite", ["aws", "floci"], ids=["aws", "floci"])
def test_runtime_teardown_e_incondicional(suite: str) -> None:
    steps = _runtime()["jobs"]["runtime-emulators"]["steps"]
    assert [step["id"] for step in steps[-2:]] == ["stop-aws", "stop-floci"]
    stop = _step({"steps": steps}, "stop-" + suite)
    assert stop["if"] == "always()"
    profile = "aws-test" if suite == "aws" else "floci"
    assert f"docker compose --profile {profile} down -v --remove-orphans" in _command(stop)
    _bounded(stop, 90)
    project = "runtime377-" + suite + "-${{ github.run_id }}-${{ github.run_attempt }}"
    assert stop["env"]["COMPOSE_PROJECT_NAME"] == project
    starts = [step for step in steps if f"--profile {profile} up -d --wait" in step.get("run", "")]
    assert len(starts) == 1
    assert starts[0]["env"]["COMPOSE_PROJECT_NAME"] == project
    assert "prune" not in yaml.safe_dump(steps)


def test_runtime_floci_isola_ambiente_aws() -> None:
    steps = _runtime()["jobs"]["runtime-emulators"]["steps"]
    aws, floci = (
        _step({"steps": steps}, "aws-contracts"),
        _step({"steps": steps}, "floci-contracts"),
    )
    assert aws["env"] == _AWS_ENV
    assert floci["env"] == {"FLOCI_ENDPOINT_URL": "http://127.0.0.1:4577"}
    for step in steps:
        if step is not aws:
            assert not any(key.startswith(("AWS_", "DYNAMODB_")) for key in step.get("env", {}))
    ids = [step.get("id") for step in steps]
    ordered = [
        "aws-contracts",
        "validate-aws",
        "floci-contracts",
        "validate-floci",
        "stop-aws",
        "stop-floci",
    ]
    assert [ids.index(identifier) for identifier in ordered] == sorted(
        ids.index(identifier) for identifier in ordered
    )


def test_runtime_preserva_suites_existentes() -> None:
    runtime = _runtime()
    original = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    assert {"aws-runtime-integration", "floci-integration", "billing-acceptance"} <= set(
        original["jobs"]
    )
    aws_commands = "\n".join(
        step.get("run", "") for step in original["jobs"]["aws-runtime-integration"]["steps"]
    )
    assert "billing" in aws_commands
    dashboard = yaml.safe_load(
        (_WORKFLOW.with_name("web-dashboard.yml")).read_text(encoding="utf-8")
    )
    commands = yaml.safe_dump(dashboard)
    assert all(word in commands for word in ("lint", "typecheck", "--coverage", "build"))
    text = yaml.safe_dump(runtime)
    assert not any(
        word in text
        for word in (
            "ci_python_gate.sh",
            "download-artifact",
            "gh run",
            "workflow_run",
            "coverage",
            "playwright",
        )
    )
    assert all("uses" not in job for job in runtime["jobs"].values())


def test_runtime_final_exige_todos_produtores() -> None:
    final = _runtime()["jobs"][_FINAL]
    assert final["name"] == _FINAL
    assert set(final["needs"]) == set(_PRODUCERS)
    assert final["if"] == "always()"
    assert final["timeout-minutes"] == 2
    assert len(final["steps"]) == 1
    step = final["steps"][0]
    assert "if" not in step
    assert step["shell"] == "bash"
    assert step["env"] == {
        "PYTHON_RESULT": "${{ needs['runtime-python'].result }}",
        "DASHBOARD_RESULT": "${{ needs['runtime-dashboard'].result }}",
        "EMULATORS_RESULT": "${{ needs['runtime-emulators'].result }}",
    }


@pytest.mark.parametrize("case", _RESULTS, ids=list(_RESULTS))
def test_runtime_final_falha_fechado(tmp_path: Path, case: str) -> None:
    body = _runtime()["jobs"][_FINAL]["steps"][0]["run"]
    environment = _environment()
    environment.update(
        zip(("PYTHON_RESULT", "DASHBOARD_RESULT", "EMULATORS_RESULT"), _RESULTS[case], strict=True)
    )
    result = _shell(body, tmp_path, environment)
    assert (result.returncode == 0) is (case == "todos_success"), result.stderr
