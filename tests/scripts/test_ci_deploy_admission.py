"""Testes do oráculo de admissão de deploy e da concorrência."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
import tarfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

import pytest
import yaml

SCRATCH_ROOT = Path(__file__).parents[2]
WORKFLOW_ROOT = SCRATCH_ROOT / ".github/workflows"
FIXTURE_PATH = SCRATCH_ROOT / "tests/scripts/vendor/actions-expressions-0.3.61.tgz"
PROVENANCE_PATH = SCRATCH_ROOT / (
    "tests/scripts/vendor/actions-expressions-0.3.61.provenance.json"
)
HELPER_PATH = SCRATCH_ROOT / "tests/scripts/actions_expression_oracle.mjs"
LICENSE_PATH = SCRATCH_ROOT / "tests/scripts/vendor/actions-expressions-0.3.61.LICENSE"
EXPECTED_SHA512 = (
    "220fad5c42ef4e396ac500160509cb367d9d3e936b68b31d4a74febc3cc33e626"
    "d6cd5212001ff67d9154fca59f14b498bdbd41acf21c0713c2452666ce9cb10"
)
EXPECTED_PROVENANCE = {
    "url": "https://registry.npmjs.org/@actions/expressions/-/expressions-0.3.61.tgz",
    "integrity": (
        "sha512-Ig+tXELvTjlqxQAWBQnLNn2dPpNraLMdSnT+vDzDPmJtbNUhIAH/"
        "Z9kVT8pZ8UtJi9vUGs8hwHE8JFJmbOnLEA=="
    ),
    "gitHead": "b71ac284fc6a7382c22ecbf3e0009a6ae216823b",
    "license": "MIT",
}


@dataclass(frozen=True)
class WorkflowSnapshot:
    name: str
    gate_if: str
    build_if: str
    deploy_if: str
    deploy_needs: tuple[str, ...]
    build_needs: tuple[str, ...]
    concurrency_group: str


@dataclass(frozen=True)
class AdmissionCase:
    event: str
    tag: str
    gate: str
    build: str
    cancelled: bool
    expected: bool


@dataclass(frozen=True)
class GateCase:
    workflow: str
    event: str
    tag: str
    expected: bool


@dataclass(frozen=True)
class GroupCase:
    workflow: str
    caller: str
    ref: str


ADMISSION_CASES = (
    AdmissionCase("push", "", "success", "success", False, True),
    AdmissionCase("push", "", "failure", "success", False, False),
    AdmissionCase("push", "", "cancelled", "success", False, False),
    AdmissionCase("push", "", "skipped", "success", False, False),
    AdmissionCase("push", "", "failure", "skipped", False, False),
    AdmissionCase("push", "", "success", "failure", False, False),
    AdmissionCase("push", "", "success", "cancelled", False, False),
    AdmissionCase("push", "", "success", "skipped", False, False),
    AdmissionCase("push", "", "success", "success", True, False),
    AdmissionCase("workflow_dispatch", "", "success", "success", False, True),
    AdmissionCase("workflow_dispatch", "", "skipped", "skipped", False, False),
    AdmissionCase("workflow_dispatch", "", "failure", "skipped", False, False),
    AdmissionCase("workflow_dispatch", "", "cancelled", "success", False, False),
    AdmissionCase("workflow_dispatch", "", "success", "failure", False, False),
    AdmissionCase("workflow_dispatch", "", "success", "success", True, False),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "skipped", "skipped", False, True
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "failure", "skipped", False, False
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "cancelled", "skipped", False, False
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "skipped", "success", False, False
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "success", "skipped", False, False
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "success", "success", False, False
    ),
    AdmissionCase(
        "workflow_dispatch", "redeploy-abc1234", "skipped", "skipped", True, False
    ),
)

GATE_CASES = (
    GateCase("deploy-develop.yml", "push", "", True),
    GateCase("deploy-develop.yml", "workflow_dispatch", "", True),
    GateCase("deploy-develop.yml", "workflow_dispatch", "develop-abc1234", False),
    GateCase("deploy-main.yml", "workflow_dispatch", "", True),
    GateCase("deploy-main.yml", "workflow_dispatch", "main-abc1234", False),
)

WORKFLOW_ADMISSION_CASES = tuple(
    (workflow, case)
    for workflow in ("deploy-develop.yml", "deploy-main.yml")
    for case in ADMISSION_CASES
    if workflow == "deploy-develop.yml" or case.event == "workflow_dispatch"
)

GROUP_CASES = (
    GroupCase("ci.yml", "CI", "refs/heads/develop"),
    GroupCase("ci.yml", "CI", "refs/heads/develop"),
    GroupCase("ci.yml", "Deploy develop", "refs/heads/develop"),
    GroupCase("ci.yml", "Deploy main", "refs/heads/main"),
    GroupCase("ci.yml", "CI", "refs/heads/main"),
    GroupCase("deploy-develop.yml", "Deploy develop", "refs/heads/develop"),
    GroupCase("deploy-develop.yml", "Deploy develop", "refs/heads/main"),
    GroupCase("deploy-main.yml", "Deploy main", "refs/heads/main"),
    # workflow_call caller differs only by case; Actions compares groups case-insensitively.
    GroupCase("ci.yml", "deploy DEVELOP", "refs/heads/develop"),
)

PUSH_ADMISSION_CASES = (
    ({"tag": "illegal-push-tag"}, "success", "success", False), ({}, "success", "success", True),
    (None, "success", "success", True), ({}, "failure", "success", False),
    (None, "success", "failure", False),
)
def _as_needs(value: Any, path: str) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(item) for item in value)
    if isinstance(value, str):
        return (value,)
    raise AssertionError(f"{path} must be a string or list")


def _condition(jobs: dict[str, Any], job: str, path: str) -> str:
    value = jobs.get(job, {}).get("if")
    if not isinstance(value, str):
        raise AssertionError(f"{path} must be a string")
    return value


def _load_yaml(path: Path) -> dict[str, Any]:
    document = yaml.load(
        path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader  # noqa: S506
    )
    if not isinstance(document, dict):
        raise AssertionError(f"{path}: workflow must be a mapping")
    return document


def load_workflow(path: Path) -> WorkflowSnapshot:
    document = _load_yaml(path)
    jobs = document.get("jobs")
    if not isinstance(jobs, dict):
        raise AssertionError(f"{path}: jobs must be a mapping")
    deploy = jobs.get("deploy")
    build = jobs.get("build")
    if not isinstance(deploy, dict) or not isinstance(build, dict):
        raise AssertionError(f"{path}: deploy/build jobs are required")
    concurrency = document.get("concurrency")
    if not isinstance(concurrency, dict) or not isinstance(concurrency.get("group"), str):
        raise AssertionError(f"{path}: concurrency.group is required")
    return WorkflowSnapshot(
        name=path.name,
        gate_if=_condition(jobs, "gate", f"{path}:jobs.gate.if"),
        build_if=_condition(jobs, "build", f"{path}:jobs.build.if"),
        deploy_if=_condition(jobs, "deploy", f"{path}:jobs.deploy.if"),
        deploy_needs=_as_needs(deploy.get("needs"), f"{path}:jobs.deploy.needs"),
        build_needs=_as_needs(build.get("needs"), f"{path}:jobs.build.needs"),
        concurrency_group=concurrency["group"],
    )


def load_concurrency_group(path: Path) -> str:
    concurrency = _load_yaml(path).get("concurrency")
    if not isinstance(concurrency, dict) or not isinstance(concurrency.get("group"), str):
        raise AssertionError(f"{path}: concurrency.group is required")
    return concurrency["group"]


def _context(case: AdmissionCase) -> dict[str, object]:
    return {
        "github": {"event_name": case.event},
        "inputs": {"tag": case.tag},
        "needs": {
            "gate": {"result": case.gate},
            "build": {"result": case.build},
        },
    }


def _group_context(case: GroupCase) -> dict[str, object]:
    return {"github": {"workflow": case.caller, "ref": case.ref}}


def _push_context(inputs: dict[str, object] | None, gate: str, build: str) -> dict[str, object]:
    return {
        "github": {"event_name": "push"},
        **({"inputs": inputs} if inputs is not None else {}),
        "needs": {"gate": {"result": gate}, "build": {"result": build}},
    }

def _safe_extract(fixture: Path, destination: Path) -> Path:
    with tarfile.open(fixture, mode="r:gz") as archive:
        for member in archive.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or path.parts[:1] != ("package",):
                raise AssertionError(f"unsafe archive path={member.name}")
            if member.isdir():
                continue
            if not member.isreg():
                raise AssertionError(f"unexpected archive type={member.name}")
            target = destination.joinpath(*path.parts[1:])
            target.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise AssertionError(f"archive member unreadable={member.name}")
            target.write_bytes(source.read())
    return destination


def _license_bytes_match(fixture: Path, license_path: Path) -> bool:
    with tarfile.open(fixture, mode="r:gz") as archive:
        license_file = archive.extractfile("package/LICENSE")
        if license_file is None:
            raise AssertionError("package/LICENSE is missing")
        return license_file.read() == license_path.read_bytes()


@pytest.fixture(scope="session")
def oracle_package(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path, str]:
    node_path = shutil.which("node")
    if node_path is None:
        raise AssertionError("Node >=20 is required; node executable not found")
    version = subprocess.run(  # noqa: S603
        [node_path, "--version"], capture_output=True, text=True, check=False
    )
    if version.returncode != 0 or int(version.stdout.split(".")[0].lstrip("v")) < 20:
        raise AssertionError(f"Node >=20 is required; output={version.stdout!r}")
    provenance = json.loads(PROVENANCE_PATH.read_text(encoding="utf-8"))
    digest = hashlib.sha512(FIXTURE_PATH.read_bytes()).hexdigest()
    assert digest == EXPECTED_SHA512 == provenance["sha512_hex"]
    assert provenance["version"] == "0.3.61"
    assert provenance["size_bytes"] == FIXTURE_PATH.stat().st_size
    for key, expected in EXPECTED_PROVENANCE.items():
        assert provenance[key] == expected
    assert _license_bytes_match(FIXTURE_PATH, LICENSE_PATH)
    root = tmp_path_factory.mktemp("actions-expressions")
    return _safe_extract(FIXTURE_PATH, root), HELPER_PATH, node_path


def run_oracle(
    oracle_package: tuple[Path, Path, str], requests: list[dict[str, object]]
) -> list[object]:
    package_root, helper, node_path = oracle_package
    process = subprocess.run(  # noqa: S603
        [node_path, str(helper)],
        input=json.dumps({"package_root": str(package_root), "requests": requests}),
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode != 0:
        raise AssertionError(f"oracle failed: {process.stderr.strip()}")
    return [item["value"] for item in json.loads(process.stdout)["results"]]


def run_oracle_error(
    oracle_package: tuple[Path, Path, str], request: dict[str, object]
) -> str:
    package_root, helper, node_path = oracle_package
    process = subprocess.run(  # noqa: S603
        [node_path, str(helper)],
        input=json.dumps({"package_root": str(package_root), "requests": [request]}),
        capture_output=True,
        text=True,
        check=False,
    )
    assert process.returncode != 0
    return process.stderr.strip()


def test_extensoes_de_status_e_resultado_booleano(
    oracle_package: tuple[Path, Path, str],
) -> None:
    requests = [
        {"mode": "expression", "source": "always()", "context": {}, "cancelled": True},
        {"mode": "expression", "source": "!cancelled()", "context": {}, "cancelled": False},
        {"mode": "expression", "source": "!cancelled()", "context": {}, "cancelled": True},
    ]
    assert run_oracle(oracle_package, requests) == [True, True, False]


def test_oraculo_rejeita_string_contexto_e_status_nao_modelado(
    oracle_package: tuple[Path, Path, str],
) -> None:
    assert "must be boolean" in run_oracle_error(
        oracle_package,
        {"mode": "expression", "source": "'false'", "context": {}, "cancelled": False},
    )
    assert "unsupported status function: success" in run_oracle_error(
        oracle_package,
        {"mode": "expression", "source": "success()", "context": {}, "cancelled": False},
    )
    assert "unsupported status function: success" in run_oracle_error(
        oracle_package,
        {
            "mode": "expression",
            "source": "false && success()",
            "context": {},
            "cancelled": False,
        },
    )
    assert "unsupported status function: failure" in run_oracle_error(
        oracle_package,
        {
            "mode": "expression",
            "source": "false && failure()",
            "context": {},
            "cancelled": False,
        },
    )
    assert "Unrecognized named-value" in run_oracle_error(
        oracle_package,
        {"mode": "expression", "source": "runner.os", "context": {}, "cancelled": False},
    )
    assert "Unrecognized named-value" in run_oracle_error(
        oracle_package,
        {
            "mode": "expression",
            "source": "false && runner.os",
            "context": {},
            "cancelled": False,
        },
    )


def test_oraculo_rejeita_default_success_com_needs(
    oracle_package: tuple[Path, Path, str],
) -> None:
    message = run_oracle_error(
        oracle_package,
        {
            "mode": "expression",
            "source": "needs.build.result == 'success'",
            "context": {"needs": {"build": {"result": "success"}}},
            "cancelled": False,
        },
    )
    assert "implicit success() is not modeled" in message


def test_oraculo_normaliza_funcoes_de_status_casefold(
    oracle_package: tuple[Path, Path, str],
) -> None:
    requests = [
        {
            "mode": "expression",
            "source": "ALWAYS()",
            "context": {"needs": {}},
            "cancelled": True,
        },
        {
            "mode": "expression",
            "source": "CANCELLED()",
            "context": {"needs": {}},
            "cancelled": True,
        },
    ]
    values = run_oracle(oracle_package, requests)
    assert values == [True, True]
    assert all(type(value) is bool for value in values)


def test_oraculo_rejeita_status_casefold_nao_modelado_em_short_circuit(
    oracle_package: tuple[Path, Path, str],
) -> None:
    for source, name in (("false && SUCCESS()", "success"), ("false && FAILURE()", "failure")):
        assert f"unsupported status function: {name}" in run_oracle_error(
            oracle_package,
            {"mode": "expression", "source": source, "context": {}, "cancelled": False},
        )


def test_oraculo_rejeita_license_com_whitespace_extra(tmp_path: Path) -> None:
    altered = tmp_path / "altered.LICENSE"
    altered.write_bytes(LICENSE_PATH.read_bytes() + b" ")
    assert _license_bytes_match(FIXTURE_PATH, LICENSE_PATH)
    assert not _license_bytes_match(FIXTURE_PATH, altered)


def test_carrega_condicoes_reais_e_prova_needs_separado() -> None:
    snapshots = {
        name: load_workflow(WORKFLOW_ROOT / name)
        for name in ("deploy-develop.yml", "deploy-main.yml")
    }
    for snapshot in snapshots.values():
        assert "gate" in snapshot.build_needs
        assert {"gate", "build"}.issubset(snapshot.deploy_needs)


@pytest.mark.parametrize("case", GATE_CASES)
def test_gate_e_build_carregam_if_real(
    case: GateCase, oracle_package: tuple[Path, Path, str]
) -> None:
    snapshot = load_workflow(WORKFLOW_ROOT / case.workflow)
    context = {"github": {"event_name": case.event}, "inputs": {"tag": case.tag}}
    requests = [
        {"mode": "expression", "source": snapshot.gate_if, "context": context, "cancelled": False},
        {"mode": "expression", "source": snapshot.build_if, "context": context, "cancelled": False},
    ]
    assert run_oracle(oracle_package, requests) == [case.expected, case.expected]


@pytest.mark.parametrize(("workflow", "case"), WORKFLOW_ADMISSION_CASES)
def test_admissao_avalia_if_real(
    workflow: str, case: AdmissionCase, oracle_package: tuple[Path, Path, str]
) -> None:
    snapshot = load_workflow(WORKFLOW_ROOT / workflow)
    result = run_oracle(
        oracle_package,
        [
            {
                "mode": "expression",
                "source": snapshot.deploy_if,
                "context": _context(case),
                "cancelled": case.cancelled,
            }
        ],
    )[0]
    assert result is case.expected


@pytest.mark.parametrize("case", PUSH_ADMISSION_CASES)
def test_push_rejeita_tag_indevida_e_modela_inputs_ausente(
    case: tuple[dict[str, object] | None, str, str, bool],
    oracle_package: tuple[Path, Path, str],
) -> None:
    inputs, gate, build, expected = case
    snapshot = load_workflow(WORKFLOW_ROOT / "deploy-develop.yml")
    request = {"mode": "expression", "source": snapshot.deploy_if,
               "context": _push_context(inputs, gate, build), "cancelled": False}
    result = run_oracle(oracle_package, [request])[0]
    assert result is expected


def test_concurrency_compara_grupos_resolvidos_casefold(
    oracle_package: tuple[Path, Path, str],
) -> None:
    groups = []
    for case in GROUP_CASES:
        source = WORKFLOW_ROOT / case.workflow
        group_source = (
            load_concurrency_group(source)
            if case.workflow == "ci.yml"
            else load_workflow(source).concurrency_group
        )
        group = run_oracle(
            oracle_package,
            [
                {
                    "mode": "template",
                    "source": group_source,
                    "context": _group_context(case),
                    "cancelled": False,
                }
            ],
        )[0]
        groups.append((case, str(group)))
    assert groups[0][1].casefold() == groups[1][1].casefold()
    assert groups[0][1].casefold() != groups[2][1].casefold()
    assert groups[2][1].casefold() != groups[3][1].casefold()
    assert groups[0][1].casefold() != groups[4][1].casefold()
    assert groups[5][1].casefold() == groups[6][1].casefold()
    assert groups[5][1].casefold() != groups[7][1].casefold()
    assert groups[2][1].casefold() != groups[5][1].casefold()
    assert groups[3][1].casefold() != groups[7][1].casefold()
    assert groups[2][1] != groups[8][1]
    assert groups[2][1].casefold() == groups[8][1].casefold()
