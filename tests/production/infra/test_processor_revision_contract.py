"""Contrato nativo das revisões active/retained do processor."""

import json
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._processing_native import (
    MOCK_PROVIDERS,
    _native_result,
    _successful_plans,
)
from tests.production.infra._root_native_identities import (
    _override_hcl,
    assert_root_identity_producers,
)

type Node = dict[str, Any]
type Tofu = tuple[str, dict[str, str]]

ACTIVE = "processor_active_unit_task_definition_arn"
RETAINED = "processor_retained_unit_task_definition_arns"
ACCOUNT = "123456789012"
REGION = "us-east-2"
FAMILY = "cnesdata-prod-processor-unit"
REVISION = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{FAMILY}:7"
OLD_REVISION = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{FAMILY}:6"
TEST_DIRECTORY = ".terraform/processor-revision-tests"
PLAN_ARGUMENTS = (
    "test",
    "-no-color",
    "-json",
    "-verbose",
    f"-test-directory={TEST_DIRECTORY}",
)
OMIT = object()
def _records(tofu: Tofu, kind: str) -> list[Node]:
    evidence = Path(tofu[1]["TF_DATA_DIR"]) / "root-identity-records.json"
    return json.loads(evidence.read_text())[kind]


@pytest.fixture(scope="module", autouse=True)
def revision_test_directory(prod_root: Path, tofu_artifacts: dict[str, Node]) -> Iterator[Path]:
    del tofu_artifacts
    directory = prod_root / TEST_DIRECTORY
    directory.mkdir(parents=True, exist_ok=False)
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


def _variables(shared: Node, active: object = OMIT, retained: object = OMIT) -> str:
    values: dict[str, object] = {"shared_outputs": shared}
    if active is not OMIT:
        values[ACTIVE] = active
    if retained is not OMIT:
        values[RETAINED] = retained
    return "\n".join(f"    {name} = {json.dumps(value)}" for name, value in values.items())


def _run_block(shared: Node, active: object, retained: object, run_name: str) -> str:
    return (
        f'\nrun "{run_name}" {{\n  command = plan\n  variables {{\n'
        + _variables(shared, active, retained)
        + "\n  }\n}\n"
    )


def _clear_test_files(root: Path) -> None:
    for test_file in (root / TEST_DIRECTORY).glob("*.tftest.hcl"):
        test_file.unlink()


def _run_case(tofu: Tofu, root: Path, suite: str, label: str) -> subprocess.CompletedProcess[str]:
    _clear_test_files(root)
    (root / TEST_DIRECTORY / "processor-revision.tftest.hcl").write_text(suite, encoding="utf-8")
    return _native_result(tofu, root, PLAN_ARGUMENTS, label)


def _run_invalid_cases(tofu: Tofu, root: Path, shared: Node) -> subprocess.CompletedProcess[str]:
    _clear_test_files(root)
    directory = root / TEST_DIRECTORY
    for name, field, value, _ in INVALID_CASES:
        active = value if field == ACTIVE else None
        retained = [value] if field == RETAINED else [OLD_REVISION]
        header = MOCK_PROVIDERS + _override_hcl(_records(tofu, "overrides"))
        suite = header + _run_block(shared, active, retained, name)
        (directory / f"{name}.tftest.hcl").write_text(suite, encoding="utf-8")
    return _native_result(tofu, root, PLAN_ARGUMENTS, "processor-revision-invalid")


def _root_expressions(configuration: Node) -> Node:
    return configuration["root_module"]["module_calls"]["processing"]["expressions"]


def _messages(result: subprocess.CompletedProcess[str]) -> list[Node]:
    messages = []
    for line in result.stdout.splitlines():
        try:
            messages.append(json.loads(line))
        except json.JSONDecodeError as error:
            pytest.fail(f"setup_error native_json={error}")
    return messages


def _run_diagnostics(
    result: subprocess.CompletedProcess[str],
) -> tuple[dict[str, tuple[str, str]], dict[str, tuple[str, list[Node]]]]:
    statuses: dict[str, tuple[str, str]] = {}
    diagnostics: dict[str, tuple[str, list[Node]]] = {}
    for message in _messages(result):
        if message.get("type") == "test_run":
            run = message["test_run"]["run"]
            statuses[run] = (message["@testfile"], message["test_run"]["status"])
        if message.get("type") == "diagnostic":
            run = message.get("@testrun")
            test_file = message.get("@testfile")
            if not isinstance(run, str) or not isinstance(test_file, str):
                pytest.fail("setup_error diagnostic_without_test_run")
            current_file, current = diagnostics.get(run, (test_file, []))
            if current_file != test_file:
                pytest.fail(f"setup_error diagnostic_path_conflict={run}")
            diagnostics[run] = (current_file, current + [message])
    return statuses, diagnostics


def test_raiz_declara_e_encaminha_revisoes(
    tofu_artifacts: dict[str, Node],
) -> None:
    root_variables = tofu_artifacts["configuration"]["root_module"]["variables"]
    active_spec = root_variables.get(ACTIVE)
    retained_spec = root_variables.get(RETAINED)
    assert active_spec is not None, f"root_variable_missing={ACTIVE}"
    assert retained_spec is not None, f"root_variable_missing={RETAINED}"
    assert active_spec["type"] == "string"
    assert "default" in active_spec
    assert active_spec["default"] is None
    assert retained_spec["type"] == ["set", "string"]
    assert "default" in retained_spec
    assert retained_spec["default"] == []
    expressions = _root_expressions(tofu_artifacts["configuration"])
    assert expressions[ACTIVE] == {"references": [f"var.{ACTIVE}"]}
    assert expressions[RETAINED] == {"references": [f"var.{RETAINED}"]}


def test_revisoes_validas_normalizam_set_e_bootstrap(
    tofu_context: Tofu,
    prod_root: Path,
    valid_shared_outputs: Node,
) -> None:
    blocks = (
        _run_block(valid_shared_outputs, OMIT, OMIT, "defaults_omitted"),
        _run_block(valid_shared_outputs, None, None, "bootstrap_null"),
        _run_block(valid_shared_outputs, None, [OLD_REVISION], "phase_a"),
        _run_block(
            valid_shared_outputs,
            OLD_REVISION,
            [OLD_REVISION, REVISION, OLD_REVISION],
            "set_a",
        ),
        _run_block(valid_shared_outputs, OLD_REVISION, [REVISION, OLD_REVISION], "set_b"),
    )
    expected = {
        "defaults_omitted": (None, set()),
        "bootstrap_null": (None, None),
        "phase_a": (None, {OLD_REVISION}),
        "set_a": (OLD_REVISION, {OLD_REVISION, REVISION}),
        "set_b": (OLD_REVISION, {OLD_REVISION, REVISION}),
    }
    plans = _successful_plans(
        _run_case(
            tofu_context,
            prod_root,
            MOCK_PROVIDERS + _override_hcl(_records(tofu_context, "overrides")) + "".join(blocks),
            "processor-revision-valid",
        ),
        set(expected),
    )
    for plan in plans.values():
        assert_root_identity_producers(plan, _records(tofu_context, "seeds"))
    for name, (active, retained) in expected.items():
        values = plans[name]["variables"]
        assert ACTIVE in values, f"bootstrap_active_missing={name}"
        assert RETAINED in values, f"bootstrap_retained_missing={name}"
        assert values[ACTIVE]["value"] == active, f"active_effective_value={name}"
        if name == "bootstrap_null":
            assert values[RETAINED]["value"] is None
            continue
        assert set(values[RETAINED]["value"]) == retained, f"retained_effective_value={name}"


INVALID_VALUES = (
    (
        "missing_revision",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{FAMILY}",
        "task_definition_arn_shape",
    ),
    (
        "foreign_account",
        f"arn:aws:ecs:{REGION}:999999999999:task-definition/{FAMILY}:7",
        "task_definition_arn_account",
    ),
    (
        "foreign_region",
        f"arn:aws:ecs:us-west-2:{ACCOUNT}:task-definition/{FAMILY}:7",
        "task_definition_arn_region",
    ),
    (
        "foreign_service",
        f"arn:aws:s3:{REGION}:{ACCOUNT}:task-definition/{FAMILY}:7",
        "task_definition_arn_shape",
    ),
    (
        "wrong_family",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-other:7",
        "task_definition_arn_family",
    ),
    (
        "recovery_family",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-recovery:7",
        "task_definition_arn_family",
    ),
    (
        "audit_family",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-audit:7",
        "task_definition_arn_family",
    ),
    (
        "latest_revision",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{FAMILY}:latest",
        "task_definition_arn_revision",
    ),
    (
        "zero_revision",
        f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/{FAMILY}:0",
        "task_definition_arn_revision",
    ),
)
INVALID_CASES = tuple(
    (f"{field}_{name}", field, value, diagnostic)
    for field in (ACTIVE, RETAINED)
    for name, value, diagnostic in INVALID_VALUES
) + (("active_not_retained", ACTIVE, REVISION, "active_must_be_retained"),)


@pytest.fixture(scope="module")
def invalid_revision_result(
    tofu_context: Tofu, prod_root: Path, valid_shared_outputs: Node
) -> subprocess.CompletedProcess[str]:
    return _run_invalid_cases(tofu_context, prod_root, valid_shared_outputs)


@pytest.mark.parametrize("case", INVALID_CASES)
def test_rejeita_revisoes_invalidas(
    invalid_revision_result: subprocess.CompletedProcess[str],
    case: tuple[str, str, str, str],
) -> None:
    name, field, _, diagnostic = case
    statuses, diagnostics = _run_diagnostics(invalid_revision_result)
    if name not in statuses:
        pytest.fail(f"setup_error invalid_test_run_unobserved={name}")
    test_file, status = statuses[name]
    assert Path(test_file).name == f"{name}.tftest.hcl"
    assert status == "error", f"invalid_revision_accepted={name}"
    if name not in diagnostics:
        pytest.fail(f"setup_error invalid_diagnostic_unobserved={name}")
    diagnostic_file, messages = diagnostics[name]
    assert Path(diagnostic_file).name == f"{name}.tftest.hcl"
    summaries = {message.get("diagnostic", {}).get("summary") for message in messages}
    assert summaries <= {"Invalid value for variable", "Invalid value for input variable"}
    payload = json.dumps(messages)
    assert "Invalid value for variable" in payload
    assert f"var.{field}" in payload
    assert diagnostic in payload, f"invalid_revision_diagnostic={name}"
