"""Registro nativo por digest; revisões e runtime completo aguardam etapas posteriores."""

import json
import shutil
from pathlib import Path

import pytest

from tests.production.infra._processing_digest import INVALID_DIGESTS, assert_digest_rejected
from tests.production.infra._processing_native import (
    A_RUN,
    B_RUN,
    COMMAND_MODES,
    DIGEST_A,
    DIGEST_B,
    MODULE_SOURCE,
    NULL_RUN,
    PLAN_ARGUMENTS,
    ROOT_RUN,
    NativePlans,
    Node,
    Registration,
    Tofu,
    _configuration_address,
    _managed_resources,
    _module_names,
    _module_resources,
    _native_result,
    _override_hcl,
    _suite,
)


def _producers(plan: Node, configuration: Node, root: Path) -> tuple[Node, str, dict[str, str]]:
    repositories = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/processor-registry")
        if r["type"] == "aws_ecr_repository"
    ]
    roles = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/runtime-iam")
        if r["type"] == "aws_iam_role"
    ]
    assert len(repositories) == 1, f"processing_own_repositories={len(repositories)}"
    tasks = {role["index"]: role["change"]["after"]["arn"] for role in roles if "index" in role}
    executions = [role["change"]["after"]["arn"] for role in roles if "index" not in role]
    assert set(tasks) == set(COMMAND_MODES.values()), f"processing_task_role_modes={tasks!r}"
    assert len(roles) == 4, f"processing_role_count={len(roles)}"
    assert len(executions) == 1, f"processing_execution_role_count={len(executions)}"
    assert len(set(tasks.values()) | set(executions)) == 4, "processing_role_identities=aliased"
    return repositories[0], executions[0], tasks


def _registration_tuple(
    task: Node, roles: dict[str, str], execution: str, expected: str
) -> tuple[str, Registration]:
    assert task["change"]["actions"] == ["create"], f"processing_task_actions={task['address']}"
    values = task["change"]["after"]
    containers = json.loads(values["container_definitions"])
    assert len(containers) == 1, f"processing_containers={containers!r}"
    assert containers[0].get("name") == "processor", f"processing_container={containers[0]!r}"
    container = containers[0]
    command = container.get("command", [])
    assert isinstance(command, list), f"processing_command={command!r}"
    mode = COMMAND_MODES.get(tuple(command))
    assert mode is not None, f"processing_mode_command={command!r}"
    family = values.get("family")
    assert isinstance(family, str), f"processing_family_type={type(family)!r}"
    assert family, f"processing_family={family!r}"
    assert values.get("task_role_arn") == roles[mode], f"processing_task_role mode={mode}"
    assert values.get("execution_role_arn") == execution, f"processing_execution_role mode={mode}"
    image = container.get("image")
    assert image == expected, f"processing_image mode={mode} expected={expected!r} actual={image!r}"
    return mode, (family, image, values["task_role_arn"], values["execution_role_arn"])


def _registration_tuples(
    plan: Node, configuration: Node, root: Path, digest: str
) -> dict[str, Registration]:
    tasks = _managed_resources(plan, "aws_ecs_task_definition")
    assert len(tasks) == 3, f"processing_task_definitions expected=3 actual={len(tasks)}"
    repository, execution, roles = _producers(plan, configuration, root)
    prefixes = {f"module.{name}" for name in _module_names(configuration, root, MODULE_SOURCE)}
    instances = [
        child
        for child in plan["planned_values"]["root_module"].get("child_modules", [])
        if _configuration_address(child["address"]) in prefixes and child.get("resources")
    ]
    assert len(instances) == 1, f"processing_nonempty_instances={len(instances)}"
    registrations: dict[str, Registration] = {}
    expected = f"{repository['change']['after']['repository_url']}@{digest}"
    for task in tasks:
        assert task["address"].startswith(instances[0]["address"] + "."), (
            f"processing_foreign_task={task['address']}"
        )
        mode, registration = _registration_tuple(task, roles, execution, expected)
        assert mode not in registrations, f"processing_duplicate_mode={mode}"
        registrations[mode] = registration
    assert set(registrations) == set(COMMAND_MODES.values()), f"processing_modes={registrations!r}"
    assert len({item[0] for item in registrations.values()}) == 3, "processing_families=duplicated"
    return registrations


def test_raiz_instancia_processing_uma_vez(processing_plan: Node, prod_root: Path) -> None:
    calls = processing_plan["root_module"].get("module_calls", {})
    module = (prod_root / MODULE_SOURCE).resolve()
    consumers = [
        name for name, call in calls.items() if (prod_root / call["source"]).resolve() == module
    ]
    assert consumers == ["processing"], (
        f"processing_root_consumers expected=['processing'] actual={consumers!r}"
    )


def test_bootstrap_nulo_nao_registra_tasks(processing_native: NativePlans, prod_root: Path) -> None:
    for name in (ROOT_RUN, NULL_RUN):
        plan = processing_native.plans[name]
        assert plan["variables"]["image_digest"] == {"value": None}, f"processing_null_input={name}"
        assert not _managed_resources(plan, "aws_ecs_task_definition"), (
            f"processing_null_tasks={name}"
        )
        _producers(plan, processing_native.configuration, prod_root)
        logs = [
            r
            for r in _managed_resources(plan, "aws_cloudwatch_log_group")
            if r["change"]["after"].get("name") == "/ecs/cnesdata-prod-processor"
        ]
        assert len(logs) == 1, f"processing_null_log_groups={len(logs)}"


def test_digest_registra_tres_modos_no_repositorio_proprio(
    processing_native: NativePlans, prod_root: Path
) -> None:
    _registration_tuples(
        processing_native.plans[A_RUN], processing_native.configuration, prod_root, DIGEST_A
    )


def test_outro_digest_altera_imagem_dos_tres_modos(
    processing_native: NativePlans, prod_root: Path
) -> None:
    other = _registration_tuples(
        processing_native.plans[B_RUN], processing_native.configuration, prod_root, DIGEST_B
    )
    first = _registration_tuples(
        processing_native.plans[A_RUN], processing_native.configuration, prod_root, DIGEST_A
    )
    for mode in COMMAND_MODES.values():
        assert first[mode][0] == other[mode][0], f"processing_family_changed mode={mode}"
        assert first[mode][1] != other[mode][1], f"processing_digest_not_consumed mode={mode}"
        assert first[mode][2:] == other[mode][2:], f"processing_roles_changed mode={mode}"


@pytest.mark.parametrize("case", INVALID_DIGESTS, ids=[case[0] for case in INVALID_DIGESTS])
def test_image_uri_exige_digest(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path, case: tuple[str, str]
) -> None:
    evidence = Path(tofu_context[1]["TF_DATA_DIR"])
    identities = json.loads((evidence / "processing-identity-overrides.json").read_text())
    shared = {
        "shared_outputs": processing_native.plans[ROOT_RUN]["variables"]["shared_outputs"]["value"]
    }
    name = "digest_" + case[0].replace("-", "_")
    label = "processing-" + name
    suite = _suite(shared, {name: {"image_digest": case[1]}}) + _override_hcl(identities)
    directory = prod_root / ".terraform/processing-digest-tests"
    directory.mkdir(exist_ok=False)
    try:
        (directory / "digest.tftest.hcl").write_text(suite, encoding="utf-8")
        (evidence / f"{label}.tftest.hcl").write_text(suite, encoding="utf-8")
        arguments = (*PLAN_ARGUMENTS[:-1], "-test-directory=.terraform/processing-digest-tests")
        result = _native_result(tofu_context, prod_root, arguments, label)
        assert_digest_rejected(result, (prod_root, processing_native.configuration), case)
    finally:
        shutil.rmtree(directory)
