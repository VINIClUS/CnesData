"""Revisões registradas, retenção declarada e saídas públicas de processamento."""

import json
from pathlib import Path

import pytest

from tests.production.infra._processing_native import (
    A_RUN,
    B_RUN,
    COMMAND_MODES,
    MODULE_SOURCE,
    NULL_RUN,
    ROOT_RUN,
    NativePlans,
    Node,
    Tofu,
    _configuration_address,
    _configuration_resources,
    _module_names,
    _module_resources,
)
from tests.production.infra.test_ecr_ecs import _runtime_identities

PUBLIC_OUTPUTS = {
    "processor_repository_uri": ("processor_registry", "repository_uri"),
    "processor_task_definition_arns": ("processing", "task_definition_arns"),
    "processor_container_name": ("processing", "processor_container_name"),
    "processor_task_role_arns": ("runtime_iam", "task_role_arns"),
    "processor_task_role_names": ("runtime_iam", "task_role_names"),
    "processor_execution_role_arn": ("runtime_iam", "execution_role_arn"),
    "processor_execution_role_name": ("runtime_iam", "execution_role_name"),
    "processor_cluster_arn": ("processing", "cluster_arn"),
    "processor_state_machine_arn": ("processing", "state_machine_arn"),
    "processor_step_functions_role_id": ("runtime_iam", "step_functions_role_id"),
    "processor_step_functions_role_arn": ("runtime_iam", "step_functions_role_arn"),
}
MODES = set(COMMAND_MODES.values())
AWS_SOURCE = "registry.opentofu.org/hashicorp/aws"
DEFAULT_REFERENCE = (
    "https://github.com/hashicorp/terraform-provider-aws/blob/v6.54.0/"
    "website/docs/r/ecs_task_definition.html.markdown"
)


def _tasks(plan: Node, configuration: Node, root: Path) -> list[Node]:
    return [
        resource
        for resource in _module_resources(plan, configuration, root, MODULE_SOURCE)
        if resource["type"] == "aws_ecs_task_definition"
    ]


def _typed_output(plan: Node, name: str) -> Node:
    output = plan["planned_values"].get("outputs", {}).get(name)
    if not isinstance(output, dict) or not {"type", "value"} <= output.keys():
        pytest.fail(f"setup_error evaluated_output_presentation={name}")
    return output


def _public_value(plan: Node, name: str) -> object:
    change = plan.get("output_changes", {}).get(name)
    if not isinstance(change, dict) or "after" not in change or change.get("after_unknown"):
        pytest.fail(f"setup_error root_output_change={name}")
    value = _typed_output(plan, name)["value"]
    assert change["after"] == value, f"processing_root_output_evaluation={name}"
    return value


def _assert_string_map(output: Node, expected: dict[str, str]) -> None:
    value, kind = output["value"], output["type"]
    assert isinstance(value, dict), f"processing_revision_map_type={value!r}"
    assert all(isinstance(item, str) for item in value.values()), "processing_revision_map_values"
    equivalent_object = ["object", dict.fromkeys(value, "string")]
    assert kind in (["map", "string"], equivalent_object), (
        f"processing_revision_native_type={kind!r}"
    )
    assert value == expected, f"processing_revision_map expected={expected!r} actual={value!r}"


def _registered_revisions(plan: Node, configuration: Node, root: Path) -> dict[str, str]:
    tasks = _tasks(plan, configuration, root)
    assert len(tasks) == 3, f"processing_revision_task_count={len(tasks)}"
    configured = dict(_configuration_resources(configuration["root_module"]))
    account = plan["variables"]["shared_outputs"]["value"]["account_id"]
    revisions = {}
    for task in tasks:
        values = task["change"]["after"]
        containers = json.loads(values["container_definitions"])
        assert len(containers) == 1, "processing_revision_container_count"
        mode = COMMAND_MODES.get(tuple(containers[0].get("command", [])))
        assert mode in MODES, f"processing_revision_mode={mode}"
        assert mode not in revisions, f"processing_revision_duplicate={mode}"
        resource = configured[_configuration_address(task["address"])]
        provider = configuration["provider_config"][resource["provider_config_key"]]
        region = provider["expressions"]["region"]["constant_value"]
        revision, family = values["revision"], values["family"]
        assert type(revision) is int, f"processing_revision_number_type={revision!r}"
        assert revision > 0, f"processing_revision_number={revision!r}"
        expected = f"arn:aws:ecs:{region}:{account}:task-definition/{family}:{revision}"
        assert values["arn"] == expected, f"processing_revision_identity mode={mode}"
        assert values["arn"] != values["arn_without_revision"], "processing_revision_unqualified"
        revisions[mode] = values["arn"]
    assert set(revisions) == MODES, f"processing_revision_modes={revisions!r}"
    assert len(set(revisions.values())) == 3, "processing_revision_alias"
    return revisions


def test_outputs_referenciam_revisoes_registradas(
    processing_native: NativePlans, prod_root: Path
) -> None:
    configuration = processing_native.configuration
    calls = configuration["root_module"]["module_calls"]
    names = _module_names(configuration, prod_root, MODULE_SOURCE)
    assert len(names) == 1, f"processing_revision_module_count={names!r}"
    child = calls[names[0]]["module"]
    outputs = child.get("outputs", {})
    assert {"task_definition_arns", "processor_container_name"} <= outputs.keys(), (
        f"processing_revision_output_missing={sorted(outputs)}"
    )
    public = configuration["root_module"].get("outputs", {})
    assert "processor_task_definition_arns" in public, "processing_revision_output_missing=root"
    resources = child.get("resources", [])
    assert not any(
        r["mode"] == "data" and r["type"] == "aws_ecs_task_definition" for r in resources
    ), "processing_revision_latest_lookup"
    references = outputs["task_definition_arns"]["expression"].get("references", [])
    tasks = [r for r in resources if r["type"] == "aws_ecs_task_definition"]
    assert any(r["address"] in references for r in tasks), "processing_revision_producer_reference"
    expected = {}
    for name in (ROOT_RUN, NULL_RUN, A_RUN, B_RUN):
        plan = processing_native.plans[name]
        expected[name] = (
            {}
            if name in (ROOT_RUN, NULL_RUN)
            else _registered_revisions(plan, configuration, prod_root)
        )
        _assert_string_map(_typed_output(plan, "task_definition_arns"), expected[name])
        _assert_string_map(_typed_output(plan, "processor_task_definition_arns"), expected[name])
        assert _public_value(plan, "processor_task_definition_arns") == expected[name]
    assert all(expected[A_RUN][mode] != expected[B_RUN][mode] for mode in MODES), (
        "processing_revision_seeds_aliased"
    )


def _lifecycle_observation(task: Node, configuration: Node, field: str, schema: Node) -> Node:
    configured = dict(_configuration_resources(configuration["root_module"]))[
        _configuration_address(task["address"])
    ]
    attribute = schema["provider_schemas"][task["provider_name"]]["resource_schemas"][task["type"]][
        "block"
    ]["attributes"][field]
    expression = configured.get("expressions", {}).get(field)
    values = task["change"]["after"]
    raw, unknown = values.get(field), task["change"].get("after_unknown", {}).get(field, False)
    observation = {
        "address": task["address"],
        "mode": task.get("index"),
        "field": field,
        "raw_present": field in values,
        "raw_value": raw,
        "after_unknown": unknown,
        "configured_expression": expression,
        "schema_attribute": attribute,
    }
    if unknown is not False or attribute.get("type") != "bool" or not attribute.get("optional"):
        pytest.fail(f"setup_error lifecycle_unknown_or_schema={observation!r}")
    if type(raw) is bool:
        return observation | {"effective_value": raw, "interpretation": "native_known_boolean"}
    omitted = expression is None or expression == {"constant_value": None}
    if raw is not None or not omitted or attribute.get("computed", False):
        pytest.fail(f"setup_error lifecycle_default_unestablished={observation!r}")
    return observation | {
        "effective_value": False,
        "interpretation": "documented_default_for_known_omission",
        "provider_version": "6.54.0",
        "documented_default": False,
        "documentation": DEFAULT_REFERENCE,
    }


@pytest.mark.parametrize("contract", [("skip_destroy", True), ("track_latest", False)])
def test_lifecycle_preserva_revisoes_sem_seguir_latest(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path, contract: tuple[str, bool]
) -> None:
    evidence = Path(tofu_context[1]["TF_DATA_DIR"])
    artifacts = json.loads((evidence / "processing-native.artifacts.json").read_text())
    if artifacts["version"]["provider_selections"].get(AWS_SOURCE) != "6.54.0":
        pytest.fail("setup_error lifecycle_default_provider_version")
    schema = json.loads(
        (evidence / "processing-provider-schema/data/processing-schema-schema.stdout").read_text()
    )
    field, expected = contract
    observations = []
    for name in (A_RUN, B_RUN):
        tasks = _tasks(processing_native.plans[name], processing_native.configuration, prod_root)
        assert len(tasks) == 3, f"processing_lifecycle_task_count={name}"
        observations.extend(
            _lifecycle_observation(task, processing_native.configuration, field, schema)
            | {"run": name}
            for task in tasks
        )
    (evidence / f"processing-{field}-observations.json").write_text(
        json.dumps(observations, indent=2) + "\n", encoding="utf-8"
    )
    assert all(item["effective_value"] is expected for item in observations), (
        f"processing_{field} expected={expected} observations={observations!r}"
    )


def _producer_values(plan: Node, configuration: Node, root: Path) -> dict[str, object]:
    repositories = [
        r
        for r in _module_resources(plan, configuration, root, "../../modules/processor-registry")
        if r["type"] == "aws_ecr_repository"
    ]
    execution, roles = _runtime_identities(plan, configuration, root)
    assert len(repositories) == 1, "processing_public_repository_count"
    assert len(roles) == 3, "processing_public_task_role_count"
    tasks = {r["index"]: r["change"]["after"] for r in roles}
    executions = [execution["change"]["after"]]
    assert set(tasks) == MODES, "processing_public_role_modes"
    assert len(executions) == 1, "processing_public_execution_count"
    values = {
        "processor_repository_uri": repositories[0]["change"]["after"]["repository_url"],
        "processor_task_role_arns": {mode: values["arn"] for mode, values in tasks.items()},
        "processor_task_role_names": {mode: values["name"] for mode, values in tasks.items()},
        "processor_execution_role_arn": executions[0]["arn"],
        "processor_execution_role_name": executions[0]["name"],
        "processor_container_name": "processor",
    }
    for kind, output in (
        ("aws_ecs_cluster", "cluster_arn"), ("aws_sfn_state_machine", "state_machine_arn"),
    ):
        producers = [
            r for r in _module_resources(plan, configuration, root, MODULE_SOURCE)
            if r["type"] == kind
        ]
        assert len(producers) == (0 if kind == "aws_sfn_state_machine" else 1)
        values[f"processor_{output}"] = (
            producers[0]["change"]["after"]["arn"] if producers else None
        )
    sfn = next(
        r for r in _module_resources(plan, configuration, root, "../../modules/runtime-iam")
        if r["address"] == "module.runtime_iam.aws_iam_role.step_functions[0]"
    )
    for field in ("id", "arn"):
        values[f"processor_step_functions_role_{field}"] = sfn["change"]["after"][field]
    return values


def test_outputs_publicos_encaminham_produtores_e_nome_real_do_container(
    processing_native: NativePlans, prod_root: Path
) -> None:
    configuration = processing_native.configuration
    outputs = configuration["root_module"].get("outputs", {})
    assert PUBLIC_OUTPUTS.keys() <= outputs.keys(), (
        f"processing_public_output_missing={sorted(PUBLIC_OUTPUTS.keys() - outputs.keys())}"
    )
    for name, (module, child) in PUBLIC_OUTPUTS.items():
        references = outputs[name]["expression"].get("references", [])
        assert f"module.{module}.{child}" in references, f"processing_public_forwarding={name}"
    for run, plan in processing_native.plans.items():
        expected = _producer_values(plan, configuration, prod_root)
        expected["processor_task_definition_arns"] = (
            {}
            if run in (ROOT_RUN, NULL_RUN)
            else _registered_revisions(plan, configuration, prod_root)
        )
        for name, value in expected.items():
            output = _typed_output(plan, name)
            if isinstance(value, dict):
                _assert_string_map(output, value)
            elif value is not None:
                assert output["type"] == "string", f"processing_public_type={name}"
            assert _public_value(plan, name) == value, (
                f"processing_public_producer={name} run={run}"
            )
        child = _typed_output(plan, "processor_container_name")
        assert child["type"] == "string", "processing_child_container_type"
        assert child["value"] == "processor", "processing_child_container_name"
        for task in _tasks(plan, configuration, prod_root):
            containers = json.loads(task["change"]["after"]["container_definitions"])
            assert len(containers) == 1, "processing_public_container_count"
            assert containers[0].get("name") == child["value"], (
                f"processing_public_container={task['address']}"
            )
