import copy
import json
import re
from datetime import UTC, datetime
from pathlib import Path

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    ProductionNetworkPolicy,
    validate_production_state_machine,
    validate_state_machine,
)
from tests.production.infra import test_step_functions_iam as iam
from tests.production.infra._processing_native import (
    NativePlans,
    Node,
    Tofu,
    _configuration_address,
)
from tests.production.infra._step_functions_iam_contracts import (
    expected_machine_arn,
    one_resource,
    state_machine_definition,
)

ACCOUNT = iam.ACCOUNT
PREFIX = iam.PREFIX
ROOT_RUN = iam.ROOT_RUN

_task_from_definition = iam._task_from_definition
_stateful_sequence = iam._stateful_sequence


@pytest.fixture(scope="module")
def processing_stateful_context(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path
) -> tuple[NativePlans, Tofu, Path]:
    return processing_native, tofu_context, prod_root


def _assert_stateful_interfaces(context: tuple[NativePlans, Tofu, Path]) -> None:
    native, _, root = context
    processing = iam.configuration_module(native.configuration, root, iam.PROCESSING_SOURCE)
    assert "step_functions_role_arn" in iam.configuration_variable_names(processing)


def _state_resources(state: Node) -> list[Node]:
    def descend(module: Node) -> list[Node]:
        resources = list(module.get("resources", []))
        for child in module.get("child_modules", []):
            resources.extend(descend(child))
        return resources

    return descend(state.get("values", {}).get("root_module", {}))


def _state_resource(state: Node, resource_type: str, name: str) -> Node:
    return one_resource(
        [
            resource
            for resource in _state_resources(state)
            if resource.get("type") == resource_type and resource.get("name") == name
        ],
        f"state_{resource_type}_{name}",
    )


def _state_outputs(state: Node) -> dict[str, object]:
    return {

        name: output["value"]
        for name, output in state.get("values", {}).get("outputs", {}).items()
        if isinstance(output, dict) and "value" in output
    }


def _task_environments(state: Node) -> dict[str, dict[str, str]]:
    modes = {(): "unit", ("recover-once",): "recovery", ("dispatch-outbox-once",): "audit"}
    found: dict[str, dict[str, str]] = {}
    for resource in _state_resources(state):
        if resource.get("type") != "aws_ecs_task_definition":
            continue
        values = resource.get("values", {})
        containers = json.loads(values["container_definitions"])
        container = one_resource(containers, "state_task_container")
        mode = modes.get(tuple(container.get("command", [])))
        if mode is None:
            continue
        found[mode] = {
            item["name"]: item["value"]
            for item in container.get("environment", [])
            if "name" in item and "value" in item
        }
    return found


def _route_machine(route_plan: Node) -> Node:
    return one_resource(
        [
            resource

            for resource in route_plan.get("resource_changes", [])
            if resource.get("type") == "aws_sfn_state_machine"
            and _configuration_address(resource.get("address", "")).endswith(
                "aws_sfn_state_machine.processor"
            )
        ],
        "route_state_machine",
    )


def _rendered_network(route_plan: Node) -> tuple[list[str], str]:
    subnets = sorted(
        resource["change"]["after"]["id"]
        for resource in route_plan.get("resource_changes", [])
        if resource.get("type") == "aws_subnet"
        and resource.get("address", "").startswith("module.network.aws_subnet.public[")
    )
    security_groups = [
        resource["change"]["after"]["id"]
        for resource in route_plan.get("resource_changes", [])
        if resource.get("type") == "aws_security_group"
        and resource.get("address", "").endswith("aws_security_group.tasks")
    ]
    return subnets, one_resource(
        [{"id": value} for value in security_groups], "route_security_group"
    )["id"]


def _state_task_definitions(state: Node) -> list[Node]:
    return [
        resource
        for resource in _state_resources(state)
        if resource.get("type") == "aws_ecs_task_definition"
    ]


def _state_machine_resources(state: Node) -> list[Node]:
    return [
        resource
        for resource in _state_resources(state)
        if resource.get("type") == "aws_sfn_state_machine"
    ]


def _state_machine_definition(resource: Node) -> Node:
    return json.loads(resource["values"]["definition"])


def _assert_sequence_stages(
    plans: dict[str, Node], states: dict[str, Node]
) -> tuple[Node, Node]:
    phase_a = states["phase_a_register_a"]
    assert not _state_machine_resources(phase_a)
    candidates = _state_outputs(phase_a)["processor_task_definition_arns"]
    assert isinstance(candidates, dict)
    assert set(candidates) == {
        "unit",
        "recovery",
        "audit",
    }
    create_plan = plans["phase_b_create_plan"]
    create_machine = _route_machine(create_plan)
    changed = {
        resource["address"]
        for resource in create_plan.get("resource_changes", [])
        if resource.get("change", {}).get("actions") != ["no-op"]
    }
    api_address = "module.runtime_iam.aws_iam_role_policy.api[0]"
    assert create_machine["address"] == "module.processing.aws_sfn_state_machine.processor[0]"
    assert changed == {create_machine["address"], api_address}
    assert iam.resource_at(create_plan, api_address)["change"]["actions"] == ["update"]
    assert create_machine["change"]["actions"] == ["create"]
    create_state = states["phase_b_create_apply"]
    assert len(_state_machine_resources(create_state)) == 1
    assert len(_state_task_definitions(create_state)) == 3
    register_b = states["phase_a_register_b"]
    assert len(_state_task_definitions(register_b)) == 3
    create_definition = _state_machine_definition(_state_machine_resources(create_state)[0])
    register_definition = _state_machine_definition(_state_machine_resources(register_b)[0])
    assert create_definition == register_definition
    create_outputs = _state_outputs(create_state)["processor_task_definition_arns"]
    assert isinstance(create_outputs, dict)
    assert _task_from_definition(
        _state_machine_definition(_state_machine_resources(register_b)[0])
    )["Parameters"]["TaskDefinition"] == create_outputs["unit"]
    return create_state, register_b


def _validate_definition_polarity(
    definition: Node, machine_arn: str, subnets: list[str], security_group: str
) -> None:
    client = boto3.client(

        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED)
    )
    response = {
        "stateMachineArn": machine_arn,
        "name": PREFIX,
        "definition": json.dumps(definition),
        "roleArn": f"arn:aws:iam::{ACCOUNT}:role/{PREFIX}-step-functions",
        "type": "STANDARD",
        "creationDate": datetime(2026, 10, 31, tzinfo=UTC),
    }
    with Stubber(client) as stubber:
        stubber.add_response("describe_state_machine", response, {"stateMachineArn": machine_arn})
        policy = ProductionNetworkPolicy("processor", 3600, tuple(subnets), security_group)
        validate_production_state_machine(client, machine_arn, policy)
        stubber.assert_no_pending_responses()
    generic = boto3.client(
        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED)
    )
    with Stubber(generic) as stubber:
        stubber.add_response("describe_state_machine", response, {"stateMachineArn": machine_arn})
        with pytest.raises(IncompatibleStateMachine, match="assign_public_ip_mismatch"):
            validate_state_machine(generic, machine_arn, "processor", 3600)
        stubber.assert_no_pending_responses()


def _route_cluster(plan: Node) -> Node:
    return one_resource(
        [
            resource
            for resource in plan.get("resource_changes", [])
            if resource.get("type") == "aws_ecs_cluster"
            and resource.get("change", {}).get("after", {}).get("name") == f"{PREFIX}-processor"
        ],
        "route_cluster",
    )


def _assert_task_contract(
    definition: Node, task: Node, expected_active: object, network: Node
) -> None:
    assert task["Parameters"]["TaskDefinition"] == expected_active
    assert isinstance(expected_active, str)
    assert re.fullmatch(
        r"arn:aws:ecs:us-east-2:123456789012:task-definition/"
        r"cnesdata-prod-processor-unit:[1-9][0-9]*",
        expected_active,
    )
    parameters = task["Parameters"]
    assert parameters["LaunchType"] == "FARGATE"
    assert network["AssignPublicIp"] == "ENABLED"
    assert parameters["Overrides"]["ContainerOverrides"][0]["Name"] == "processor"
    environment = parameters["Overrides"]["ContainerOverrides"][0]["Environment"]
    assert len(environment) == 7, f"state_machine_environment_count={len(environment)}"
    assert {item["Name"] for item in environment} == {
        "TENANT_ID",
        "RUN_ID",
        "WAVE_ID",
        "DISPATCH_ID",
        "UNIT_ID",
        "EXECUTION_OWNER",
        "LEASE_SECONDS",
    }
    assert next(item for item in environment if item["Name"] == "LEASE_SECONDS")["Value"] == "3600"
    assert task["TimeoutSeconds"] == 3600
    assert task["Retry"] == [{
        "ErrorEquals": ["AmazonECS.Unknown"], "IntervalSeconds": 3,
        "BackoffRate": 2, "MaxAttempts": 3, "MaxDelaySeconds": 5, "JitterStrategy": "FULL",
    }]
    assert "Catch" not in task
    assert "Catch" not in definition["States"]["RunUnits"]
    assert "Distributed" not in definition["States"]["RunUnits"]
    assert "LoggingConfiguration" not in definition
    assert network["Subnets"]


def test_definition_production_tem_map_inline_e_retry_exato(
    processing_stateful_context: tuple[NativePlans, Tofu, Path],
) -> None:
    _assert_stateful_interfaces(processing_stateful_context)
    native, tofu, root = processing_stateful_context
    _, plans, states = _stateful_sequence(tofu, root, native)
    _assert_sequence_stages(plans, states)
    machine = _route_machine(plans["phase_b_route_plan"])

    definition = state_machine_definition(machine)
    task = _task_from_definition(definition)
    values = machine["change"]["after"]
    assert values["type"] == "STANDARD"
    assert not values.get("logging_configuration")
    route_plan = plans["phase_b_route_plan"]
    cluster = _route_cluster(route_plan)
    assert task["Parameters"]["Cluster"] == cluster["change"]["after"]["arn"]
    outputs = _state_outputs(states["phase_a_register_b"])
    candidates = outputs.get("processor_task_definition_arns")
    assert isinstance(candidates, dict)
    expected_active = candidates.get("unit")
    network = task["Parameters"]["NetworkConfiguration"]["AwsvpcConfiguration"]
    _assert_task_contract(definition, task, expected_active, network)
    subnets, security_group = _rendered_network(route_plan)
    assert network["Subnets"] == subnets
    assert network["SecurityGroups"] == [security_group]
    _validate_definition_polarity(
        definition,

        values["arn"],
        subnets,
        security_group,
    )


def test_arn_da_machine_iguala_ambiente_da_task_apos_criacao_active(
    processing_stateful_context: tuple[NativePlans, Tofu, Path],
) -> None:
    _assert_stateful_interfaces(processing_stateful_context)
    native, tofu, root = processing_stateful_context
    _, plans, states = _stateful_sequence(tofu, root, native)
    machine = _route_machine(plans["phase_b_route_plan"])
    machine_arn = machine["change"]["after"]["arn"]
    assert machine_arn == expected_machine_arn(plans["phase_b_route_plan"])
    for state_name in ("phase_b_create_apply", "phase_a_register_b"):
        outputs = _state_outputs(states[state_name])
        assert outputs["processor_state_machine_arn"] == machine_arn
        environments = _task_environments(states[state_name])
        for mode in ("unit", "recovery"):
            assert environments[mode]["AWS_STATE_MACHINE_ARN"] == machine_arn
        assert "AWS_STATE_MACHINE_ARN" not in environments["audit"]
    outputs = _state_outputs(states["phase_a_register_b"])
    definition = state_machine_definition(machine)
    task = _task_from_definition(definition)
    candidates = outputs["processor_task_definition_arns"]
    assert isinstance(candidates, dict)
    assert task["Parameters"]["TaskDefinition"] == candidates["unit"]


def test_transicao_de_routing_apos_phase_a_muda_somente_machine(
    processing_stateful_context: tuple[NativePlans, Tofu, Path],
) -> None:
    _assert_stateful_interfaces(processing_stateful_context)
    native, tofu, root = processing_stateful_context
    _, plans, states = _stateful_sequence(tofu, root, native)
    route_plan = plans["phase_b_route_plan"]
    changed = {

        resource["address"]: resource["change"]
        for resource in route_plan.get("resource_changes", [])
        if resource.get("change", {}).get("actions") != ["no-op"]
    }
    machine_address = _route_machine(route_plan)["address"]
    assert set(changed) == {machine_address}, f"routing_transition_delta={changed!r}"
    assert machine_address == "module.processing.aws_sfn_state_machine.processor[0]"
    assert changed[machine_address]["actions"] == ["update"]
    before = _state_resource(states["phase_a_register_b"], "aws_sfn_state_machine", "processor")
    before_definition = json.loads(before["values"]["definition"])
    after_definition = state_machine_definition(_route_machine(route_plan))
    before_task = _task_from_definition(before_definition)
    after_task = _task_from_definition(after_definition)
    assert before_task["Parameters"]["TaskDefinition"] != after_task["Parameters"]["TaskDefinition"]
    before_without = copy.deepcopy(before_definition)
    after_without = copy.deepcopy(after_definition)
    _task_from_definition(before_without)["Parameters"]["TaskDefinition"] = None
    _task_from_definition(after_without)["Parameters"]["TaskDefinition"] = None
    assert before_without == after_without
