"""Adapter AWS Step Functions do executor do processador e do probe de execução."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast

from botocore.exceptions import ClientError

from cnes_domain.control_plane.entities import require_dispatch_id
from cnes_domain.ports.environment_gate import ExecutionObservation, ExecutionPresence
from cnes_domain.ports.processing import ExecutionStatus, StartRunExecution

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from cnes_domain.ports.processing import CancelRunExecution

    class _StepFunctionsClient(Protocol):
        def describe_state_machine(self, **kwargs: str) -> dict[str, Any]: ...
        def start_execution(self, **kwargs: str) -> dict[str, Any]: ...
        def stop_execution(self, **kwargs: str) -> dict[str, Any]: ...
        def describe_execution(self, **kwargs: str) -> dict[str, Any]: ...

_STATUS = {
    "RUNNING": ExecutionStatus.RUNNING,
    "PENDING_REDRIVE": ExecutionStatus.RUNNING,
    "SUCCEEDED": ExecutionStatus.SUCCEEDED,
    "FAILED": ExecutionStatus.FAILED,
    "TIMED_OUT": ExecutionStatus.FAILED,
    "ABORTED": ExecutionStatus.CANCELED,
}
_PRESENCE = {
    ExecutionStatus.RUNNING: ExecutionPresence.RUNNING,
    ExecutionStatus.SUCCEEDED: ExecutionPresence.TERMINAL,
    ExecutionStatus.FAILED: ExecutionPresence.TERMINAL,
    ExecutionStatus.CANCELED: ExecutionPresence.TERMINAL,
}
_ECS_RUN_TASK_SYNC = "arn:aws:states:::ecs:runTask.sync"
_ASSIGN_PUBLIC_IP = "DISABLED"
_PRODUCTION_ASSIGN_PUBLIC_IP = "ENABLED"
_PRODUCTION_ECS_PARAMETERS = frozenset(
    {"Cluster", "TaskDefinition", "LaunchType", "NetworkConfiguration", "Overrides"},
)
_PRODUCTION_NETWORK_KEYS = frozenset({"AwsvpcConfiguration"})
_PRODUCTION_AWSVPC_KEYS = frozenset({"AssignPublicIp", "Subnets", "SecurityGroups"})
_PRODUCTION_OVERRIDE_KEYS = frozenset({"ContainerOverrides"})
_ENVIRONMENT_PATHS = {
    "TENANT_ID": "$.tenant_id",
    "RUN_ID": "$.run_id",
    "WAVE_ID": "$.wave_id",
    "DISPATCH_ID": "$.dispatch_id",
    "UNIT_ID": "$.unit_id",
    "EXECUTION_OWNER": "$$.Execution.Id",
}
_FAILURE_ABSORBING_FIELDS = (
    "Catch",
    "ToleratedFailureCount",
    "ToleratedFailureCountPath",
    "ToleratedFailurePercentage",
    "ToleratedFailurePercentagePath",
)


_ITEM_SELECTOR = {
    "tenant_id.$": "$.tenant_id",
    "run_id.$": "$.run_id",
    "wave_id.$": "$.wave_id",
    "dispatch_id.$": "$.dispatch_id",
    "unit_id.$": "$$.Map.Item.Value",
}


class IncompatibleStateMachine(Exception):
    """State machine fora do contrato Standard + Inline Map + ECS Fargate."""


class ProcessorExecutionUnavailable(Exception):
    """Falha AWS normalizada ou conflito de nome de execução."""


@dataclass(frozen=True, slots=True)
class ProductionNetworkPolicy:
    """Contrato exato do perfil de produção: IP público efêmero, subnets e SG fixos."""

    processor_container_name: str
    lease_seconds: int
    subnet_ids: tuple[str, ...]
    security_group_id: str


def _error_code(error: ClientError) -> str:
    return cast("dict[str, dict[str, str]]", error.response)["Error"]["Code"]


def _payload(request: StartRunExecution) -> str:
    StartRunExecution.model_validate(request.model_dump())
    return json.dumps(
        {
            "tenant_id": request.tenant_id,
            "run_id": request.run_id,
            "wave_id": request.wave_id,
            "dispatch_id": request.dispatch_id,
            "unit_ids": list(request.unit_ids),
            "max_concurrency": request.max_concurrency,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _describe_state_machine(client: BaseClient, state_machine_arn: str) -> dict[str, Any]:
    try:
        sfn = cast("_StepFunctionsClient", client)
        return sfn.describe_state_machine(stateMachineArn=state_machine_arn)
    except ClientError as error:
        raise ProcessorExecutionUnavailable(_error_code(error)) from error


def _start_state_of_type(
    machine: dict[str, Any], state_type: str,
) -> tuple[dict[str, Any] | None, int]:
    states = machine.get("States", {})
    matches = [name for name, state in states.items() if state.get("Type") == state_type]
    start = states.get(machine.get("StartAt")) if matches == [machine.get("StartAt")] else None
    return start, len(matches)


def _validate_failures_propagate(state: dict[str, Any]) -> None:
    if any(field in state for field in _FAILURE_ABSORBING_FIELDS):
        raise IncompatibleStateMachine("unit_failures_must_propagate")


def _validate_map_items(run_map: dict[str, Any]) -> None:
    if run_map.get("ItemsPath") != "$.unit_ids":
        raise IncompatibleStateMachine("map_items_must_be_unit_ids")
    if run_map.get("ItemSelector") != _ITEM_SELECTOR:
        raise IncompatibleStateMachine("map_item_selector_mismatch")


def _inline_map(definition: dict[str, Any]) -> dict[str, Any]:
    run_map, count = _start_state_of_type(definition, "Map")
    if count != 1:
        raise IncompatibleStateMachine("single_map_required")
    if run_map is None:
        raise IncompatibleStateMachine("map_must_be_start_state")
    _validate_failures_propagate(run_map)
    _validate_map_items(run_map)
    processor = run_map.get("ItemProcessor", {})
    if processor.get("ProcessorConfig", {}).get("Mode") != "INLINE":
        raise IncompatibleStateMachine("map_must_be_inline")
    if run_map.get("MaxConcurrencyPath") != "$.max_concurrency":
        raise IncompatibleStateMachine("map_concurrency_must_be_explicit")
    return processor


def _ecs_parameters(processor: dict[str, Any]) -> dict[str, Any]:
    task, count = _start_state_of_type(processor, "Task")
    if count != 1 or task is None or task.get("Resource") != _ECS_RUN_TASK_SYNC:
        raise IncompatibleStateMachine("single_ecs_sync_task_required")
    _validate_failures_propagate(task)
    parameters = task.get("Parameters", {})
    if parameters.get("LaunchType") != "FARGATE":
        raise IncompatibleStateMachine("launch_type_must_be_fargate")
    if not parameters.get("TaskDefinition"):
        raise IncompatibleStateMachine("task_definition_required")
    return parameters


def _awsvpc_configuration(parameters: dict[str, Any]) -> dict[str, Any]:
    network = parameters.get("NetworkConfiguration", {}).get("AwsvpcConfiguration", {})
    if not network.get("Subnets"):
        raise IncompatibleStateMachine("subnets_required")
    return network


def _validate_network(parameters: dict[str, Any]) -> None:
    if _awsvpc_configuration(parameters).get("AssignPublicIp") != _ASSIGN_PUBLIC_IP:
        raise IncompatibleStateMachine("assign_public_ip_mismatch")


def _validate_production_network(
    parameters: dict[str, Any], policy: ProductionNetworkPolicy,
) -> None:
    network = _awsvpc_configuration(parameters)
    if network.get("AssignPublicIp") != _PRODUCTION_ASSIGN_PUBLIC_IP:
        raise IncompatibleStateMachine("assign_public_ip_mismatch")
    if network.get("Subnets") != list(policy.subnet_ids):
        raise IncompatibleStateMachine("subnets_mismatch")
    if network.get("SecurityGroups") != [policy.security_group_id]:
        raise IncompatibleStateMachine("security_groups_mismatch")
    if network.keys() != _PRODUCTION_AWSVPC_KEYS:
        raise IncompatibleStateMachine("network_parameter_forbidden")
    if parameters["NetworkConfiguration"].keys() != _PRODUCTION_NETWORK_KEYS:
        raise IncompatibleStateMachine("network_parameter_forbidden")


def _validate_production_parameters(parameters: dict[str, Any]) -> None:
    if parameters.keys() - _PRODUCTION_ECS_PARAMETERS:
        raise IncompatibleStateMachine("ecs_parameter_forbidden")
    overrides = parameters.get("Overrides", {})
    container_overrides = overrides.get("ContainerOverrides", [])
    if overrides.keys() - _PRODUCTION_OVERRIDE_KEYS or len(container_overrides) > 1:
        raise IncompatibleStateMachine("ecs_override_forbidden")


def _validate_production_topology(definition: dict[str, Any]) -> None:
    run_map = definition["States"][definition["StartAt"]]
    processor = run_map["ItemProcessor"]
    task = processor["States"][processor["StartAt"]]
    single = len(definition["States"]) == 1 and len(processor["States"]) == 1
    if not single or run_map.get("End") is not True or task.get("End") is not True:
        raise IncompatibleStateMachine("production_topology_mismatch")


def _container_environment(
    parameters: dict[str, Any], container_name: str,
) -> dict[str, dict[str, Any]]:
    overrides = parameters.get("Overrides", {}).get("ContainerOverrides", [])
    matches = [override for override in overrides if override.get("Name") == container_name]
    if len(matches) != 1:
        raise IncompatibleStateMachine("processor_container_override_missing")
    if matches[0].keys() - {"Name", "Environment"}:
        raise IncompatibleStateMachine("container_override_must_only_set_environment")
    variables = matches[0].get("Environment", [])
    names = [variable.get("Name") for variable in variables]
    if len(set(names)) != len(names):
        raise IncompatibleStateMachine("duplicate_environment_variable")
    return {
        variable.get("Name"): {key: value for key, value in variable.items() if key != "Name"}
        for variable in variables
    }


def _validate_environment(environment: dict[str, dict[str, Any]], lease_seconds: int) -> None:
    lease = {"Value": str(lease_seconds)}
    if environment.get("LEASE_SECONDS") != lease:
        raise IncompatibleStateMachine("lease_seconds_mismatch")
    expected = {name: {"Value.$": path} for name, path in _ENVIRONMENT_PATHS.items()}
    if environment != {**expected, "LEASE_SECONDS": lease}:
        raise IncompatibleStateMachine("environment_bindings_mismatch")


def _validate_processor(
    parameters: dict[str, Any], container_name: str, lease_seconds: int,
) -> None:
    _validate_environment(_container_environment(parameters, container_name), lease_seconds)


def _standard_definition(client: BaseClient, state_machine_arn: str) -> dict[str, Any]:
    described = _describe_state_machine(client, state_machine_arn)
    if described["type"] != "STANDARD":
        raise IncompatibleStateMachine("workflow_must_be_standard")
    return json.loads(described["definition"])


def validate_state_machine(
    client: BaseClient,
    state_machine_arn: str,
    processor_container_name: str,
    lease_seconds: int,
) -> None:
    """Valida a state machine contra o contrato Standard + Inline Map + ECS Fargate.

    Raises:
        IncompatibleStateMachine: definição fora do contrato.
        ProcessorExecutionUnavailable: falha AWS ao descrever a state machine.
    """
    parameters = _ecs_parameters(_inline_map(_standard_definition(client, state_machine_arn)))
    _validate_network(parameters)
    _validate_processor(parameters, processor_container_name, lease_seconds)


def validate_production_state_machine(
    client: BaseClient, state_machine_arn: str, policy: ProductionNetworkPolicy,
) -> None:
    """Valida o contrato genérico com o override de rede exato do perfil de produção.

    Raises:
        IncompatibleStateMachine: definição fora do contrato de produção.
        ProcessorExecutionUnavailable: falha AWS ao descrever a state machine.
    """
    definition = _standard_definition(client, state_machine_arn)
    parameters = _ecs_parameters(_inline_map(definition))
    _validate_production_topology(definition)
    _validate_production_parameters(parameters)
    _validate_production_network(parameters, policy)
    _validate_processor(parameters, policy.processor_container_name, policy.lease_seconds)


def _require_state_machine_arn(state_machine_arn: str) -> str:
    if ":stateMachine:" not in state_machine_arn:
        raise ValueError("state_machine_arn=invalid")
    return state_machine_arn


def _execution_arn(state_machine_arn: str, execution_name: str) -> str:
    prefix, _, machine = state_machine_arn.rpartition(":stateMachine:")
    return f"{prefix}:execution:{machine}:{execution_name}"


def _execution_status(state: str) -> ExecutionStatus:
    if state not in _STATUS:
        raise ValueError(f"execution_status={state}")
    return _STATUS[state]


class StepFunctionsExecutor:
    """Delega a execução de um dispatch a uma state machine do Step Functions."""

    def __init__(self, client: BaseClient, state_machine_arn: str) -> None:
        self._client = cast("_StepFunctionsClient", client)
        self._state_machine_arn = _require_state_machine_arn(state_machine_arn)

    def start(self, request: StartRunExecution) -> str:
        """Inicia a execução; reaproveita a existente em replay do mesmo dispatch."""
        payload = _payload(request)
        try:
            response = self._client.start_execution(
                stateMachineArn=self._state_machine_arn,
                name=request.dispatch_id,
                input=payload,
            )
        except ClientError as error:
            if _error_code(error) != "ExecutionAlreadyExists":
                raise ProcessorExecutionUnavailable(_error_code(error)) from error
            return self._confirm_existing(request.dispatch_id, payload)
        return response["executionArn"]

    def cancel(self, request: CancelRunExecution) -> None:
        """Solicita a interrupção da execução; não acessa a AWS sem um ref."""
        if request.execution_ref is None:
            return
        cause = f"tenant_id={request.tenant_id} run_id={request.run_id}"
        try:
            self._client.stop_execution(executionArn=request.execution_ref, cause=cause)
        except ClientError as error:
            raise ProcessorExecutionUnavailable(_error_code(error)) from error

    def status(self, execution_ref: str) -> ExecutionStatus:
        """Traduz o status da execução para `ExecutionStatus`, falhando fechado."""
        return _execution_status(self._describe_execution(execution_ref)["status"])

    def _describe_execution(self, execution_ref: str) -> dict[str, Any]:
        try:
            return self._client.describe_execution(executionArn=execution_ref)
        except ClientError as error:
            raise ProcessorExecutionUnavailable(_error_code(error)) from error

    def _confirm_existing(self, execution_name: str, payload: str) -> str:
        execution_ref = _execution_arn(self._state_machine_arn, execution_name)
        if self._describe_execution(execution_ref).get("input") != payload:
            raise ProcessorExecutionUnavailable("execution_name_conflict")
        return execution_ref


def _require_unqualified_state_machine_arn(state_machine_arn: str) -> str:
    _, _, machine = _require_state_machine_arn(state_machine_arn).rpartition(":stateMachine:")
    # A version/alias ARN derives an execution ARN that never exists: a false absence proof.
    if ":" in machine:
        raise ValueError("state_machine_arn=qualified")
    return state_machine_arn


class StepFunctionsExecutionProbe:
    """`ExecutionProbePort` da execução Step Functions nomeada pelo dispatch_id."""

    def __init__(self, client: BaseClient, state_machine_arn: str) -> None:
        self._client = cast("_StepFunctionsClient", client)
        self._state_machine_arn = _require_unqualified_state_machine_arn(state_machine_arn)

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        """Classifica a execução do dispatch; só `ExecutionDoesNotExist` vira ABSENT.

        Raises:
            ValueError: dispatch_id fora de hex16 ou status desconhecido.
            ProcessorExecutionUnavailable: outra falha AWS ao descrever a execução.
        """
        execution_arn = _execution_arn(
            self._state_machine_arn, require_dispatch_id(dispatch_id, "dispatch_id"),
        )
        try:
            described = self._client.describe_execution(executionArn=execution_arn)
        except ClientError as error:
            if _error_code(error) != "ExecutionDoesNotExist":
                raise ProcessorExecutionUnavailable(_error_code(error)) from error
            return ExecutionObservation(ExecutionPresence.ABSENT, None)
        presence = _PRESENCE[_execution_status(described["status"])]
        return ExecutionObservation(presence, execution_arn)
