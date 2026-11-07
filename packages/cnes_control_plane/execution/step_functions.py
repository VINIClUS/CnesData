"""Adapter AWS Step Functions do executor do processador."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol, cast

from botocore.exceptions import ClientError

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
_ECS_RUN_TASK_SYNC = "arn:aws:states:::ecs:runTask.sync"
_ASSIGN_PUBLIC_IP = "DISABLED"
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


def _validate_network(parameters: dict[str, Any]) -> None:
    network = parameters.get("NetworkConfiguration", {}).get("AwsvpcConfiguration", {})
    if not network.get("Subnets"):
        raise IncompatibleStateMachine("subnets_required")
    if network.get("AssignPublicIp") != _ASSIGN_PUBLIC_IP:
        raise IncompatibleStateMachine("assign_public_ip_mismatch")


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
    described = _describe_state_machine(client, state_machine_arn)
    if described["type"] != "STANDARD":
        raise IncompatibleStateMachine("workflow_must_be_standard")
    parameters = _ecs_parameters(_inline_map(json.loads(described["definition"])))
    _validate_network(parameters)
    environment = _container_environment(parameters, processor_container_name)
    _validate_environment(environment, lease_seconds)


class StepFunctionsExecutor:
    """Delega a execução de um dispatch a uma state machine do Step Functions."""

    def __init__(self, client: BaseClient, state_machine_arn: str) -> None:
        if ":stateMachine:" not in state_machine_arn:
            raise ValueError("state_machine_arn=invalid")
        self._client = cast("_StepFunctionsClient", client)
        self._state_machine_arn = state_machine_arn

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
        state = self._describe_execution(execution_ref)["status"]
        if state not in _STATUS:
            raise ValueError(f"execution_status={state}")
        return _STATUS[state]

    def _describe_execution(self, execution_ref: str) -> dict[str, Any]:
        try:
            return self._client.describe_execution(executionArn=execution_ref)
        except ClientError as error:
            raise ProcessorExecutionUnavailable(_error_code(error)) from error

    def _confirm_existing(self, execution_name: str, payload: str) -> str:
        execution_ref = self._existing_execution_arn(execution_name)
        if self._describe_execution(execution_ref).get("input") != payload:
            raise ProcessorExecutionUnavailable("execution_name_conflict")
        return execution_ref

    def _existing_execution_arn(self, execution_name: str) -> str:
        prefix, _, machine = self._state_machine_arn.rpartition(":stateMachine:")
        return f"{prefix}:execution:{machine}:{execution_name}"
