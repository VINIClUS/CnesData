"""Probe da execução Step Functions nomeada pelo dispatch_id, sem dependência de pydantic."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol, cast

from botocore.exceptions import ClientError

from cnes_domain.control_plane.dispatch_identity import request_json_sha256, require_dispatch_id
from cnes_domain.ports.environment_gate import ExecutionObservation, ExecutionPresence
from cnes_domain.ports.execution_status import ExecutionStatus

if TYPE_CHECKING:
    from botocore.client import BaseClient

    class StepFunctionsClient(Protocol):
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


class ProcessorExecutionUnavailable(Exception):
    """Falha AWS normalizada ou conflito de nome de execução."""


def error_code(error: ClientError) -> str:
    """Returns: o código AWS do `ClientError`."""
    return cast("dict[str, dict[str, str]]", error.response)["Error"]["Code"]


def require_state_machine_arn(state_machine_arn: str) -> str:
    """Raises: ValueError: ARN sem `:stateMachine:`."""
    if ":stateMachine:" not in state_machine_arn:
        raise ValueError("state_machine_arn=invalid")
    return state_machine_arn


def execution_arn(state_machine_arn: str, execution_name: str) -> str:
    """Returns: o ARN da execução nomeada na state machine."""
    prefix, _, machine = state_machine_arn.rpartition(":stateMachine:")
    return f"{prefix}:execution:{machine}:{execution_name}"


def execution_status(state: str) -> ExecutionStatus:
    """Raises: ValueError: status AWS desconhecido."""
    if state not in _STATUS:
        raise ValueError(f"execution_status={state}")
    return _STATUS[state]


def require_unqualified_state_machine_arn(state_machine_arn: str) -> str:
    """Raises: ValueError: ARN inválido ou qualificado por versão/alias."""
    _, _, machine = require_state_machine_arn(state_machine_arn).rpartition(":stateMachine:")
    # A version/alias ARN derives an execution ARN that never exists: a false absence proof.
    if ":" in machine:
        raise ValueError("state_machine_arn=qualified")
    return state_machine_arn


class StepFunctionsExecutionProbe:
    """`ExecutionProbePort` da execução Step Functions nomeada pelo dispatch_id."""

    def __init__(self, client: BaseClient, state_machine_arn: str) -> None:
        self._client = cast("StepFunctionsClient", client)
        self._state_machine_arn = require_unqualified_state_machine_arn(state_machine_arn)

    def probe(self, dispatch_id: str) -> ExecutionObservation:
        """Classifica a execução do dispatch; só `ExecutionDoesNotExist` vira ABSENT.

        Raises:
            ValueError: dispatch_id fora de hex16 ou status desconhecido.
            ProcessorExecutionUnavailable: outra falha AWS ao descrever a execução.
        """
        arn = execution_arn(
            self._state_machine_arn,
            require_dispatch_id(dispatch_id, "dispatch_id"),
        )
        try:
            described = self._client.describe_execution(executionArn=arn)
        except ClientError as error:
            if error_code(error) != "ExecutionDoesNotExist":
                raise ProcessorExecutionUnavailable(error_code(error)) from error
            evidence = json.dumps(
                {"executionArn": arn, "error": "ExecutionDoesNotExist"},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            return ExecutionObservation(ExecutionPresence.ABSENT, None, evidence=evidence)
        presence = _PRESENCE[execution_status(described["status"])]
        original = {
            key: described[key] for key in ("executionArn", "status", "input") if key in described
        }
        evidence = json.dumps(original, sort_keys=True, separators=(",", ":")).encode()
        digest = (
            request_json_sha256(json.loads(described["input"])) if "input" in described else None
        )
        return ExecutionObservation(presence, arn, digest, evidence)
