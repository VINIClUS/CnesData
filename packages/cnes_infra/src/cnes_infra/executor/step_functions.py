"""Adapter AWS Step Functions do executor do processador."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from botocore.exceptions import ClientError

from cnes_domain.ports.processing import ExecutionStatus

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from cnes_domain.ports.processing import CancelRunExecution, StartRunExecution

_STATUS = {
    "RUNNING": ExecutionStatus.RUNNING,
    "PENDING_REDRIVE": ExecutionStatus.RUNNING,
    "SUCCEEDED": ExecutionStatus.SUCCEEDED,
    "FAILED": ExecutionStatus.FAILED,
    "TIMED_OUT": ExecutionStatus.FAILED,
    "ABORTED": ExecutionStatus.CANCELED,
}


def _error_code(error: ClientError) -> str:
    return error.response["Error"]["Code"]


def _payload(request: StartRunExecution) -> str:
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


class StepFunctionsExecutor:
    """Delega a execução de um dispatch a uma state machine do Step Functions."""

    def __init__(self, client: BaseClient, state_machine_arn: str) -> None:
        if ":stateMachine:" not in state_machine_arn:
            raise ValueError("state_machine_arn=invalid")
        self._client = client
        self._state_machine_arn = state_machine_arn

    def start(self, request: StartRunExecution) -> str:
        """Inicia a execução; reaproveita a existente em replay do mesmo dispatch."""
        try:
            response = self._client.start_execution(
                stateMachineArn=self._state_machine_arn,
                name=request.dispatch_id,
                input=_payload(request),
            )
        except ClientError as error:
            if _error_code(error) != "ExecutionAlreadyExists":
                raise
            return self._existing_execution_arn(request.dispatch_id)
        return response["executionArn"]

    def cancel(self, request: CancelRunExecution) -> None:
        """Solicita a interrupção da execução; não acessa a AWS sem um ref."""
        if request.execution_ref is None:
            return
        cause = f"tenant_id={request.tenant_id} run_id={request.run_id}"
        self._client.stop_execution(executionArn=request.execution_ref, cause=cause)

    def status(self, execution_ref: str) -> ExecutionStatus:
        """Traduz o status da execução para `ExecutionStatus`, falhando fechado."""
        response = self._client.describe_execution(executionArn=execution_ref)
        state = response["status"]
        if state not in _STATUS:
            raise ValueError(f"execution_status={state}")
        return _STATUS[state]

    def _existing_execution_arn(self, execution_name: str) -> str:
        prefix, _, machine = self._state_machine_arn.rpartition(":stateMachine:")
        return f"{prefix}:execution:{machine}:{execution_name}"
