"""Contrato do adapter Step Functions do executor do processador."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionStatus,
    ProcessorExecutorPort,
    StartRunExecution,
)
from cnes_infra.executor.step_functions import StepFunctionsExecutor

_STATE_MACHINE_ARN = "arn:aws:states:us-east-1:1:stateMachine:cnes"
_EXECUTION_ARN = "arn:aws:states:us-east-1:1:execution:cnes:fedcba9876543210"
_NOW = datetime(2026, 7, 15, 12, tzinfo=UTC)


def _client() -> Any:
    return boto3.client(
        "stepfunctions",
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED),
    )


def _request(**overrides: Any) -> StartRunExecution:
    fields: dict[str, Any] = {
        "tenant_id": "354130",
        "run_id": "run-1",
        "wave_id": "0123456789abcdef",
        "dispatch_id": "fedcba9876543210",
        "unit_ids": ("unit-1",),
        "max_concurrency": 4,
    }
    fields.update(overrides)
    return StartRunExecution(**fields)


def _expected_payload(request: StartRunExecution) -> str:
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


def _start_params(request: StartRunExecution) -> dict[str, Any]:
    return {
        "stateMachineArn": _STATE_MACHINE_ARN,
        "name": request.dispatch_id,
        "input": _expected_payload(request),
    }


class _DescribeStub:
    """Cliente falso que devolve um status arbitrário, sem validação do modelo."""

    def __init__(self, status: str) -> None:
        self._status = status

    def describe_execution(self, **kwargs: Any) -> dict[str, str]:
        return {"status": self._status}


def test_step_functions_envia_ids_e_max_concurrency() -> None:
    client = _client()
    request = _request()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_response(
            "start_execution",
            {"executionArn": _EXECUTION_ARN, "startDate": _NOW},
            _start_params(request),
        )
        ref = executor.start(request)
        stubber.assert_no_pending_responses()

    assert ref == _EXECUTION_ARN


def test_replay_da_mesma_dispatch_retorna_execution_ref_existente() -> None:
    client = _client()
    request = _request()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "start_execution",
            service_error_code="ExecutionAlreadyExists",
            service_message="Execution Already Exists",
            http_status_code=400,
            expected_params=_start_params(request),
        )
        ref = executor.start(request)
        stubber.assert_no_pending_responses()

    assert ref == _EXECUTION_ARN


def test_propaga_erro_desconhecido_do_start() -> None:
    client = _client()
    request = _request()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "start_execution",
            service_error_code="ThrottlingException",
            http_status_code=429,
            expected_params=_start_params(request),
        )
        with pytest.raises(ClientError, match="ThrottlingException"):
            executor.start(request)
        stubber.assert_no_pending_responses()


@pytest.mark.parametrize(
    ("service_status", "expected"),
    [
        ("RUNNING", ExecutionStatus.RUNNING),
        ("PENDING_REDRIVE", ExecutionStatus.RUNNING),
        ("SUCCEEDED", ExecutionStatus.SUCCEEDED),
        ("FAILED", ExecutionStatus.FAILED),
        ("TIMED_OUT", ExecutionStatus.FAILED),
        ("ABORTED", ExecutionStatus.CANCELED),
    ],
    ids=["executando", "redrive_pendente", "sucesso", "falha", "expirado", "cancelado"],
)
def test_status_mapeia_estado_terminal(service_status: str, expected: ExecutionStatus) -> None:
    client = _client()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_response(
            "describe_execution",
            {
                "executionArn": _EXECUTION_ARN,
                "stateMachineArn": _STATE_MACHINE_ARN,
                "status": service_status,
                "startDate": _NOW,
            },
            {"executionArn": _EXECUTION_ARN},
        )
        assert executor.status(_EXECUTION_ARN) is expected
        stubber.assert_no_pending_responses()


def test_rejeita_status_desconhecido() -> None:
    executor = StepFunctionsExecutor(_DescribeStub("UNKNOWN"), _STATE_MACHINE_ARN)

    with pytest.raises(ValueError, match="execution_status=UNKNOWN"):
        executor.status(_EXECUTION_ARN)


def test_rejeita_state_machine_arn_invalido() -> None:
    with pytest.raises(ValueError, match="state_machine_arn=invalid"):
        StepFunctionsExecutor(MagicMock(), "arn:aws:states:us-east-1:1:activity:cnes")


def test_cancel_chama_stop_execution() -> None:
    client = _client()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_response(
            "stop_execution",
            {"stopDate": _NOW},
            {
                "executionArn": _EXECUTION_ARN,
                "cause": "tenant_id=354130 run_id=run-1",
            },
        )
        executor.cancel(
            CancelRunExecution(tenant_id="354130", run_id="run-1", execution_ref=_EXECUTION_ARN)
        )
        stubber.assert_no_pending_responses()


def test_cancel_sem_execution_ref_nao_acessa_aws() -> None:
    client = MagicMock()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    executor.cancel(CancelRunExecution(tenant_id="354130", run_id="run-1", execution_ref=None))

    assert client.mock_calls == []


def test_implementa_processor_executor_port() -> None:
    executor = StepFunctionsExecutor(MagicMock(), _STATE_MACHINE_ARN)

    assert isinstance(executor, ProcessorExecutorPort)
