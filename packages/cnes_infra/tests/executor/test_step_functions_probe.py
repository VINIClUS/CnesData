"""Probe proven-absent da execução Step Functions nomeada pelo dispatch_id."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
    EndpointConnectionError,
    ReadTimeoutError,
)
from botocore.stub import Stubber

from cnes_domain.ports.environment_gate import (
    ExecutionObservation,
    ExecutionPresence,
    ExecutionProbePort,
)
from cnes_infra.executor.step_functions import (
    ProcessorExecutionUnavailable,
    StepFunctionsExecutionProbe,
)

_STATE_MACHINE_ARN = "arn:aws:states:us-east-2:000000000000:stateMachine:cnesdata-prod"
_DISPATCH_ID = "0123456789abcdef"
_EXECUTION_ARN = "arn:aws:states:us-east-2:000000000000:execution:cnesdata-prod:0123456789abcdef"
_ENDPOINT = "https://states.us-east-2.amazonaws.com"
_NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)


def _client() -> Any:
    return boto3.client(
        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED),
    )


def _probe(client: Any) -> StepFunctionsExecutionProbe:
    return StepFunctionsExecutionProbe(client, _STATE_MACHINE_ARN)


def _observe(status: str) -> ExecutionObservation:
    client = _client()
    with Stubber(client) as stubber:
        stubber.add_response(
            "describe_execution",
            {
                "executionArn": _EXECUTION_ARN,
                "stateMachineArn": _STATE_MACHINE_ARN,
                "status": status,
                "startDate": _NOW,
            },
            {"executionArn": _EXECUTION_ARN},
        )
        observation = _probe(client).probe(_DISPATCH_ID)
        stubber.assert_no_pending_responses()
    return observation


def _client_failing_with(code: str) -> Any:
    client = _client()
    stubber = Stubber(client)
    stubber.add_client_error(
        "describe_execution",
        service_error_code=code,
        http_status_code=400,
        expected_params={"executionArn": _EXECUTION_ARN},
    )
    stubber.activate()
    return client


def test_implementa_execution_probe_port() -> None:
    assert isinstance(_probe(MagicMock()), ExecutionProbePort)


def test_execucao_inexistente_prova_ausencia() -> None:
    observation = _probe(_client_failing_with("ExecutionDoesNotExist")).probe(_DISPATCH_ID)

    assert observation == ExecutionObservation(ExecutionPresence.ABSENT, None)


@pytest.mark.parametrize(
    "status", ["RUNNING", "PENDING_REDRIVE"], ids=["executando", "redrive_pendente"],
)
def test_execucao_viva_ou_em_redrive_e_running_com_execution_ref(status: str) -> None:
    assert _observe(status) == ExecutionObservation(ExecutionPresence.RUNNING, _EXECUTION_ARN)


@pytest.mark.parametrize(
    "status",
    ["SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"],
    ids=["sucesso", "falha", "expirada", "abortada"],
)
def test_status_terminal_e_terminal_com_execution_ref(status: str) -> None:
    assert _observe(status) == ExecutionObservation(ExecutionPresence.TERMINAL, _EXECUTION_ARN)


def test_status_desconhecido_falha_fechado_sem_terminal_nem_ausencia() -> None:
    client = MagicMock()
    client.describe_execution.return_value = {"status": "PAUSED"}

    with pytest.raises(ValueError, match="execution_status=PAUSED"):
        _probe(client).probe(_DISPATCH_ID)

    client.describe_execution.assert_called_once_with(executionArn=_EXECUTION_ARN)


@pytest.mark.parametrize(
    "code",
    ["ThrottlingException", "AccessDeniedException", "InvalidArn", "KmsThrottlingException"],
    ids=["throttling", "acesso_negado", "arn_invalido", "kms"],
)
def test_outro_erro_do_cliente_nunca_prova_ausencia(code: str) -> None:
    probe = _probe(_client_failing_with(code))

    with pytest.raises(ProcessorExecutionUnavailable, match=code) as raised:
        probe.probe(_DISPATCH_ID)

    assert isinstance(raised.value.__cause__, ClientError)


@pytest.mark.parametrize(
    "error",
    [ReadTimeoutError(endpoint_url=_ENDPOINT), EndpointConnectionError(endpoint_url=_ENDPOINT)],
    ids=["timeout", "sem_conexao"],
)
def test_erro_ambiguo_de_transporte_propaga_sem_provar_ausencia(error: BotoCoreError) -> None:
    client = MagicMock()
    client.describe_execution.side_effect = error

    with pytest.raises(BotoCoreError) as raised:
        _probe(client).probe(_DISPATCH_ID)

    assert raised.value is error


@pytest.mark.parametrize(
    "dispatch_id",
    [
        "0123456789ABCDEF",
        "0123456789abcde",
        "0123456789abcdef0",
        "0123456789abcdeg",
        "",
        "0123456789abcdef\n",
        "outra:0123456789abcdef",
    ],
    ids=["maiusculo", "curto", "longo", "nao_hex", "vazio", "quebra_de_linha", "segmento_de_arn"],
)
def test_dispatch_id_invalido_nao_acessa_aws(dispatch_id: str) -> None:
    client = MagicMock()

    with pytest.raises(ValueError, match="invalid_dispatch_id"):
        _probe(client).probe(dispatch_id)

    assert client.mock_calls == []


def test_rejeita_state_machine_arn_invalido() -> None:
    activity_arn = "arn:aws:states:us-east-2:000000000000:activity:cnesdata-prod"

    with pytest.raises(ValueError, match="state_machine_arn=invalid"):
        StepFunctionsExecutionProbe(MagicMock(), activity_arn)


@pytest.mark.parametrize("qualifier", ["1", "live"], ids=["versao", "alias"])
def test_rejeita_arn_qualificado_que_derivaria_execucao_inexistente(qualifier: str) -> None:
    with pytest.raises(ValueError, match="state_machine_arn=qualified"):
        StepFunctionsExecutionProbe(MagicMock(), f"{_STATE_MACHINE_ARN}:{qualifier}")
