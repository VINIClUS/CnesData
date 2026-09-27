"""Contrato do adapter Step Functions do executor do processador."""

from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_domain.ports.processing import (
    CancelRunExecution,
    ExecutionStatus,
    ProcessorExecutorPort,
    StartRunExecution,
)
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    ProcessorExecutionUnavailable,
    StepFunctionsExecutor,
    validate_state_machine,
)

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "step_functions"
_ECS_SYNC = "arn:aws:states:::ecs:runTask.sync"
_ECS_PARAMETERS = ("States", "RunUnits", "ItemProcessor", "States", "RunProcessor", "Parameters")
_ITEM_STATES = ("States", "RunUnits", "ItemProcessor", "States")
_NETWORK = (*_ECS_PARAMETERS, "NetworkConfiguration", "AwsvpcConfiguration")
_CONTAINER = (*_ECS_PARAMETERS, "Overrides", "ContainerOverrides", 0)
_ENVIRONMENT = (*_CONTAINER, "Environment")
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


def _describe_execution_response(status: str, execution_input: str) -> dict[str, Any]:
    return {
        "executionArn": _EXECUTION_ARN,
        "stateMachineArn": _STATE_MACHINE_ARN,
        "status": status,
        "startDate": _NOW,
        "input": execution_input,
    }


def _definition(fixture_name: str) -> dict[str, Any]:
    return json.loads((_FIXTURES / fixture_name).read_text(encoding="utf-8"))


def _mutate(definition: dict[str, Any], path: tuple[Any, ...], value: Any) -> dict[str, Any]:
    mutated = copy.deepcopy(definition)
    parent = mutated
    for key in path[:-1]:
        parent = parent[key]
    if value is None:
        del parent[path[-1]]
    else:
        parent[path[-1]] = value
    return mutated


def _leaf_diff(left: Any, right: Any, pointer: str = "") -> list[str]:
    if isinstance(left, dict) and isinstance(right, dict):
        keys = sorted(left.keys() | right.keys())
        return [
            diff
            for key in keys
            for diff in _leaf_diff(left.get(key), right.get(key), f"{pointer}/{key}")
        ]
    return [] if left == right else [pointer]


def _client_for_definition(definition: dict[str, Any], workflow_type: str = "STANDARD") -> Any:
    client = _client()
    stubber = Stubber(client)
    stubber.add_response(
        "describe_state_machine",
        {
            "stateMachineArn": _STATE_MACHINE_ARN,
            "name": "cnes",
            "definition": json.dumps(definition),
            "roleArn": "arn:aws:iam::000000000000:role/cnes-states",
            "type": workflow_type,
            "creationDate": _NOW,
        },
        {"stateMachineArn": _STATE_MACHINE_ARN},
    )
    stubber.activate()
    return client


def _client_with_existing_execution(
    status: str = "FAILED", describe_error: str | None = None, existing_input: str | None = None,
) -> Any:
    request = _request()
    client = _client()
    stubber = Stubber(client)
    stubber.add_client_error(
        "start_execution",
        service_error_code="ExecutionAlreadyExists",
        http_status_code=400,
        expected_params=_start_params(request),
    )
    if describe_error is None:
        stubber.add_response(
            "describe_execution",
            _describe_execution_response(status, existing_input or _expected_payload(request)),
            {"executionArn": _EXECUTION_ARN},
        )
    else:
        stubber.add_client_error(
            "describe_execution",
            service_error_code=describe_error,
            http_status_code=400,
            expected_params={"executionArn": _EXECUTION_ARN},
        )
    stubber.activate()
    return client


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
        stubber.add_response(
            "describe_execution",
            _describe_execution_response("SUCCEEDED", _expected_payload(request)),
            {"executionArn": _EXECUTION_ARN},
        )
        ref = executor.start(request)
        stubber.assert_no_pending_responses()

    assert ref == _EXECUTION_ARN


def test_normaliza_erro_desconhecido_do_start() -> None:
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
        with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
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


def test_aceita_standard_inline_map_com_ecs_fargate() -> None:
    client = _client_for_definition(_definition("standard_inline_ecs.json"))

    validate_state_machine(client, _STATE_MACHINE_ARN, "processor", 300)


def test_fixture_distribuida_difere_so_no_modo_do_map() -> None:
    diff = _leaf_diff(
        _definition("standard_inline_ecs.json"), _definition("distributed_map.json"),
    )

    assert diff == ["/States/RunUnits/ItemProcessor/ProcessorConfig/Mode"]


@pytest.mark.parametrize(
    ("fixture_name", "workflow_type", "error"),
    [
        ("standard_inline_ecs.json", "EXPRESS", "workflow_must_be_standard"),
        ("distributed_map.json", "STANDARD", "map_must_be_inline"),
    ],
    ids=["express", "distribuido"],
)
def test_rejeita_workflow_incompativel(fixture_name: str, workflow_type: str, error: str) -> None:
    client = _client_for_definition(_definition(fixture_name), workflow_type)

    with pytest.raises(IncompatibleStateMachine, match=error):
        validate_state_machine(client, _STATE_MACHINE_ARN, "processor", 300)


@pytest.mark.parametrize(
    ("path", "value", "error"),
    [
        (("States", "RunUnits", "Type"), "Pass", "single_map_required"),
        (("States", "Extra"), {"Type": "Map"}, "single_map_required"),
        (("States", "RunUnits", "MaxConcurrencyPath"), None, "map_concurrency_must_be_explicit"),
        (("States", "RunUnits", "ItemProcessor", "ProcessorConfig"), None, "map_must_be_inline"),
        ((*_ECS_PARAMETERS[:-1], "Resource"), _ECS_SYNC[:-5], "single_ecs_sync_task_required"),
        (
            (*_ITEM_STATES, "Extra"),
            {"Type": "Task", "Resource": _ECS_SYNC},
            "single_ecs_sync_task_required",
        ),
        ((*_ECS_PARAMETERS, "LaunchType"), "EC2", "launch_type_must_be_fargate"),
        ((*_NETWORK, "AssignPublicIp"), "ENABLED", "assign_public_ip_mismatch"),
        ((*_CONTAINER, "Name"), "other", "processor_container_override_missing"),
        ((*_ENVIRONMENT, 6, "Value"), "600", "lease_seconds_mismatch"),
        ((*_ENVIRONMENT, 0, "Value.$"), "$.run_id", "environment_bindings_mismatch"),
        ((*_ENVIRONMENT, 5), {"Name": "OTHER", "Value": "x"}, "environment_bindings_mismatch"),
    ],
    ids=[
        "sem_map", "dois_maps", "concorrencia_implicita", "modo_implicito", "task_sem_sync",
        "duas_tasks", "ec2", "ip_publico", "container_errado", "lease_divergente",
        "binding_divergente", "variavel_faltante",
    ],
)
def test_rejeita_definicao_ecs_incompativel(path: tuple[Any, ...], value: Any, error: str) -> None:
    definition = _mutate(_definition("standard_inline_ecs.json"), path, value)
    client = _client_for_definition(definition)

    with pytest.raises(IncompatibleStateMachine, match=error):
        validate_state_machine(client, _STATE_MACHINE_ARN, "processor", 300)


def test_normaliza_erro_ao_descrever_state_machine() -> None:
    client = _client()

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "describe_state_machine",
            service_error_code="AccessDeniedException",
            http_status_code=400,
            expected_params={"stateMachineArn": _STATE_MACHINE_ARN},
        )
        with pytest.raises(ProcessorExecutionUnavailable, match="AccessDeniedException"):
            validate_state_machine(client, _STATE_MACHINE_ARN, "processor", 300)


def test_inicia_execucao_deterministica_com_ids_sem_dados() -> None:
    client = MagicMock()
    client.start_execution.return_value = {"executionArn": _EXECUTION_ARN}

    ref = StepFunctionsExecutor(client, _STATE_MACHINE_ARN).start(_request())

    kwargs = client.start_execution.call_args.kwargs
    assert ref == _EXECUTION_ARN
    assert kwargs["name"] == "fedcba9876543210"
    assert json.loads(kwargs["input"]) == {
        "tenant_id": "354130",
        "run_id": "run-1",
        "wave_id": "0123456789abcdef",
        "dispatch_id": "fedcba9876543210",
        "unit_ids": ["unit-1"],
        "max_concurrency": 4,
    }


def test_dispatch_novo_da_mesma_onda_abre_nova_execucao() -> None:
    client = MagicMock()
    client.start_execution.side_effect = [
        {"executionArn": "arn:execution:dispatch-a"},
        {"executionArn": "arn:execution:dispatch-b"},
    ]
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    first = executor.start(_request(dispatch_id="1111111111111111"))
    retry = executor.start(_request(dispatch_id="2222222222222222"))

    assert first != retry
    assert [call.kwargs["name"] for call in client.start_execution.call_args_list] == [
        "1111111111111111",
        "2222222222222222",
    ]


def test_replay_da_mesma_tentativa_e_idempotente() -> None:
    client = _client_with_existing_execution(status="FAILED")

    assert StepFunctionsExecutor(client, _STATE_MACHINE_ARN).start(_request()) == _EXECUTION_ARN


def test_rejeita_replay_com_input_divergente() -> None:
    client = _client_with_existing_execution(existing_input='{"tenant_id":"outro"}')

    with pytest.raises(ProcessorExecutionUnavailable, match="execution_name_conflict"):
        StepFunctionsExecutor(client, _STATE_MACHINE_ARN).start(_request())


def test_falha_ao_descrever_existente_e_normalizada() -> None:
    client = _client_with_existing_execution(describe_error="ThrottlingException")

    with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
        StepFunctionsExecutor(client, _STATE_MACHINE_ARN).start(_request())


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"wave_id": "onda-1"}, "invalid_wave_id"),
        ({"dispatch_id": "FEDCBA9876543210"}, "invalid_dispatch_id"),
        ({"unit_ids": ()}, "unit_ids_required"),
        ({"unit_ids": ("unit-1", "unit-1")}, "duplicate_unit_id"),
        ({"unit_ids": (" ",)}, "blank_value"),
        ({"max_concurrency": 0}, "positive_value_required"),
        ({"max_concurrency": -1}, "positive_value_required"),
    ],
    ids=[
        "wave_invalida", "dispatch_invalido", "sem_unidades", "unidade_duplicada",
        "unidade_vazia", "concorrencia_zero", "concorrencia_negativa",
    ],
)
def test_rejeita_payload_invalido_sem_acessar_aws(overrides: dict[str, Any], error: str) -> None:
    client = MagicMock()
    fields = _request().model_dump()
    fields.update(overrides)
    request = StartRunExecution.model_construct(**fields)

    with pytest.raises(ValueError, match=error):
        StepFunctionsExecutor(client, _STATE_MACHINE_ARN).start(request)

    assert client.mock_calls == []


def test_normaliza_erro_do_cancel() -> None:
    client = _client()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_client_error("stop_execution", service_error_code="ExecutionDoesNotExist")
        with pytest.raises(ProcessorExecutionUnavailable, match="ExecutionDoesNotExist"):
            executor.cancel(CancelRunExecution(
                tenant_id="354130", run_id="run-1", execution_ref=_EXECUTION_ARN,
            ))


def test_normaliza_erro_do_status() -> None:
    client = _client()
    executor = StepFunctionsExecutor(client, _STATE_MACHINE_ARN)

    with Stubber(client) as stubber:
        stubber.add_client_error("describe_execution", service_error_code="ThrottlingException")
        with pytest.raises(ProcessorExecutionUnavailable, match="ThrottlingException"):
            executor.status(_EXECUTION_ARN)
