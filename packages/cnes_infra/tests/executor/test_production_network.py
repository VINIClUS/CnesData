"""Override de rede de produção do AWS-012: IP público só com rede exata."""

from __future__ import annotations

import copy
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore import UNSIGNED
from botocore.config import Config
from botocore.stub import Stubber

from cnes_infra.aws.settings import AwsRuntimeConfigurationError, AwsRuntimeSettings
from cnes_infra.executor.step_functions import (
    IncompatibleStateMachine,
    ProcessorExecutionUnavailable,
    ProductionNetworkPolicy,
    validate_production_state_machine,
    validate_state_machine,
)
from packages.cnes_infra.tests.aws.production_values import (
    PRODUCTION_SECURITY_GROUP_ID,
    PRODUCTION_STATE_MACHINE_ARN,
    PRODUCTION_SUBNET_IDS,
    PRODUCTION_VALUES,
)

_FIXTURES = Path(__file__).parents[1] / "fixtures" / "step_functions"
_PRODUCTION_FIXTURE = "production_public_ip.json"
_GENERIC_FIXTURE = "standard_inline_ecs.json"
_NOW = datetime(2026, 10, 11, 12, tzinfo=UTC)
_FIRST_SUBNET, _SECOND_SUBNET = PRODUCTION_SUBNET_IDS
_EXTRA_SUBNET = "subnet-00000000000000003"
_OTHER_SECURITY_GROUP = "sg-00000000000000002"
_OTHER_ROLE = "arn:aws:iam::000000000000:role/other"
_POLICY = ProductionNetworkPolicy(
    "processor", 3600, PRODUCTION_SUBNET_IDS, PRODUCTION_SECURITY_GROUP_ID,
)
_ALLOWED_PARAMETERS = (
    "Cluster", "TaskDefinition", "LaunchType", "NetworkConfiguration", "Overrides",
)
_MAP = ("States", "RunUnits")
_ITEM_STATES = (*_MAP, "ItemProcessor", "States")
_TASK = (*_ITEM_STATES, "RunProcessor")
_ECS_PARAMETERS = (*_TASK, "Parameters")
_NETWORK_CONFIGURATION = (*_ECS_PARAMETERS, "NetworkConfiguration")
_NETWORK = (*_NETWORK_CONFIGURATION, "AwsvpcConfiguration")
_OVERRIDES = (*_ECS_PARAMETERS, "Overrides")
_CONTAINER = (*_OVERRIDES, "ContainerOverrides", 0)
_ENVIRONMENT = (*_CONTAINER, "Environment")
_CATCH_ALL = [{"ErrorEquals": ["States.ALL"], "Next": "Done"}]
_PASS = {"Type": "Pass", "End": True}
_SIDECAR = {"Name": "sidecar", "Command": ["sh"]}


def _definition(fixture_name: str = _PRODUCTION_FIXTURE) -> dict[str, Any]:
    return json.loads((_FIXTURES / fixture_name).read_text(encoding="utf-8"))


def _at(definition: dict[str, Any], path: tuple[Any, ...]) -> Any:
    node: Any = definition
    for key in path:
        node = node[key]
    return node


def _mutate(definition: dict[str, Any], path: tuple[Any, ...], value: Any) -> dict[str, Any]:
    mutated = copy.deepcopy(definition)
    parent = _at(mutated, path[:-1])
    if value is None:
        del parent[path[-1]]
    else:
        parent[path[-1]] = value
    return mutated


def _client(definition: dict[str, Any] | None = None, workflow_type: str = "STANDARD") -> Any:
    client = boto3.client(
        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED),
    )
    stubber = Stubber(client)
    stubber.add_response(
        "describe_state_machine",
        {
            "stateMachineArn": PRODUCTION_STATE_MACHINE_ARN,
            "name": "cnesdata-prod",
            "definition": json.dumps(definition or _definition()),
            "roleArn": "arn:aws:iam::000000000000:role/cnes-states",
            "type": workflow_type,
            "creationDate": _NOW,
        },
        {"stateMachineArn": PRODUCTION_STATE_MACHINE_ARN},
    )
    stubber.activate()
    return client


def _assert_rejected(
    definition: dict[str, Any], error: str, workflow_type: str = "STANDARD",
) -> None:
    client = _client(definition, workflow_type)

    with pytest.raises(IncompatibleStateMachine, match=f"^{error}$"):
        validate_production_state_machine(client, PRODUCTION_STATE_MACHINE_ARN, _POLICY)


def _with_static_concurrency() -> dict[str, Any]:
    definition = _mutate(_definition(), (*_MAP, "MaxConcurrencyPath"), None)
    return _mutate(definition, (*_MAP, "MaxConcurrency"), 2)


def _with_dynamic_parameter(name: str, keep_static: bool) -> dict[str, Any]:
    definition = _definition()
    if not keep_static:
        definition = _mutate(definition, (*_ECS_PARAMETERS, name), None)
    return _mutate(definition, (*_ECS_PARAMETERS, f"{name}.$"), "$.injected")


def test_producao_aceita_ip_publico_com_rede_exata() -> None:
    validate_production_state_machine(_client(), PRODUCTION_STATE_MACHINE_ARN, _POLICY)


def test_politica_derivada_dos_settings_de_producao_aceita_a_fixture() -> None:
    settings = AwsRuntimeSettings.from_mapping(PRODUCTION_VALUES)
    policy = ProductionNetworkPolicy(
        settings.processor_container_name, settings.processor_lease_seconds,
        settings.public_subnet_ids, PRODUCTION_SECURITY_GROUP_ID,
    )

    validate_production_state_machine(_client(), settings.state_machine_arn, policy)

    assert policy == _POLICY
    assert settings.security_group_id == PRODUCTION_SECURITY_GROUP_ID


def test_fixture_de_producao_difere_da_generica_so_em_rede_e_lease() -> None:
    generic = _definition(_GENERIC_FIXTURE)
    network = {
        "AssignPublicIp": "ENABLED",
        "Subnets": list(PRODUCTION_SUBNET_IDS),
        "SecurityGroups": [PRODUCTION_SECURITY_GROUP_ID],
    }
    expected = _mutate(_mutate(generic, _NETWORK, network), (*_ENVIRONMENT, 6, "Value"), "3600")

    assert _definition() == expected


def test_validador_generico_continua_rejeitando_ip_publico() -> None:
    with pytest.raises(IncompatibleStateMachine, match=r"^assign_public_ip_mismatch$"):
        validate_state_machine(_client(), PRODUCTION_STATE_MACHINE_ARN, "processor", 3600)


@pytest.mark.parametrize(
    ("fixture_name", "value"),
    [
        (_PRODUCTION_FIXTURE, "DISABLED"),
        (_PRODUCTION_FIXTURE, None),
        (_GENERIC_FIXTURE, "DISABLED"),
    ],
    ids=["desabilitado", "ausente", "fixture_generica"],
)
def test_producao_rejeita_ip_publico_desabilitado(fixture_name: str, value: str | None) -> None:
    definition = _mutate(_definition(fixture_name), (*_NETWORK, "AssignPublicIp"), value)

    _assert_rejected(definition, "assign_public_ip_mismatch")


def test_producao_rejeita_superset_de_subnets() -> None:
    subnets = [*PRODUCTION_SUBNET_IDS, _EXTRA_SUBNET]

    _assert_rejected(_mutate(_definition(), (*_NETWORK, "Subnets"), subnets), "subnets_mismatch")


@pytest.mark.parametrize(
    ("subnets", "error"),
    [
        ([_FIRST_SUBNET], "subnets_mismatch"),
        ([_SECOND_SUBNET], "subnets_mismatch"),
        ([_SECOND_SUBNET, _FIRST_SUBNET], "subnets_mismatch"),
        ([_FIRST_SUBNET, _FIRST_SUBNET, _SECOND_SUBNET], "subnets_mismatch"),
        ([_FIRST_SUBNET, _EXTRA_SUBNET], "subnets_mismatch"),
        (",".join(PRODUCTION_SUBNET_IDS), "subnets_mismatch"),
        ([], "subnets_required"),
        (None, "subnets_required"),
    ],
    ids=[
        "falta_a_segunda", "falta_a_primeira", "reordenada", "duplicada", "trocada", "escalar",
        "vazia", "ausente",
    ],
)
def test_producao_rejeita_subnet_faltando_ou_reordenada(
    subnets: list[str] | str | None, error: str,
) -> None:
    _assert_rejected(_mutate(_definition(), (*_NETWORK, "Subnets"), subnets), error)


@pytest.mark.parametrize(
    "security_groups",
    [
        [_OTHER_SECURITY_GROUP],
        [PRODUCTION_SECURITY_GROUP_ID, _OTHER_SECURITY_GROUP],
        [_OTHER_SECURITY_GROUP, PRODUCTION_SECURITY_GROUP_ID],
        PRODUCTION_SECURITY_GROUP_ID,
        [],
        None,
    ],
    ids=["divergente", "sobrando", "sobrando_antes", "escalar", "vazio", "ausente"],
)
def test_producao_rejeita_security_group_divergente(
    security_groups: list[str] | str | None,
) -> None:
    definition = _mutate(_definition(), (*_NETWORK, "SecurityGroups"), security_groups)

    _assert_rejected(definition, "security_groups_mismatch")


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ((*_NETWORK, "Subnets.$"), "$.subnets"),
        ((*_NETWORK, "SecurityGroups.$"), "$.security_groups"),
        ((*_NETWORK, "AssignPublicIp.$"), "$.assign_public_ip"),
        ((*_NETWORK, "Extra"), "value"),
        ((*_NETWORK_CONFIGURATION, "AwsvpcConfiguration.$"), "$.network"),
    ],
    ids=[
        "subnets_dinamicas", "security_groups_dinamicos", "ip_dinamico", "campo_desconhecido",
        "awsvpc_dinamico",
    ],
)
def test_producao_exige_chaves_exatas_na_rede(path: tuple[Any, ...], value: str) -> None:
    _assert_rejected(_mutate(_definition(), path, value), "network_parameter_forbidden")


@pytest.mark.parametrize(
    "launch_type", ["EC2", "EXTERNAL", None], ids=["ec2", "externo", "ausente"],
)
def test_producao_rejeita_launch_type_ec2(launch_type: str | None) -> None:
    definition = _mutate(_definition(), (*_ECS_PARAMETERS, "LaunchType"), launch_type)

    _assert_rejected(definition, "launch_type_must_be_fargate")


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("CapacityProviderStrategy", [{"CapacityProvider": "FARGATE_SPOT", "Weight": 1}]),
        ("LoadBalancers", [{"ContainerName": "processor", "ContainerPort": 8000}]),
        ("ServiceRegistries", [{"ContainerName": "processor", "ContainerPort": 8000}]),
        ("PlacementConstraints", [{"Type": "distinctInstance"}]),
        ("EnableExecuteCommand", True),
    ],
    ids=["fargate_spot", "load_balancer", "service_registry", "placement", "execute_command"],
)
def test_producao_rejeita_parametro_ecs_proibido(name: str, value: Any) -> None:
    definition = _mutate(_definition(), (*_ECS_PARAMETERS, name), value)

    _assert_rejected(definition, "ecs_parameter_forbidden")


@pytest.mark.parametrize("name", _ALLOWED_PARAMETERS)
def test_producao_rejeita_toda_chave_dinamica_em_parameters(name: str) -> None:
    definition = _with_dynamic_parameter(name, keep_static=True)

    _assert_rejected(definition, "ecs_parameter_forbidden")


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("Cluster", "ecs_parameter_forbidden"),
        ("NetworkConfiguration", "ecs_parameter_forbidden"),
        ("Overrides", "ecs_parameter_forbidden"),
        ("TaskDefinition", "task_definition_required"),
        ("LaunchType", "launch_type_must_be_fargate"),
    ],
)
def test_producao_rejeita_chave_dinamica_no_lugar_da_estatica(name: str, error: str) -> None:
    _assert_rejected(_with_dynamic_parameter(name, keep_static=False), error)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        ((*_OVERRIDES, "TaskRoleArn"), _OTHER_ROLE),
        ((*_OVERRIDES, "ExecutionRoleArn"), _OTHER_ROLE),
        ((*_OVERRIDES, "Cpu"), "4096"),
        ((*_OVERRIDES, "Memory"), "30720"),
        ((*_OVERRIDES, "ContainerOverrides.$"), "$.overrides"),
        ((*_OVERRIDES, "ContainerOverrides"), [_at(_definition(), _CONTAINER), _SIDECAR]),
    ],
    ids=["task_role", "execution_role", "cpu", "memoria", "overrides_dinamicos", "sidecar"],
)
def test_producao_rejeita_override_alem_do_processor(path: tuple[Any, ...], value: Any) -> None:
    _assert_rejected(_mutate(_definition(), path, value), "ecs_override_forbidden")


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("States", "Done"), _PASS),
        ((*_MAP, "End"), None),
        ((*_MAP, "End"), False),
        ((*_ITEM_STATES, "Done"), _PASS),
        ((*_TASK, "End"), None),
        ((*_TASK, "End"), "true"),
    ],
    ids=[
        "estado_extra_no_topo", "map_sem_end", "map_end_falso", "estado_extra_no_item",
        "task_sem_end", "task_end_textual",
    ],
)
def test_producao_exige_topologia_exata(path: tuple[Any, ...], value: Any) -> None:
    _assert_rejected(_mutate(_definition(), path, value), "production_topology_mismatch")


@pytest.mark.parametrize("concurrency", ["2", "40"])
def test_producao_rejeita_concorrencia_maior_que_um(concurrency: str) -> None:
    values = PRODUCTION_VALUES | {"AWS_PROCESSOR_MAX_CONCURRENCY": concurrency}

    with pytest.raises(
        AwsRuntimeConfigurationError, match=r"^production_concurrency_must_be_one$",
    ):
        AwsRuntimeSettings.from_mapping(values)


@pytest.mark.parametrize(
    ("definition", "error"),
    [
        (_mutate(_definition(), (*_ECS_PARAMETERS, "Count"), 2), "ecs_parameter_forbidden"),
        (_with_static_concurrency(), "map_concurrency_must_be_explicit"),
    ],
    ids=["count_multiplica_tasks", "concorrencia_fixa_no_map"],
)
def test_producao_rejeita_concorrencia_escondida_na_asl(
    definition: dict[str, Any], error: str,
) -> None:
    _assert_rejected(definition, error)


@pytest.mark.parametrize(
    ("path", "value", "error"),
    [
        ((*_MAP, "Catch"), _CATCH_ALL, "unit_failures_must_propagate"),
        ((*_MAP, "ToleratedFailurePercentage"), 100, "unit_failures_must_propagate"),
        ((*_TASK, "Catch"), _CATCH_ALL, "unit_failures_must_propagate"),
        ((*_MAP, "ItemProcessor", "ProcessorConfig", "Mode"), "DISTRIBUTED", "map_must_be_inline"),
        ((*_MAP, "ItemsPath"), "$.other", "map_items_must_be_unit_ids"),
        ((*_TASK, "Resource"), "arn:aws:states:::ecs:runTask", "single_ecs_sync_task_required"),
        ((*_ECS_PARAMETERS, "TaskDefinition"), None, "task_definition_required"),
        ((*_CONTAINER, "Name"), "other", "processor_container_override_missing"),
        ((*_CONTAINER, "Command"), ["noop"], "container_override_must_only_set_environment"),
        ((*_ENVIRONMENT, 6, "Value"), "300", "lease_seconds_mismatch"),
        ((*_ENVIRONMENT, 0, "Value.$"), "$.run_id", "environment_bindings_mismatch"),
    ],
    ids=[
        "catch_no_map", "falha_tolerada", "catch_na_task", "distribuido", "items_divergente",
        "task_sem_sync", "sem_task_definition", "container_errado", "comando",
        "lease_generico", "binding_divergente",
    ],
)
def test_producao_mantem_checagens_genericas(path: tuple[Any, ...], value: Any, error: str) -> None:
    _assert_rejected(_mutate(_definition(), path, value), error)


def test_producao_exige_workflow_standard() -> None:
    _assert_rejected(_definition(), "workflow_must_be_standard", workflow_type="EXPRESS")


def test_producao_normaliza_erro_ao_descrever_state_machine() -> None:
    client = boto3.client(
        "stepfunctions", region_name="us-east-2", config=Config(signature_version=UNSIGNED),
    )

    with Stubber(client) as stubber:
        stubber.add_client_error(
            "describe_state_machine",
            service_error_code="AccessDeniedException",
            http_status_code=400,
            expected_params={"stateMachineArn": PRODUCTION_STATE_MACHINE_ARN},
        )
        with pytest.raises(ProcessorExecutionUnavailable, match=r"^AccessDeniedException$"):
            validate_production_state_machine(client, PRODUCTION_STATE_MACHINE_ARN, _POLICY)


def test_politica_de_rede_de_producao_e_imutavel() -> None:
    with pytest.raises(FrozenInstanceError):
        _POLICY.subnet_ids = (_EXTRA_SUBNET,)  # type: ignore[misc]
