"""Espelho do ensaio fargate-write: o payload é a task recovery renderizada, salvo o permitido."""

import copy
import json
from pathlib import Path

import pytest

from tests.integration.aws_capability._fargate_mirror import (
    TaskSettings,
    task_definition_payload,
)
from tests.production.infra._processing_native import NativePlans, Node
from tests.production.infra.test_processing_security import _witnesses

TASK_KEYS = {
    "family", "requiresCompatibilities", "networkMode", "cpu", "memory", "executionRoleArn",
    "taskRoleArn", "runtimePlatform", "volumes", "containerDefinitions", "tags",
}
MAPPED_VALUES = {
    "requires_compatibilities", "network_mode", "cpu", "memory", "runtime_platform", "volume",
}
IGNORED_VALUES = {
    "family", "execution_role_arn", "task_role_arn", "tags", "tags_all", "id", "arn",
    "arn_without_revision", "revision", "container_definitions", "skip_destroy", "track_latest",
}
ALLOWED_ENV_VALUES = {
    "AWS_CONTROL_PLANE_TABLE", "AWS_DATA_BUCKET", "AWS_STATE_MACHINE_ARN",
    "AWS_PUBLIC_SUBNET_IDS", "AWS_SECURITY_GROUP_ID",
}
PLACEHOLDER = "<permitido>"
ENTRY_POINT = ("python", "-c", "print(1)")
SETTINGS = TaskSettings(
    family="cnesdata-cap-a1b2c3d4-recovery",
    image="111111111111.dkr.ecr.us-east-2.amazonaws.com/cnesdata-cap-a1b2c3d4@sha256:" + "c" * 64,
    execution_role_arn="arn:aws:iam::111111111111:role/cnesdata-cap/cnesdata-cap-a1b2c3d4-exec",
    task_role_arn="arn:aws:iam::111111111111:role/cnesdata-cap/cnesdata-cap-a1b2c3d4-task",
    log_group="cnesdata-cap-a1b2c3d4",
    control_plane_table="cnesdata-cap-a1b2c3d4",
    data_bucket="cnesdata-cap-a1b2c3d4",
    state_machine_arn="arn:aws:states:us-east-2:111111111111:stateMachine:cnesdata-cap-a1b2c3d4",
    subnet_ids=("subnet-0bbb2220", "subnet-0aaa1110"),
    security_group_id="sg-0cap000000000001",
    tags={"Project": "CnesData", "Purpose": "aws-capability", "Run": "a1b2c3d4"},
)


def masked(container: Node) -> Node:
    """Returns: container com os campos permitidos trocados por um marcador fixo."""
    value = copy.deepcopy(container)
    value["image"] = PLACEHOLDER
    value["logConfiguration"]["options"]["awslogs-group"] = PLACEHOLDER
    names = [item["name"] for item in value["environment"]]
    assert len(names) == len(set(names)), "mirror environment_duplicate"
    value["environment"] = {
        item["name"]: PLACEHOLDER if item["name"] in ALLOWED_ENV_VALUES else item["value"]
        for item in value["environment"]
    }
    return value


def container_differences(payload_container: Node, rendered_container: Node) -> list[str]:
    left, right = masked(payload_container), masked(rendered_container)
    keys = sorted(set(left) | set(right))
    return [key for key in keys if left.get(key) != right.get(key)]


def task_differences(payload: Node, values: Node) -> list[str]:
    runtime = values["runtime_platform"][0]
    expected = {
        "requiresCompatibilities": values["requires_compatibilities"],
        "networkMode": values["network_mode"],
        "cpu": values["cpu"],
        "memory": values["memory"],
        "runtimePlatform": {
            "cpuArchitecture": runtime["cpu_architecture"],
            "operatingSystemFamily": runtime["operating_system_family"],
        },
        "volumes": sorted(({"name": v["name"]} for v in values["volume"]), key=str),
    }
    actual = {key: payload.get(key) for key in expected}
    actual["volumes"] = sorted(actual["volumes"] or [], key=str)
    found = [key for key in expected if actual[key] != expected[key]]
    unmapped = [
        f"unmapped:{key}"
        for key, value in values.items()
        if key not in MAPPED_VALUES | IGNORED_VALUES and value not in (None, "", [], {}, False)
    ]
    return found + unmapped


def payload() -> Node:
    return task_definition_payload(SETTINGS)


def test_payload_tem_exatamente_as_chaves_de_task_do_modulo_e_so_o_permitido_varia() -> None:
    assert set(payload()) == TASK_KEYS
    assert payload()["family"] == SETTINGS.family
    assert payload()["executionRoleArn"] == SETTINGS.execution_role_arn
    assert payload()["taskRoleArn"] == SETTINGS.task_role_arn


def test_espelho_container_e_task_iguais_ao_modo_recovery_renderizado(
    processing_native: NativePlans, prod_root: Path
) -> None:
    witnesses = list(_witnesses(processing_native, prod_root, "recovery"))
    assert witnesses, "mirror witnesses=empty"
    built = payload()
    assert set(built) == TASK_KEYS
    for _, task, rendered in witnesses:
        container = built["containerDefinitions"][0]
        assert len(built["containerDefinitions"]) == 1
        assert "entryPoint" not in container
        assert container_differences(container, rendered) == []
        assert task_differences(built, task["change"]["after"]) == []


def test_espelho_com_entrypoint_do_probe_difere_so_no_entrypoint(
    processing_native: NativePlans, prod_root: Path
) -> None:
    plain = payload()["containerDefinitions"][0]
    probing = task_definition_payload(SETTINGS, ENTRY_POINT)["containerDefinitions"][0]
    assert probing["entryPoint"] == list(ENTRY_POINT)
    assert {k: v for k, v in probing.items() if k != "entryPoint"} == plain
    witnesses = list(_witnesses(processing_native, prod_root, "recovery"))
    assert witnesses, "mirror witnesses=empty"
    for _, _, rendered in witnesses:
        assert container_differences(plain, rendered) == []
        assert "command" in rendered
        assert plain["command"] == rendered["command"] == ["recover-once"]


def test_valores_permitidos_chegam_do_settings_e_subnets_saem_ordenadas() -> None:
    container = payload()["containerDefinitions"][0]
    env = {item["name"]: item["value"] for item in container["environment"]}
    assert container["image"] == SETTINGS.image
    assert container["logConfiguration"]["options"]["awslogs-group"] == SETTINGS.log_group
    assert env["AWS_CONTROL_PLANE_TABLE"] == SETTINGS.control_plane_table
    assert env["AWS_DATA_BUCKET"] == SETTINGS.data_bucket
    assert env["AWS_STATE_MACHINE_ARN"] == SETTINGS.state_machine_arn
    assert env["AWS_SECURITY_GROUP_ID"] == SETTINGS.security_group_id
    assert env["AWS_PUBLIC_SUBNET_IDS"] == "subnet-0aaa1110,subnet-0bbb2220"
    assert {item["key"]: item["value"] for item in payload()["tags"]} == dict(SETTINGS.tags)


def _mutations() -> dict[str, tuple[str, object]]:
    return {
        "comando": ("command", ["dispatch-outbox-once"]),
        "usuario": ("user", "root"),
        "raiz_gravavel": ("readonlyRootFilesystem", False),
        "privilegiado": ("privileged", True),
        "linux": ("linuxParameters", {"capabilities": {"drop": []}}),
        "mounts": ("mountPoints", []),
        "log_driver": ("logConfiguration", {"logDriver": "json-file", "options": {}}),
        "chave_extra": ("essential", False),
    }


@pytest.mark.parametrize("case", sorted(_mutations()))
def test_comparador_acusa_qualquer_divergencia_fora_do_permitido(case: str) -> None:
    container = payload()["containerDefinitions"][0]
    assert container_differences(container, copy.deepcopy(container)) == []
    key, value = _mutations()[case]
    changed = copy.deepcopy(container)
    changed[key] = value
    assert container_differences(container, changed) != []


def test_comparador_acusa_variavel_de_ambiente_fora_da_lista_permitida() -> None:
    container = payload()["containerDefinitions"][0]
    changed = json.loads(json.dumps(container))
    changed["environment"].append({"name": "EXTRA", "value": "1"})
    assert container_differences(container, changed) == ["environment"]
    swapped = json.loads(json.dumps(container))
    for item in swapped["environment"]:
        if item["name"] == "TMPDIR":
            item["value"] = "/var/tmp"  # noqa: S108
    assert container_differences(container, swapped) == ["environment"]


def test_comparador_ignora_valor_de_variavel_permitida_e_imagem_e_grupo_de_log() -> None:
    container = payload()["containerDefinitions"][0]
    changed = json.loads(json.dumps(container))
    changed["image"] = "outra@sha256:" + "d" * 64
    changed["logConfiguration"]["options"]["awslogs-group"] = "outro"
    for item in changed["environment"]:
        if item["name"] in ALLOWED_ENV_VALUES:
            item["value"] = "x"
    assert container_differences(container, changed) == []


def values_of(built: Node) -> Node:
    runtime = built["runtimePlatform"]
    return {
        "requires_compatibilities": built["requiresCompatibilities"],
        "network_mode": built["networkMode"],
        "cpu": built["cpu"],
        "memory": built["memory"],
        "runtime_platform": [{
            "cpu_architecture": runtime["cpuArchitecture"],
            "operating_system_family": runtime["operatingSystemFamily"],
        }],
        "volume": [{"name": item["name"]} for item in built["volumes"]],
        "family": built["family"],
        "ephemeral_storage": [],
    }


TASK_MUTANTS = {
    "cpu": ("cpu", "512"),
    "memory": ("memory", "1024"),
    "rede": ("networkMode", "bridge"),
    "compatibilidade": ("requiresCompatibilities", ["EC2"]),
    "plataforma": (
        "runtimePlatform", {"cpuArchitecture": "ARM64", "operatingSystemFamily": "LINUX"}
    ),
    "volume_a_menos": ("volumes", [{"name": "tmp"}, {"name": "logs"}]),
    "volume_a_mais": ("volumes", [{"name": n} for n in ("tmp", "logs", "data", "extra")]),
    "volume_renomeado": ("volumes", [{"name": n} for n in ("tmp", "logs", "dados")]),
}


@pytest.mark.parametrize("case", sorted(TASK_MUTANTS))
def test_comparador_de_task_acusa_mutante_de_cada_atributo_mapeado(case: str) -> None:
    built = payload()
    values = values_of(built)
    assert task_differences(built, values) == []
    key, value = TASK_MUTANTS[case]
    assert task_differences({**built, key: value}, values) != []


def test_comparador_de_task_falha_com_atributo_nao_vazio_do_plano_fora_do_mapeado() -> None:
    built = payload()
    values = values_of(built)
    extra = {**values, "ephemeral_storage": [{"size_in_gib": 30}]}
    assert task_differences(built, extra) == ["unmapped:ephemeral_storage"]
    ignored = {**values, "family": "x", "tags_all": {"a": "b"}, "skip_destroy": True}
    assert task_differences(built, ignored) == []
