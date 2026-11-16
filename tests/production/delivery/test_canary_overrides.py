"""Overrides devolvidos pelo ECS no binding do canary; nenhuma execução AWS."""

from __future__ import annotations

import copy
from datetime import timedelta, timezone
from typing import Any

import pytest

from scripts.promotion import run_canary as runner
from tests.production.delivery.test_canary import activation as activation_fixture
from tests.production.delivery.test_canary import case as case_fixture
from tests.production.delivery.test_canary import github as github_fixture
from tests.production.delivery.test_canary_replay import stopped

activation = activation_fixture
case = case_fixture
github = github_fixture

INFERENCE = "inferenceAcceleratorOverrides"
MODES = ["full", "stopped"]
LOCAL = timezone(timedelta(hours=-3))
STOPPED_MS = {
    "connectivityAt": 2654,
    "pullStartedAt": 10580,
    "pullStoppedAt": 23756,
    "startedAt": 24445,
    "executionStoppedAt": 35453,
    "stoppingAt": 45512,
    "stoppedAt": 59322,
}
UUID = "0f1e2d3c-4b5a-4968-8776-a5b4c3d2e1f0"
IMAGE_DIGEST = "sha256:" + "b" * 64


def _attachment(stopped_task: bool) -> dict[str, Any]:
    details = [("subnetId", "subnet-0123456789abcdef0")]
    if stopped_task:
        details += [
            ("networkInterfaceId", "eni-0123456789abcdef0"),
            ("macAddress", "0a:1b:2c:3d:4e:5f"),
            ("privateDnsName", "ip-10-0-1-23.us-east-2.compute.internal"),
            ("privateIPv4Address", "10.0.1.23"),
        ]
    return {
        "details": [{"name": name, "value": value} for name, value in details],
        "id": UUID,
        "status": "DELETED" if stopped_task else "PRECREATED",
        "type": "ElasticNetworkInterface",
    }


def _container(arn: str, stopped_task: bool) -> dict[str, Any]:
    container: dict[str, Any] = {
        "containerArn": arn.replace(":task/", ":container/") + "/" + UUID,
        "cpu": "0",
        "healthStatus": "UNKNOWN",
        "image": "123456789012.dkr.ecr.us-east-2.amazonaws.com/cnesdata-processor@" + IMAGE_DIGEST,
        "lastStatus": "STOPPED" if stopped_task else "PENDING",
        "name": "processor",
        "networkBindings": [],
        "networkInterfaces": [],
        "taskArn": arn,
    }
    if stopped_task:
        container |= {
            "exitCode": 0,
            "imageDigest": IMAGE_DIGEST,
            "networkInterfaces": [{"attachmentId": UUID, "privateIpv4Address": "10.0.1.23"}],
            "runtimeId": arn.rsplit("/", 1)[1] + "-1234567890",
        }
    return container


def _redacted_task(contract: dict[str, Any], origin: dict[str, Any], stopped_task: bool) -> Any:
    arn, created = origin["taskArn"], origin["createdAt"].astimezone(LOCAL)
    revision = contract["revisions"]["audit"]
    task: dict[str, Any] = {
        "attachments": [_attachment(stopped_task)],
        "attributes": [{"name": "ecs.cpu-architecture", "value": "x86_64"}],
        "availabilityZone": "us-east-2a",
        "clusterArn": contract["cluster"],
        "containers": [_container(arn, stopped_task)],
        "cpu": "256",
        "createdAt": created,
        "desiredStatus": "STOPPED" if stopped_task else "RUNNING",
        "enableExecuteCommand": False,
        "ephemeralStorage": {"sizeInGiB": 20},
        "fargateEphemeralStorage": {"sizeInGiB": 20},
        "group": "family:" + revision.rsplit("/", 1)[1].rsplit(":", 1)[0],
        "healthStatus": "UNKNOWN",
        "lastStatus": "STOPPED" if stopped_task else "PROVISIONING",
        "launchType": "FARGATE",
        "memory": "512",
        "overrides": copy.deepcopy(runner._run_request(contract)["overrides"]) | {INFERENCE: []},
        "platformFamily": "Linux",
        "platformVersion": "1.4.0",
        "startedBy": contract["canary_id"],
        "taskArn": arn,
        "taskDefinitionArn": revision,
        "version": 5 if stopped_task else 1,
    }
    if stopped_task:
        task |= {
            "connectivity": "CONNECTED",
            "stopCode": "EssentialContainerExited",
            "stoppedReason": "Essential container in task exited",
        }
        task |= {key: created + timedelta(milliseconds=ms) for key, ms in STOPPED_MS.items()}
    return task


def _ecs_task(value: tuple[Any, ...], mode: str) -> dict[str, Any]:
    contract, api, _, original = value
    origin = api.responses["run_task"]["tasks"][0]
    name = "describe_tasks" if mode == "stopped" else "run_task"
    if mode == "stopped":
        stopped(api, original)
    api.responses[name]["tasks"][0] = _redacted_task(contract, origin, mode == "stopped")
    return api.responses[name]["tasks"][0]


def _denied(value: tuple[Any, ...]) -> None:
    contract, api, clock, _ = value
    with pytest.raises(ValueError, match=r"^canary=run_overrides$"):
        runner.run_canary(contract, api, clock, lambda arn: api)


@pytest.mark.parametrize("mode", MODES)
def test_aceita_inference_accelerator_overrides_vazio_devolvido_pelo_ecs(case, mode):
    contract, api, clock, original = case
    _ecs_task(case, mode)
    assert runner.run_canary(contract, api, clock, lambda arn: api) == original


@pytest.mark.parametrize("mode", MODES)
def test_shape_real_sem_inference_ja_passa_e_isola_a_causa(case, mode):
    contract, api, clock, original = case
    del _ecs_task(case, mode)["overrides"][INFERENCE]
    assert runner.run_canary(contract, api, clock, lambda arn: api) == original


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    "value",
    [[{"deviceName": "device-1", "deviceType": "eia2.medium"}], (), ({"deviceName": "d"},)],
    ids=["lista-nao-vazia", "tupla-vazia", "tupla-nao-vazia"],
)
def test_recusa_inference_accelerator_overrides_nao_vazio_ou_fora_de_lista(case, mode, value):
    _ecs_task(case, mode)["overrides"][INFERENCE] = value
    _denied(case)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    ("key", "value"),
    [("ephemeralStorage", {"sizeInGiB": 21}), ("cpu", ""), ("memory", "512")],
)
def test_recusa_outra_chave_extra_em_overrides_mesmo_vazia(case, mode, key, value):
    _ecs_task(case, mode)["overrides"][key] = value
    _denied(case)


@pytest.mark.parametrize("mode", MODES)
def test_recusa_environment_diferente_com_inference_vazio(case, mode):
    container = _ecs_task(case, mode)["overrides"]["containerOverrides"][0]
    container["environment"][0]["value"] += "-foreign"
    _denied(case)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize(
    ("key", "value"),
    [("command", ["other"]), ("environmentFiles", []), ("resourceRequirements", [])],
)
def test_recusa_container_override_com_campo_extra(case, mode, key, value):
    _ecs_task(case, mode)["overrides"]["containerOverrides"][0][key] = value
    _denied(case)


@pytest.mark.parametrize(
    ("key", "value"),
    [("unknownOverrides", []), (INFERENCE, {}), (INFERENCE, None), (INFERENCE, "")],
    ids=["outra-chave-vazia", "inference-dict", "inference-none", "inference-str"],
)
def test_binding_so_remove_inference_exatamente_lista_vazia(case, key, value):
    contract, api, clock, _ = case
    task = _redacted_task(contract, api.responses["run_task"]["tasks"][0], True)
    task["overrides"][key] = value
    with pytest.raises(ValueError, match=r"^canary=run_overrides$"):
        runner._task_binding(contract, task, clock)
