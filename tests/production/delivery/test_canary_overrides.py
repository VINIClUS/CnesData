"""Overrides devolvidos pelo ECS no binding do canary; nenhuma execução AWS."""

from __future__ import annotations

import copy
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


def _ecs_task(value: tuple[Any, ...], mode: str) -> dict[str, Any]:
    _, api, _, original = value
    if mode == "stopped":
        stopped(api, original)
    task = api.responses["describe_tasks" if mode == "stopped" else "run_task"]["tasks"][0]
    task["overrides"][INFERENCE] = []
    return task


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
    task = copy.deepcopy(api.responses["run_task"]["tasks"][0])
    task["overrides"][INFERENCE] = []
    task["overrides"][key] = value
    with pytest.raises(ValueError, match=r"^canary=run_overrides$"):
        runner._task_binding(contract, task, clock)
