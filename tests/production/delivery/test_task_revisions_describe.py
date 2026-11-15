"""DescribeTaskDefinition da revisão recriada: padrões false do provider aws 6.54."""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

from tests.production.delivery._phase_a_fixtures import ACCOUNT, ECR, REGION

type Node = dict[str, Any]
MODES = ("unit", "recovery", "audit")
PREFIX = f"arn:aws:ecs:{REGION}:{ACCOUNT}:task-definition/cnesdata-prod-processor-"
IMAGE = ECR + "@sha256:" + "2" * 64
REVISIONS = {mode: PREFIX + mode + ":3" for mode in MODES}


def _contract(mode: str) -> Node:
    return {
        "family": "cnesdata-prod-processor-" + mode,
        "networkMode": "awsvpc",
        "requiresCompatibilities": ["FARGATE"],
        "cpu": "256",
        "memory": "512",
        "executionRoleArn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-execution",
        "taskRoleArn": f"arn:aws:iam::{ACCOUNT}:role/cnesdata-prod-processor-{mode}",
        "runtimePlatform": {"cpuArchitecture": "X86_64", "operatingSystemFamily": "LINUX"},
        "volumes": [{"name": name, "host": {}} for name in ("tmp", "cache", "work")],
        "containerDefinitions": [{"name": "processor", "image": IMAGE}],
    }


def _described(mode: str) -> Node:
    result = copy.deepcopy(_contract(mode)) | {
        "taskDefinitionArn": REVISIONS[mode],
        "revision": 3,
        "status": "ACTIVE",
        "enableFaultInjection": False,
    }
    for volume in result["volumes"]:
        volume["configuredAtLaunch"] = False
    return result


class TaskBoundary:
    def __init__(self, mutate: Callable[[str, Node], None] | None = None) -> None:
        self.mutate = mutate

    def describe_task_definition(self, arn: str) -> Node:
        mode = next(mode for mode, value in REVISIONS.items() if value == arn)
        result = _described(mode)
        if self.mutate is not None:
            self.mutate(mode, result)
        return result


def _verify(boundary: TaskBoundary, contracts: dict[str, Node] | None = None) -> Node:
    from scripts.promotion.verify_task_revisions import verify_revisions

    context = {
        "region": REGION,
        "account_id": ACCOUNT,
        "ecr_image": IMAGE,
        "processor_active_unit_task_definition_arn": None,
        "processor_retained_unit_task_definition_arns": [],
        "task_definitions": contracts or {mode: _contract(mode) for mode in MODES},
    }
    return verify_revisions(context, dict(REVISIONS), boundary)


def _drop_fault_injection(mode: str, result: Node) -> None:
    del result["enableFaultInjection"]


def _drop_configured_at_launch(mode: str, result: Node) -> None:
    for volume in result["volumes"]:
        del volume["configuredAtLaunch"]


def test_aceita_padroes_false_e_fingerprint_do_describe_bruto():
    from scripts.promotion.verify_task_revisions import fingerprint

    result = _verify(TaskBoundary())
    assert result["processor_task_definition_arns"] == REVISIONS
    assert result["task_fingerprints"] == sorted(
        (
            {"address": REVISIONS[mode], "fingerprint_sha256": fingerprint(_described(mode))}
            for mode in MODES
        ),
        key=lambda item: item["address"],
    )


@pytest.mark.parametrize("mutate", [_drop_fault_injection, _drop_configured_at_launch])
def test_aceita_cada_padrao_false_sozinho(mutate):
    _verify(TaskBoundary(mutate))


def test_aceita_contrato_que_ja_declara_os_padroes_false():
    contracts = {mode: _described(mode) for mode in MODES}
    for contract in contracts.values():
        for key in ("taskDefinitionArn", "revision", "status"):
            del contract[key]
    _verify(TaskBoundary(), contracts)


@pytest.mark.parametrize("value", [True, None, 0])
def test_recusa_enable_fault_injection_que_nao_e_false(value):
    def mutate(mode: str, result: Node) -> None:
        if mode == "recovery":
            result["enableFaultInjection"] = value

    with pytest.raises(ValueError, match=r"^revision=unknown_functional$"):
        _verify(TaskBoundary(mutate))


@pytest.mark.parametrize("value", [True, 0])
def test_recusa_configured_at_launch_que_nao_e_false(value):
    def mutate(mode: str, result: Node) -> None:
        if mode == "audit":
            result["volumes"][1]["configuredAtLaunch"] = value

    with pytest.raises(ValueError, match=r"^revision=functional_drift$"):
        _verify(TaskBoundary(mutate))


@pytest.mark.parametrize(
    ("key", "value"),
    [("pidMode", "host"), ("ephemeralStorage", {"sizeInGiB": 200}), ("futureFlag", False)],
)
def test_recusa_chave_nova_desconhecida_no_describe(key, value):
    def mutate(mode: str, result: Node) -> None:
        if mode == "unit":
            result[key] = value

    with pytest.raises(ValueError, match=r"^revision=unknown_functional$"):
        _verify(TaskBoundary(mutate))


def test_recusa_outra_chave_false_em_volume():
    def mutate(mode: str, result: Node) -> None:
        if mode == "recovery":
            result["volumes"][2]["futureFlag"] = False

    with pytest.raises(ValueError, match=r"^revision=functional_drift$"):
        _verify(TaskBoundary(mutate))
