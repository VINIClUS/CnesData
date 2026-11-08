"""Plan nativo da raiz com revisões de scheduler, sobre a sessão do processing."""

import json
from pathlib import Path
from typing import Any

import pytest

from tests.production.infra._processing_native import (
    ROOT_RUN,
    NativePlans,
    Tofu,
    _processing_runs,
)

type Node = dict[str, Any]

ACCOUNT = "123456789012"
PREFIX = "cnesdata-prod"
SCHEDULED_RUN = "raiz_com_schedulers"
CLUSTER_ARN = f"arn:aws:ecs:us-east-2:{ACCOUNT}:cluster/{PREFIX}-processor"
REVISION = f"arn:aws:ecs:us-east-2:{ACCOUNT}:task-definition/{PREFIX}-processor-%s:%d"
SCHEDULER_INPUTS = {
    "scheduler_recovery_task_definition_arn": REVISION % ("recovery", 7),
    "scheduler_audit_task_definition_arn": REVISION % ("audit", 3),
}
CLUSTER_OVERRIDE = {
    "address": "module.processing.aws_ecs_cluster.processor",
    "fields": {"arn": {"value": CLUSTER_ARN}},
}


_CACHE: dict[str, Node] = {}


def scheduled_root_plan(tofu: Tofu, prod_root: Path) -> Node:
    evidence = Path(tofu[1]["TF_DATA_DIR"])
    if str(evidence) in _CACHE:
        return _CACHE[str(evidence)]
    overrides = json.loads((evidence / "processing-identity-overrides.json").read_text())
    plans = _processing_runs(
        tofu,
        prod_root,
        ("athena-root", {SCHEDULED_RUN: SCHEDULER_INPUTS}, [*overrides, CLUSTER_OVERRIDE]),
    )
    _CACHE[str(evidence)] = plans[SCHEDULED_RUN]
    return plans[SCHEDULED_RUN]


def resources(plan: Node, kind: str) -> list[Node]:
    return [
        item
        for item in plan["resource_changes"]
        if item["mode"] == "managed" and item["type"] == kind
    ]


def after_of(plan: Node, address: str) -> Node:
    matches = [item for item in plan["resource_changes"] if item["address"] == address]
    assert len(matches) == 1, f"resource_missing address={address}"
    return matches[0]["change"]["after"]


def planned_output(plan: Node, name: str) -> Any:
    outputs = plan["planned_values"]["outputs"]
    assert name in outputs, f"output_missing name={name}"
    return outputs[name]["value"]



@pytest.fixture(name="bootstrap_plan", scope="module")
def fixture_bootstrap_plan(processing_native: NativePlans) -> Node:
    return processing_native.plans[ROOT_RUN]


@pytest.fixture(name="scheduled_plan", scope="module")
def fixture_scheduled_plan(
    processing_native: NativePlans, tofu_context: Tofu, prod_root: Path
) -> Node:
    assert processing_native.plans
    return scheduled_root_plan(tofu_context, prod_root)


@pytest.fixture(name="all_root_plans", scope="module")
def fixture_all_root_plans(
    processing_native: NativePlans, scheduled_plan: Node
) -> dict[str, Node]:
    return {**processing_native.plans, SCHEDULED_RUN: scheduled_plan}
