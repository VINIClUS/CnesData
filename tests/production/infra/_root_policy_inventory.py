"""Exact inventory of aws_iam_role_policy resources attached by the prod root."""

from typing import Any

from tests.production.infra._step_functions_iam_contracts import (
    POLICY_ATTACHMENT_TYPES,
    policy_grants,
)

type Node = dict[str, Any]
type Inventory = dict[str, tuple[str, str]]

PRODUCT_NAMESPACE = "cnesdata-prod"
RUNTIME_PREFIX = "module.runtime_iam.aws_iam_role_policy"
PROCESSOR_MODES = ("unit", "recovery", "audit")


def runtime_iam_entries(plan: Node) -> Inventory:
    namespace = PRODUCT_NAMESPACE
    sfn, processor = f"{namespace}-step-functions", f"{namespace}-processor"
    api = plan["variables"]["shared_outputs"]["value"]["roles_anywhere"]["api_role_arn"]
    return {
        f"{RUNTIME_PREFIX}.execution": (f"{processor}-pull-logs", f"{processor}-execution"),
        f"{RUNTIME_PREFIX}.step_functions[0]": (f"{sfn}-common", sfn),
        f"{RUNTIME_PREFIX}.api[0]": (f"{namespace}-api-runtime", api.rsplit("/", 1)[1]),
        **{
            f'{RUNTIME_PREFIX}.runtime["{mode}"]': (
                f"{processor}-{mode}-runtime",
                f"{processor}-{mode}",
            )
            for mode in PROCESSOR_MODES
        },
    }


def processing_entries(plan: Node) -> Inventory:
    sfn = f"{PRODUCT_NAMESPACE}-step-functions"
    return {"module.processing.aws_iam_role_policy.step_functions_ecs[0]": (f"{sfn}-ecs", sfn)}


def schedulers_entries(plan: Node) -> Inventory:
    namespace = PRODUCT_NAMESPACE
    return {
        f'module.schedulers.aws_iam_role_policy.run["{mode}"]': (
            f"{namespace}-scheduler-{mode}-run",
            f"{namespace}-scheduler-{mode}",
        )
        for mode in ("recovery", "audit")
    }


def athena_entries(plan: Node) -> Inventory:
    cutoff = f"{PRODUCT_NAMESPACE}-athena-cutoff"
    return {"module.athena.aws_iam_role_policy.cutoff": (f"{cutoff}-workgroup", cutoff)}


def expected_root_policy_inventory(plan: Node) -> Inventory:
    return {
        **runtime_iam_entries(plan),
        **processing_entries(plan),
        **schedulers_entries(plan),
        **athena_entries(plan),
    }


def assert_root_policy_inventory(plan: Node) -> None:
    expected = expected_root_policy_inventory(plan)
    resources = [
        item
        for item in plan["resource_changes"]
        if item["mode"] == "managed" and item["type"] in POLICY_ATTACHMENT_TYPES
    ]
    observed = {
        item["address"]: (item["change"]["after"].get("name"), item["change"]["after"].get("role"))
        for item in resources
    }
    assert len(resources) == len(observed), "iam_policy_inventory duplicate_address"
    assert observed == expected, f"iam_policy_inventory expected={expected!r} observed={observed!r}"
    for item in resources:
        grants = policy_grants(item["change"]["after"]["policy"])
        assert grants, f"missing_grants address={item['address']}"
        if item["address"].startswith(RUNTIME_PREFIX):
            assert all(action != "ecs:RunTask" for action, _, _ in grants), "runtime_iam_run_task"
