"""Exact inventory of aws_iam_role_policy resources attached by the prod root."""

import json
from fnmatch import fnmatchcase
from typing import Any

from tests.production.infra._step_functions_iam_contracts import (
    POLICY_ATTACHMENT_TYPES,
    literal_grants,
    policy_grants,
)

type Node = dict[str, Any]
type Inventory = dict[str, tuple[str, str]]

PRODUCT_NAMESPACE = "cnesdata-prod"
RUNTIME_PREFIX = "module.runtime_iam.aws_iam_role_policy"
PROCESSOR_MODES = ("unit", "recovery", "audit")
ALIASES_MODO = ("mode_seed", "mode_dispatch", "mode_audit_launch", "mode_observe")


def _entradas_modo(plano: Node) -> Inventory:
    entrada = plano["variables"].get("mode_canary", {}).get("value")
    if entrada is None:
        return {}
    return {
        f"{RUNTIME_PREFIX}.{alias}[0]": (f"{nome}-runtime", nome)
        for alias in ALIASES_MODO
        for nome in [f"{PRODUCT_NAMESPACE}-{alias.replace('_', '-')}"]
    }


def _entradas_canary(plano: Node) -> Inventory:
    if plano["variables"].get("canary_release_id", {}).get("value") is None:
        return {}
    nomes = {
        "canary_task": ("canary-task-runtime", "canary-task"),
        "canary_execution": ("canary-pull-logs", "canary-execution"),
        "canary_seeder": ("canary-seeder-put", "canary-seeder"),
        "canary_verifier": ("canary-verifier-read", "canary-verifier"),
        "promotion_fence_operator": ("promotion-fence-operator-gate", "promotion-fence-operator"),
    }
    return {
        f"{RUNTIME_PREFIX}.{alias}[0]": (
            f"{PRODUCT_NAMESPACE}-{valores[0]}",
            f"{PRODUCT_NAMESPACE}-{valores[1]}",
        )
        for alias, valores in nomes.items()
    }


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
        **_entradas_canary(plan),
        **_entradas_modo(plan),
    }


def processing_entries(plan: Node) -> Inventory:
    sfn = f"{PRODUCT_NAMESPACE}-step-functions"
    entradas = {"module.processing.aws_iam_role_policy.step_functions_ecs[0]": (f"{sfn}-ecs", sfn)}
    if plan["variables"].get("canary_release_id", {}).get("value") is not None:
        entradas["module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"] = (
            f"{PRODUCT_NAMESPACE}-canary-verifier-run-task",
            f"{PRODUCT_NAMESPACE}-canary-verifier",
        )
    return entradas


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
        if _validar_launcher_modo(plan, item):
            continue
        grants = policy_grants(item["change"]["after"]["policy"])
        assert grants, f"missing_grants address={item['address']}"
        if item["address"].startswith(RUNTIME_PREFIX):
            assert all(not fnmatchcase("ecs:RunTask", action) for action, _, _ in grants), (
                "runtime_iam_run_task"
            )


def _validar_launcher_modo(plano: Node, item: Node) -> bool:
    aliases = {
        f"{RUNTIME_PREFIX}.mode_{a}[0]": m
        for a, m in (("dispatch", "recovery"), ("audit_launch", "audit"))
    }
    endereco = item["address"]
    if endereco not in aliases:
        return False
    entrada = plano["variables"]["mode_canary"]["value"]
    assert entrada is not None, "mode_inventory_input=required"
    documento = item["change"]["after"]["policy"]
    modo = aliases[endereco]
    if modo == "audit" and entrada["context_kind"] == "readiness":
        assert json.loads(documento) == {
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*"}],
        }
        return True
    grants = policy_grants(documento)
    recursos = entrada["resources"]
    prazo = {"DateLessThan": {"aws:CurrentTime": entrada["enabled_until"]}}
    esperado = literal_grants(
        ("ecs:RunTask",),
        (recursos["revisions"][modo],),
        prazo | {"ArnEquals": {"ecs:cluster": recursos["cluster_arn"]}},
    ) | literal_grants(
        ("iam:PassRole",),
        (recursos["task_role_arns"][modo], recursos["execution_role_arn"]),
        prazo | {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}},
    )
    observado = frozenset(
        g
        for g in grants
        if any(fnmatchcase(acao, g[0]) for acao in ("ecs:RunTask", "iam:PassRole"))
    )
    assert observado == esperado, "mode_inventory_launch=expected_exact_grants"
    return True
