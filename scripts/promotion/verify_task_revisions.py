"""Confere revisões exatas e observações IAM tipadas antes da assinatura."""

from __future__ import annotations

import random
import re
import time
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from release.verify_candidate import require, timestamp
from scripts.promotion._canonical import object_hash

type Node = dict[str, Any]
TASK_REQUIRED = {
    "family",
    "networkMode",
    "requiresCompatibilities",
    "cpu",
    "memory",
    "executionRoleArn",
    "taskRoleArn",
    "runtimePlatform",
    "volumes",
    "containerDefinitions",
}
TASK_METADATA = {
    "taskDefinitionArn",
    "revision",
    "status",
    "requiresAttributes",
    "compatibilities",
    "registeredAt",
    "registeredBy",
    "deregisteredAt",
    "tags",
}


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def sleep(self, delay: float) -> None: ...
    def jitter(self) -> float: ...


class SystemClock:
    """Relógio real para tentativas finitas, separado do relógio controlado dos testes."""

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, delay: float) -> None:
        time.sleep(delay)

    def jitter(self) -> float:
        return random.uniform(0, 0.25)

    def utcnow(self) -> datetime:
        return datetime.now(UTC)


class IamNotReady(RuntimeError):
    """Readback ainda indisponível, sem observação contratual divergente."""


def _case(
    identity: tuple[str, str], permission: tuple[str, str], context: Node, expected: bool
) -> Node:
    case_id, principal = identity
    action, resource = permission
    return {
        "case_id": case_id,
        "principal": principal,
        "action": action,
        "resource": resource,
        "context": context,
        "expected": expected,
    }


def _task_cases(contract: Node) -> list[Node]:
    principals, revisions = contract["principals"], contract["revisions"]
    cluster = {"ecs:cluster": contract["cluster_arn"]}
    cases = [
        _case(
            ("sfn-candidate-unit", principals["sfn"]), ("ecs:RunTask", revisions["unit"]), {}, True
        )
    ]
    cases += [
        _case((f"sfn-retained-{index}", principals["sfn"]), ("ecs:RunTask", arn), {}, True)
        for index, arn in enumerate(contract["retained_before"])
    ]
    cases += [
        _case(
            ("sfn-candidate-" + mode, principals["sfn"]),
            ("ecs:RunTask", revisions[mode]),
            {},
            False,
        )
        for mode in ("recovery", "audit")
    ]
    cases.append(
        _case(
            ("verifier-audit", principals["verifier"]),
            ("ecs:RunTask", revisions["audit"]),
            cluster,
            True,
        )
    )
    cases += [
        _case(
            ("verifier-" + mode, principals["verifier"]),
            ("ecs:RunTask", revisions[mode]),
            cluster,
            False,
        )
        for mode in ("unit", "recovery")
    ]
    cases.append(
        _case(
            ("verifier-other-cluster", principals["verifier"]),
            ("ecs:RunTask", revisions["audit"]),
            {"ecs:cluster": contract["other_cluster_arn"]},
            False,
        )
    )
    return cases + _scheduler_cases(contract)


def _scheduler_cases(contract: Node) -> list[Node]:
    principals, revisions = contract["principals"], contract["revisions"]
    cluster = {"ecs:cluster": contract["cluster_arn"]}
    cases = []
    for mode in ("recovery", "audit"):
        principal = principals["scheduler_" + mode]
        previous = contract["scheduler_previous"][mode]
        if previous is None:
            require(principal is None, "iam=scheduler_without_revision")
            continue
        require(type(principal) is str and principal, "iam=scheduler_principal")
        cases.append(
            _case(
                ("scheduler-" + mode + "-previous", principal),
                ("ecs:RunTask", previous),
                cluster,
                True,
            )
        )
        require(previous != revisions[mode], "iam=scheduler_candidate_is_previous")
        cases.append(
            _case(
                ("scheduler-" + mode + "-candidate", principal),
                ("ecs:RunTask", revisions[mode]),
                cluster,
                False,
            )
        )
    return cases


def _pass_role_cases(contract: Node) -> list[Node]:
    cases = []
    service = {"iam:PassedToService": "ecs-tasks.amazonaws.com"}
    for name, own, foreign in (
        ("sfn", "runtime_roles", "canary_roles"),
        ("verifier", "canary_roles", "runtime_roles"),
    ):
        principal = contract["principals"][name]
        require(len(contract[own]) == 2, "iam=task_execution_roles")
        for index, role in enumerate(contract[own]):
            cases.append(
                _case(
                    (f"{name}-pass-role-{index}", principal), ("iam:PassRole", role), service, True
                )
            )
            cases.append(
                _case(
                    (f"{name}-pass-wrong-service-{index}", principal),
                    ("iam:PassRole", role),
                    {"iam:PassedToService": "lambda.amazonaws.com"},
                    False,
                )
            )
        for index, role in enumerate(contract[foreign]):
            cases.append(
                _case(
                    (f"{name}-pass-foreign-role-{index}", principal),
                    ("iam:PassRole", role),
                    service,
                    False,
                )
            )
    return cases


def _data_cases(contract: Node) -> list[Node]:
    table, production = contract["canary_table_arn"], contract["control_plane_table_arn"]
    seeder, fence = contract["principals"]["seeder"], contract["principals"]["fence"]
    tenant = {"dynamodb:LeadingKeys": ["TENANT#_GLOBAL"]}
    fence_key = {"dynamodb:LeadingKeys": ["ENV#prod#FENCE"]}
    semaphore = {"dynamodb:LeadingKeys": ["ENV#prod#SEMAPHORE"]}
    return [
        _case(("seeder-canary", seeder), ("dynamodb:PutItem", table), tenant, True),
        _case(("seeder-production-table", seeder), ("dynamodb:PutItem", production), tenant, False),
        _case(("seeder-wrong-key", seeder), ("dynamodb:PutItem", table), fence_key, False),
        _case(("fence-read", fence), ("dynamodb:GetItem", production), fence_key, True),
        _case(("fence-semaphore-read", fence), ("dynamodb:GetItem", production), semaphore, True),
        _case(("fence-update", fence), ("dynamodb:UpdateItem", production), fence_key, True),
        _case(
            ("fence-semaphore-update", fence), ("dynamodb:UpdateItem", production), semaphore, False
        ),
        _case(("fence-canary-table", fence), ("dynamodb:UpdateItem", table), fence_key, False),
    ]


def authorization_matrix(contract: Node) -> list[Node]:
    """Returns: casos positivos/negativos por principal, recurso e contexto exatos."""
    cases = _task_cases(contract) + _pass_role_cases(contract) + _data_cases(contract)
    require(
        len(cases) <= 256 and len({item["case_id"] for item in cases}) == len(cases),
        "iam=matrix_limit",
    )
    return sorted(cases, key=lambda item: item["case_id"])


def fingerprint(value: Node) -> str:
    """Returns: fingerprint canônico do conteúdo observado."""
    return object_hash(value)


def revision_arn(value: Any, context: Node, mode: str) -> str:
    """Returns: ARN de revisão positiva na família, conta e região governadas."""
    prefix = (
        f"arn:aws:ecs:{context['region']}:{context['account_id']}:task-definition/"
        f"cnesdata-prod-processor-{mode}:"
    )
    require(
        type(value) is str
        and value.startswith(prefix)
        and bool(re.fullmatch(r"[1-9][0-9]*", value[len(prefix) :])),
        "revision=identity",
    )
    return value


def retained_inputs(context: Node, revisions: Node) -> Node:
    """Returns: active preservado e retained ordenado, sem ativar a candidata."""
    require(set(revisions) == {"unit", "recovery", "audit"}, "revision=modes")
    for mode, arn in revisions.items():
        revision_arn(arn, context, mode)
    before = context["processor_retained_unit_task_definition_arns"]
    require(
        type(before) is list and len(before) <= 128 and before == sorted(set(before)),
        "retained=shape",
    )
    for arn in before:
        revision_arn(arn, context, "unit")
    active = context["processor_active_unit_task_definition_arn"]
    require(active is None or active in before, "retained=active_missing")
    after = sorted(set(before) | {revisions["unit"]})
    require(len(after) <= 128, "retained=limit")
    return {"active_before": active, "retained_before": before, "retained_after": after}


def verify_revisions(context: Node, revisions: Node, api: Any) -> Node:
    """Returns: revisões reais conferidas; API não oferece registro nesta fronteira."""
    retained = retained_inputs(context, revisions)
    expected = context["task_definitions"]
    require(set(expected) == set(revisions), "revision=contract_modes")
    fingerprints = []
    for mode, arn in revisions.items():
        observed = api.describe_task_definition(arn)
        require(
            observed["taskDefinitionArn"] == arn and observed["status"] == "ACTIVE",
            "revision=readback_identity",
        )
        require(set(expected[mode]) >= TASK_REQUIRED, "revision=incomplete_contract")
        require(set(observed) <= set(expected[mode]) | TASK_METADATA, "revision=unknown_functional")
        definition = {key: observed[key] for key in expected[mode]}
        require(definition == expected[mode], "revision=functional_drift")
        containers = definition["containerDefinitions"]
        require(
            len(containers) == 1 and containers[0]["image"] == context["ecr_image"],
            "revision=image",
        )
        fingerprints.append({"address": arn, "fingerprint_sha256": fingerprint(observed)})
    for arn in retained["retained_before"]:
        observed = api.describe_task_definition(arn)
        require(
            observed["taskDefinitionArn"] == arn and observed["status"] == "ACTIVE",
            "revision=retained_inactive",
        )
    return retained | {
        "processor_task_definition_arns": revisions,
        "task_fingerprints": sorted(fingerprints, key=lambda item: item["address"]),
    }


def _complete(snapshot: Node, expected: Node) -> None:
    require(
        set(snapshot) == {"policies", "trust", "roles", "evaluations", "network"},
        "iam=readback_fields",
    )
    require(
        all(
            type(snapshot[key]) is dict and snapshot[key]
            for key in ("policies", "trust", "roles", "network")
        ),
        "iam=incomplete",
    )
    raw = snapshot["evaluations"]
    require(type(raw) is list and 0 < len(raw) <= 256, "iam=matrix")
    require(all(type(item) is dict for item in raw), "iam=matrix_items")
    evaluations = cast("list[Node]", raw)
    require(len({item["case_id"] for item in evaluations}) == len(evaluations), "iam=duplicate")
    require({item["expected"] for item in evaluations} == {True, False}, "iam=positive_negative")
    require(
        all(
            type(item["expected"]) is bool
            and type(item["allowed"]) is bool
            and item["expected"] == item["allowed"]
            for item in evaluations
        ),
        "iam=verdict",
    )
    require(snapshot == expected, "iam=contract_mismatch")


def wait_for_iam(readback: Any, clock: Clock, expected: Node) -> Node:
    """Returns: duas observações completas estáveis; nunca prova de execução real."""
    started = clock.monotonic()
    first: tuple[float, str] | None = None
    interval = 2.0
    while True:
        elapsed = clock.monotonic() - started
        require(0 <= elapsed < 300, "iam=timeout")
        try:
            observed = readback(started + 300)
        except IamNotReady:
            observed = None
        elapsed = clock.monotonic() - started
        require(0 <= elapsed < 300, "iam=timeout")
        if observed is not None:
            _complete(observed, expected)
            digest = fingerprint(observed)
            if first is None:
                first = elapsed, digest
            require(digest == first[1], "iam=readback_divergence")
            if elapsed - first[0] >= 10:
                return {
                    "proof_kind": "iam-readback-and-evaluation",
                    "first_readback_seconds": first[0],
                    "second_readback_seconds": elapsed,
                    "readback_sha256": digest,
                    "matrix_sha256": fingerprint({"evaluations": observed["evaluations"]}),
                    "deadline_seconds": 300,
                }
        jitter = clock.jitter()
        require(0 <= jitter <= 0.25, "iam=jitter")
        delay = min(interval + jitter, 10.0, 300 - elapsed)
        clock.sleep(delay)
        interval = min(interval * 2, 10.0)


def verify_role_producer(address: str, expression: Node, witness: Node, changes: Node) -> str:
    """Raises: ValueError sem produtor canônico conhecido para role.id simbólico."""
    module = address.split(".aws_iam_role_policy.", maxsplit=1)[0]
    resolver = expression.get("source_resolver")
    if resolver is not None:
        producer = resolver.role_producer(address)
        require(producer in witness["producers"] and producer in changes, "graph=role_producer")
        return producer
    references = expression["role"]["references"]
    pattern = r"(aws_iam_role\.[A-Za-z0-9_]+\[0\])(?:\.[A-Za-z0-9_]+)?"
    targets = {
        module + "." + match[1] for ref in references if (match := re.fullmatch(pattern, ref))
    }
    require(len(targets) == 1, "graph=role_expression")
    producer = targets.pop()
    require(
        producer in witness["producers"]
        and producer in changes
        and changes[producer]["type"] == "aws_iam_role",
        "graph=role_producer",
    )
    change = changes[producer]["change"]
    name = change["after"].get("name")
    require(
        type(name) is str
        and name.startswith("cnesdata-prod-")
        and not change.get("after_unknown", {}).get("name"),
        "graph=role_name",
    )
    return producer


def _prove_iam(result: Node, api: Any, governed: Node) -> Node:
    outputs, clock = result["outputs"], governed["clock"]
    require(api.read_network() == governed["network"], "network=readback")
    current = api.read_active_cloudfront_evidence()
    require(
        current["plan"] == "FREE"
        and current["status"] == "ACTIVE"
        and all(current[key] == value for key, value in governed["cloudfront"].items())
        and timestamp(current["observed_at"]) <= clock.utcnow() < timestamp(current["expires_at"]),
        "cost=live_cloudfront_binding",
    )
    context = governed["iam_context"] | {
        "revisions": outputs["processor_task_definition_arns"],
        "retained_before": outputs["retained_before"],
    }
    cases = authorization_matrix(context)
    roles = {
        item["address"]: item["fingerprint_sha256"]
        for item in outputs["resource_fingerprints"]
        if item["address"] in governed["iam_role_addresses"]
    }
    policies = {
        item["address"]: item["fingerprint_sha256"] for item in outputs["grant_fingerprints"]
    }
    require(
        set(roles) == set(governed["iam_role_addresses"]) and roles and policies,
        "iam=incomplete_fingerprints",
    )
    expected = {
        "policies": policies,
        "trust": roles,
        "roles": roles,
        "network": governed["network"],
        "evaluations": [case | {"allowed": case["expected"]} for case in cases],
    }
    return wait_for_iam(lambda deadline: api.read_iam(cases, deadline), clock, expected)
