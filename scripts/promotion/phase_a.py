"""Valida o delta Phase A e reconcilia retries antes de emitir evidência."""

from __future__ import annotations

import json
import re
from typing import Any

from release.verify_candidate import require

type Node = dict[str, Any]
TASK_PREFIX = "module.processing.aws_ecs_task_definition.processor"
SFN_POLICY = "module.processing.aws_iam_role_policy.step_functions_ecs[0]"
VERIFIER_POLICY = "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]"
CANARY_ROLES = {"canary_task", "canary_execution", "canary_seeder", "canary_verifier",
                "promotion_fence_operator"}
STORAGE_TYPES = {"aws_dynamodb_table", "aws_s3_bucket", "aws_s3_bucket_public_access_block",
                 "aws_s3_bucket_ownership_controls", "aws_s3_bucket_versioning",
                 "aws_s3_bucket_server_side_encryption_configuration",
                 "aws_s3_bucket_object_lock_configuration", "aws_s3_bucket_lifecycle_configuration",
                 "aws_s3_bucket_policy"}
COMPUTED = {"arn", "arn_without_revision", "id", "revision", "unique_id", "create_date",
            "tags_all", "latest_stream_arn", "latest_stream_label", "stream_arn", "stream_label",
            "bucket_domain_name", "bucket_regional_domain_name", "hosted_zone_id", "region"}


def retry_action(identity: Node, observation: Node) -> str:
    """Returns: decisão sem mutação; efeitos ambíguos exigem recuperação auditada."""
    require(set(identity) == {"release_id", "candidate_sha256", "image_digest",
                              "plan_binding_sha256"}, "retry=identity_fields")
    require(observation.get("identity") == identity, "recovery_required=identity_mismatch")
    status = observation.get("status")
    if status == "applied":
        return "read-only"
    require(status == "planned" and observation.get("effects_confirmed") is False
            and observation.get("state_matches") is True
            and observation.get("plan_valid") is True, "recovery_required=partial_or_unknown")
    return "apply-exact-plan"


def _kind(address: str) -> str:
    task = re.fullmatch(re.escape(TASK_PREFIX) + r'\["(unit|recovery|audit)"\]', address)
    if task:
        return "task"
    if address == SFN_POLICY:
        return "sfn"
    if address == VERIFIER_POLICY:
        return "verifier"
    role = re.fullmatch(r"module\.runtime_iam\.(aws_iam_role|aws_iam_role_policy)\.(\w+)\[0\]",
                        address)
    if role and role[2] in CANARY_ROLES:
        return "role" if role[1] == "aws_iam_role" else "policy"
    storage = re.fullmatch(r"module\.processing\.(\w+)\.canary\[0\]", address)
    if storage and storage[1] in STORAGE_TYPES:
        return "storage"
    raise ValueError(f"delta=address_forbidden address={address}")


def _functional(value: Node) -> Node:
    return {key: item for key, item in value.items() if key not in COMPUTED}


def _task_delta(before: Node | None, after: Node, context: Node) -> None:
    require(after.get("skip_destroy") is True and after.get("track_latest") is False,
            "delta=revision_retention")
    containers = json.loads(after["container_definitions"])
    require(len(containers) == 1 and containers[0]["image"] == context["ecr_image"],
            "delta=task_image")
    if before is None:
        return
    old = json.loads(before["container_definitions"])
    require(len(old) == 1, "delta=old_containers")
    old[0]["image"] = containers[0]["image"]
    require(old == containers, "delta=container_functional_change")
    first, second = _functional(before), _functional(after)
    first.pop("container_definitions", None)
    second.pop("container_definitions", None)
    require(first == second, "delta=task_functional_change")


def _sfn_delta(before: Node | None, after: Node, context: Node) -> None:
    policy = json.loads(after["policy"])
    statements = policy["Statement"]
    runs = [item for item in statements if item.get("Action") == "ecs:RunTask"]
    require(len(runs) == 1 and set(runs[0]) == {"Effect", "Action", "Resource"}
            and runs[0]["Effect"] == "Allow", "delta=sfn_run_task")
    resources = sorted(set(context["retained_before"]) | {context["candidate_unit_arn"]})
    require(runs[0]["Resource"] == resources, "delta=sfn_resources")
    if before is not None:
        previous = json.loads(before["policy"])
        old_other = [item for item in previous["Statement"]
                     if item.get("Action") != "ecs:RunTask"]
        new_other = [item for item in statements if item.get("Action") != "ecs:RunTask"]
        require(old_other == new_other and previous.get("Version") == policy.get("Version"),
                "delta=sfn_other_grants")
        require({key: value for key, value in before.items() if key not in COMPUTED | {"policy"}}
                == {key: value for key, value in after.items()
                    if key not in COMPUTED | {"policy"}}, "delta=sfn_role")


def _verifier_delta(after: Node, context: Node) -> None:
    require(after["role"] == context["canary_verifier_role_id"], "delta=verifier_role")
    policy = json.loads(after["policy"])
    require(policy == {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Action": "ecs:RunTask", "Resource": context["candidate_audit_arn"],
        "Condition": {"ArnEquals": {"ecs:cluster": context["cluster_arn"]}},
    }]}, "delta=verifier_grants")


def _approved(change: Node, address: str, context: Node) -> None:
    require(address in context.get("resources", {}), "delta=missing_source_contract")
    require(_functional(change["after"]) == context["resources"][address],
            "delta=source_contract_mismatch")
    actions = change["actions"]
    require(actions in (["create"], ["update"], ["create", "delete"], ["delete", "create"]),
            "delta=actions")
    if "delete" in actions:
        require(address.startswith(TASK_PREFIX) and change["before"].get("skip_destroy") is True,
                "delta=destructive_change")


def _canary_delta(before: Node | None, after: Node, kind: str) -> None:
    if before is None:
        return
    allowed = {"tags", "assume_role_policy"} if kind == "role" else {"policy"}
    previous = {key: value for key, value in _functional(before).items() if key not in allowed}
    current = {key: value for key, value in _functional(after).items() if key not in allowed}
    require(previous == current, "delta=canary_functional_change")


def _changed_resource(resource: Node, context: Node) -> None:
    change, address = resource["change"], resource["address"]
    kind = _kind(address)
    _approved(change, address, context)
    before, after = change["before"], change["after"]
    if kind == "task":
        _task_delta(before, after, context)
    elif kind == "sfn":
        _sfn_delta(before, after, context)
    elif kind == "verifier":
        _verifier_delta(after, context)
    elif kind == "storage":
        require(change["actions"] == ["create"], "delta=storage_reconfiguration")
    else:
        _canary_delta(before, after, kind)


def verify_delta(plan: Node, context: Node) -> None:
    """Raises: ValueError para endereço/campo fora do contrato imutável de origem."""
    resources = plan["resource_changes"]
    require(len({item["address"] for item in resources}) == len(resources), "delta=duplicates")
    for resource in resources:
        if resource["mode"] != "managed":
            continue
        change = resource["change"]
        if change["actions"] == ["no-op"]:
            require(change["before"] == change["after"], "delta=false_noop")
            continue
        _changed_resource(resource, context)
