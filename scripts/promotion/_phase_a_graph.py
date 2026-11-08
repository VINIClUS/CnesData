"""Confere grafo IAM aprovado e produz sua vista pública reduzida."""

from __future__ import annotations

import hashlib
import re
from typing import Any

from release.verify_candidate import require
from scripts.promotion._canonical import STATE_LIMIT, canonical_bytes, decode_json, object_hash
from scripts.promotion._iam_source import SourceResolver
from scripts.promotion._plan_contract import closed
from scripts.promotion.verify_task_revisions import verify_role_producer

type Node = dict[str, Any]


def _configuration(module: Node, prefix: str = "") -> Node:
    result = {prefix + item["address"]: item for item in module.get("resources", [])}
    for name, call in module.get("module_calls", {}).items():
        result |= _configuration(call["module"], prefix + "module." + name + ".")
    return result


def _policy_witness(resource: Node, witness: Node, expressions: Node, scope: Node) -> None:
    changes = scope["changes"]
    closed(witness, "address references producers before_policy_sha256")
    require(
        sorted(set(expressions["policy"]["references"])) == witness["references"]
        and witness["producers"] == sorted(set(witness["producers"]))
        and witness["producers"],
        "graph=expression_edges",
    )
    for producer in witness["producers"]:
        require(
            producer in changes and "create" in changes[producer]["change"]["actions"],
            "graph=producer_creation",
        )
        require(
            not changes[producer]["change"].get("after_unknown", {}).get("name"),
            "graph=producer_name",
        )
    address, change = resource["address"], resource["change"]
    task = "module.processing.aws_ecs_task_definition.processor"
    required = {
        "module.processing.aws_iam_role_policy.step_functions_ecs[0]": task + '["unit"]',
        "module.processing.aws_iam_role_policy.canary_verifier_run_task[0]": task + '["audit"]',
    }
    require(
        address not in required or required[address] in witness["producers"], "graph=task_producer"
    )
    before = change.get("before")
    previous = object_hash(decode_json(before["policy"].encode())) if before else None
    require(previous == witness["before_policy_sha256"], "graph=before_grants")
    resolver = scope["resolver"]
    if not isinstance(resolver, SourceResolver):
        raise ValueError("iam=resolver_missing")
    require(resolver.precheck(address) == set(witness["producers"]), "iam=source_producers")
    if change["after_unknown"].get("role") is True:
        verify_role_producer(address, expressions | {"source_resolver": resolver}, witness, changes)


def validate_graph(plan: Node, approved: Node, source_files: Node, resolver: Any = None) -> Node:
    """Returns: prova de fonte/configuração independente para IAM conhecido e simbólico."""
    closed(approved, "source_files configuration_sha256 unknown_policies known_policies")
    _validate_source_files(approved, source_files)
    require(
        object_hash(plan["configuration"], STATE_LIMIT) == approved["configuration_sha256"],
        "graph=configuration",
    )
    configuration = _configuration(plan["configuration"]["root_module"])
    changes = {
        item["address"]: item
        for item in plan["resource_changes"]
        if item["change"]["actions"] != ["no-op"]
    }
    witnesses = {item["address"]: item for item in approved["unknown_policies"]}
    require(
        [item["address"] for item in approved["unknown_policies"]] == sorted(witnesses)
        and len(witnesses) <= 32,
        "graph=witness_order",
    )
    seen = set()
    for address, item in changes.items():
        if item["type"] != "aws_iam_role_policy":
            continue
        change = item["change"]
        unknown = _policy_unknown(change)
        if unknown.get("policy") is True or unknown.get("role") is True:
            require(address in witnesses, "graph=missing_witness")
            local = re.sub(r"\[[^]]+\]", "", address)
            _policy_witness(
                item,
                witnesses[address],
                configuration[local]["expressions"],
                {"changes": changes, "resolver": resolver},
            )
            seen.add(address)
        if unknown.get("policy") is True:
            require(change["after"].get("policy") is None, "graph=unknown_policy_value")
        else:
            require(
                address in approved["known_policies"]
                and object_hash(decode_json(change["after"]["policy"].encode()))
                == approved["known_policies"][address],
                "graph=known_policy",
            )
    require(seen == set(witnesses), "graph=unused_witness")
    return approved | {"governance_sha256": object_hash(approved)}


def _validate_source_files(approved: Node, source_files: Node) -> None:
    entries: list[Node] = approved["source_files"]
    require(
        type(entries) is list
        and 0 < len(entries) <= 32
        and entries == sorted(entries, key=lambda item: item["path"]),
        "graph=source_entries",
    )
    require(len({item["path"] for item in entries}) == len(entries), "graph=source_duplicates")
    for entry in entries:
        closed(entry, "path sha256")
        require(
            entry["path"] in source_files
            and hashlib.sha256(source_files[entry["path"]]).hexdigest() == entry["sha256"],
            "graph=source_bytes",
        )


def redacted_plan(plan_json: Node, source_sha: str, json_sha256: str) -> Node:
    """Returns: vista pública sem values, outputs, ARNs ou chaves de instâncias."""
    changes = [
        {
            "address_shape": _redacted_address(item["address"]),
            "address_sha256": hashlib.sha256(item["address"].encode()).hexdigest(),
            "type": item["type"],
            "actions": item["change"]["actions"],
        }
        for item in plan_json["resource_changes"]
        if item["change"]["actions"] not in (["no-op"], ["read"])
    ]
    value = {
        "view_version": 1,
        "repository": "VINIClUS/CnesData",
        "source_sha": source_sha,
        "purpose": "phase-a",
        "plan_json_sha256": json_sha256,
        "changes": sorted(
            changes, key=lambda item: (item["address_shape"], item["address_sha256"])
        ),
    }
    require(
        not re.search(rb"arn:|https?://|-----BEGIN|AKIA[A-Z0-9]{16}", canonical_bytes(value)),
        "redacted=private_value",
    )
    return value


def review_graph(plan: Node, context: Node) -> Node:
    unknown = any(
        item["type"] == "aws_iam_role_policy"
        and any(
            item["change"].get("after_unknown", {}).get(key) is True for key in ("policy", "role")
        )
        for item in plan["resource_changes"]
    )
    resolver = SourceResolver(plan, context["source_files"], context) if unknown else None
    if resolver is not None:
        context["iam_resolver"] = resolver
    return validate_graph(plan, context["approved_graph"], context["source_files"], resolver)


def resolved_change(resource: Node, context: Node, unknown: set[str]) -> Node | None:
    require("approved_graph" in context and "graph_proof" in context, "delta=graph_required")
    require(
        context["graph_proof"]
        == context["approved_graph"]
        | {"governance_sha256": object_hash(context["approved_graph"])},
        "delta=graph_required",
    )
    require(unknown <= {"policy", "role"}, "delta=unknown_functional")
    resolver = context.get("iam_resolver")
    if not isinstance(resolver, SourceResolver):
        raise ValueError("iam=resolver_missing")
    if "readback_resources" not in context:
        return None
    if resolver.readbacks is None:
        resolver.bind(context["readback_resources"])
    address, change = resource["address"], resource["change"]
    after = context["readback_resources"][address]
    known = {
        key: value
        for key, value in change["after"].items()
        if key not in unknown | {"arn", "id", "name_prefix", "tags_all"}
    }
    require(all(after.get(key) == value for key, value in known.items()), "delta=known_readback")
    resolver.verify(address, after)
    return after


def _redacted_address(address: str) -> str:
    pattern = r'\[(?:"(?:\\.|[^"\\])*"|[0-9]+)\]'
    shape = re.sub(pattern, "[redacted]", address)
    require(
        bool(re.fullmatch(r"[A-Za-z0-9_.-]+(?:\[redacted\][A-Za-z0-9_.-]*)*", shape)),
        "redacted=address_shape",
    )
    return shape


def _policy_unknown(change: Node) -> Node:
    unknown = change.get("after_unknown", {})
    require(
        not any(
            value
            for key, value in unknown.items()
            if key not in {"id", "policy", "role", "name_prefix"}
        ),
        "graph=unknown_field",
    )
    return unknown
