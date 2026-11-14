"""Oracle e recibo do application-prepare mode-canary: oito endereços, commitments e prazos."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from fnmatch import fnmatchcase
from itertools import pairwise
from typing import Any, Final, cast

from scripts.promotion.mode_canary_contract import (
    ALIASES,
    ContextManifest,
    ContractError,
    parse_context_manifest,
    session_names,
)

type Node = dict[str, Any]
MODE: Final = "mode-canary"
PROOF_SCHEMA: Final = "cnesdata.mode-canary-prepare-proof/v1"
RECEIPT_SCHEMA: Final = "cnesdata.mode-canary-prepare-receipt/v1"
OUTPUT: Final = "mode_canary_capabilities"
PREFIX: Final = "module.runtime_iam"
ADDRESSES: Final = tuple(
    f"{PREFIX}.{kind}.{alias}[0]" for alias in ALIASES
    for kind in ("aws_iam_role", "aws_iam_role_policy")
)
_LIFECYCLE: Final = {"initial": frozenset({"create", "no-op"}),
                     "established": frozenset({"update", "no-op"})}
_VERSION: Final = "2012-10-17"
_SHAPES: Final = {
    "revisions": {"audit", "recovery", "unit"}, "task_role_arns": {"audit", "recovery", "unit"},
    "network": {"public_subnet_ids", "security_group_id"}, "schedule_arns": {"audit", "recovery"},
    "alarm_arns": {"audit_outbox_backlog", "audit_repeated_failure", "recovery_repeated_failure"},
    "scope": {"audit_object_keys", "output_object_keys", "raw_object_keys", "run_ids", "tenant_id"},
}
_RESOURCE_FIELDS: Final = frozenset({
    "table_arn", "index_arn", "data_bucket_arn", "audit_bucket_arn", "cluster_arn", "machine_arn",
    "execution_role_arn", "log_group_arn", *_SHAPES,
})
_KEY_CHARS: Final = re.compile(r"[0-9a-z-]+")
_DDB: Final = {
    "Seed": ("GetItem", "PutItem"),
    "Dispatch": ("GetItem", "Query", "PutItem", "UpdateItem", "ConditionCheckItem"),
    "Fence": ("GetItem", "ConditionCheckItem"), "Semaphore": ("GetItem", "UpdateItem"),
    "Attempts": ("UpdateItem",), "Observe": ("GetItem", "Query"), "Cursor": ("GetItem",),
}
_FIXED_KEYS: Final = {"Fence": "ENV#prod#FENCE", "Semaphore": "ENV#prod#SEMAPHORE",
                      "Attempts": "ENV#prod#ATTEMPTS", "Cursor": "OUTBOX#CURSOR"}
_S3: Final = {
    "Raw": (("s3:PutObject", "s3:GetObject"), "data_bucket_arn", "raw_object_keys"),
    "Outputs": (("s3:GetObject",), "data_bucket_arn", "output_object_keys"),
    "Audit": (("s3:GetObject", "s3:GetObjectRetention"), "audit_bucket_arn", "audit_object_keys"),
}
_CLOSED_POLICY: Final = {"Version": _VERSION,
                         "Statement": [{"Effect": "Deny", "Action": "*", "Resource": "*"}]}
_AUTHORIZATION: Final = ("name", "role", "policy", "assume_role_policy", "max_session_duration")
_INPUT_FIELDS: Final = frozenset({
    "schema_version", "context_kind", "application_principal_arn", "owner", "context_manifest",
    "delegation_sessions", "resources", "enabled_until",
})
_ENVELOPE_FIELDS: Final = frozenset({
    "mode", "context_kind", "source_sha", "source_closure_sha256", "shared_fragment_sha256",
    "activation_sha256", "owner", "context_manifest_sha256", "predecessor_checkpoint_sha256",
    "preparation_admitted_at", "preparation_deadline", "mode_canary_inputs_sha256",
})
_PRINCIPAL: Final = re.compile(r"arn:aws:iam::([0-9]{12}):role/[A-Za-z0-9+=,.@_-]+")
_HEX64: Final = re.compile(r"[0-9a-f]{64}")
_STAMP: Final = "%Y-%m-%dT%H:%M:%SZ"
_READBACK_SPACING: Final = timedelta(seconds=10)


def _require(condition: bool, field: str) -> None:
    if not condition:
        raise ContractError(f"reason=invalid field={field}")


def canonical_text(value: Any) -> str:
    """Returns: JSON ordenado, compacto, UTF-8 sem escape e LF terminal único."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _moment(value: Any, field: str) -> datetime:
    try:
        return datetime.strptime(cast("str", value), _STAMP).replace(tzinfo=UTC)
    except (TypeError, ValueError) as error:
        raise ContractError(f"reason=invalid field={field}") from error


def _role_name(alias: str) -> str:
    return "cnesdata-prod-" + alias.replace("_", "-")


def _closed(context_kind: str, alias: str) -> bool:
    return context_kind == "readiness" and alias == "mode_audit_launch"


def _check_resources(value: Any) -> None:
    _require(type(value) is dict and set(cast("Node", value)) == _RESOURCE_FIELDS, "resources")
    resources = cast("Node", value)
    for key, shape in _SHAPES.items():
        item = resources[key]
        _require(type(item) is dict and set(cast("Node", item)) == shape, f"resources.{key}")
    scope = cast("Node", resources["scope"])
    names = [scope["tenant_id"], *cast("list[Any]", scope["run_ids"])]
    _require(all(type(name) is str and _KEY_CHARS.fullmatch(name) for name in names),
             "resources.scope")


def check_mode_canary_input(value: Any) -> ContextManifest:
    """Returns: Manifest de admissão do input `mode_canary` fechado.

    Raises: ContractError: Campos extras, owner/contexto/recursos/sessões divergentes do manifest.
    """
    _require(type(value) is dict and set(cast("Node", value)) == _INPUT_FIELDS, "mode_canary")
    fields = cast("Node", value)
    manifest = parse_context_manifest(fields["context_manifest"])
    kind = manifest.fields["context_kind"]
    _require(fields["schema_version"] == 2 and fields["context_kind"] == kind, "context_kind")
    _require(fields["owner"] == manifest.fields["owner"], "owner")
    principal = fields["application_principal_arn"]
    _require(type(principal) is str and _PRINCIPAL.fullmatch(principal) is not None,
             "application_principal_arn")
    _require(_sha(canonical_text(fields["resources"])) == manifest.fields["resources_sha256"],
             "resources")
    _check_resources(fields["resources"])
    _require(fields["delegation_sessions"] == session_names(manifest), "delegation_sessions")
    enabled = _moment(fields["enabled_until"], "enabled_until")
    _require(enabled > _moment(manifest.fields["admitted_at"], "admitted_at"), "enabled_until")
    return manifest


def _changes(plan: Node) -> dict[str, Node]:
    managed = [item for item in plan["resource_changes"] if item["mode"] == "managed"]
    changes = {item["address"]: item for item in managed}
    _require(len(changes) == len(managed), "resource_changes")
    return changes


def _action(change: Node, lifecycle: str, address: str) -> str:
    actions = change["actions"]
    _require(len(actions) == 1 and actions[0] in _LIFECYCLE[lifecycle], address)
    _require(not change.get("importing") and not change.get("replace_paths"), address)
    unknown = change.get("after_unknown") or {}
    _require(not any(unknown.get(name) for name in _AUTHORIZATION), address)
    return cast("str", actions[0])


def _statements(text: Any, field: str) -> list[Node]:
    try:
        document = json.loads(cast("str", text))
    except (TypeError, ValueError) as error:
        raise ContractError(f"reason=invalid field={field}") from error
    _require(type(document) is dict and type(document.get("Statement")) is list, field)
    return cast("list[Node]", document["Statement"])


def _jsonencode(document: Node) -> str:
    text = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    for char in "<>&\u2028\u2029":
        text = text.replace(char, f"\\u{ord(char):04x}")
    return text


def _allow(sid: str | None, actions: list[str], resource: Any, condition: Node) -> Node:
    rule = {"Effect": "Allow", "Action": actions, "Resource": resource, "Condition": condition}
    return rule if sid is None else {"Sid": sid} | rule


def _partitions(scope: Node) -> list[str]:
    tenant = "TENANT#" + scope["tenant_id"].encode().hex()
    return [tenant, *(f"{tenant}#RUN#{run.encode().hex()}" for run in scope["run_ids"])]


def _dynamodb(name: str, resources: Node, expiry: Node) -> Node:
    fixed = _FIXED_KEYS.get(name)
    keys = [fixed] if fixed else _partitions(resources["scope"])
    condition = expiry | {"ForAllValues:StringEquals": {"dynamodb:LeadingKeys": keys},
                          "Null": {"dynamodb:LeadingKeys": "false"}}
    actions = [f"dynamodb:{action}" for action in _DDB[name]]
    return _allow(name, actions, resources["table_arn"], condition)


def _objects_rule(name: str, resources: Node, expiry: Node) -> Node:
    actions, bucket, keys = _S3[name]
    targets = [f"{resources[bucket]}/{key}" for key in resources["scope"][keys]]
    return _allow(name, list(actions), targets, expiry)


def _ecs(resources: Node, mode: str, expiry: Node) -> list[Node]:
    cluster = expiry | {"ArnEquals": {"ecs:cluster": resources["cluster_arn"]}}
    passed = expiry | {"StringEquals": {"iam:PassedToService": "ecs-tasks.amazonaws.com"}}
    roles = [resources["task_role_arns"][mode], resources["execution_role_arn"]]
    return [_allow("Launch", ["ecs:RunTask"], resources["revisions"][mode], cluster),
            _allow("Roles", ["iam:PassRole"], roles, passed)]


def _tasks(resources: Node, actions: list[str], expiry: Node) -> Node:
    cluster = expiry | {"ArnEquals": {"ecs:cluster": resources["cluster_arn"]}}
    tasks = resources["cluster_arn"].replace(":cluster/", ":task/") + "/*"
    return _allow("Tasks", actions, tasks, cluster)


def _machine(machine: str, dispatch: bool, expiry: Node) -> list[Node]:
    describe, executions = ["states:DescribeStateMachine"], ["states:DescribeExecution"]
    if dispatch:
        describe, executions = [*describe, "states:StartExecution"], [
            *executions, "states:StopExecution"]
    target = machine.replace(":stateMachine:", ":execution:") + ":*"
    return [_allow("Machine", describe, machine, expiry),
            _allow("Executions", executions, target, expiry)]


def _observer_reads(resources: Node, readiness: bool, expiry: Node) -> list[Node]:
    log_group = resources["log_group_arn"]
    reads = [
        (["ecs:DescribeTaskDefinition"], ["*"]),
        (["scheduler:GetSchedule"], [resources["schedule_arns"][k]
                                     for k in sorted(resources["schedule_arns"])]),
        (["cloudwatch:DescribeAlarms"], [resources["alarm_arns"][k]
                                         for k in sorted(resources["alarm_arns"])]),
        (["logs:GetLogEvents"], [f"{log_group}:log-stream:{mode}/processor/*"
                                 for mode in ("unit", "recovery", "audit")]),
    ]
    rules = [_allow(None, actions, targets, expiry) for actions, targets in reads]
    if readiness:
        cluster = expiry | {"ArnEquals": {"ecs:cluster": resources["cluster_arn"]}}
        rules += [_allow(None, ["ecs:ListTasks"], "*", cluster),
                  _allow(None, ["cloudwatch:GetMetricData"], "*", expiry)]
    return rules


def _grants(fields: Node) -> dict[str, list[Node]]:
    resources = cast("Node", fields["resources"])
    expiry = {"DateLessThan": {"aws:CurrentTime": fields["enabled_until"]}}
    ddb = {name: _dynamodb(name, resources, expiry) for name in _DDB}
    s3 = {name: _objects_rule(name, resources, expiry) for name in _S3}
    machine, describe = resources["machine_arn"], ["ecs:DescribeTasks"]
    return {
        "mode_seed": [ddb["Seed"], s3["Raw"]],
        "mode_dispatch": [
            ddb["Dispatch"], ddb["Fence"], ddb["Semaphore"], ddb["Attempts"],
            *_ecs(resources, "recovery", expiry),
            _tasks(resources, [*describe, "ecs:StopTask"], expiry),
            *_machine(machine, True, expiry),
        ],
        "mode_audit_launch": [*_ecs(resources, "audit", expiry),
                              _tasks(resources, describe, expiry)],
        "mode_observe": [
            ddb["Observe"], ddb["Cursor"], s3["Outputs"], s3["Audit"],
            _tasks(resources, describe, expiry), *_machine(machine, False, expiry),
            *_observer_reads(resources, fields["context_kind"] == "readiness", expiry),
        ],
    }


def _trust(fields: Node, alias: str) -> Node:
    closed = _closed(fields["context_kind"], alias)
    rule: Node = {"Effect": "Deny" if closed else "Allow", "Action": "sts:AssumeRole",
                  "Principal": {"AWS": fields["application_principal_arn"]}}
    if not closed:
        rule["Condition"] = {
            "DateLessThan": {"aws:CurrentTime": fields["enabled_until"]},
            "StringEquals": {"sts:RoleSessionName": fields["delegation_sessions"][alias]},
        }
    return {"Version": _VERSION, "Statement": [rule]}


def _documents(fields: Node) -> dict[str, tuple[Node, Node]]:
    grants = _grants(fields)
    return {alias: (_trust(fields, alias),
                    _CLOSED_POLICY if _closed(fields["context_kind"], alias)
                    else {"Version": _VERSION, "Statement": grants[alias]})
            for alias in ALIASES}


def _listed(value: Any) -> list[Any]:
    return cast("list[Any]", value) if type(value) is list else [value]


def _grants_run_task(rule: Any) -> bool:
    if type(rule) is not dict:
        return True
    fields = {str(key).lower(): value for key, value in cast("Node", rule).items()}
    if str(fields.get("effect", "")).lower() != "allow":
        return False
    if "notaction" in fields:
        return True
    return any(fnmatchcase("ecs:runtask", str(item).lower())
               for item in _listed(fields.get("action")))


def _check_run_task(text: Any, policy: Node, alias: str) -> None:
    granted = [rule for rule in _statements(text, f"policy.{alias}") if _grants_run_task(rule)]
    expected = [rule for rule in policy["Statement"] if _grants_run_task(rule)]
    _require(granted == expected, f"run_task.{alias}")


def _delegation(
    changes: dict[str, Node], fields: Node, alias: str, documents: tuple[Node, Node],
) -> Node:
    role_address, policy_address = (f"{PREFIX}.{kind}.{alias}[0]"
                                    for kind in ("aws_iam_role", "aws_iam_role_policy"))
    role = cast("Node", changes[role_address]["change"]["after"])
    policy = cast("Node", changes[policy_address]["change"]["after"])
    name = _role_name(alias)
    _require(role["name"] == policy["role"] == name, f"name.{alias}")
    _require(role["max_session_duration"] == 3600, f"max_session_duration.{alias}")
    _require(policy["name"] == f"{name}-runtime", f"policy_name.{alias}")
    account = cast("re.Match[str]", _PRINCIPAL.fullmatch(fields["application_principal_arn"]))
    _require(role.get("arn") == f"arn:aws:iam::{account[1]}:role/{name}", f"arn.{alias}")
    _check_run_task(policy["policy"], documents[1], alias)
    trust, text = (_jsonencode(document) for document in documents)
    _require(role["assume_role_policy"] == trust, f"trust.{alias}")
    _require(policy["policy"] == text, f"policy.{alias}")
    closed = _closed(fields["context_kind"], alias)
    return {
        "role_arn": role["arn"], "role_name": name, "role_address": role_address,
        "policy_address": policy_address, "policy_name": policy["name"],
        "session_names": [] if closed else fields["delegation_sessions"][alias],
        "trust_sha256": _sha(trust), "policy_sha256": _sha(text),
    }


def _objects(changes: dict[str, Node]) -> Node:
    return {address: {name: changes[address]["change"]["after"].get(name)
                      for name in _AUTHORIZATION}
            for address in ADDRESSES}


def _expected_output(manifest: ContextManifest, fields: Node, delegations: Node) -> Node:
    return {
        "schema_version": 2, "context_kind": fields["context_kind"],
        "owner_sha256": fields["owner"]["owner_sha256"],
        "generation": fields["owner"]["generation"],
        "context_manifest_sha256": manifest.sha256,
        "activation_sha256": manifest.fields["activation_sha256"],
        "resources_sha256": manifest.fields["resources_sha256"], "delegations": delegations,
        "enabled_until": fields["enabled_until"],
    }


def validate_mode_canary_prepare(plan: Node, mode_canary: Node, lifecycle: str) -> Node:
    """Returns: Prova do plano: ações dos oito endereços, objetos e capabilities.

    Raises: ContractError: Endereço extra mutante, ação fora do ciclo, unknown, RunTask fora das
        duas exceções, trust/policy fora dos bytes derivados do input, output divergente.
    """
    _require(lifecycle in _LIFECYCLE, "lifecycle")
    manifest = check_mode_canary_input(mode_canary)
    changes = _changes(plan)
    _require(set(ADDRESSES) <= set(changes), "addresses")
    actions = {address: _action(changes[address]["change"], lifecycle, address)
               for address in ADDRESSES}
    for address, item in changes.items():
        _require(address in actions or item["change"]["actions"] == ["no-op"], address)
    documents = _documents(mode_canary)
    delegations = {alias: _delegation(changes, mode_canary, alias, documents[alias])
                   for alias in ALIASES}
    envelope = plan["planned_values"]["outputs"].get(OUTPUT) or {}
    _require(envelope.get("sensitive") is True, OUTPUT)
    expected = _expected_output(manifest, mode_canary, delegations)
    _require(envelope.get("value") == expected, OUTPUT)
    return {
        "schema": PROOF_SCHEMA, "context_kind": mode_canary["context_kind"],
        "lifecycle": lifecycle, "addresses": actions,
        "objects_sha256": _sha(canonical_text(_objects(changes))),
        "capabilities_sha256": _sha(canonical_text(expected)),
    }


def _check_envelope(envelope: Any, manifest: ContextManifest, mode_canary: Node) -> Node:
    _require(type(envelope) is dict and set(cast("Node", envelope)) == _ENVELOPE_FIELDS,
             "envelope")
    fields, source = cast("Node", envelope), manifest.fields
    _require(fields["mode"] == MODE, "mode")
    expected = {
        "context_kind": source["context_kind"], "source_sha": source["product_source_sha"],
        "source_closure_sha256": source["product_source_closure_sha256"],
        "activation_sha256": source["activation_sha256"], "owner": source["owner"],
        "context_manifest_sha256": manifest.sha256,
        "preparation_admitted_at": source["admitted_at"],
        "preparation_deadline": source["deadline"],
        "mode_canary_inputs_sha256": _sha(canonical_text(mode_canary)),
    }
    for name, value in expected.items():
        _require(fields[name] == value, name)
    _require(type(fields["shared_fragment_sha256"]) is str
             and _HEX64.fullmatch(fields["shared_fragment_sha256"]) is not None,
             "shared_fragment_sha256")
    predecessor = fields["predecessor_checkpoint_sha256"]
    _require(predecessor is None or (type(predecessor) is str
                                     and _HEX64.fullmatch(predecessor) is not None),
             "predecessor_checkpoint_sha256")
    return fields


def admit_prepare(envelope: Node, mode_canary: Node, previous: bytes | None) -> bytes:
    """Returns: Envelope canônico da variante mode-canary, sem campos F.

    Raises: ContractError: Envelope fora do fechado, divergente do keeper/manifest ou reentrada
        com bytes diferentes (prazo/checkpoint nunca renovam).
    """
    manifest = check_mode_canary_input(mode_canary)
    text = canonical_text(_check_envelope(envelope, manifest, mode_canary)).encode()
    _require(previous is None or previous == text, "reentry")
    return text


@dataclass(frozen=True, slots=True)
class PrepareEvidence:
    """Bytes de plano/estado, readbacks IAM e pendências observados pelo produtor shared."""

    plan_binary_sha256: str
    plan_json_sha256: str
    checkpoint_before: str
    checkpoint_after: str
    readbacks: list[Node]
    observed_at: str
    pending: list[str]
    unknown: list[str]


def _check_readbacks(evidence: PrepareEvidence, proof: Node, deadline: datetime) -> None:
    readbacks = evidence.readbacks
    _require(len(readbacks) >= 2, "readbacks")
    moments = [_moment(item.get("observed_at"), "readbacks") for item in readbacks]
    _require(all(later - earlier >= _READBACK_SPACING for earlier, later in pairwise(moments)),
             "readbacks")
    _require(moments[-1] <= deadline, "readbacks")
    _require(all(set(item) == {"observed_at", "objects_sha256"}
                 and item["objects_sha256"] == proof["objects_sha256"] for item in readbacks),
             "readbacks")


def prepare_receipt(envelope: bytes, proof: Node, evidence: PrepareEvidence) -> bytes:
    """Returns: Recibo canônico do prepare com os 20 campos fechados.

    Raises: ContractError: Menos de dois readbacks concordantes espaçados >=10s ou fora do prazo.
    """
    fields = json.loads(envelope)
    deadline = _moment(fields["preparation_deadline"], "preparation_deadline")
    _check_readbacks(evidence, proof, deadline)
    _require(_moment(evidence.observed_at, "observed_at") <= deadline, "observed_at")
    for name in ("plan_binary_sha256", "plan_json_sha256", "checkpoint_before",
                 "checkpoint_after"):
        _require(_HEX64.fullmatch(getattr(evidence, name)) is not None, name)
    return canonical_text({
        "schema": RECEIPT_SCHEMA, "mode": MODE, "context_kind": fields["context_kind"],
        "owner_sha256": fields["owner"]["owner_sha256"],
        "generation": fields["owner"]["generation"],
        "context_manifest_sha256": fields["context_manifest_sha256"],
        "activation_sha256": fields["activation_sha256"],
        "plan_binary_sha256": evidence.plan_binary_sha256,
        "plan_json_sha256": evidence.plan_json_sha256,
        "policy_proof_sha256": _sha(canonical_text(proof)),
        "source_closure_sha256": fields["source_closure_sha256"],
        "checkpoint_before": evidence.checkpoint_before,
        "checkpoint_after": evidence.checkpoint_after, "readbacks": evidence.readbacks,
        "capabilities_sha256": proof["capabilities_sha256"],
        "preparation_admitted_at": fields["preparation_admitted_at"],
        "preparation_deadline": fields["preparation_deadline"],
        "observed_at": evidence.observed_at, "pending": evidence.pending,
        "unknown": evidence.unknown,
    }).encode()


__all__ = [
    "ADDRESSES",
    "MODE",
    "PrepareEvidence",
    "admit_prepare",
    "check_mode_canary_input",
    "prepare_receipt",
    "validate_mode_canary_prepare",
]
