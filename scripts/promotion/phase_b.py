"""Review and attest the two closed Phase B Terraform oracles."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
MACHINE = "module.processing.aws_sfn_state_machine.processor[0]"
API_POLICY = "module.runtime_iam.aws_iam_role_policy.api[0]"
SFN_ACTIONS = {
    "states:DescribeStateMachine",
    "states:StartExecution",
    "states:DescribeExecution",
    "states:StopExecution",
}
SCHEDULER_PREFIX = "module.schedulers."


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def object_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _changes(plan: Mapping[str, Any]) -> list[Node]:
    changes = plan.get("resource_changes")
    if not isinstance(changes, list):
        raise ValueError("plan=resource_changes")
    return [item for item in changes if item.get("mode") == "managed"]


def _changed(plan: Mapping[str, Any], address: str) -> list[Node]:
    return [item for item in _changes(plan) if item.get("address") == address]


def _policy_actions(policy: Any) -> set[str]:
    if isinstance(policy, str):
        policy = json.loads(policy)
    statements = policy.get("Statement", [])
    actions: set[str] = set()
    for statement in statements:
        values = statement.get("Action", [])
        actions.update([values] if isinstance(values, str) else values)
    return actions


def _assert_no_scheduler_drift(plan: Mapping[str, Any]) -> None:
    changed = [
        item
        for item in _changes(plan)
        if item["address"].startswith(SCHEDULER_PREFIX)
        and item["change"].get("actions") != ["no-op"]
    ]
    if changed:
        raise ValueError("delta=scheduler")


def _assert_task_revisions_unchanged(plan: Mapping[str, Any]) -> None:
    changed = [
        item
        for item in _changes(plan)
        if "aws_ecs_task_definition" in item["address"]
        and item["change"].get("actions") != ["no-op"]
    ]
    if changed:
        raise ValueError("delta=task_revision")


def _document(value: Any) -> Node:
    return json.loads(value) if isinstance(value, str) else deepcopy(value)


def _machine_contract(change: Node, context: Mapping[str, Any], initial: bool) -> None:
    before, after = change.get("before") or {}, change.get("after") or {}
    expected = context["machine_after"]
    computed = {
        "arn",
        "id",
        "creation_date",
        "description",
        "revision_id",
        "state_machine_version_arn",
        "status",
        "version_description",
        "tags_all",
        "region",
        "name_prefix",
        "definition",
    }

    def functional(value):
        return {
            key: item
            for key, item in value.items()
            if key not in computed and item not in (None, [], {})
        }

    if functional(after) != functional(expected):
        raise ValueError("oracle=machine_attributes")
    if _document(after.get("definition")) != _document(expected.get("definition")):
        raise ValueError("oracle=machine_definition")
    if not initial:
        old_definition = _document(before["definition"])
        task = old_definition["States"]["RunUnits"]["ItemProcessor"]["States"]["RunProcessor"]
        if task["Parameters"]["TaskDefinition"] != context["active_before"]:
            raise ValueError("oracle=active_before")
        task["Parameters"]["TaskDefinition"] = context["unit_revision"]
        if old_definition != _document(after["definition"]) or functional(before) != functional(
            after
        ):
            raise ValueError("oracle=established_non_route_change")


def _initial_policy(change: Node, context: Mapping[str, Any]) -> None:
    from scripts.promotion.phase_a import COMPUTED

    before, after = change.get("before") or {}, change.get("after") or {}
    old, new = _document(before.get("policy", {})), _document(after.get("policy", {}))
    machine = context["state_machine_arn"]
    additions = [
        {
            "Effect": "Allow",
            "Action": ["states:DescribeStateMachine", "states:StartExecution"],
            "Resource": machine,
        },
        {
            "Effect": "Allow",
            "Action": ["states:DescribeExecution", "states:StopExecution"],
            "Resource": machine.replace(":stateMachine:", ":execution:") + ":*",
        },
    ]
    remaining = list(new.get("Statement", []))
    for statement in additions:
        if remaining.count(statement) != 1:
            raise ValueError("oracle=initial_sfn_grant")
        remaining.remove(statement)
    if {**new, "Statement": remaining} != old:
        raise ValueError("oracle=initial_other_grants")

    def clean(value):
        return {key: item for key, item in value.items() if key not in COMPUTED | {"policy"}}

    if clean(before) != clean(after):
        raise ValueError("oracle=initial_policy_identity")


def review_phase_b_plan(
    plan: Mapping[str, Any], active_unit_arn: str | None, context: Mapping[str, Any] | None = None
) -> str:
    """Revisa delta exato contra o contrato derivado da Phase A autenticada."""
    _assert_no_scheduler_drift(plan)
    _assert_task_revisions_unchanged(plan)
    if context is None or context.get("active_before") != active_unit_arn:
        raise ValueError("oracle=original_phase_a_context_required")
    initial = active_unit_arn is None
    mode = "initial" if initial else "established"
    machine = _changed(plan, MACHINE)
    if len(machine) != 1 or machine[0]["change"].get("actions") != (
        ["create"] if initial else ["update"]
    ):
        raise ValueError("oracle=machine_action")
    if any(
        machine[0]["change"].get("after_unknown", {}).get(key)
        for key in ("definition", "role_arn", "name", "type")
    ):
        raise ValueError("oracle=machine_unknown")
    _machine_contract(machine[0]["change"], context, initial)
    policy = _changed(plan, API_POLICY)
    if initial:
        if len(policy) != 1 or policy[0]["change"].get("actions") != ["update"]:
            raise ValueError("oracle=initial_api_policy")
        _initial_policy(policy[0]["change"], context)
    allowed = {MACHINE, API_POLICY} if initial else {MACHINE}
    drift = [
        item["address"]
        for item in _changes(plan)
        if item["change"].get("actions") != ["no-op"] and item["address"] not in allowed
    ]
    if drift:
        raise ValueError(f"delta=unexpected resources={sorted(drift)!r}")
    return mode


def _require_utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _routes(routes: Mapping[str, Any], expected_machine: str) -> Node:
    required = {
        "mode",
        "state_machine_arn",
        "unit_revision",
        "recovery_revision",
        "audit_revision",
        "unit_environment_state_machine_arn",
        "recovery_environment_state_machine_arn",
    }
    if set(routes) != required or routes["state_machine_arn"] != expected_machine:
        raise ValueError("routes=schema")
    if any(
        routes[key] != expected_machine
        for key in ("unit_environment_state_machine_arn", "recovery_environment_state_machine_arn")
    ):
        raise ValueError("routes=environment_arn")
    if routes["mode"] not in {"initial", "established"}:
        raise ValueError("routes=mode")
    return dict(routes)


@dataclass(frozen=True)
class PhaseBContext:
    """Referências originais autenticadas; não reconstruídas de um JSON do caller."""

    canary: Node
    phase_a: Node
    machine: Node
    activation: bytes | None = None

    @property
    def oracle(self) -> Node:
        return {
            "active_before": self.phase_a["active_before"],
            "unit_revision": self.canary["revisions"]["unit"],
            "state_machine_arn": self.machine["arn"],
            "machine_after": self.machine,
        }


def prepare_phase_b(reference: Node, services: Node, governed: Node) -> PhaseBContext:
    """Reautentica activation/candidato/Phase A antes de preparar o delta."""
    from release.verify_activation import admit_ext2, verify_activation
    from scripts.promotion.run_canary import _contract

    now, governance = governed["now"], governed["activation"]
    verified = verify_activation(
        github=services["github"],
        s3=services["s3"],
        governance=governance,
        now=now,
        ext2_post_upload_receipt_v1=reference,
        historical=True,
    )
    request = {
        "request_version": 1,
        "mode": "historical",
        "inputs": None,
        "source_sha": governance["provenance"]["source_sha"],
        "ext2_post_upload_receipt_v1": reference,
    }
    admitted = admit_ext2(
        verified.payload_bytes,
        request=request,
        github=services["github"],
        governance=governance,
        now=now,
        historical=True,
    )
    return _context_from_admitted(_contract(admitted, governed), verified.payload_bytes)


def _context_from_admitted(contract: Node, activation: bytes | None = None) -> PhaseBContext:
    from scripts.promotion.run_canary import validate_contract

    validate_contract(contract)
    phase_a = json.loads(contract["_admitted"]["phase_a_bytes"])
    network = contract["network"]
    values = {
        "cluster_arn": contract["cluster"],
        "task_definition_arn": contract["revisions"]["unit"],
        "public_subnet_ids": json.dumps(sorted(network["subnets"])),
        "security_group_id": network["security_group"],
    }
    template = (
        Path(__file__).resolve().parents[2]
        / "infra/opentofu/modules/processing/state_machine.asl.json"
    )
    definition = template.read_text()
    for key, value in values.items():
        definition = definition.replace("${" + key + "}", value)
    machine = {
        "name": "cnesdata-prod",
        "type": "STANDARD",
        "role_arn": f"arn:aws:iam::{contract['account_id']}:role/cnesdata-prod-step-functions",
        "arn": f"arn:aws:states:{contract['region']}:{contract['account_id']}:stateMachine:cnesdata-prod",
        "definition": json.dumps(json.loads(definition)),
    }
    return PhaseBContext(contract, phase_a, machine, activation)


def build_phase_b_evidence(
    close_drain: Mapping[str, Any],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    plan: bytes,
    audit: bytes,
    applied: bytes,
    routes: Mapping[str, Any],
    observed_at: datetime,
) -> Node:
    """Emite hashes dos bytes originais do plano, auditoria e aplicação exata."""
    if close_drain.get("kind") != "close-drain" or close_drain.get("permit") is not None:
        raise ValueError("phase_b=close_drain")
    if (
        before.get("lineage") != after.get("lineage")
        or after.get("serial", -1) <= before.get("serial", -1)
        or before.get("object_version") == after.get("object_version")
    ):
        raise ValueError("checkpoint=lineage_or_serial")
    if not all(isinstance(value, bytes) and value for value in (plan, audit, applied)):
        raise ValueError("phase_b=original_bytes_required")
    return {
        "version": 1,
        "kind": "phase-b",
        "common": dict(close_drain["common"]),
        "close_drain_sha256": object_hash(close_drain),
        "before": dict(before),
        "after": dict(after),
        "plan_sha256": hashlib.sha256(plan).hexdigest(),
        "audit_sha256": hashlib.sha256(audit).hexdigest(),
        "apply_sha256": hashlib.sha256(applied).hexdigest(),
        "routes": dict(routes),
        "observed_at": _require_utc(observed_at),
    }


def run_phase_b(plan: Mapping[str, Any], context: PhaseBContext) -> str:
    """Revisa somente o contexto originalmente autenticado da activation."""
    from scripts.promotion.run_canary import validate_contract

    validate_contract(context.canary)
    if context.activation is None:
        raise ValueError("phase_b=original_activation_required")
    if context != _context_from_admitted(context.canary, context.activation):
        raise ValueError("phase_b=retained_handoff_changed")
    return review_phase_b_plan(plan, context.phase_a["active_before"], context.oracle)


def execute_reviewed_phase_b(
    context: PhaseBContext,
    close: Mapping[str, Any],
    native: NativePhaseB,
    approval: Mapping[str, Any],
) -> Node:
    """Aplica somente o binário original cujo relatório independente foi fixado."""
    binary, audit_path = Path(approval["plan_path"]), Path(approval["audit_path"])
    raw, audit = binary.read_bytes(), audit_path.read_bytes()
    common = close["common"]
    if (
        context.activation is None
        or common["activation_sha256"] != hashlib.sha256(context.activation).hexdigest()
    ):
        raise ValueError("phase_b=activation_binding")
    for key in ("repository", "release_id", "source_sha", "candidate_sha256", "canary_id"):
        if common[key] != context.canary[key]:
            raise ValueError("phase_b=release_binding")
    native.assert_closed(close)
    if hashlib.sha256(audit).hexdigest() != approval["audit_sha256"]:
        raise ValueError("phase_b=original_audit_hash")
    report = json.loads(audit)
    before, state = native.checkpoint()
    if (
        report.get("decision") != "ACCEPT"
        or report.get("plan_sha256") != hashlib.sha256(raw).hexdigest()
        or report.get("checkpoint") != before
        or report.get("source_sha") != close["common"]["source_sha"]
    ):
        raise ValueError("phase_b=exact_independent_audit_required")
    with TemporaryDirectory(prefix="reviewed-phase-b-") as directory:
        reviewed = Path(directory) / "reviewed.tfplan"
        reviewed.write_bytes(raw)
        reviewed.chmod(0o400)
        run_phase_b(native.show(reviewed), context)
        if native.checkpoint()[0] != before or reviewed.read_bytes() != raw:
            raise ValueError("phase_b=checkpoint_or_binary_changed")
        native.assert_closed(close)
        applied = native.apply(reviewed)
        if reviewed.read_bytes() != raw:
            raise ValueError("phase_b=applied_binary_changed")
    native.assert_clean()
    after, _ = native.checkpoint()
    routes = native.routes(context, state)
    native.assert_closed(close)
    return build_phase_b_evidence(
        close, before, after, raw, audit, applied, routes, datetime.now(UTC)
    )
