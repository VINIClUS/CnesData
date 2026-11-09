"""Verify Phase B routing, task environment readback and closed handoff."""

from __future__ import annotations

import base64
import hashlib
import json
import subprocess
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from scripts.promotion.phase_b import PhaseBContext

Node = dict[str, Any]


def _canonical(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def object_hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _utc(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=datetime_not_utc")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def verify_task_environment_arns(
    task_definitions: Mapping[str, Mapping[str, Any]], expected_state_machine_arn: str
) -> None:
    """Require unit and recovery tasks to carry the exact machine ARN."""
    for mode in ("unit", "recovery"):
        environment = task_definitions.get(mode, {}).get("environment", {})
        if isinstance(environment, list):
            environment = {item["name"]: item.get("value") for item in environment}
        if environment.get("AWS_STATE_MACHINE_ARN") != expected_state_machine_arn:
            raise ValueError(f"routes={mode}_environment_arn")


def verify_routes(
    routes: Mapping[str, Any],
    expected_state_machine_arn: str,
    activation_unit_revision: str,
    previous_recovery_revision: str,
    previous_audit_revision: str,
    task_definitions: Mapping[str, Mapping[str, Any]],
) -> Node:
    """Verify route domains and exact ECS/SFN readback identities."""
    required = {
        "mode",
        "state_machine_arn",
        "unit_revision",
        "recovery_revision",
        "audit_revision",
        "unit_environment_state_machine_arn",
        "recovery_environment_state_machine_arn",
    }
    if set(routes) != required:
        raise ValueError("routes=schema")
    if routes["state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=state_machine_arn")
    if routes["unit_environment_state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=unit_arn")
    if routes["recovery_environment_state_machine_arn"] != expected_state_machine_arn:
        raise ValueError("routes=recovery_arn")
    if routes["unit_revision"] != activation_unit_revision:
        raise ValueError("routes=unit_revision")
    if routes["recovery_revision"] != previous_recovery_revision:
        raise ValueError("routes=recovery_revision")
    if routes["audit_revision"] != previous_audit_revision:
        raise ValueError("routes=audit_revision")
    if routes["mode"] not in {"initial", "established"}:
        raise ValueError("routes=mode")
    verify_task_environment_arns(task_definitions, expected_state_machine_arn)
    return dict(routes)


def _identity(member: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(
        member[key]
        for key in (
            "dispatch_id",
            "request_sha256",
            "generation",
            "initial_state",
            "execution_ref",
            "resolution",
        )
    )


def _post_members(
    gate: Any, probe: Any, close: Mapping[str, Any], observed_at: datetime
) -> tuple[list[Node], list[Node]]:
    from scripts.promotion.environment_gate import (
        PersistedMembershipSource,
        _probe_member,
        _source_members,
    )

    state = gate.observe_environment_gate("prod")
    fence = {
        "environment": "prod",
        "state": state.fence.state.value,
        "promotion_id": state.fence.promotion_id,
        "version": state.fence.version,
        "changed_at": _utc(state.fence.changed_at),
    }
    if fence != close["fence"] or state.permit is not None:
        raise ValueError("closed_ready=strong_fence_or_permit")
    source = PersistedMembershipSource(gate, state)
    current = _source_members(source)
    if [_identity(item) for item in current] != [_identity(item) for item in close["members"]]:
        raise ValueError("closed_ready=membership_changed")
    proofs = []
    for member in current:
        observation = _probe_member(member, probe)
        if observation.evidence is None:
            raise ValueError("closed_ready=original_probe_required")
        original = json.loads(source.proof_bytes(member["dispatch_id"]))
        original["post_b_observation"] = json.loads(observation.evidence)
        original["observed_at"] = _utc(observed_at)
        raw = _canonical(original)
        member["observed_at"] = _utc(observed_at)
        member["proof_sha256"] = hashlib.sha256(raw).hexdigest()
        proofs.append(
            {"sha256": member["proof_sha256"], "bytes_base64": base64.b64encode(raw).decode()}
        )
    if gate.observe_environment_gate("prod") != state:
        raise ValueError("closed_ready=strong_readback_changed")
    return current, proofs


def build_closed_ready(
    close_drain: Mapping[str, Any],
    phase_b: Mapping[str, Any],
    gate: Any,
    probe: Any,
    observed_at: datetime,
    owner: Mapping[str, Any],
    generation: int,
    admitted_at: datetime,
    deadline: datetime,
) -> Node:
    """Reobserva FENCE/SEM e cada execução antes de emitir o handoff original."""
    if close_drain.get("kind") != "close-drain" or phase_b.get("kind") != "phase-b":
        raise ValueError("closed_ready=source_kind")
    if phase_b.get("close_drain_sha256") != object_hash(close_drain):
        raise ValueError("closed_ready=close_hash")
    common = close_drain["common"]
    if (
        common != phase_b["common"]
        or owner != common["caller"]
        or generation != common["promotion_generation"]
    ):
        raise ValueError("closed_ready=original_exclusion")
    fixed = datetime.fromisoformat(close_drain["fixed_deadline"].replace("Z", "+00:00"))
    after = datetime.fromisoformat(phase_b["observed_at"].replace("Z", "+00:00"))
    if (
        observed_at != admitted_at
        or observed_at < after
        or not admitted_at < deadline
        or deadline > min(fixed, admitted_at + timedelta(seconds=180))
        or (fixed - admitted_at).total_seconds() < 1440
    ):
        raise ValueError("closed_ready=stage_deadline")
    current, proofs = _post_members(gate, probe, close_drain, observed_at)
    return {
        "version": 1,
        "kind": "closed-ready",
        "common": dict(common),
        "close_drain_sha256": object_hash(close_drain),
        "phase_b_sha256": object_hash(phase_b),
        "fence": dict(close_drain["fence"]),
        "permit": None,
        "members": current,
        "proofs": proofs,
        "observed_at": _utc(observed_at),
        "exclusion": {
            "owner": dict(owner),
            "generation": generation,
            "membership_sha256": object_hash(current),
            "acquired_at": close_drain["close_confirmed_at"],
            "observed_at": _utc(observed_at),
        },
        "stage": {
            "name": "api",
            "admitted_at": _utc(admitted_at),
            "deadline": _utc(deadline),
            "later_seconds": 840,
        },
        "decision": None,
    }


verify_task_arns = verify_task_environment_arns


class NativePhaseB:
    """Fronteiras nativas do binário OpenTofu, backend versionado e rotas AWS."""

    def __init__(
        self,
        directory: Path,
        clients: Mapping[str, Any],
        backend: Mapping[str, str],
        tofu: str = "tofu",
    ) -> None:
        self.directory, self.clients, self.backend, self.tofu = directory, clients, backend, tofu

    def checkpoint(self) -> tuple[Node, Node]:
        response = self.clients["s3"].get_object(
            Bucket=self.backend["bucket"], Key=self.backend["key"]
        )
        raw = response["Body"].read()
        state = json.loads(raw)
        version = response.get("VersionId")
        if not version or version == "null":
            raise ValueError("phase_b=versioned_checkpoint_required")
        return (
            {
                "lineage": state["lineage"],
                "serial": state["serial"],
                "state_sha256": hashlib.sha256(raw).hexdigest(),
                "object_version": version,
            },
            state,
        )

    def assert_closed(self, close: Mapping[str, Any]) -> None:
        from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane

        gate = DynamoDBControlPlane(
            self.clients["dynamodb"], self.backend["control_plane_table"], lambda: datetime.now(UTC)
        )
        state = gate.observe_environment_gate("prod")
        assert state.fence.changed_at is not None
        fence = {
            "environment": "prod",
            "state": state.fence.state.value,
            "promotion_id": state.fence.promotion_id,
            "version": state.fence.version,
            "changed_at": _utc(state.fence.changed_at),
        }
        deadline = datetime.fromisoformat(close["fixed_deadline"].replace("Z", "+00:00"))
        if (
            fence != close["fence"]
            or state.permit is not None
            or (deadline - datetime.now(UTC)).total_seconds() < 1440
        ):
            raise ValueError("phase_b=closed_deadline_readback")

    def show(self, binary: Path) -> Node:
        result = subprocess.run(
            [self.tofu, "show", "-json", str(binary.resolve())],
            cwd=self.directory,
            check=True,
            capture_output=True,
            timeout=300,
        )
        return json.loads(result.stdout)

    def apply(self, binary: Path) -> bytes:
        result = subprocess.run(
            [self.tofu, "apply", "-input=false", "-no-color", str(binary.resolve())],
            cwd=self.directory,
            check=True,
            capture_output=True,
            timeout=300,
        )
        return result.stdout + result.stderr

    def assert_clean(self) -> None:
        result = subprocess.run(
            [self.tofu, "plan", "-input=false", "-no-color", "-detailed-exitcode"],
            cwd=self.directory,
            check=False,
            capture_output=True,
            timeout=60,
        )
        if result.returncode != 0:
            raise ValueError("phase_b=post_apply_drift")

    def routes(self, context: PhaseBContext, previous: Node) -> Node:
        from scripts.promotion.verify_routes import verify_routes

        expected = context.machine["arn"]
        machine = self.clients["stepfunctions"].describe_state_machine(stateMachineArn=expected)
        if machine["stateMachineArn"] != expected or json.loads(
            machine["definition"]
        ) != json.loads(context.machine["definition"]):
            raise ValueError("phase_b=machine_readback")
        revisions = {
            "unit": context.canary["revisions"]["unit"],
            **self._scheduler_revisions(previous),
        }
        tasks = {}
        for mode in ("unit", "recovery"):
            task = self.clients["ecs"].describe_task_definition(taskDefinition=revisions[mode])[
                "taskDefinition"
            ]
            if (
                task["taskDefinitionArn"] != revisions[mode]
                or len(task["containerDefinitions"]) != 1
            ):
                raise ValueError("phase_b=task_readback")
            tasks[mode] = {"environment": task["containerDefinitions"][0]["environment"]}
        route = {
            "mode": "initial" if context.phase_a["active_before"] is None else "established",
            "state_machine_arn": expected,
            **{f"{mode}_revision": arn for mode, arn in revisions.items()},
            "unit_environment_state_machine_arn": expected,
            "recovery_environment_state_machine_arn": expected,
        }
        return verify_routes(
            route, expected, revisions["unit"], revisions["recovery"], revisions["audit"], tasks
        )

    def _scheduler_revisions(self, state: Node) -> Node:
        revisions = {}
        for resource in state["resources"]:
            if resource.get("type") != "aws_scheduler_schedule":
                continue
            for instance in resource["instances"]:
                original = instance["attributes"]
                actual = self.clients["scheduler"].get_schedule(
                    Name=original["name"], GroupName=original["group_name"]
                )
                target = original["target"][0]
                arn = target["ecs_parameters"][0]["task_definition_arn"]
                if (
                    actual["Target"]["EcsParameters"]["TaskDefinitionArn"] != arn
                    or actual["Target"]["Arn"] != target["arn"]
                ):
                    raise ValueError("phase_b=scheduler_readback")
                for mode in ("recovery", "audit"):
                    if f"processor-{mode}:" in arn:
                        revisions[mode] = arn
        if set(revisions) != {"recovery", "audit"}:
            raise ValueError("phase_b=original_schedulers_required")
        return revisions
