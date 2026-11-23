"""Verify Phase B routing, task environment readback and closed handoff."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from scripts.promotion._closed_ready import utc_text
from scripts.promotion._stage_windows import FINAL_RESERVE, close_windows, later_seconds

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from scripts.promotion.phase_b import PhaseBContext

Node = dict[str, Any]


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
            "changed_at": utc_text(state.fence.changed_at),
        }
        windows = close_windows(close, "phase_b")
        budget = windows["stages"]["api"] + later_seconds(windows, "api") + FINAL_RESERVE
        deadline = datetime.fromisoformat(close["fixed_deadline"].replace("Z", "+00:00"))
        if (
            fence != close["fence"]
            or state.permit is not None
            or (deadline - datetime.now(UTC)).total_seconds() < budget
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
            **self._routing_revisions(context, previous),
        }
        tasks = {}
        for mode in ("unit", "recovery", "audit"):
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

    def _routing_revisions(self, context: PhaseBContext, state: Node) -> Node:
        if context.phase_a["active_before"] is not None:
            return self._scheduler_revisions(state)
        if any(
            resource.get("type") == "aws_scheduler_schedule" and resource.get("instances")
            for resource in state["resources"]
        ):
            raise ValueError("phase_b=initial_schedulers_must_be_absent")
        return {mode: context.canary["revisions"][mode] for mode in ("recovery", "audit")}

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
