"""Drain do trabalho Standard e agendado antigo, com prazo e cancelamento só aprovado."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from cnes_domain.ports.processing import CancelRunExecution
from cnes_infra.executor.step_functions import StepFunctionsExecutor
from scripts.promotion.prune_revisions import FAMILIES, paginate, revision_family

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from scripts.promotion.environment_gate import DrainClock

Node = dict[str, Any]
POLL_SECONDS = 5.0
CLEAN_READS = 2
_NAME = re.compile(r"[0-9A-Za-z_-]{1,80}")


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


@dataclass(frozen=True)
class DrainTarget:
    """Máquina, cluster, instante da troca de rota, revisões atuais e retidas, prazo."""

    state_machine_arn: str
    cluster_arn: str
    switched_at: datetime
    current: frozenset[str]
    retained: frozenset[str]
    seconds: float


@dataclass(frozen=True)
class CancelApproval:
    """Aprovação explícita: quem, por quê e cada execução exata com tenant e run."""

    approver: str
    reason: str
    executions: tuple[tuple[str, str, str], ...]

    def sha256(self) -> str:
        data = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(data.encode()).hexdigest()


@dataclass(frozen=True)
class DrainResult:
    """Desfecho do drain; sem drain, as revisões em voo continuam protegidas."""

    drained: bool
    in_flight: frozenset[str]
    executions: tuple[str, ...]
    cancelled: tuple[Node, ...]
    observations: int


def _validate(approval: CancelApproval | None, machine: str) -> None:
    if approval is None:
        return
    prefix = machine.replace(":stateMachine:", ":execution:", 1) + ":"
    _require(bool(approval.approver and approval.reason and approval.executions)
             and all(arn.startswith(prefix) and _NAME.fullmatch(arn[len(prefix):])
                     and tenant and run for arn, tenant, run in approval.executions),
             "drain=approval")


def _old_revisions(ecs: Any, target: DrainTarget) -> tuple[set[str], bool]:
    tasks, complete = paginate(ecs.list_tasks, "taskArns",
                             {"cluster": target.cluster_arn, "desiredStatus": "RUNNING"})
    revisions: set[str] = set()
    for start in range(0, len(tasks), 100):
        described = ecs.describe_tasks(cluster=target.cluster_arn, tasks=tasks[start:start + 100])
        complete = complete and not described.get("failures")
        revisions |= {task["taskDefinitionArn"] for task in described.get("tasks", [])
                      if revision_family(task["taskDefinitionArn"]) in FAMILIES
                      and task["taskDefinitionArn"] not in target.current}
    return revisions, complete


def _observe(clients: Mapping[str, Any], target: DrainTarget
             ) -> tuple[list[str], set[str], bool]:
    listed, complete = paginate(clients["stepfunctions"].list_executions, "executions",
                              {"stateMachineArn": target.state_machine_arn,
                               "statusFilter": "RUNNING"})
    executions = sorted(item["executionArn"] for item in listed
                        if item["startDate"] < target.switched_at)
    revisions, tasks_complete = _old_revisions(clients["ecs"], target)
    return executions, revisions, complete and tasks_complete


def _cancel(clients: Mapping[str, Any], target: DrainTarget, approval: CancelApproval,
            executions: list[str]) -> list[Node]:
    executor = StepFunctionsExecutor(clients["stepfunctions"], target.state_machine_arn)
    cancelled: list[Node] = []
    for arn, tenant, run in approval.executions:
        if arn in executions:
            executor.cancel(CancelRunExecution(tenant_id=tenant, run_id=run, execution_ref=arn))
            cancelled.append({"execution_arn": arn, "tenant_id": tenant, "run_id": run,
                              "approval_sha256": approval.sha256()})
    return cancelled


def _in_flight(target: DrainTarget, executions: list[str], revisions: set[str],
               complete: bool) -> frozenset[str]:
    unmapped = target.retained - target.current if executions or not complete else frozenset()
    return frozenset(revisions) | unmapped


def drain_old_work(clients: Mapping[str, Any], target: DrainTarget, clock: DrainClock,
                   approval: CancelApproval | None = None) -> DrainResult:
    """Returns: Drenado após duas leituras limpas no prazo. Raises: ValueError: Aprovação."""
    _validate(approval, target.state_machine_arn)
    limit, clean, observations = clock.monotonic() + target.seconds, 0, 0
    cancelled: list[Node] = []
    while True:
        executions, revisions, complete = _observe(clients, target)
        observations += 1
        pending = [arn for arn in executions
                   if arn not in {item["execution_arn"] for item in cancelled}]
        if approval is not None and pending:
            cancelled += _cancel(clients, target, approval, pending)
        clean = clean + 1 if complete and not executions and not revisions else 0
        if clean >= CLEAN_READS:
            return DrainResult(True, frozenset(), (), tuple(cancelled), observations)
        if clock.monotonic() >= limit:
            return DrainResult(False, _in_flight(target, executions, revisions, complete),
                               tuple(executions), tuple(cancelled), observations)
        clock.sleep(POLL_SECONDS)
