"""Leitura forte das rotas, watchdog da janela de 30 minutos e reabertura do fence."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

from cnes_domain.control_plane.environment_gate import ReopenPromotionFence
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from scripts.promotion.phase_b import _require_utc, object_hash
from scripts.promotion.rollback_api_static import CONVERGENCE_SECONDS, RECOVERY_SECONDS
from scripts.promotion.verify_routes import verify_task_environment_arns

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.phase_c_recovery import PhaseCContext
    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
MODES: Final = ("recovery", "audit")
WINDOW_SECONDS: Final = 1800


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


class WindowCutoff(ValueError):
    """Passo recusado pelo watchdog; carrega a decisão de convergência com o fence fechado."""

    def __init__(self, decision: Node) -> None:
        super().__init__(f"window=cutoff step={decision['step']}")
        self.decision = decision


@dataclass(frozen=True)
class PhaseCWatchdog:
    """Prazo F original do close-drain; nenhum passo novo depois de F menos a reserva."""

    closed_at: datetime
    fixed_deadline: datetime

    @classmethod
    def from_close(cls, close: Mapping[str, Any]) -> PhaseCWatchdog:
        """Raises: ValueError: F original além de 1800 s desde o fechamento confirmado."""
        closed, fixed = _instant(close["close_confirmed_at"]), _instant(close["fixed_deadline"])
        _require(closed < fixed <= closed + timedelta(seconds=WINDOW_SECONDS), "window=fixed")
        return cls(closed, fixed)

    @property
    def cutoff(self) -> datetime:
        return self.fixed_deadline - timedelta(seconds=RECOVERY_SECONDS)

    def admit(self, step: str, now: datetime, seconds: int) -> None:
        """Raises: WindowCutoff: Passo que terminaria depois do corte da reserva de recovery."""
        if now + timedelta(seconds=seconds) > self.cutoff:
            raise WindowCutoff(self.converge(step, now))

    def converge(self, step: str, now: datetime) -> Node:
        """Returns: Decisão de convergência às rotas anteriores, fence fechado, sem reset de F."""
        deadline = min(self.fixed_deadline, now + timedelta(seconds=RECOVERY_SECONDS))
        return {
            "version": 1,
            "action": "converge-prior-routes",
            "step": step,
            "fence": "CLOSED",
            "reopen": False,
            "choice_at": _require_utc(now),
            "fixed_deadline": _require_utc(self.fixed_deadline),
            "recovery_deadline": _require_utc(deadline),
            "convergence_budget_seconds": CONVERGENCE_SECONDS,
        }


@dataclass(frozen=True)
class CanaryAcceptance:
    """Aceite de um mode-result, emitido só pelos aceites de recovery e audit."""

    mode: str
    common: Node
    phase_c_sha256: str
    result_sha256: str
    routing_sha256: str
    task_receipts: int
    observed_at: str
    freeze_proof_sha256: str | None
    deadline_drill_sha256: str | None
    operational_requirements: tuple[str, ...]


def observe_gate(native: NativePhaseB) -> tuple[DynamoDBControlPlane, Any]:
    """Returns: Adapter do control plane e leitura forte do gate de ambiente prod."""
    gate = DynamoDBControlPlane(native.clients["dynamodb"], native.backend["control_plane_table"],
                                lambda: datetime.now(UTC))
    return gate, gate.observe_environment_gate("prod")


def assert_fence(state: Any, close: Mapping[str, Any]) -> None:
    """Raises: ValueError: Fence diferente do fechado no close-drain ou permit em voo."""
    fence, expected = state.fence, close["fence"]
    _require(fence.state.value == "CLOSED" and fence.promotion_id == expected["promotion_id"]
             and fence.version == expected["version"], "fence=closed_readback")
    _require(state.permit is None, "fence=permit_in_flight")


def _machine_route(native: NativePhaseB, context: PhaseCContext) -> str:
    expected = context.phase_b.machine
    machine = native.clients["stepfunctions"].describe_state_machine(
        stateMachineArn=expected["arn"])
    _require(machine["stateMachineArn"] == expected["arn"]
             and json.loads(machine["definition"]) == json.loads(expected["definition"]),
             "routes=machine_readback")
    return context.revisions["unit"]


def _schedule_routes(native: NativePhaseB, state: Mapping[str, Any]) -> dict[str, str | None]:
    routes: dict[str, str | None] = dict.fromkeys(MODES)
    for resource in state["resources"]:
        if resource.get("type") != "aws_scheduler_schedule":
            continue
        for instance in resource["instances"]:
            original = instance["attributes"]
            arn = original["target"][0]["ecs_parameters"][0]["task_definition_arn"]
            actual = native.clients["scheduler"].get_schedule(
                Name=original["name"], GroupName=original["group_name"])["Target"]
            _require(actual["EcsParameters"]["TaskDefinitionArn"] == arn
                     and actual["Arn"] == original["target"][0]["arn"],
                     "routes=scheduler_readback")
            modes = [mode for mode in MODES if f"processor-{mode}:" in arn]
            _require(len(modes) == 1 and routes[modes[0]] is None, "routes=scheduler_mode")
            routes[modes[0]] = arn
    return routes


def read_routes(native: NativePhaseB, context: PhaseCContext,
                state: Mapping[str, Any]) -> dict[str, str | None]:
    """Returns: Rotas lidas forte da máquina, dos Schedulers e do state; ambiente das tasks."""
    routes = {"unit": _machine_route(native, context), **_schedule_routes(native, state)}
    environments = {}
    for mode in ("unit", "recovery"):
        revision = routes[mode]
        _require(revision is not None, f"routes={mode}_missing")
        task = native.clients["ecs"].describe_task_definition(taskDefinition=revision)
        definition = task["taskDefinition"]
        _require(definition["taskDefinitionArn"] == revision
                 and len(definition["containerDefinitions"]) == 1, "routes=task_readback")
        environments[mode] = {"environment": definition["containerDefinitions"][0]["environment"]}
    verify_task_environment_arns(environments, context.phase_b.machine["arn"])
    return routes


def _canary(context: PhaseCContext, index: int, accepted: Any) -> bool:
    common = context.close["common"]
    return (
        isinstance(accepted, CanaryAcceptance)
        and accepted.mode == MODES[index]
        and accepted.phase_c_sha256 == object_hash(context.accepted[index])
        and accepted.common.get("activation_sha256") == context.activation_sha256
        and accepted.common.get("promotion_generation") == common["promotion_generation"]
        and accepted.task_receipts == 1
        and (accepted.mode != "audit" or accepted.freeze_proof_sha256 is not None)
    )


def canary_status(context: PhaseCContext) -> str:
    """Returns: accepted-open só com drill operacional. Raises: ValueError: Aceite inválido."""
    canaries = context.canaries
    _require(len(canaries) == len(MODES) == len(context.accepted), "reopen=canaries")
    _require(all(_canary(context, index, item) for index, item in enumerate(canaries)),
             "reopen=canary_binding")
    complete = all(
        item.deadline_drill_sha256 is not None and not item.operational_requirements
        for item in canaries
    )
    return "accepted-open" if complete else "routes-open-operational-pending"


def _reopen(native: NativePhaseB, close: Mapping[str, Any], now: datetime) -> None:
    gate, _ = observe_gate(native)
    promotion = close["fence"]["promotion_id"]
    _require(gate.reopen_promotion_fence(ReopenPromotionFence("prod", promotion, now)),
             "reopen=fence_cas")
    state = gate.observe_environment_gate("prod")
    _require(state.fence.state.value == "OPEN" and state.permit is None, "reopen=readback")


def accept_and_reopen(context: PhaseCContext, native: NativePhaseB) -> Node:
    """Reabre só com as fases C e os canaries registrados no contexto, state limpo e no prazo."""
    _require(context.next_mode is None, "reopen=phase_c_incomplete")
    status = canary_status(context)
    close = context.close
    before, state = native.checkpoint()
    _require(before == context.checkpoint, "reopen=checkpoint_changed")
    native.assert_clean()
    routes = read_routes(native, context, state)
    _require(routes == context.revisions, "reopen=mixed_routes")
    _require(native.checkpoint()[0] == before, "reopen=checkpoint_changed")
    assert_fence(observe_gate(native)[1], close)
    now = datetime.now(UTC)
    context.watchdog.admit("reopen", now, 0)
    _reopen(native, close, now)
    return {
        "version": 1,
        "kind": "route-acceptance",
        "status": status,
        "common": dict(close["common"]),
        "close_drain_sha256": object_hash(close),
        "phase_c_sha256": [object_hash(item) for item in context.accepted],
        "canaries_sha256": object_hash([asdict(item) for item in context.canaries]),
        "checkpoint": before,
        "routes": routes,
        "reopened_at": _require_utc(now),
    }
