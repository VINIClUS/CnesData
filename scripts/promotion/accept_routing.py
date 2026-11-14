"""Aceite das rotas pós-Phase C, reabertura do fence e watchdog da janela de 30 minutos."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Final

from scripts.promotion.phase_b import _require_utc, object_hash
from scripts.promotion.phase_c_recovery import MODES, read_routes
from scripts.promotion.rollback_api_static import CONVERGENCE_SECONDS, RECOVERY_SECONDS

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.phase_c_recovery import PhaseCContext
    from scripts.promotion.verify_routes import NativePhaseB

Node = dict[str, Any]
WINDOW_SECONDS: Final = 1800


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise ValueError(reason)


class WindowCutoff(RuntimeError):
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


def _canaries(context: PhaseCContext, canaries: Mapping[str, Node]) -> str:
    _require(set(canaries) == set(MODES), "reopen=canaries")
    for mode, accepted in canaries.items():
        _require(accepted.get("mode") == mode
                 and accepted["common"]["activation_sha256"] == context.activation_sha256,
                 "reopen=canary_binding")
    complete = all(
        item["deadline_drill_sha256"] is not None and not item["operational_requirements"]
        for item in canaries.values()
    )
    return "accepted-open" if complete else "routes-open-operational-pending"


def _reopen(native: NativePhaseB, close: Mapping[str, Any], now: datetime) -> None:
    from cnes_domain.control_plane.environment_gate import ReopenPromotionFence
    from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane

    gate = DynamoDBControlPlane(native.clients["dynamodb"], native.backend["control_plane_table"],
                                lambda: datetime.now(UTC))
    promotion = close["fence"]["promotion_id"]
    _require(gate.reopen_promotion_fence(ReopenPromotionFence("prod", promotion, now)),
             "reopen=fence_cas")
    state = gate.observe_environment_gate("prod")
    _require(state.fence.state.value == "OPEN" and state.permit is None, "reopen=readback")


def accept_and_reopen(context: PhaseCContext, close: Mapping[str, Any], native: NativePhaseB,
                      canaries: Mapping[str, Node]) -> Node:
    """Reabre só com unit, recovery e audit da activation aceitos, state limpo e dentro de F."""
    _require(context.next_mode is None, "reopen=phase_c_incomplete")
    status = _canaries(context, canaries)
    watchdog = PhaseCWatchdog.from_close(close)
    native.assert_closed(close)
    before, state = native.checkpoint()
    _require(before == context.checkpoint, "reopen=checkpoint_changed")
    native.assert_clean()
    routes = read_routes(native, context, state)
    _require(routes == context.revisions, "reopen=mixed_routes")
    _require(native.checkpoint()[0] == before, "reopen=checkpoint_changed")
    native.assert_closed(close)
    now = datetime.now(UTC)
    watchdog.admit("reopen", now, 0)
    _reopen(native, close, now)
    return {
        "version": 1,
        "kind": "route-acceptance",
        "status": status,
        "common": dict(close["common"]),
        "close_drain_sha256": object_hash(close),
        "phase_c_sha256": [object_hash(item) for item in context.accepted],
        "canaries_sha256": object_hash(dict(canaries)),
        "checkpoint": before,
        "routes": routes,
        "reopened_at": _require_utc(now),
    }
