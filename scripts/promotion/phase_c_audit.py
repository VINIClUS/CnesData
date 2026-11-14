"""Phase C de audit: plan só do Scheduler audit após recovery e aceite do canary sob freeze."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scripts.promotion.phase_c_recovery import (
    mode_result,
    review_scheduler_plan,
    summary,
    verify_mode_route,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.phase_c_recovery import PhaseCContext

Node = dict[str, Any]


def review_audit_plan(plan: Mapping[str, Any], context: PhaseCContext) -> str:
    """Returns: initial/established. Raises: ValueError: Plan muda unit, recovery ou fora de ordem."""
    return review_scheduler_plan(plan, "audit", context)


def accept_audit_canary(raw: bytes, context: PhaseCContext, readbacks: Node) -> Node:
    """Returns: Aceite do audit. Raises: ValueError: Sem prova do freeze simulado ou pendente."""
    verify_mode_route(readbacks, "audit", context)
    result = mode_result(raw, "audit", context, readbacks)
    if result["freeze_proof_sha256"] is None:
        raise ValueError("canary=freeze_proof")
    return summary(raw, result)
