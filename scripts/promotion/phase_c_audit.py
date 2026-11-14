"""Phase C de audit: plan só do Scheduler audit após recovery e aceite do canary sob freeze."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scripts.promotion.phase_c_recovery import accept_mode_canary, review_scheduler_plan

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.phase_c_recovery import PhaseCContext

Node = dict[str, Any]


def review_audit_plan(plan: Mapping[str, Any], context: PhaseCContext) -> str:
    """Returns: initial/established. Raises: ValueError: Plan fora do contrato do audit."""
    return review_scheduler_plan(plan, "audit", context)


def accept_audit_canary(raw: bytes, context: PhaseCContext, readbacks: Node,
                        gate_state: Any) -> PhaseCContext:
    """Returns: Contexto com o audit aceito. Raises: ValueError: Sem prova do freeze simulado."""
    accepted = accept_mode_canary(raw, "audit", context,
                                  {"readbacks": readbacks, "gate": gate_state})
    if accepted.canaries[-1].freeze_proof_sha256 is None:
        raise ValueError("canary=freeze_proof")
    return accepted
