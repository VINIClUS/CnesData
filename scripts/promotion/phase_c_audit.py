"""Phase C de audit: plan só do Scheduler audit após recovery e aceite do canary sob freeze."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from scripts.promotion._phase_c_core import accept_audit_canary
from scripts.promotion.phase_c_recovery import review_scheduler_plan

if TYPE_CHECKING:
    from collections.abc import Mapping

    from scripts.promotion.phase_c_recovery import PhaseCContext

Node = dict[str, Any]


def review_audit_plan(plan: Mapping[str, Any], context: PhaseCContext) -> str:
    """Returns: initial/established. Raises: ValueError: Plan fora do contrato do audit."""
    return review_scheduler_plan(plan, "audit", context)


__all__ = ["accept_audit_canary", "review_audit_plan"]
