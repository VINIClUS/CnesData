"""DynamoDB dispatch-start CAS entry points."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        CancelReservedRunDispatch,
        ClaimRunDispatchStart,
    )
    from cnes_domain.control_plane.entities import RunDispatch


def claim_start(adapter: Any, command: ClaimRunDispatchStart) -> RunDispatch | None:
    """Claim RESERVED before crossing the executor boundary."""
    return adapter.claim_run_dispatch_start(command)


def cancel_reserved(adapter: Any, command: CancelReservedRunDispatch) -> RunDispatch | None:
    """Cancel only a dispatch that never won STARTING."""
    return adapter.cancel_reserved_run_dispatch(command)


claim_dispatch_start = claim_start
cancel_before_send = cancel_reserved
