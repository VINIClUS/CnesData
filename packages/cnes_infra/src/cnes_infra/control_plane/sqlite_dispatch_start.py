"""SQLite dispatch-start CAS entry points."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from cnes_infra.control_plane import sqlite_claims

if TYPE_CHECKING:
    from cnes_domain.control_plane.commands import (
        CancelReservedRunDispatch,
        ClaimRunDispatchStart,
    )
    from cnes_domain.control_plane.entities import RunDispatch


def claim_start(store: Any, command: ClaimRunDispatchStart) -> RunDispatch | None:
    """Claim RESERVED before crossing the executor boundary."""
    return sqlite_claims.claim_run_dispatch_start(store, command)


def cancel_reserved(store: Any, command: CancelReservedRunDispatch) -> RunDispatch | None:
    """Cancel only a dispatch that never won STARTING."""
    return sqlite_claims.cancel_reserved_run_dispatch(store, command)


claim_dispatch_start = claim_start
cancel_before_send = cancel_reserved
