from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from cnes_domain.control_plane.commands import CancelReservedRunDispatch, ClaimRunDispatchStart
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim

NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)
DISPATCH = "0123456789abcdef"


@pytest.mark.parametrize("command_type", [ClaimRunDispatchStart, CancelReservedRunDispatch])
@pytest.mark.parametrize("dispatch_id", [DISPATCH, "fedcba9876543210"])
def test_comando_exige_claim_do_proprio_dispatch(command_type, dispatch_id: str) -> None:
    claim = DispatchStartClaim(
        dispatch_id=DISPATCH,
        generation=1,
        permit_token=1,
        fence_version=0,
        request_sha256="a" * 64,
    )
    values = {"tenant_id": "tenant", "run_id": "run", "dispatch_id": dispatch_id, "claim": claim}
    if command_type is ClaimRunDispatchStart:
        values.update(now=NOW, lease_seconds=60)
    else:
        values.update(canceled_at=NOW)
    if dispatch_id != DISPATCH:
        with pytest.raises(ValidationError, match="dispatch_claim_identity_mismatch"):
            command_type.model_validate(values)
    else:
        command = command_type.model_validate(values)
        assert command.claim == claim
        assert command.environment == "prod"
