from datetime import UTC, datetime, timedelta

import pytest

from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchState

NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)


def _claim() -> DispatchStartClaim:
    return DispatchStartClaim(
        dispatch_id="0123456789abcdef",
        generation=1,
        permit_token=7,
        fence_version=3,
        request_sha256="a" * 64,
        promotion_id="r1",
    )


def test_claim_model_exposes_starting_state_facts() -> None:
    dispatch = RunDispatch(
        tenant_id="tenant",
        run_id="run",
        wave_id="fedcba9876543210",
        dispatch_id="0123456789abcdef",
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.STARTING,
        lease_until=NOW + timedelta(minutes=5),
        start_claim=_claim(),
    )

    assert dispatch.state is DispatchState.STARTING
    assert dispatch.start_claim == _claim()


def test_dispatch_start_claim_round_trips_through_json() -> None:
    dispatch = RunDispatch(
        tenant_id="tenant",
        run_id="run",
        wave_id="fedcba9876543210",
        dispatch_id="0123456789abcdef",
        generation=1,
        unit_ids=("unit",),
        state=DispatchState.STARTING,
        lease_until=NOW + timedelta(minutes=5),
        start_claim=_claim(),
    )

    restored = RunDispatch.model_validate_json(dispatch.model_dump_json())

    assert restored.start_claim == _claim()


def test_claim_rejeita_dispatch_diferente() -> None:
    with pytest.raises(ValueError, match="dispatch_id"):
        DispatchStartClaim(
            dispatch_id="bad",
            generation=1,
            permit_token=7,
            fence_version=3,
            request_sha256="a" * 64,
            promotion_id="r1",
        )


def test_claim_allows_unknown_fence_version() -> None:
    claim = DispatchStartClaim(
        dispatch_id="0123456789abcdef",
        generation=1,
        permit_token=7,
        fence_version=0,
        request_sha256="a" * 64,
        promotion_id=None,
    )

    assert claim.fence_version == 0


def test_claim_rejects_non_canonical_hex() -> None:
    with pytest.raises(ValueError, match="invalid_dispatch_id"):
        DispatchStartClaim(
            dispatch_id="0123456789ABCDEF",
            generation=1,
            permit_token=7,
            fence_version=0,
            request_sha256="a" * 64,
            promotion_id=None,
        )
