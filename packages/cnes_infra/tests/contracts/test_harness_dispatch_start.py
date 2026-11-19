"""O fake do contrato preserva a disputa entre claim e cancelamento antes do envio."""

from datetime import timedelta

import pytest

from cnes_domain.control_plane.commands import (
    CancelReservedRunDispatch,
    ClaimRunDispatchStart,
    ReserveRunDispatch,
)
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, StartAdmission
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.ports.environment_gate import UnitAdmission
from packages.cnes_infra.tests.contracts.clock import MutableClock
from packages.cnes_infra.tests.contracts.test_contract_harness import _NOW, FakeControlPlane


@pytest.fixture
def reserved():
    fake = FakeControlPlane(MutableClock(_NOW))
    dispatch = fake.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id="tenant",
            run_id="run",
            wave_id="a" * 16,
            unit_ids=("unit",),
            now=_NOW,
            lease_seconds=60,
        )
    )
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=1,
        fence_version=0,
        request_sha256="a" * 64,
    )
    command = ClaimRunDispatchStart(
        tenant_id="tenant",
        run_id="run",
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        now=_NOW,
        lease_seconds=60,
    )
    cancel = CancelReservedRunDispatch(
        tenant_id="tenant",
        run_id="run",
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        canceled_at=_NOW,
    )
    return fake, dispatch, command, cancel


def test_fake_claim_vence_cancelamento_e_autentica_bind_e_finish(reserved) -> None:
    fake, dispatch, command, cancel = reserved
    claimed = fake.claim_run_dispatch_start(command)
    assert claimed.state is DispatchState.STARTING
    assert claimed.start_claim == command.claim
    assert fake.claim_run_dispatch_start(command) is None
    assert fake.cancel_reserved_run_dispatch(cancel) is None
    assert fake.get_run_dispatch("tenant", "run") == claimed
    assert fake.get_run_dispatch("outro-tenant", "run") is None
    attempt = StartAdmission(UnitAdmission("prod", 1), _NOW, 60)
    with pytest.raises(Conflict):
        fake.bind_run_dispatch(attempt.bind(dispatch, "execution", None))
    assert fake.get_run_dispatch("tenant", "run") == claimed
    started = fake.bind_run_dispatch(attempt.bind(dispatch, "execution", command.claim))
    assert started.state is DispatchState.STARTED
    assert started.execution_ref == "execution"
    with pytest.raises(Conflict):
        fake.finish_run_dispatch(attempt.cancel(dispatch, None))
    assert fake.get_run_dispatch("tenant", "run") == started
    finished = fake.finish_run_dispatch(attempt.cancel(dispatch, command.claim))
    assert finished.state is DispatchState.TERMINAL
    assert finished.terminal_outcome is DispatchOutcome.CANCELED


def test_fake_cancelamento_vence_claim_e_preserva_prova(reserved) -> None:
    fake, _dispatch, command, cancel = reserved
    canceled = fake.cancel_reserved_run_dispatch(cancel)
    assert canceled.state is DispatchState.TERMINAL
    assert canceled.terminal_outcome is DispatchOutcome.CANCELED
    assert canceled.canceled_before_send
    assert canceled.start_claim == command.claim
    assert fake.claim_run_dispatch_start(command) is None
    assert fake.cancel_reserved_run_dispatch(cancel) is None
    assert fake.get_run_dispatch("tenant", "run") == canceled


@pytest.mark.parametrize("mismatch", ["tenant", "dispatch", "generation", "expired"])
def test_fake_recusa_claim_obsoleto_sem_mudar_reserva(reserved, mismatch) -> None:
    fake, dispatch, command, cancel = reserved
    updates = {
        "tenant": {"tenant_id": "other"},
        "dispatch": {"dispatch_id": "b" * 16},
        "generation": {"claim": command.claim.model_copy(update={"generation": 2})},
        "expired": {"now": _NOW + timedelta(seconds=60)},
    }
    assert fake.claim_run_dispatch_start(command.model_copy(update=updates[mismatch])) is None
    assert fake.get_run_dispatch("tenant", "run") == dispatch
    if mismatch != "expired":
        assert (
            fake.cancel_reserved_run_dispatch(cancel.model_copy(update=updates[mismatch])) is None
        )
        assert fake.get_run_dispatch("tenant", "run") == dispatch
