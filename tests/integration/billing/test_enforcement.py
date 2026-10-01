"""Integração G11: revogação, fences do companion e publicação sobre os adapters reais."""

from collections.abc import Iterator
from pathlib import Path

import pytest

from cnes_domain.billing.errors import PublishDenied
from cnes_domain.billing.execution import PublicationGuard
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.enums import RunState, RunUnitState
from cnes_domain.control_plane.errors import Conflict, FenceRejected, LeaseLost
from tests.integration.billing._enforcement_stack import (
    DYNAMO_STRIPE,
    MATRIX,
    FailingExecutor,
    RevokingPolicy,
    commit_command,
    commit_event,
    composed_policy,
    consumed_runs,
    direct_publish_command,
    drive_to_publishing,
    fail_command,
    fail_event,
    first_wave_claim,
    pointer_of,
    publish,
    reservation_of,
    revoke,
    revoker,
)
from tests.integration.billing._execution_stack import (
    RUN_ID,
    TENANT,
    Stack,
    active_dispatch,
    billing_state,
    claim_command,
    create_processing_run,
    open_stack,
    overwrite_companion,
)
from tests.integration.billing.test_execution_binding import reserve_and_bind_canonical

RACE_ERRORS = (FenceRejected, LeaseLost, Conflict)


@pytest.fixture(params=MATRIX)
def stack(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Stack]:
    with open_stack(request.param, tmp_path) as opened:
        yield opened


@pytest.fixture
def stripe(tmp_path: Path) -> Iterator[Stack]:
    with open_stack(DYNAMO_STRIPE, tmp_path) as opened:
        yield opened


@pytest.fixture(autouse=True)
def sleeps(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    delays: list[float] = []
    monkeypatch.setattr("cnes_infra.control_plane.dynamodb_billing.sleep", delays.append)
    return delays


def fence_companion(stack: Stack) -> None:
    token = billing_state(stack).fencing_token
    overwrite_companion(stack, cancel_requested=True, fencing_token=token + 1)


def test_revogacao_imediata_impede_publicacao_por_fence_antigo(stripe: Stack) -> None:
    drive_to_publishing(stripe)
    stale = billing_state(stripe).fencing_token
    policy = RevokingPolicy(composed_policy(stripe), revoker(stripe))

    with pytest.raises(PublishDenied, match=r"reason=(admin_revoked|stale_fence)"):
        publish(stripe, policy)

    guard = policy.seen[0].binding_context
    assert isinstance(guard, PublicationGuard)
    assert guard.expected_run_fencing_token == stale
    assert pointer_of(stripe) is None
    assert stripe.plane.get_run(TENANT, RUN_ID).state is RunState.PUBLISHING
    assert reservation_of(stripe).status is not ReservationStatus.CONSUMED


def test_publicacao_com_policy_composta_avanca_pointer_nos_tres_casos(stack: Stack) -> None:
    drive_to_publishing(stack)
    before = consumed_runs(stack) if stack.case.stripe else None

    result = publish(stack, composed_policy(stack))

    assert pointer_of(stack).version_id == result.version.version_id
    assert stack.plane.get_run(TENANT, RUN_ID).state is RunState.PUBLISHED
    if stack.case.stripe:
        assert reservation_of(stack).status is ReservationStatus.CONSUMED
        assert consumed_runs(stack) == before


def test_companion_cancelado_rejeita_commit_fail_e_novo_claim(stack: Stack) -> None:
    create_processing_run(stack)
    dispatch, unit = first_wave_claim(stack)
    assert len(dispatch.unit_ids) >= 2
    fence_companion(stack)

    with pytest.raises(FenceRejected):
        stack.plane.commit_run_unit(commit_command(dispatch, unit), commit_event())
    with pytest.raises(FenceRejected):
        stack.plane.fail_run_unit(fail_command(dispatch, unit), fail_event())

    other = claim_command(stack, dispatch, dispatch.unit_ids[1])
    assert stack.plane.claim_run_unit(other) is None
    stored = {item.unit_id: item for item in stack.plane.list_run_units(TENANT, RUN_ID)}
    assert stored[unit.unit_id].state is RunUnitState.LEASED
    assert stack.plane.get_run(TENANT, RUN_ID).state is RunState.PROCESSING


def test_revogacao_entre_claim_e_commit_rejeita_unidade_e_bloqueia_publicacao(
    stripe: Stack,
) -> None:
    create_processing_run(stripe)
    dispatch, unit = first_wave_claim(stripe)
    permit = composed_policy(stripe)(stripe.plane.get_run(TENANT, RUN_ID))
    before = billing_state(stripe).fencing_token

    result = revoke(stripe)

    assert result.fenced_run_ids == (RUN_ID,)
    assert billing_state(stripe).fencing_token == before + 1
    assert stripe.plane.get_run(TENANT, RUN_ID).state in {
        RunState.CANCEL_REQUESTED, RunState.CANCELED,
    }
    with pytest.raises(RACE_ERRORS):
        stripe.plane.commit_run_unit(commit_command(dispatch, unit), commit_event())
    with pytest.raises(RACE_ERRORS):
        stripe.plane.fail_run_unit(fail_command(dispatch, unit), fail_event())
    with pytest.raises((PublishDenied, Conflict)):
        stripe.plane.publish_dataset(direct_publish_command(stripe, permit))
    assert pointer_of(stripe) is None
    assert stripe.plane.get_run(TENANT, RUN_ID).state is not RunState.PUBLISHING


def test_falha_do_executor_na_revogacao_e_reportada_e_pointer_nao_muda(stripe: Stack) -> None:
    create_processing_run(stripe)
    first_wave_claim(stripe)
    executor = FailingExecutor()

    result = revoke(stripe, executor)

    assert result.cancel_failures == (RUN_ID,)
    assert result.fenced_run_ids == (RUN_ID,)
    assert len(executor.requests) == 1
    assert pointer_of(stripe) is None


def test_claim_repara_bind_do_companion_quando_dispatch_foi_iniciado_sem_ele(
    stripe: Stack,
) -> None:
    create_processing_run(stripe)
    dispatch = reserve_and_bind_canonical(stripe)
    assert billing_state(stripe).execution_dispatch_id != dispatch.dispatch_id

    claimed = stripe.plane.claim_run_unit(claim_command(stripe, dispatch, dispatch.unit_ids[0]))

    assert claimed is not None
    assert claimed.lease_owner == "worker-a"
    assert billing_state(stripe).execution_dispatch_id == dispatch.dispatch_id
    assert active_dispatch(stripe).dispatch_id == dispatch.dispatch_id
