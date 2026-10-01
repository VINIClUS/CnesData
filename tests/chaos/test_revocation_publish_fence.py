"""Revogação imediata concorrente à publicação nunca avança o pointer do dataset."""

import pytest

pytest.importorskip("moto")

from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

from cnes_domain.billing.errors import PublishDenied
from cnes_domain.billing.models import ReservationStatus
from cnes_domain.control_plane.entities import Run
from cnes_domain.control_plane.enums import RunState
from tests.integration.billing._enforcement_stack import (
    DYNAMO_STRIPE,
    RevokingPolicy,
    composed_policy,
    drive_to_publishing,
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
    billing_state,
    open_stack,
)

pytestmark = [pytest.mark.chaos]

DENIAL = r"reason=(admin_revoked|stale_fence|stale_entitlement)"


@pytest.fixture
def stripe(tmp_path: Path) -> Iterator[Stack]:
    with open_stack(DYNAMO_STRIPE, tmp_path) as opened:
        yield opened


def run_of(stack: Stack) -> Run:
    return stack.plane.get_run(TENANT, RUN_ID)


def arm_before_transaction(
    stack: Stack, monkeypatch: pytest.MonkeyPatch, action: Callable[[], Any]
) -> None:
    original = stack.plane._transact

    def transact(*args: Any, **kwargs: Any) -> None:
        monkeypatch.setattr(stack.plane, "_transact", original)
        action()
        original(*args, **kwargs)

    monkeypatch.setattr(stack.plane, "_transact", transact)


def test_pausa_apos_policy_e_revogacao_antes_do_publish_preserva_pointer(stripe: Stack) -> None:
    drive_to_publishing(stripe)
    policy = RevokingPolicy(composed_policy(stripe), revoker(stripe))

    with pytest.raises(PublishDenied, match=DENIAL):
        publish(stripe, policy)

    assert len(policy.seen) == 1
    assert pointer_of(stripe) is None
    assert run_of(stripe).state is RunState.PUBLISHING
    assert any(key.endswith("run-manifest.json") for key in stripe.store.objects)
    promoted = {f"normalized/{TENANT}/CNES_LOCAL/2026-01/{RUN_ID}/a.bin",
                f"reconciliation/{TENANT}/2026-01/{RUN_ID}/a.bin",
                f"serving/{TENANT}/{RUN_ID}/a.json"}
    assert promoted <= set(stripe.store.objects)
    assert any(key.startswith(f"tmp/{TENANT}/{RUN_ID}/") for key in stripe.store.objects)
    with pytest.raises(PublishDenied, match="reason=admin_revoked"):
        composed_policy(stripe)(run_of(stripe))


def test_run_em_publishing_nao_e_revogavel_mas_publicacao_e_negada(stripe: Stack) -> None:
    drive_to_publishing(stripe)
    before = billing_state(stripe)

    result = revoke(stripe)

    after = billing_state(stripe)
    assert result.fenced_run_ids == ()
    assert (after.cancel_requested, after.fencing_token) == (False, before.fencing_token)
    assert run_of(stripe).state is RunState.PUBLISHING
    with pytest.raises(PublishDenied):
        publish(stripe, composed_policy(stripe))
    assert pointer_of(stripe) is None
    assert run_of(stripe).state is RunState.PUBLISHING


def test_revogacao_concorrente_ao_publish_nao_avanca_pointer(
    stripe: Stack, monkeypatch: pytest.MonkeyPatch
) -> None:
    drive_to_publishing(stripe)
    policy = composed_policy(stripe)
    arm_before_transaction(stripe, monkeypatch, lambda: revoke(stripe))

    with pytest.raises(PublishDenied):
        publish(stripe, policy)

    assert pointer_of(stripe) is None
    assert run_of(stripe).state is RunState.PUBLISHING
    assert reservation_of(stripe).status is ReservationStatus.RESERVED
