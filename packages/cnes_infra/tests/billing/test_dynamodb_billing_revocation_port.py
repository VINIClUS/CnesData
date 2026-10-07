"""Revogação exposta pelo control plane DynamoDB delega ao store sem duplicar lógica."""

from collections.abc import Iterator

import pytest

from cnes_domain.control_plane.enums import RunState
from packages.cnes_infra.tests.billing.quota_support import ACCOUNT, TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    create_run,
    open_env,
    revocation_event,
    revoke_command,
    stored_run,
)


@pytest.fixture
def env() -> Iterator[RevEnv]:
    with open_env() as opened:
        create_run(opened)
        yield opened


def test_lista_os_runs_revogaveis_igual_ao_store(env: RevEnv) -> None:
    page = env.plane.list_revocable_runs(ACCOUNT, 10, None)

    assert page == env.store.list_revocable_runs(ACCOUNT, 10, None)
    assert [state.run_id for state in page.runs] == ["run-01"]


def test_cerca_o_run_pelo_control_plane_e_o_lista_cancelado_com_fence(env: RevEnv) -> None:
    command = revoke_command(env)

    fenced = env.plane.request_run_revocation(command, revocation_event())

    assert (fenced.cancel_requested, fenced.fencing_token) == (True, 1)
    assert env.store.get_run_billing_state(TENANT, "run-01") == fenced
    assert stored_run(env).state is RunState.CANCEL_REQUESTED
    assert env.plane.list_revocable_runs(ACCOUNT, 10, None) == env.store.list_revocable_runs(
        ACCOUNT, 10, None
    )
