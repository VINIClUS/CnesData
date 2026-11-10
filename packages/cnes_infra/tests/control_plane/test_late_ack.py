"""Regressões do ACK tardio no bind nativo SQLite/DynamoDB."""

from datetime import timedelta

import pytest

from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.control_plane.errors import Conflict
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _claim_unit_command,
    _commit_command,
    _event,
)
from packages.cnes_infra.tests.control_plane.test_dynamodb_promotion_snapshot import (
    ddb,  # noqa: F401
)
from packages.cnes_infra.tests.control_plane.test_sqlite_dispatch_start import (
    _bind,
)
from packages.cnes_infra.tests.control_plane.test_sqlite_dispatch_start import (
    started_store as started_store_fixture,  # noqa: F401
)


def _publish(adapter, dispatch):
    run = adapter.get_run(dispatch.tenant_id, dispatch.run_id)
    assert run is not None
    adapter.put_run(run.model_copy(update={"state": RunState.PUBLISHED}))


def _complete_unit(adapter, dispatch, claim):
    clock = MutableClock(dispatch.lease_until - timedelta(seconds=1))
    unit = adapter.claim_run_unit(
        _claim_unit_command(dispatch.dispatch_id, "worker-a", clock)
    )
    assert unit is not None
    adapter.commit_run_unit(
        _commit_command(dispatch.dispatch_id, "worker-a", unit.fencing_token),
        _event("late-ack-unit"),
    )


def test_ack_tardio_persiste_bind_e_replaya_idempotente(started_store_fixture):  # noqa: F811
    adapter, (dispatch, _, _, claim, command) = started_store_fixture
    claimed = adapter.claim_run_dispatch_start(command)
    assert claimed is not None
    _complete_unit(adapter, dispatch, claim)
    _publish(adapter, dispatch)
    bind = _bind(dispatch, claim)

    started = adapter.bind_run_dispatch(bind)

    assert started.state is DispatchState.STARTED
    assert started.execution_ref == bind.execution_ref
    assert adapter.bind_run_dispatch(bind) == started


def test_ack_tardio_recusa_unidade_incompleta_sem_escrita(started_store_fixture):  # noqa: F811
    adapter, (dispatch, _, _, claim, command) = started_store_fixture
    claimed = adapter.claim_run_dispatch_start(command)
    assert claimed is not None
    _publish(adapter, dispatch)
    before = adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id)

    with pytest.raises(Conflict, match="not_processing"):
        adapter.bind_run_dispatch(_bind(dispatch, claim))

    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == before


def test_ack_tardio_recusa_ref_divergente_no_replay_sem_escrita(started_store_fixture):  # noqa: F811
    adapter, (dispatch, _, _, claim, command) = started_store_fixture
    claimed = adapter.claim_run_dispatch_start(command)
    assert claimed is not None
    _complete_unit(adapter, dispatch, claim)
    _publish(adapter, dispatch)
    bind = _bind(dispatch, claim)
    started = adapter.bind_run_dispatch(bind)
    divergent = bind.model_copy(update={"execution_ref": "exec-other"})

    with pytest.raises(Conflict, match=r"dispatch_bind(?:ing)?_conflict"):
        adapter.bind_run_dispatch(divergent)

    assert adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id) == started
