"""Fences do companion de billing no control plane SQLite: unidades e publicação."""

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from cnes_domain.billing.errors import BillingDisabledError, PublishDenied
from cnes_domain.control_plane.commands import FailRunUnit
from cnes_domain.control_plane.enums import RunState, RunUnitState
from cnes_domain.control_plane.errors import FenceRejected
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.control_plane.dynamodb_billing import authorized_run_records
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.billing.billing_factories import NOW
from packages.cnes_infra.tests.billing.test_control_plane_extensions import authorized
from packages.cnes_infra.tests.contracts.clock import (
    MutableClock,
    _claim_unit,
    _commit_command,
    _event,
    _prepare_unit,
    _run,
)
from packages.cnes_infra.tests.contracts.control_plane_contract import _publish

TENANT = "354130"
RUN_ID = "run-a"


@pytest.fixture
def clock() -> MutableClock:
    return MutableClock(NOW)


@pytest.fixture
def adapter(tmp_path: Path, clock: MutableClock) -> SQLiteControlPlane:
    plane = SQLiteControlPlane(tmp_path / "control.sqlite3", clock.now)
    plane.initialize()
    return plane


def put_companion(adapter: SQLiteControlPlane, **changes: Any) -> None:
    base = authorized_run_records(authorized(run_id=RUN_ID), NOW).state
    data = json.dumps(encode_run_billing_state(replace(base, **changes)))
    with adapter.write_transaction() as connection:
        connection.execute(
            "INSERT INTO run_billing_states (tenant_id, run_id, data) VALUES (?, ?, ?) "
            "ON CONFLICT (tenant_id, run_id) DO UPDATE SET data = excluded.data",
            (TENANT, RUN_ID, data),
        )


def cancel_companion(adapter: SQLiteControlPlane) -> None:
    put_companion(adapter, cancel_requested=True, fencing_token=2)


def units(adapter: SQLiteControlPlane) -> tuple[Any, ...]:
    return adapter.list_run_units(TENANT, RUN_ID)


def test_run_legado_sem_companion_reivindica_confirma_e_falha_unidade(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    completed = adapter.commit_run_unit(command, _event("unit-done"))
    assert completed.state is RunUnitState.SUCCEEDED


def test_companion_nao_cancelado_permite_reivindicar_e_confirmar(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    put_companion(adapter)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    assert adapter.commit_run_unit(command, _event("unit-done")).state is RunUnitState.SUCCEEDED


def test_companion_cancelado_nega_claim_sem_gravar(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    cancel_companion(adapter)
    before = units(adapter)
    assert _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a") is None
    assert units(adapter) == before
    assert adapter.get_run(TENANT, RUN_ID).state is RunState.PROCESSING


def test_companion_cancelado_apos_claim_rejeita_commit_na_mesma_transacao(
    adapter, clock
) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    cancel_companion(adapter)
    before = units(adapter)
    command = _commit_command(dispatch.dispatch_id, "worker-a", claimed.fencing_token)
    with pytest.raises(FenceRejected, match="dispatch_fence_rejected"):
        adapter.commit_run_unit(command, _event("unit-done"))
    assert units(adapter) == before
    assert adapter.pending_outbox(10) == ()


def test_companion_cancelado_rejeita_falha_de_unidade(adapter, clock) -> None:
    dispatch = _prepare_unit(adapter, clock)
    claimed = _claim_unit(adapter, clock, dispatch.dispatch_id, "worker-a")
    cancel_companion(adapter)
    before = units(adapter)
    command = FailRunUnit(
        tenant_id=TENANT, run_id=RUN_ID, unit_id="unit-a", dispatch_id=dispatch.dispatch_id,
        owner="worker-a", fencing_token=claimed.fencing_token, error_code="boom",
        retryable=True,
    )
    with pytest.raises(FenceRejected, match="dispatch_fence_rejected"):
        adapter.fail_run_unit(command, _event("unit-failed"))
    assert units(adapter) == before


def publishing(adapter: SQLiteControlPlane) -> Any:
    adapter.put_run(_run(RUN_ID, RunState.PUBLISHING))
    return _publish(RUN_ID, "published-a", None, False)


def test_publica_run_legado_sem_companion(adapter) -> None:
    command = publishing(adapter)
    assert adapter.publish_dataset(command).version_id == RUN_ID


def test_publica_quando_fence_do_companion_igual_ao_permit(adapter) -> None:
    command = publishing(adapter)
    put_companion(adapter, fencing_token=command.publication_permit.fencing_token)
    assert adapter.publish_dataset(command).version_id == RUN_ID


def test_publicacao_negada_com_cancelamento_nao_grava_nada(adapter) -> None:
    command = publishing(adapter)
    cancel_companion(adapter)
    with pytest.raises(PublishDenied, match="reason=run_cancel_requested"):
        adapter.publish_dataset(command)
    assert adapter.get_dataset_pointer(TENANT, "gold") is None
    assert adapter.get_dataset_version(TENANT, "gold", RUN_ID) is None
    assert adapter.get_run(TENANT, RUN_ID).state is RunState.PUBLISHING
    assert adapter.pending_outbox(10) == ()


def test_publicacao_negada_com_fence_obsoleto(adapter) -> None:
    command = publishing(adapter)
    put_companion(adapter, fencing_token=command.publication_permit.fencing_token + 1)
    with pytest.raises(PublishDenied, match="reason=stale_fence"):
        adapter.publish_dataset(command)
    assert adapter.get_run(TENANT, RUN_ID).state is RunState.PUBLISHING


def test_replay_de_publicacao_concluida_ignora_companion(adapter) -> None:
    command = publishing(adapter)
    pointer = adapter.publish_dataset(command)
    cancel_companion(adapter)
    assert adapter.publish_dataset(command) == pointer


def test_revogacao_indisponivel_com_billing_desabilitado(adapter) -> None:
    with pytest.raises(BillingDisabledError, match="operation=list_revocable_runs"):
        adapter.list_revocable_runs("ba_01", 10, None)
    with pytest.raises(BillingDisabledError, match="operation=request_run_revocation"):
        adapter.request_run_revocation(None, None)
