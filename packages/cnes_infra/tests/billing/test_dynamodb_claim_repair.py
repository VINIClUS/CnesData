"""Claim: reparo do bind do companion e checagem do companion em modo disabled."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast

import pytest

from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.control_plane.commands import BindRunDispatch, ClaimRunUnit
from cnes_domain.control_plane.enums import RunStage
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_billing import ClaimDeferred
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import TENANT
from packages.cnes_infra.tests.billing.revocation_support import (
    RevEnv,
    claim_unit,
    create_run,
    finish_wave,
    make_unit,
    put_units,
    reserve_wave,
    start_wave,
)
from packages.cnes_infra.tests.billing.revocation_support import open_env as open_rev_env
from packages.cnes_infra.tests.billing.test_control_plane_extensions import (
    Env,
    open_env,
)
from packages.cnes_infra.tests.billing.test_control_plane_extensions_claims import (
    UNIT_ID,
    bind_companion,
    claim,
    delete_companion,
    plane_for,
    processing_run,
    start_dispatch,
)

STRIPE = BillingMode.STRIPE
STRIPE_SETTINGS = BillingSettings(STRIPE, BillingEnforcementMode.ENFORCE, 0)


@pytest.fixture
def env() -> Iterator[Env]:
    with open_env() as opened:
        yield opened


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("cnes_infra.control_plane.dynamodb_billing.sleep", lambda _: None)


def once(env: Env, hook: Any) -> None:
    def run() -> None:
        env.spy.before_transact = None
        hook()

    env.spy.before_transact = run


def set_cancel(env: Env, plane: DynamoDBControlPlane) -> None:
    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(replace(state, cancel_requested=True))
    )


def claim_command(dispatch: Any) -> ClaimRunUnit:
    return ClaimRunUnit(
        tenant_id=TENANT, run_id="run-01", unit_id=UNIT_ID, dispatch_id=dispatch.dispatch_id,
        owner="worker-a", now=NOW, lease_seconds=60,
    )


def test_reparo_vincula_companion_a_dispatch_iniciado_e_reivindica(env: Env) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    claimed = claim(plane, dispatch)

    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    assert claimed is not None
    assert state.execution_generation == dispatch.generation
    assert (state.execution_dispatch_id, state.execution_ref) == (dispatch.dispatch_id, "exec-1")
    assert state.execution_unit_ids == dispatch.unit_ids


def test_dispatch_reservado_sem_referencia_segue_pendente_de_bind(
    env: Env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    calls: list[Any] = []
    monkeypatch.setattr(plane, "bind_run_execution", calls.append)

    result = plane._claim_run_unit_once(claim_command(dispatch))

    assert result is ClaimDeferred.BIND_PENDING
    assert calls == []
    assert claim(plane, dispatch) is None


def test_reparo_bloqueado_por_cancelamento_nega_claim(env: Env) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    once(env, lambda: set_cancel(env, plane))

    assert claim(plane, dispatch) is None
    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    assert (state.cancel_requested, state.execution_generation) == (True, 0)


def test_cancelamento_apos_reparo_e_antes_do_claim_nega_claim(
    env: Env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    original = plane.bind_run_execution

    def bind_then_cancel(command: Any) -> Any:
        bound = original(command)
        set_cancel(env, plane)
        return bound

    monkeypatch.setattr(plane, "bind_run_execution", bind_then_cancel)

    assert claim(plane, dispatch) is None
    assert cast("Any", plane.get_run_billing_state(TENANT, "run-01")).cancel_requested is True


def test_reparo_concorrente_com_bind_legitimo_ainda_reivindica(env: Env) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    once(env, lambda: bind_companion(env.other_plane(), dispatch))

    assert claim(plane, dispatch) is not None
    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    assert state.execution_dispatch_id == dispatch.dispatch_id


def test_reparo_com_erro_retryable_adia_o_claim(
    env: Env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    def contended(_: Any) -> None:
        raise RetryableBillingError("run_execution_contended")

    monkeypatch.setattr(plane, "bind_run_execution", contended)

    assert plane._claim_run_unit_once(claim_command(dispatch)) is ClaimDeferred.BIND_PENDING
    assert claim(plane, dispatch) is None


def test_reparo_com_erro_permanente_nega_o_claim(
    env: Env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    def stale(_: Any) -> None:
        raise PermanentBillingError("run_execution_stale")

    monkeypatch.setattr(plane, "bind_run_execution", stale)

    assert plane._claim_run_unit_once(claim_command(dispatch)) is None


def test_reparo_sem_efeito_no_companion_nega_o_claim(
    env: Env, monkeypatch: pytest.MonkeyPatch,
) -> None:
    plane = plane_for(env, STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    monkeypatch.setattr(plane, "bind_run_execution", lambda _: None)

    assert plane._claim_run_unit_once(claim_command(dispatch)) is None


def test_reparo_envia_vinculo_anterior_quando_ha_geracao_previa() -> None:
    with open_rev_env() as env:
        plane = DynamoDBControlPlane(
            env.spy, TABLE_NAME, env.clock.now, billing=STRIPE_SETTINGS
        )
        second = repair_second_wave(env)

        claimed = claim_unit(env, second, "unit-recon", plane)

        state = plane.get_run_billing_state(TENANT, "run-01")
        assert state is not None
        assert claimed is not None
        assert state.execution_generation == second.generation == 2
        assert state.execution_dispatch_id == second.dispatch_id


def repair_second_wave(env: RevEnv) -> Any:
    create_run(env)
    put_units(env, (
        make_unit("unit-norm", RunStage.NORMALIZE),
        make_unit("unit-recon", RunStage.RECONCILE, depends_on_unit_ids=("unit-norm",)),
    ))
    first = start_wave(env, ("unit-norm",), None)
    finish_wave(env, first)
    second = reserve_wave(env, ("unit-recon",), first)
    env.plane.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=TENANT, run_id="run-01", dispatch_id=second.dispatch_id,
            execution_ref="exec-2", now=env.clock.now(), lease_seconds=300,
        )
    )
    return second


def test_disabled_nega_claim_com_cancelamento_solicitado(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    set_cancel(env, plane)

    assert claim(plane, dispatch) is None


def test_disabled_verifica_o_companion_na_transacao_do_claim(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    assert claim(plane, dispatch) is not None
    pk, sk = run_billing_key(TENANT, "run-01")
    keys = [
        item["ConditionCheck"]["Key"]
        for item in env.spy.transactions[-1]
        if "ConditionCheck" in item
    ]
    assert {"pk": {"S": pk}, "sk": {"S": sk}} in keys


def test_disabled_nega_claim_se_companion_muda_antes_da_transacao(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    changed = replace(state, updated_at=NOW + timedelta(seconds=5))
    once(env, lambda: env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(changed)
    ))

    assert claim(plane, dispatch) is None


def test_disabled_reivindica_sem_companion(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)
    delete_companion(env)

    assert claim(plane, dispatch) is not None
