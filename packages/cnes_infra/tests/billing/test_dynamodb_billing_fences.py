"""Fence do companion de billing em commit e fail de unidade."""

from collections.abc import Iterator
from dataclasses import replace
from typing import Any, cast

import pytest

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.control_plane.commands import CommitRunUnit, FailRunUnit, TransitionRun
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent, RunUnit
from cnes_domain.control_plane.enums import RunState, RunUnitState
from cnes_domain.control_plane.errors import FenceRejected
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import TENANT
from packages.cnes_infra.tests.billing.test_control_plane_extensions import (
    OTHER_DISPATCH,
    Env,
    binding,
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
DISABLED = BillingMode.DISABLED
ENFORCE = BillingEnforcementMode.ENFORCE
OUTPUT = ManifestRef(
    manifest_id="out", manifest_key=f"raw/{TENANT}/CNES/2026-08/out/manifest.json"
)


@pytest.fixture
def env() -> Iterator[Env]:
    with open_env() as opened:
        yield opened


def overwrite(env: Env, plane: DynamoDBControlPlane, **changes: Any) -> None:
    state = plane.get_run_billing_state(TENANT, "run-01")
    assert state is not None
    env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(replace(state, **changes))
    )


def bind_other_dispatch(env: Env, plane: DynamoDBControlPlane) -> None:
    if cast("Any", plane.get_run_billing_state(TENANT, "run-01")).execution_generation == 0:
        plane.bind_run_execution(binding(dispatch_id=OTHER_DISPATCH, wave_id="d" * 16))
    else:
        overwrite(env, plane, execution_dispatch_id=OTHER_DISPATCH)


def mutate(env: Env, plane: DynamoDBControlPlane, mutation: str) -> None:
    if mutation == "missing":
        delete_companion(env)
    elif mutation == "cancel":
        overwrite(env, plane, cancel_requested=True)
    elif mutation == "other_dispatch":
        bind_other_dispatch(env, plane)


def leased(env: Env, mode: BillingMode) -> tuple[DynamoDBControlPlane, RunUnit]:
    plane = plane_for(env, mode)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    if mode is STRIPE:
        bind_companion(plane, dispatch)
    claimed = claim(plane, dispatch)
    assert claimed is not None
    return plane, claimed


def event() -> OutboxEvent:
    return OutboxEvent(
        tenant_id=TENANT, event_id="evt-unit", event_type="unit.finished",
        aggregate_id="run-01", payload={}, created_at=NOW, delivered_at=None,
    )


def commit(plane: DynamoDBControlPlane, unit: RunUnit) -> RunUnit:
    return plane.commit_run_unit(
        CommitRunUnit(
            tenant_id=TENANT, run_id="run-01", unit_id=UNIT_ID,
            dispatch_id=cast("str", unit.dispatch_id),
            owner="worker-a", fencing_token=unit.fencing_token, output_manifests=(OUTPUT,),
        ),
        event(),
    )


def fail(plane: DynamoDBControlPlane, unit: RunUnit) -> RunUnit:
    return plane.fail_run_unit(
        FailRunUnit(
            tenant_id=TENANT, run_id="run-01", unit_id=UNIT_ID,
            dispatch_id=cast("str", unit.dispatch_id),
            owner="worker-a", fencing_token=unit.fencing_token, error_code="boom",
            retryable=True,
        ),
        event(),
    )


FINISH = {"commit": (commit, RunUnitState.SUCCEEDED), "fail": (fail, RunUnitState.FAILED_RETRYABLE)}
CASES = [
    (STRIPE, "missing", False),
    (STRIPE, "cancel", False),
    (STRIPE, "other_dispatch", False),
    (STRIPE, "bound", True),
    (DISABLED, "missing", True),
    (DISABLED, "cancel", False),
    (DISABLED, "other_dispatch", True),
]


def stored_state(plane: DynamoDBControlPlane) -> RunUnitState:
    return plane.list_run_units(TENANT, "run-01")[0].state


@pytest.mark.parametrize("action", sorted(FINISH))
@pytest.mark.parametrize(("mode", "mutation", "allowed"), CASES)
def test_unidade_respeita_o_companion_ao_finalizar(
    env: Env, action: str, mode: BillingMode, mutation: str, allowed: bool,
) -> None:
    plane, unit = leased(env, mode)
    mutate(env, plane, mutation)
    finish, final_state = FINISH[action]

    if allowed:
        assert finish(plane, unit).state is final_state
        assert stored_state(plane) is final_state
    else:
        with pytest.raises(FenceRejected):
            finish(plane, unit)
        assert stored_state(plane) is RunUnitState.LEASED


@pytest.mark.parametrize("action", sorted(FINISH))
def test_companion_e_verificado_na_mesma_transacao_da_unidade(env: Env, action: str) -> None:
    plane, unit = leased(env, DISABLED)
    FINISH[action][0](plane, unit)

    pk, sk = run_billing_key(TENANT, "run-01")
    items = env.spy.transactions[-1]
    checks = [item["ConditionCheck"] for item in items if "ConditionCheck" in item]
    assert {"pk": {"S": pk}, "sk": {"S": sk}} in [check["Key"] for check in checks]


def cancel_before_transaction(env: Env, plane: DynamoDBControlPlane) -> None:
    def hook() -> None:
        env.spy.before_transact = None
        overwrite(env, plane, cancel_requested=True)

    env.spy.before_transact = hook


def test_commit_e_rejeitado_se_companion_muda_entre_leitura_e_transacao(env: Env) -> None:
    plane, unit = leased(env, STRIPE)
    cancel_before_transaction(env, plane)

    with pytest.raises(FenceRejected):
        commit(plane, unit)

    assert stored_state(plane) is RunUnitState.LEASED


def test_fail_e_rejeitado_se_companion_muda_entre_leitura_e_transacao(env: Env) -> None:
    plane, unit = leased(env, STRIPE)
    cancel_before_transaction(env, plane)

    with pytest.raises(FenceRejected):
        fail(plane, unit)

    assert stored_state(plane) is RunUnitState.LEASED


def revoke_before_transaction(env: Env, plane: DynamoDBControlPlane) -> None:
    def hook() -> None:
        env.spy.before_transact = None
        overwrite(env, plane, cancel_requested=True)
        plane.transition_run(
            TransitionRun(
                tenant_id=TENANT, run_id="run-01", expected_state=RunState.PROCESSING,
                new_state=RunState.CANCEL_REQUESTED, missing_sources=(),
            ),
            event().model_copy(update={"event_id": "evt-cancel"}),
        )

    env.spy.before_transact = hook


@pytest.mark.parametrize("action", sorted(FINISH))
def test_revogacao_entre_leitura_e_transacao_e_classificada_como_fence(
    env: Env, action: str,
) -> None:
    plane, unit = leased(env, STRIPE)
    revoke_before_transaction(env, plane)

    with pytest.raises(FenceRejected):
        FINISH[action][0](plane, unit)

    assert stored_state(plane) is RunUnitState.LEASED
