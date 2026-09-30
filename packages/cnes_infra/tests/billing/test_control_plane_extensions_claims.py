"""Claim de unidade condicionado ao companion de billing no modo stripe."""

from collections.abc import Iterator
from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import Mock

import pytest

from cnes_domain.billing.models import BillingEnforcementMode
from cnes_domain.control_plane.commands import (
    BindRunDispatch,
    ClaimRunUnit,
    PutRunUnits,
    ReserveRunDispatch,
    TransitionRun,
)
from cnes_domain.control_plane.entities import ManifestRef, OutboxEvent, RunDispatch, RunUnit
from cnes_domain.control_plane.enums import RunStage, RunState, RunUnitState
from cnes_domain.profiles import BillingMode
from cnes_infra.aws.runtime import AwsClients, build_aws_runtime
from cnes_infra.billing.dynamodb_quota_items import encode_run_billing_state
from cnes_infra.billing.keys import run_billing_key
from cnes_infra.billing.settings import BillingSettings
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.dynamodb_keys import item_key
from packages.cnes_infra.tests.aws.test_runtime import _LOCKED, _settings
from packages.cnes_infra.tests.billing.billing_factories import NOW, TABLE_NAME
from packages.cnes_infra.tests.billing.quota_support import TENANT
from packages.cnes_infra.tests.billing.test_control_plane_extensions import (
    WAVE,
    Env,
    authorized,
    binding,
    open_env,
)

UNIT_ID = "unit-001"


@pytest.fixture
def env() -> Iterator[Env]:
    with open_env() as opened:
        yield opened


ENFORCE = BillingEnforcementMode.ENFORCE


def billing(
    mode: BillingMode, enforcement: BillingEnforcementMode = ENFORCE,
) -> BillingSettings:
    return BillingSettings(mode, enforcement, 60)


def plane_for(
    env: Env, mode: BillingMode, enforcement: BillingEnforcementMode = ENFORCE,
) -> DynamoDBControlPlane:
    settings = billing(mode, enforcement)
    return DynamoDBControlPlane(env.spy, TABLE_NAME, env.clock.now, billing=settings)


def unit() -> RunUnit:
    key = f"raw/{TENANT}/CNES/2026-08/input/manifest.json"
    return RunUnit(
        tenant_id=TENANT, run_id="run-01", unit_id=UNIT_ID, stage=RunStage.NORMALIZE,
        source_type="CNES", file_subtype="LFCES", partition="all", depends_on_unit_ids=(),
        input_manifests=(ManifestRef(manifest_id="input", manifest_key=key),),
        state=RunUnitState.PENDING, attempt=0, fencing_token=0, lease_owner=None,
        lease_until=None, dispatch_id=None, output_manifests=(), error_code=None,
    )


def processing_run(plane: DynamoDBControlPlane) -> RunDispatch:
    plane.create_unmetered_run(authorized())
    event = OutboxEvent(
        tenant_id=TENANT, event_id="evt-processing", event_type="run.processing",
        aggregate_id="run-01", payload={}, created_at=NOW, delivered_at=None,
    )
    plane.transition_run(
        TransitionRun(
            tenant_id=TENANT, run_id="run-01", expected_state=RunState.WAITING_INPUTS,
            new_state=RunState.PROCESSING, missing_sources=(),
        ),
        event,
    )
    plane.put_run_units(
        PutRunUnits(
            tenant_id=TENANT, run_id="run-01", expected_run_state=RunState.PROCESSING,
            units=(unit(),),
        )
    )
    return plane.reserve_run_dispatch(
        ReserveRunDispatch(
            tenant_id=TENANT, run_id="run-01", wave_id=WAVE, unit_ids=(UNIT_ID,),
            now=NOW, lease_seconds=300,
        )
    )


def start_dispatch(plane: DynamoDBControlPlane, dispatch: RunDispatch, ref: str = "exec-1") -> Any:
    return plane.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=TENANT, run_id="run-01", dispatch_id=dispatch.dispatch_id,
            execution_ref=ref, now=NOW, lease_seconds=300,
        )
    )


def bind_companion(plane: DynamoDBControlPlane, dispatch: RunDispatch, ref: str = "exec-1") -> None:
    plane.bind_run_execution(
        binding(dispatch_id=dispatch.dispatch_id, wave_id=dispatch.wave_id, execution_ref=ref)
    )


def claim(plane: DynamoDBControlPlane, dispatch: RunDispatch) -> RunUnit | None:
    return plane.claim_run_unit(
        ClaimRunUnit(
            tenant_id=TENANT, run_id="run-01", unit_id=UNIT_ID,
            dispatch_id=dispatch.dispatch_id, owner="worker-a", now=NOW, lease_seconds=60,
        )
    )


def delete_companion(env: Env) -> None:
    env.client.delete_item(TableName=TABLE_NAME, Key=item_key(*run_billing_key(TENANT, "run-01")))


def test_modo_desabilitado_reivindica_sem_companion(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    delete_companion(env)

    assert claim(plane, dispatch) is not None


def test_modo_desabilitado_reivindica_dispatch_reservado(env: Env) -> None:
    plane = plane_for(env, BillingMode.DISABLED)
    dispatch = processing_run(plane)

    assert claim(plane, dispatch) is not None


def test_modo_stripe_reivindica_apos_vinculacao_canonica(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    bind_companion(plane, dispatch)

    claimed = claim(plane, dispatch)

    assert claimed is not None
    assert claimed.lease_owner == "worker-a"


def test_modo_stripe_nega_claim_sem_companion(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    delete_companion(env)

    assert claim(plane, dispatch) is None


def test_modo_stripe_nega_claim_de_dispatch_reservado(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)

    assert claim(plane, dispatch) is None


def test_modo_stripe_nega_claim_com_companion_nao_vinculado(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    assert claim(plane, dispatch) is None


def test_modo_stripe_nega_claim_com_companion_de_outro_dispatch(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    plane.bind_run_execution(binding(dispatch_id="c" * 16, wave_id="d" * 16))

    assert claim(plane, dispatch) is None


def test_modo_stripe_nega_claim_com_referencia_divergente(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    bind_companion(plane, dispatch, ref="exec-other")

    assert claim(plane, dispatch) is None


def test_modo_stripe_nega_claim_com_cancelamento_solicitado(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    bind_companion(plane, dispatch)
    state = plane.get_run_billing_state(TENANT, "run-01")
    env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(replace(state, cancel_requested=True))
    )

    assert claim(plane, dispatch) is None


def test_modo_stripe_aborta_claim_se_companion_muda_antes_da_transacao(env: Env) -> None:
    plane = plane_for(env, BillingMode.STRIPE)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    bind_companion(plane, dispatch)
    state = plane.get_run_billing_state(TENANT, "run-01")
    changed = replace(state, updated_at=NOW + timedelta(seconds=5))
    env.spy.before_transact = lambda: env.client.put_item(
        TableName=TABLE_NAME, Item=encode_run_billing_state(changed)
    )

    assert claim(plane, dispatch) is None


def runtime_plane(env: Env, **options: Any) -> DynamoDBControlPlane:
    s3 = Mock()
    s3.get_object_lock_configuration.return_value = _LOCKED
    clients = AwsClients(dynamodb=env.client, s3=s3, step_functions=Mock())
    settings = replace(_settings(), control_plane_table=TABLE_NAME)
    return build_aws_runtime(settings, clients, env.clock.now, **options).control_plane


def test_runtime_aws_padrao_nao_exige_companion_no_claim(env: Env) -> None:
    plane = runtime_plane(env)
    dispatch = processing_run(plane)

    assert claim(plane, dispatch) is not None


def test_runtime_aws_stripe_exige_companion_vinculado_no_claim(env: Env) -> None:
    plane = runtime_plane(env, billing=billing(BillingMode.STRIPE))
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    assert claim(plane, dispatch) is None
    bind_companion(plane, dispatch)
    assert claim(plane, dispatch) is not None


@pytest.mark.parametrize(
    "enforcement", [BillingEnforcementMode.OFF, BillingEnforcementMode.SHADOW],
)
def test_stripe_sem_enforce_exige_vinculo_quando_companion_existe(
    env: Env, enforcement: BillingEnforcementMode,
) -> None:
    plane = plane_for(env, BillingMode.STRIPE, enforcement)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)

    assert claim(plane, dispatch) is None
    bind_companion(plane, dispatch)
    assert claim(plane, dispatch) is not None


@pytest.mark.parametrize(
    "enforcement", [BillingEnforcementMode.OFF, BillingEnforcementMode.SHADOW],
)
def test_stripe_sem_enforce_reivindica_run_legado_sem_companion(
    env: Env, enforcement: BillingEnforcementMode,
) -> None:
    plane = plane_for(env, BillingMode.STRIPE, enforcement)
    dispatch = processing_run(plane)
    start_dispatch(plane, dispatch)
    delete_companion(env)

    assert claim(plane, dispatch) is not None
