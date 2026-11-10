"""Regressões de concorrência na fronteira DynamoDB/SQLite e callers reais."""

from datetime import UTC, datetime, timedelta
from typing import Any, cast

import boto3
import pytest
from moto import mock_aws

from cnes_domain.control_plane.commands import BindRunDispatch, ClaimRunDispatchStart
from cnes_domain.control_plane.dispatch_start import DispatchStartClaim
from cnes_domain.control_plane.environment_gate import (
    AcquireUnitPermit,
    BindUnitPermit,
    ClosePromotionFence,
    PermitHolder,
    ReleaseUnitPermit,
)
from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
from cnes_domain.ports.environment_gate import ExecutionObservation, ExecutionPresence
from cnes_infra.control_plane.dynamodb_adapter import DynamoDBControlPlane
from cnes_infra.control_plane.sqlite_adapter import SQLiteControlPlane
from packages.cnes_infra.tests.contracts.clock import MutableClock, _prepare_unit
from packages.cnes_infra.tests.control_plane.test_dynamodb_adapter import _TABLE_NAME, _create_table
from packages.cnes_infra.tests.control_plane.test_environment_gate import ScriptedClient

NOW = datetime(2026, 11, 9, 12, tzinfo=UTC)


@pytest.fixture
def ddb():
    with mock_aws():
        raw = boto3.client("dynamodb", region_name="us-east-1")
        _create_table(raw)
        client = ScriptedClient(raw)
        adapter = DynamoDBControlPlane(client, _TABLE_NAME, lambda: NOW)
        yield adapter, client


def seed(adapter):
    dispatch = _prepare_unit(adapter, MutableClock(NOW))
    holder = PermitHolder(dispatch.tenant_id, dispatch.run_id, dispatch.dispatch_id)
    permit = adapter.acquire_unit_permit(AcquireUnitPermit("prod", holder, NOW, 300)).permit
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=permit.token,
        fence_version=0,
        request_sha256="a" * 64,
    )
    command = ClaimRunDispatchStart(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        now=NOW,
        lease_seconds=300,
    )
    return dispatch, holder, permit, claim, command


def test_fecha_depois_de_liberar_permit(ddb):
    adapter, _ = ddb
    _, _, permit, _, _ = seed(adapter)
    assert adapter.release_unit_permit(ReleaseUnitPermit("prod", permit.token, NOW, NOW))
    assert adapter.observe_environment_gate("prod").permit is None
    assert adapter.close_promotion_fence(ClosePromotionFence("prod", "release-a", NOW))


def test_bind_concorrente_consta_no_snapshot(ddb):
    adapter, client = ddb
    _, _, permit, _, _ = seed(adapter)
    rival = DynamoDBControlPlane(client.client, _TABLE_NAME, lambda: NOW)
    client.inject(
        "transact_write_items",
        lambda: rival.bind_unit_permit(BindUnitPermit("prod", permit.token, "exec-a", NOW)),
    )
    assert adapter.close_promotion_fence(ClosePromotionFence("prod", "release-a", NOW))
    state = adapter.observe_environment_gate("prod")
    assert state.permit.execution_ref == "exec-a"
    assert state.fence.closed_membership[0]["execution_ref"] == "exec-a"


def test_claim_perde_apos_close(ddb):
    adapter, _ = ddb
    _, _, _, _, command = seed(adapter)
    assert adapter.close_promotion_fence(ClosePromotionFence("prod", "release-a", NOW))
    assert adapter.claim_run_dispatch_start(command) is None


def test_snapshot_congela_claim_starting(ddb):
    adapter, _ = ddb
    _, _, _, claim, command = seed(adapter)
    assert adapter.claim_run_dispatch_start(command) is not None
    assert adapter.close_promotion_fence(ClosePromotionFence("prod", "release-a", NOW))
    member = adapter.observe_environment_gate("prod").fence.closed_membership[0]
    assert (member["start_state"], member["generation"], member["request_sha256"]) == (
        "STARTING",
        claim.generation,
        claim.request_sha256,
    )


def test_terminal_libera_permit(ddb):
    adapter, _ = ddb
    dispatch, holder, permit, claim, command = seed(adapter)
    assert adapter.claim_run_dispatch_start(command) is not None
    adapter.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            execution_ref="exec-a",
            now=NOW,
            lease_seconds=300,
            claim=claim,
        )
    )
    adapter.bind_unit_permit(BindUnitPermit("prod", permit.token, "exec-a", NOW))

    class Probe:
        def probe(self, dispatch_id):
            return ExecutionObservation(ExecutionPresence.TERMINAL, "exec-a")

    EnvironmentAdmission(adapter, Probe(), "prod", 300, adapter).settle(holder, NOW)
    assert adapter.observe_environment_gate("prod").permit is None


@pytest.mark.parametrize("elapsed", [0, 3600])
def test_sqlite_vincula_claim(tmp_path, elapsed):
    adapter = SQLiteControlPlane(tmp_path / "cp.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch = _prepare_unit(adapter, MutableClock(NOW))
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=1,
        fence_version=0,
        request_sha256="a" * 64,
    )
    assert adapter.claim_run_dispatch_start(
        ClaimRunDispatchStart(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            claim=claim,
            now=NOW,
            lease_seconds=300,
        )
    )
    bound = adapter.bind_run_dispatch(
        BindRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            claim=claim,
            execution_ref="exec-a",
            now=NOW + timedelta(seconds=elapsed),
            lease_seconds=300,
        )
    )
    assert bound.execution_ref == "exec-a"

    from cnes_domain.control_plane.commands import ReserveRunDispatch

    replay = adapter.reserve_run_dispatch(ReserveRunDispatch(
        tenant_id=dispatch.tenant_id, run_id=dispatch.run_id, wave_id=dispatch.wave_id,
        unit_ids=dispatch.unit_ids, now=NOW + timedelta(hours=2), lease_seconds=300))
    assert replay.dispatch_id == bound.dispatch_id
    assert replay.start_claim == claim


@pytest.mark.parametrize("caller", ["api", "processor"])
def test_caller_nativo_nao_envia_apos_close(ddb, caller):
    from central_api.services.run_planning import _start_and_bind as api_start
    from cnes_domain.orchestration.planner import RunPlan, logical_wave_id
    from cnes_domain.ports.processing import ExecutionCallbacks, ExecutionPolicyConfig
    from cnes_infra.executor.step_functions import StepFunctionsExecutor
    from data_processor.orchestration.coordinator import _start_and_bind as processor_start
    from data_processor.orchestration.coordinator import allow_execution, noop_execution_started

    adapter, _ = ddb
    dispatch = _prepare_unit(adapter, MutableClock(NOW))
    dispatch_units = tuple(
        unit
        for unit in adapter.list_run_units(dispatch.tenant_id, dispatch.run_id)
        if unit.unit_id in dispatch.unit_ids
    )
    dispatch = dispatch.model_copy(update={"wave_id": logical_wave_id(dispatch_units)})
    adapter._put_direct(adapter._dispatch_item(dispatch))

    class Probe:
        def probe(self, dispatch_id):
            return ExecutionObservation(ExecutionPresence.ABSENT, None)

    class PausedAdmission(EnvironmentAdmission):
        def admit(self, holder, now):
            result = super().admit(holder, now)
            assert adapter.close_promotion_fence(ClosePromotionFence("prod", "release-a", NOW))
            return result

    client = RecordingStartClient()
    executor = StepFunctionsExecutor(
        cast("Any", client), "arn:aws:states:us-east-1:123456789012:stateMachine:processor"
    )
    admission = PausedAdmission(adapter, Probe(), "prod", 300, adapter)
    execution = ExecutionPolicyConfig(
        1, 300, ExecutionCallbacks(allow_execution, noop_execution_started), admission
    )
    plan = RunPlan(
        adapter.get_run(dispatch.tenant_id, dispatch.run_id),
        adapter.list_run_units(dispatch.tenant_id, dispatch.run_id),
        (),
        (),
        1,
    )
    (api_start if caller == "api" else processor_start)(
        adapter, executor, execution, plan, dispatch, NOW
    )
    assert client.calls == []


def test_claim_tem_um_unico_vencedor(ddb):
    adapter, _ = ddb
    _, _, _, _, command = seed(adapter)
    assert adapter.claim_run_dispatch_start(command) is not None
    assert adapter.claim_run_dispatch_start(command) is None


def test_retry_close_preserva_snapshot_depois_de_liberacao(ddb):
    adapter, _ = ddb
    _, _, permit, _, _ = seed(adapter)
    command = ClosePromotionFence("prod", "release-a", NOW)
    assert adapter.close_promotion_fence(command)
    original = adapter.observe_environment_gate("prod").fence
    assert adapter.release_unit_permit(ReleaseUnitPermit("prod", permit.token, NOW, NOW))
    assert adapter.close_promotion_fence(command)
    assert adapter.observe_environment_gate("prod").fence == original


class RecordingStartClient:
    def __init__(self):
        self.calls = []

    def start_execution(self, **request):
        self.calls.append(request)
        return {"executionArn": "arn:aws:states:us-east-1:123456789012:execution:processor:abc"}


def test_release_cancel_before_send_recusa_claim_starting(ddb):
    adapter, _ = ddb
    _, _, permit, claim, command = seed(adapter)
    assert adapter.claim_run_dispatch_start(command) is not None
    assert not adapter.release_unit_permit(
        ReleaseUnitPermit(
            "prod",
            permit.token,
            NOW,
            NOW,
            claim.generation,
            "CANCEL_BEFORE_SEND",
            b'{"resolution":"CANCEL_BEFORE_SEND"}',
        )
    )
    assert adapter.observe_environment_gate("prod").permit is not None
    assert adapter.observe_environment_gate("prod").settlement is None


def test_sqlite_cancelamento_reserved_registra_prova_distinta(tmp_path):
    from cnes_domain.control_plane.commands import CancelReservedRunDispatch

    adapter = SQLiteControlPlane(tmp_path / "cancel.sqlite", lambda: NOW)
    adapter.initialize()
    dispatch = _prepare_unit(adapter, MutableClock(NOW))
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=1,
        fence_version=0,
        request_sha256="a" * 64,
    )
    canceled = adapter.cancel_reserved_run_dispatch(
        CancelReservedRunDispatch(
            tenant_id=dispatch.tenant_id,
            run_id=dispatch.run_id,
            dispatch_id=dispatch.dispatch_id,
            claim=claim,
            canceled_at=NOW,
        )
    )
    assert canceled is not None
    assert canceled.canceled_before_send is True
    reloaded = adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id)
    assert reloaded is not None
    assert reloaded.canceled_before_send is True
    assert (
        adapter.claim_run_dispatch_start(
            ClaimRunDispatchStart(
                tenant_id=dispatch.tenant_id,
                run_id=dispatch.run_id,
                dispatch_id=dispatch.dispatch_id,
                claim=claim,
                now=NOW,
                lease_seconds=300,
            )
        )
        is None
    )
