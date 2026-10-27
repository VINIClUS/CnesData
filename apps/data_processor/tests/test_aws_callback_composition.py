"""Callbacks reais, binding persistido e efeitos de billing da composição AWS."""

import inspect
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from typing import Any
from unittest.mock import patch

import pytest

from apps.data_processor.tests.test_aws_mode_composition import _TABLE_NAME, _OfflineAws, _values
from apps.data_processor.tests.test_aws_unit_composition import (
    _EXECUTION_PREFIX,
    _FIXTURES,
    _NOW,
    _RUN,
    _TENANT,
    _assert_normalized,
    _complete,
    _dispatch,
    _event,
    _hold,
    _message,
    _offline_aws,
    _prepare_materialize,
    _seed_graph,
    _seed_output,
)
from cnes_domain.billing import BillingConcurrencyPolicy, RunExecutionPermit
from cnes_domain.billing.commands import SnapshotWrite
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import ReadConsistency, SubscriptionStatus
from cnes_domain.control_plane.commands import TransitionRun
from cnes_domain.control_plane.entities import RunDispatch
from cnes_domain.control_plane.enums import DispatchState, RunState
from cnes_domain.control_plane.errors import ControlPlaneErrorCode, LeaseLost
from cnes_domain.ports.processing import ExecutionPermit
from cnes_infra.aws.processor_settings import AwsRecoverySettings, AwsUnitSettings
from cnes_infra.billing.dynamodb_projection import DynamoEntitlementProjection
from cnes_infra.billing.settings import BillingSettings
from data_processor.aws_composition import (
    AwsExecutionPolicy,
    build_aws_recovery_runtime,
    build_aws_unit_runtime,
)
from data_processor.orchestration.coordinator import RecoveryFailed, noop_execution_started

__all__ = ["_offline_aws"]


@contextmanager
def _policy_returns() -> Iterator[list[ExecutionPermit]]:
    previous = sys.getprofile()
    produced: list[ExecutionPermit] = []
    code = BillingConcurrencyPolicy.__call__.__code__

    def observe(frame: Any, event: Any, value: Any) -> None:
        if previous is not None:
            previous(frame, event, value)
        if frame.f_code is code and event == "return" and isinstance(value, ExecutionPermit):
            produced.append(value)

    sys.setprofile(observe)
    try:
        yield produced
    finally:
        sys.setprofile(previous)


def _sf_schedule(aws: _OfflineAws, prior: RunDispatch | None = None, status: str = "RUNNING"):
    def describe(**request: str) -> dict[str, str]:
        arn = request["executionArn"]
        if prior is not None and arn == prior.execution_ref:
            return {"status": status}
        name = aws.step_functions.start_execution.call_args.kwargs["name"]
        assert arn == _EXECUTION_PREFIX + name
        return {"status": "RUNNING"}

    def start(**request: Any) -> dict[str, str]:
        return {"executionArn": _EXECUTION_PREFIX + request["name"]}

    aws.step_functions.start_execution.side_effect = start
    aws.step_functions.describe_execution.side_effect = describe


def _binding_callback(
    aws: _OfflineAws, generation: int, produced: list[ExecutionPermit], clock: Any = _NOW,
):
    received: list[ExecutionPermit] = []
    previous = aws.control_plane.get_run_billing_state(_TENANT, _RUN)

    def started(run, request, execution_ref, permit) -> None:
        active = aws.control_plane.get_active_run_dispatch(_TENANT, _RUN)
        state = aws.control_plane.get_run_billing_state(_TENANT, _RUN)
        assert active is not None
        assert state is not None
        assert active.state is DispatchState.STARTED
        assert (active.generation, state.execution_generation) == (generation, generation)
        assert (execution_ref, active.execution_ref, state.execution_ref) == (
            _EXECUTION_PREFIX + request.dispatch_id,
        ) * 3
        assert (request.dispatch_id, state.execution_dispatch_id) == (active.dispatch_id,) * 2
        assert (request.wave_id, state.execution_wave_id) == (active.wave_id,) * 2
        expected_units = ("unit-r",) if generation == 2 else ("unit-n",)
        assert (request.unit_ids, state.execution_unit_ids) == (expected_units,) * 2
        if generation == 2:
            units = aws.control_plane.list_run_units(_TENANT, _RUN)
            normal = next(unit for unit in units if unit.unit_id == "unit-n")
            assert normal.state.value == "SUCCEEDED"
            assert len(normal.output_manifests) == 1
        assert (run.tenant_id, run.run_id) == ("354130", "fixture-cnes-run-v1")
        assert (permit.max_concurrency, permit.policy_version, permit.fencing_token) == (1, 1, 0)
        assert permit is produced[-1]
        context = permit.binding_context
        assert isinstance(context, RunExecutionPermit)
        assert (context.billing_account_id, context.generation) == ("ba_01", generation)
        assert (context.authorized_at, state.updated_at) == (clock, clock)
        assert context.expected_previous_dispatch_id == (
            previous.execution_dispatch_id if previous is not None else None
        )
        received.append(permit)

    return started, received


def _billing_values(enforcement: str = "enforce") -> dict[str, str]:
    return _values() | {
        "BILLING_MODE": "stripe", "BILLING_ENFORCEMENT_MODE": enforcement,
        "BILLING_CACHE_TTL_SECONDS": "0",
    }


def _prepare_publication(aws: _OfflineAws) -> None:
    dispatch = _prepare_materialize(aws, "stripe")
    serving = _seed_output(
        aws, "unit-m", f"serving/354130/{_RUN}/overview.json",
        (_FIXTURES / "cnes-serving-v1.json").read_bytes(),
    )
    _complete(aws, dispatch, "unit-m", (serving,))
    aws.control_plane.transition_run(TransitionRun(
        tenant_id=_TENANT, run_id=_RUN, expected_state=RunState.PROCESSING,
        new_state=RunState.PUBLISHING,
    ), _event("publishing"))
    assert all(unit.output_manifests for unit in aws.control_plane.list_run_units(_TENANT, _RUN))


@pytest.mark.parametrize("status", ["RUNNING", "ABORTED"])
def test_callback_de_unidade_observa_binding_persistido_e_permit_original(
    offline_aws: _OfflineAws, status: str,
) -> None:
    _seed_graph(offline_aws, "disabled")
    dispatch = _dispatch(offline_aws, "unit-n", 1)
    held = _hold(offline_aws, dispatch)
    before = offline_aws.control_plane.get_run_billing_state(_TENANT, _RUN)
    _sf_schedule(offline_aws, dispatch, status)
    with _policy_returns() as produced:
        started, received = _binding_callback(offline_aws, 2, produced)
        settings = AwsUnitSettings.from_mapping(_values("minimal"))
        policy = AwsExecutionPolicy(BillingSettings.from_mapping(_values()), started)
        runtime = build_aws_unit_runtime(settings, offline_aws.session, lambda: _NOW, policy)
        runtime.unit_handler.handle(_message(dispatch, "unit-n"))

    _assert_normalized(offline_aws)
    assert len(received) == (1 if status == "ABORTED" else 0)
    assert offline_aws.step_functions.start_execution.call_count == len(received)
    assert len(produced) == len(received)
    if status == "RUNNING":
        assert offline_aws.control_plane.get_active_run_dispatch(_TENANT, _RUN) == dispatch
        assert offline_aws.control_plane.get_run_billing_state(_TENANT, _RUN) == before
        assert offline_aws.control_plane.observe_environment_gate("prod").permit == held
    else:
        active = offline_aws.control_plane.get_active_run_dispatch(_TENANT, _RUN)
        assert active is not None
        assert (active.generation, active.unit_ids) == (2, ("unit-r",))


def test_callback_de_recovery_encadeia_billing_antes_do_callback_explicito(
    offline_aws: _OfflineAws,
) -> None:
    _seed_graph(offline_aws, "stripe")
    _sf_schedule(offline_aws)
    with _policy_returns() as produced:
        started, received = _binding_callback(offline_aws, 1, produced)
        settings = AwsRecoverySettings.from_mapping(_values("minimal"))
        policy = AwsExecutionPolicy(BillingSettings.from_mapping(_billing_values()), started)
        runtime = build_aws_recovery_runtime(settings, offline_aws.session, lambda: _NOW, policy)
        result = runtime.recovery.run_once(100)

    assert (result.scanned, result.recovered) == (1, 1)
    assert len(produced) == len(received) == 1
    offline_aws.step_functions.start_execution.assert_called_once()


@pytest.mark.parametrize("enforcement", ["off", "shadow", "enforce"])
def test_recovery_respeita_off_shadow_enforce_sem_companion(
    offline_aws: _OfflineAws, enforcement: str, caplog: pytest.LogCaptureFixture,
) -> None:
    _seed_graph(offline_aws)
    _sf_schedule(offline_aws)
    received: list[ExecutionPermit] = []

    def started(run, request, execution_ref, permit) -> None:
        assert (permit.max_concurrency, permit.policy_version, permit.fencing_token) == (1, 1, 0)
        assert permit.binding_context.billing_account_id == "local-354130"
        received.append(permit)

    settings = AwsRecoverySettings.from_mapping(_values("minimal"))
    policy = AwsExecutionPolicy(BillingSettings.from_mapping(_billing_values(enforcement)), started)
    runtime = build_aws_recovery_runtime(settings, offline_aws.session, lambda: _NOW, policy)
    if enforcement == "enforce":
        with pytest.raises(RecoveryFailed) as error:
            runtime.recovery.run_once(100)
        assert error.value.failed == 1
        causes = [record.exc_info[1] for record in caplog.records if record.exc_info]
        assert any(isinstance(cause, EntitlementDenied) for cause in causes)
        assert any("run_billing_state_missing" in str(cause) for cause in causes)
    else:
        assert runtime.recovery.run_once(100).recovered == 1
    assert len(received) == (0 if enforcement == "enforce" else 1)
    assert offline_aws.step_functions.start_execution.call_count == len(received)
    assert offline_aws.control_plane.get_run_billing_state(_TENANT, _RUN) is None
    active = offline_aws.control_plane.get_active_run_dispatch(_TENANT, _RUN)
    assert active is not None
    assert active.state is (DispatchState.RESERVED if enforcement == "enforce"
                            else DispatchState.STARTED)


@pytest.mark.parametrize("revoked", [False, True], ids=["active", "admin-revoked"])
def test_publicacao_enforce_usa_snapshot_real(offline_aws: _OfflineAws, revoked: bool) -> None:
    _prepare_publication(offline_aws)
    projection = DynamoEntitlementProjection(offline_aws.dynamodb, _TABLE_NAME, lambda: _NOW)
    if revoked:
        snapshot = projection.get_snapshot("ba_01", ReadConsistency.STRONG)
        assert snapshot is not None
        snapshot = replace(snapshot, entitlement_version=2,
                           subscription_status=SubscriptionStatus.ADMIN_REVOKED)
        assert projection.compare_and_set_snapshot(SnapshotWrite(1, snapshot, ()))
    settings = AwsRecoverySettings.from_mapping(_values("minimal"))
    policy = AwsExecutionPolicy(
        BillingSettings.from_mapping(_billing_values()), noop_execution_started,
    )
    runtime = build_aws_recovery_runtime(settings, offline_aws.session, lambda: _NOW, policy)
    if revoked:
        with pytest.raises(RecoveryFailed) as error:
            runtime.recovery.run_once(100)
        assert error.value.failed == 1
    else:
        assert runtime.recovery.run_once(100).recovered == 1
    pointer = offline_aws.control_plane.get_dataset_pointer(_TENANT, "cnes")
    assert (pointer is None) == revoked
    run = offline_aws.control_plane.get_run(_TENANT, _RUN)
    assert run is not None
    assert run.state is (RunState.PUBLISHING if revoked else RunState.PUBLISHED)
    events = offline_aws.control_plane.read_outbox_page(None, 100).events
    assert sum(e.event_type == "reconciliation.published" for e in events) == (0 if revoked else 1)


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_factory_aplica_policy_explicita_apesar_do_ambiente_desabilitado(
    offline_aws: _OfflineAws, mode: str,
) -> None:
    _seed_graph(offline_aws)
    policy = AwsExecutionPolicy(
        BillingSettings.from_mapping(_billing_values()), noop_execution_started,
    )
    with patch.dict(os.environ, _values(), clear=True):
        assert os.environ["BILLING_MODE"] == "disabled"
        if mode == "unit":
            dispatch = _dispatch(offline_aws, "unit-n", 1)
            settings = AwsUnitSettings.from_mapping(_values("minimal"))
            runtime = build_aws_unit_runtime(settings, offline_aws.session, lambda: _NOW, policy)
            with pytest.raises(LeaseLost) as error:
                runtime.unit_handler.handle(_message(dispatch, "unit-n"))
            assert error.value.code is ControlPlaneErrorCode.UNIT_NOT_LEASED
        else:
            settings = AwsRecoverySettings.from_mapping(_values("minimal"))
            runtime = build_aws_recovery_runtime(
                settings, offline_aws.session, lambda: _NOW, policy,
            )
            with pytest.raises(RecoveryFailed) as error:
                runtime.recovery.run_once(100)
            assert error.value.failed == 1
    assert offline_aws.control_plane.get_run_billing_state(_TENANT, _RUN) is None
    offline_aws.step_functions.start_execution.assert_not_called()


@pytest.mark.parametrize("mode", ["unit", "recovery"])
def test_factory_transporta_callback_e_relogio_explicitos(
    offline_aws: _OfflineAws, mode: str,
) -> None:
    clock = _NOW + timedelta(seconds=17)
    _seed_graph(offline_aws, "disabled" if mode == "unit" else "stripe")
    dispatch = _dispatch(offline_aws, "unit-n", 1) if mode == "unit" else None
    if dispatch is not None:
        _hold(offline_aws, dispatch)
    _sf_schedule(offline_aws, dispatch, "ABORTED")
    with _policy_returns() as produced:
        started, received = _binding_callback(
            offline_aws, 2 if mode == "unit" else 1, produced, clock,
        )
        billing = BillingSettings.from_mapping(_values() if mode == "unit" else _billing_values())
        policy = AwsExecutionPolicy(billing, started)
        factory = build_aws_unit_runtime if mode == "unit" else build_aws_recovery_runtime
        signature = inspect.signature(factory)
        assert tuple(signature.parameters) == ("settings", "session", "clock", "policy")
        assert all(p.default is inspect.Parameter.empty for p in signature.parameters.values())
        signature.bind(object(), offline_aws.session, lambda: clock, policy)
        with pytest.raises(TypeError):
            signature.bind(object(), offline_aws.session, lambda: clock)
        if mode == "unit":
            settings = AwsUnitSettings.from_mapping(_values("minimal"))
            runtime = build_aws_unit_runtime(settings, offline_aws.session, lambda: clock, policy)
            assert dispatch is not None
            runtime.unit_handler.handle(_message(dispatch, "unit-n"))
            _assert_normalized(offline_aws, "2026-01-15T12:00:17+00:00")
        else:
            settings = AwsRecoverySettings.from_mapping(_values("minimal"))
            runtime = build_aws_recovery_runtime(
                settings, offline_aws.session, lambda: clock, policy,
            )
            assert runtime.recovery.run_once(100).recovered == 1
    assert len(produced) == len(received) == 1
    offline_aws.step_functions.start_execution.assert_called_once()
