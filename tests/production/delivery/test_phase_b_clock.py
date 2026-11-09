"""Confirmação forte tardia e replay preservam a janela original."""

from datetime import timedelta

import pytest

from scripts.promotion.environment_gate import DrainError, close_and_drain
from tests.production.delivery.test_phase_b import COMMON, EXPIRATIONS, NOW, Clock, Probe
from tests.production.delivery.test_phase_b import ddb as ddb_fixture

ddb = ddb_fixture


def test_close_demora_e_replay_nao_rearma_o_prazo(ddb):
    adapter, client = ddb
    clock = Clock()
    original = client.transact_write_items

    def delayed(**request):
        response = original(**request)
        clock.value += 30
        return response

    client.transact_write_items = delayed
    first = close_and_drain(
        adapter,
        Probe(),
        None,
        "prod",
        "r1",
        NOW,
        expirations=EXPIRATIONS,
        clock=clock,
        common=COMMON,
    )
    assert first["close_confirmed_at"] == "2026-11-09T12:00:30Z"
    assert first["fixed_deadline"] == "2026-11-09T12:30:00Z"
    receipt = adapter.observe_environment_gate("prod").fence.close_receipt
    replay = close_and_drain(
        adapter,
        Probe(),
        None,
        "prod",
        "r1",
        NOW + timedelta(minutes=10),
        expirations=EXPIRATIONS,
        clock=Clock(),
        common=COMMON,
    )
    assert replay["close_confirmed_at"] == first["close_confirmed_at"]
    assert replay["fixed_deadline"] == first["fixed_deadline"]
    assert adapter.observe_environment_gate("prod").fence.close_receipt == receipt
    with pytest.raises(DrainError, match="deadline_exceeded"):
        close_and_drain(
            adapter,
            Probe(),
            None,
            "prod",
            "r1",
            NOW + timedelta(minutes=31),
            expirations=EXPIRATIONS,
            clock=Clock(),
            common=COMMON,
        )


@pytest.mark.parametrize(("status", "outcome"), [("FAILED", "FAILED"), ("TIMED_OUT", "FAILED"),
                                           ("ABORTED", "CANCELED")])
def test_reconcile_conserva_o_resultado_terminal_original(ddb, status, outcome):
    from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
    from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe
    from tests.production.delivery.test_phase_b import MACHINE_ARN, _claim, _seed

    adapter, _ = ddb
    dispatch, request, permit = _seed(adapter)
    _claim(adapter, dispatch, request, permit)

    class Terminal(Probe):
        def describe_execution(self, **kwargs):
            response = super().describe_execution(**kwargs)
            return response | {"status": status}

    probe = StepFunctionsExecutionProbe(Terminal(request, terminal=True), MACHINE_ARN)
    EnvironmentAdmission(adapter, probe, "prod", 300, adapter).reconcile(NOW)
    settled = adapter.get_run_dispatch(dispatch.tenant_id, dispatch.run_id)
    assert settled.terminal_outcome.value == outcome
    assert adapter.observe_environment_gate("prod").permit is None


def test_cancelamento_reserved_persistido_retoma_apos_falha_de_release(ddb):
    from cnes_domain.control_plane.commands import CancelReservedRunDispatch
    from cnes_domain.control_plane.dispatch_start import DispatchStartClaim, request_sha256
    from cnes_domain.control_plane.environment_gate import ClosePromotionFence
    from cnes_domain.orchestration.dispatch_admission import EnvironmentAdmission
    from cnes_infra.executor.step_functions import StepFunctionsExecutionProbe
    from tests.production.delivery.test_phase_b import MACHINE_ARN, _seed

    adapter, _ = ddb
    dispatch, request, permit = _seed(adapter)
    assert adapter.close_promotion_fence(ClosePromotionFence("prod", "r1", NOW))
    claim = DispatchStartClaim(
        dispatch_id=dispatch.dispatch_id,
        generation=dispatch.generation,
        permit_token=permit.token,
        fence_version=0,
        request_sha256=request_sha256(request),
    )
    command = CancelReservedRunDispatch(
        tenant_id=dispatch.tenant_id,
        run_id=dispatch.run_id,
        dispatch_id=dispatch.dispatch_id,
        claim=claim,
        canceled_at=NOW,
    )
    assert adapter.cancel_reserved_run_dispatch(command).canceled_before_send is True
    assert adapter.observe_environment_gate("prod").permit is not None
    probe = StepFunctionsExecutionProbe(Probe(request), MACHINE_ARN)
    EnvironmentAdmission(adapter, probe, "prod", 300, adapter).reconcile(NOW)
    result = close_and_drain(
        adapter,
        probe,
        None,
        "prod",
        "r1",
        NOW,
        expirations=EXPIRATIONS,
        clock=Clock(),
        common=COMMON,
    )
    assert result["members"][0]["resolution"] == "CANCEL_BEFORE_SEND"
