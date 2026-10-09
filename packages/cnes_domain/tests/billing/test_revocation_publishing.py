"""Testes de runs em PUBLISHING negadas após revogação e cancelamento por dispatch."""

from dataclasses import replace
from datetime import timedelta

import pytest

from cnes_domain.billing.errors import RetryableBillingError
from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.billing.revocation import (
    PUBLICATION_DENIABLE_RUN_STATES,
    REVOKED_REASON_CODE,
    FailDeniedPublicationCommand,
    RevocationPhase,
    RevocationProgress,
)
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState, RunState
from packages.cnes_domain.tests.billing.revocation_fakes import (
    ACCOUNT,
    NOW,
    TENANT,
    Harness,
    _dispatch,
    _snapshot,
)


def _publishing() -> Harness:
    harness = Harness()
    harness.store.add_run("run_01", RunState.PUBLISHING)
    harness.store.add_run("run_02", ref="exec-2")
    return harness


def test_run_em_publishing_vira_failed_e_libera_reserva_na_revogacao() -> None:
    harness = _publishing()
    result = harness.revoke()
    assert harness.store.runs["run_01"].state is RunState.FAILED
    assert harness.store.released_reservations == ["res-1"]
    event = harness.store.failed_events[0]
    assert event.event_type == "run.failed"
    assert event.event_id == f"run.failed.revoked:{TENANT}:run_01"
    assert event.payload == {
        "billing_account_id": ACCOUNT,
        "entitlement_version": 4,
        "fencing_token": 7,
        "reason_code": REVOKED_REASON_CODE,
    }
    command = harness.store.failed_commands[0]
    assert (command.run_id, command.expected_fencing_token) == ("run_01", 7)
    assert result.failed_run_ids == ("run_01",)
    assert result.fenced_run_ids == ("run_02",)


def test_run_em_publishing_com_progresso_superado_fica_intacta() -> None:
    harness = _publishing()
    harness.projection.snapshot = _snapshot(entitlement_version=5)
    harness.store.progress = RevocationProgress(ACCOUNT, 4, RevocationPhase.FENCING, None, NOW)
    result = harness.service.resume_pending(ACCOUNT, "admin-1")
    assert harness.store.runs["run_01"].state is RunState.PUBLISHING
    assert result.failed_run_ids == ()


def test_run_em_publishing_com_companion_fenceado_fica_intacta() -> None:
    harness = _publishing()
    state = harness.store.states["run_01"]
    harness.store.states["run_01"] = replace(state, cancel_requested=True)
    result = harness.revoke()
    assert harness.store.runs["run_01"].state is not RunState.FAILED
    assert result.failed_run_ids == ()
    assert harness.store.failed_commands == []


def test_run_listada_em_estado_nao_revogavel_e_ignorada() -> None:
    harness = Harness()
    harness.store.add_run("run_01")
    harness.store.get_run_state["run_01"] = RunState.PUBLISHED
    result = harness.revoke()
    assert result.fenced_run_ids == ()
    assert result.failed_run_ids == ()
    assert harness.store.fence_requests == 0


def test_run_publishing_sem_mudanca_na_loja_nao_conta_como_falha() -> None:
    harness = _publishing()
    harness.store.fail_denied_publication = lambda *_: False
    assert harness.revoke().failed_run_ids == ()


def test_contencao_transitoria_ao_falhar_publicacao_e_repetida() -> None:
    harness = _publishing()
    harness.store.fail_stale_times = 1
    result = harness.revoke()
    assert result.failed_run_ids == ("run_01",)


def test_contencao_persistente_ao_falhar_publicacao_levanta_erro_retryable() -> None:
    harness = _publishing()
    harness.store.fail_stale_times = 99
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "run_revocation_contended"


def test_conta_com_acesso_negado_por_perda_tambem_falha_publicacao() -> None:
    harness = _publishing()
    lost = _snapshot(subscription_status=SubscriptionStatus.CANCELED)
    harness.projection.snapshot = lost
    result = harness.service.enforce_access_loss(lost, "reconciler")
    assert result.failed_run_ids == ("run_01",)


def test_executor_cancela_dispatch_com_lease_expirado() -> None:
    harness = Harness()
    harness.store.add_run("run_01", ref="exec-1")
    expired = harness.store.dispatches["run_01"]
    harness.store.dispatches["run_01"] = expired.model_copy(
        update={"lease_until": NOW - timedelta(days=1)}
    )
    harness.revoke()
    assert [r.execution_ref for r in harness.executor.requests] == ["exec-1"]


def test_executor_nao_cancela_dispatch_terminal() -> None:
    harness = Harness()
    harness.store.add_run("run_01", ref="exec-1")
    started = harness.store.dispatches["run_01"]
    harness.store.dispatches["run_01"] = started.model_copy(
        update={"state": DispatchState.TERMINAL, "terminal_outcome": DispatchOutcome.SUCCEEDED}
    )
    harness.revoke()
    assert harness.executor.requests == []


def test_executor_nao_cancela_dispatch_reservado_sem_referencia() -> None:
    harness = Harness()
    harness.store.add_run("run_01")
    harness.store.dispatches["run_01"] = _dispatch("run_01", None)
    harness.revoke()
    assert harness.executor.requests == []


def test_servico_nao_consulta_dispatch_ativo() -> None:
    harness = Harness()
    harness.store.add_run("run_01", ref="exec-1")
    harness.store.get_active_run_dispatch = lambda *_: pytest.fail("lease_dependent")
    harness.revoke()
    assert len(harness.executor.requests) == 1


def test_estados_de_publicacao_negavel_contem_apenas_publishing() -> None:
    assert frozenset({RunState.PUBLISHING}) == PUBLICATION_DENIABLE_RUN_STATES


def _command(**overrides: object) -> FailDeniedPublicationCommand:
    values: dict[str, object] = {
        "tenant_id": TENANT,
        "run_id": "run_01",
        "expected_fencing_token": 0,
        "reason_code": REVOKED_REASON_CODE,
        "failed_at": NOW,
    }
    return FailDeniedPublicationCommand(**{**values, **overrides})


def test_comando_de_falha_de_publicacao_valido_e_aceito() -> None:
    assert _command().expected_fencing_token == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"tenant_id": ""},
        {"run_id": " "},
        {"expected_fencing_token": -1},
        {"reason_code": ""},
        {"reason_code": "x" * 129},
        {"reason_code": "Motivo livre"},
        {"failed_at": NOW.replace(tzinfo=None)},
    ],
)
def test_comando_de_falha_de_publicacao_rejeita_campos_invalidos(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(ValueError):
        _command(**overrides)
