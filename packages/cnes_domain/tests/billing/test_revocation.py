"""Testes da revogação imediata de entitlement."""

import pytest

from cnes_domain.billing.errors import (
    BillingDisabledError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.models import ReadConsistency, SubscriptionStatus
from cnes_domain.billing.revocation import (
    CancelRunUnitsCommand,
    CancelRunUnitsResult,
    RevocationPhase,
    RevocationProgress,
    RevocationResult,
    RevocationSettings,
    RevocationStorePort,
    RevokeRunCommand,
)
from cnes_domain.control_plane.enums import RunState
from packages.cnes_domain.tests.billing.revocation_fakes import (
    ACCOUNT,
    LATER,
    NOW,
    REASON,
    TENANT,
    FakeStore,
    Harness,
    _command,
    _dispatch,
    _snapshot,
    _state,
    _two_runs,
)


def test_store_fake_cumpre_o_port() -> None:
    assert isinstance(FakeStore([]), RevocationStorePort)


def test_revogacao_invalida_snapshot_e_fences_antes_de_cancelar_executor() -> None:
    harness = _two_runs()
    result = harness.revoke()
    assert harness.calls[:5] == [
        "snapshot_admin_revoked",
        "run_01_fence_incremented",
        "run_02_fence_incremented",
        "executor_run_01_cancel",
        "executor_run_02_cancel",
    ]
    assert [r.execution_ref for r in harness.executor.requests] == ["exec-1", "exec-2"]
    snapshot = harness.projection.snapshot
    assert snapshot.subscription_status is SubscriptionStatus.ADMIN_REVOKED
    assert snapshot.entitlement_version == 4
    assert harness.projection.consistencies[0] is ReadConsistency.STRONG
    audit = harness.projection.writes[0].audit_events[0]
    assert (audit.event_type, audit.reason_code, audit.actor_id) == (
        "entitlement.revoked", REASON, "admin-1",
    )
    assert audit.attributes["previous_status"] == "active"
    assert result == RevocationResult(4, ("run_01", "run_02"), ())


def test_eventos_de_run_carregam_codigo_fixo_e_nao_o_motivo_administrativo() -> None:
    harness = _two_runs()
    harness.revoke()
    event = harness.store.events[0]
    assert event.event_id == "run.cancel_requested:tenant-1:run_01:4"
    assert event.payload["reason_code"] == "revoked"
    assert event.payload["fencing_token"] == 8
    assert harness.store.commands[0].reason_code == "revoked"
    assert REASON not in str(event.payload)
    canceled = [e for e in harness.audit.events if e.event_type == "run.canceled"]
    assert [e.reason_code for e in canceled] == ["revoked", "revoked"]
    assert canceled[0].event_id == "run.canceled:acct-1:tenant-1:run_01"
    assert harness.projection.snapshot.valid_until == LATER


def test_falha_step_functions_nao_restaura_fence() -> None:
    harness = _two_runs()
    harness.executor.fail = True
    result = harness.revoke()
    assert result.cancel_failures == ("run_01", "run_02")
    assert all(s.fencing_token == 8 and s.cancel_requested for s in harness.store.states.values())
    assert all(r.state is RunState.CANCELED for r in harness.store.runs.values())
    assert len(harness.executor.requests) == 2


def test_retry_de_revogacao_nao_incrementa_fence_nem_cancela_de_novo() -> None:
    harness = _two_runs()
    first = harness.revoke()
    fences, cancels = harness.store.fence_requests, len(harness.executor.requests)
    second = harness.revoke()
    assert harness.store.fence_requests == fences == 2
    assert len(harness.executor.requests) == cancels == 2
    assert second == RevocationResult(first.entitlement_version, (), ())
    assert len(harness.projection.writes) == 1


@pytest.mark.parametrize("reason", ["", "   ", "x" * 129])
def test_rejeita_reason_code_vazio(reason: str) -> None:
    with pytest.raises(ValueError):
        _command(reason_code=reason)


def test_modo_disabled_falha_fechado_sem_tocar_executor() -> None:
    harness = _two_runs()
    harness.projection.disabled = True
    with pytest.raises(BillingDisabledError):
        harness.revoke()
    assert harness.calls == []
    assert harness.executor.requests == []
    assert harness.store.progress is None


def test_snapshot_ausente_e_erro_permanente() -> None:
    harness = Harness()
    harness.projection.snapshot = None
    with pytest.raises(PermanentBillingError) as error:
        harness.revoke()
    assert error.value.code == "entitlement_snapshot_missing"


def test_cas_perdido_uma_vez_rele_e_conclui() -> None:
    harness = _two_runs()
    harness.projection.lose_cas = 1
    assert harness.revoke().entitlement_version == 4
    assert len(harness.projection.consistencies) == 2


def test_cas_perdido_tres_vezes_e_contencao() -> None:
    harness = _two_runs()
    harness.projection.lose_cas = 3
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "revocation_snapshot_contended"
    assert harness.executor.requests == []


def test_snapshot_ja_revogado_sem_progresso_inicia_progresso() -> None:
    harness = _two_runs()
    harness.projection.snapshot = _snapshot(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED, entitlement_version=5
    )
    result = harness.revoke()
    assert result == RevocationResult(5, ("run_01", "run_02"), ())
    assert harness.projection.writes == []
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_snapshot_com_versao_maior_por_webhook_retoma_progresso_armazenado() -> None:
    harness = _two_runs()
    harness.projection.snapshot = _snapshot(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED, entitlement_version=6
    )
    harness.store.progress = RevocationProgress(
        ACCOUNT, 5, RevocationPhase.FENCING, None, NOW
    )
    result = harness.revoke()
    assert result.entitlement_version == 5
    assert result.fenced_run_ids == ("run_01", "run_02")
    assert harness.store.events[0].event_id.endswith(":5")


def test_nova_revogacao_apos_reativacao_reinicia_progresso_antigo() -> None:
    harness = _two_runs()
    harness.store.progress = RevocationProgress(
        ACCOUNT, 2, RevocationPhase.COMPLETE, None, NOW
    )
    result = harness.revoke()
    assert result.entitlement_version == 4
    assert result.fenced_run_ids == ("run_01", "run_02")


def test_conflito_ao_iniciar_progresso_usa_o_vencedor() -> None:
    harness = _two_runs()
    harness.store.rival = RevocationProgress(
        ACCOUNT, 4, RevocationPhase.COMPLETE, None, NOW
    )
    assert harness.revoke() == RevocationResult(4, (), ())
    assert harness.store.fence_requests == 0


def test_conflito_ao_iniciar_progresso_sem_vencedor_e_contencao() -> None:
    harness = _two_runs()
    harness.store.reject_first = True
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "revocation_progress_contended"


def test_conflito_ao_salvar_avanco_e_contencao() -> None:
    harness = _two_runs()
    harness.store.reject_after = 1
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "revocation_progress_contended"
    assert harness.executor.requests == []


def test_runs_ja_fenceadas_ou_nao_revogaveis_sao_ignoradas() -> None:
    harness = _two_runs()
    harness.store.states["run_01"] = _state("run_01", cancel_requested=True, fencing_token=8)
    harness.store.get_run_state["run_02"] = RunState.PUBLISHING
    result = harness.revoke()
    assert result.fenced_run_ids == ()
    assert harness.store.fence_requests == 0
    assert [r.run_id for r in harness.executor.requests] == ["run_01"]


def test_fence_obsoleto_e_repetido_ate_funcionar() -> None:
    harness = _two_runs()
    harness.store.stale_times = 2
    assert harness.revoke().fenced_run_ids == ("run_01", "run_02")


def test_fence_obsoleto_tres_vezes_e_contencao() -> None:
    harness = _two_runs()
    harness.store.stale_times = 3
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "run_revocation_contended"
    assert harness.executor.requests == []


def test_outro_erro_retryable_do_fence_propaga() -> None:
    harness = _two_runs()
    harness.store.fail_on["request_run_revocation"] = RetryableBillingError("db_unavailable")
    with pytest.raises(RetryableBillingError) as error:
        harness.revoke()
    assert error.value.code == "db_unavailable"


@pytest.mark.parametrize("missing", ["run", "state"])
def test_run_ou_companion_ausente_e_erro_permanente(missing: str) -> None:
    harness = _two_runs()
    (harness.store.hide_run if missing == "run" else harness.store.hide_state).add("run_01")
    with pytest.raises(PermanentBillingError) as error:
        harness.revoke()
    assert error.value.code == "run_revocation_missing"


def test_run_sem_dispatch_ativo_ou_sem_execution_ref_nao_chama_executor() -> None:
    harness = _two_runs()
    del harness.store.dispatches["run_01"]
    harness.store.dispatches["run_02"] = _dispatch("run_02", None)
    result = harness.revoke()
    assert result.fenced_run_ids == ("run_01", "run_02")
    assert harness.executor.requests == []


def test_fencing_pagina_por_pagina_antes_de_qualquer_cancelamento() -> None:
    harness = Harness(page=1)
    for run_id in ("run_01", "run_02", "run_03"):
        harness.store.add_run(run_id, ref=f"exec-{run_id}")
    result = harness.revoke()
    assert result.fenced_run_ids == ("run_01", "run_02", "run_03")
    fences = [i for i, c in enumerate(harness.calls) if c.endswith("_fence_incremented")]
    cancels = [i for i, c in enumerate(harness.calls) if c.startswith("executor_")]
    assert max(fences) < min(cancels)
    assert len(cancels) == 3


def test_unidades_em_varios_lotes_repassam_cursor_em_memoria() -> None:
    harness = Harness()
    harness.store.add_run("run_01", ref="exec-1")
    harness.store.units_needed["run_01"] = 3
    harness.revoke()
    assert harness.store.unit_cursors == [None, "u1", "u2"]
    assert [e.event_type for e in harness.audit.events].count("run.canceled") == 1


def test_finalizacao_processa_a_pagina_inteira_e_salva_progresso_uma_vez() -> None:
    harness = _two_runs()
    harness.store.units_needed = {"run_01": 2, "run_02": 2}
    harness.revoke()
    phases = [p.phase for p in harness.store.saved]
    assert phases == [
        RevocationPhase.FENCING,
        RevocationPhase.CANCELING,
        RevocationPhase.FINALIZING,
        RevocationPhase.COMPLETE,
    ]
    assert harness.store.unit_calls == {"run_01": 2, "run_02": 2}
    assert [e.aggregate_id for e in harness.audit.events if e.event_type == "run.canceled"] == [
        "run_01", "run_02",
    ]


def test_queda_entre_lotes_de_unidades_retoma_e_converge() -> None:
    harness = Harness()
    harness.store.add_run("run_01", ref="exec-1")
    harness.store.units_needed["run_01"] = 3
    harness.store.crash_on_unit_call = 2
    with pytest.raises(RuntimeError):
        harness.revoke()
    assert harness.store.progress.phase is RevocationPhase.FINALIZING
    result = harness.revoke()
    assert result == RevocationResult(4, (), ())
    assert harness.store.runs["run_01"].state is RunState.CANCELED
    assert harness.store.unit_cursors == [None, "u1", None]
    assert [e.event_type for e in harness.audit.events].count("run.canceled") == 1


def test_run_cancelado_por_terceiro_durante_a_revogacao_e_liquidado_e_auditado() -> None:
    harness = _two_runs()

    def finalize_elsewhere(request) -> None:
        run = harness.store.runs[request.run_id]
        harness.store.runs[request.run_id] = run.model_copy(update={"state": RunState.CANCELED})

    harness.executor.on_cancel = finalize_elsewhere
    harness.revoke()
    assert harness.store.unit_calls == {"run_01": 1, "run_02": 1}
    canceled = [e.aggregate_id for e in harness.audit.events if e.event_type == "run.canceled"]
    assert canceled == ["run_01", "run_02"]
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_run_cancelado_sem_fence_nao_e_listado_nem_liquidado() -> None:
    harness = _two_runs()
    harness.store.add_run("run_00", RunState.CANCELED)
    result = harness.revoke()
    assert result.fenced_run_ids == ("run_01", "run_02")
    assert "run_00" not in harness.store.unit_calls


def test_queda_ao_salvar_pagina_de_cancelamento_recancela_so_aquela_pagina() -> None:
    harness = _two_runs(page=1)
    harness.store.crash_save_phase = RevocationPhase.CANCELING
    with pytest.raises(RuntimeError):
        harness.revoke()
    assert [r.run_id for r in harness.executor.requests] == ["run_01"]
    assert harness.store.fence_requests == 2
    harness.revoke()
    assert [r.run_id for r in harness.executor.requests] == ["run_01", "run_01", "run_02"]
    assert harness.store.fence_requests == 2


def test_finalizacao_sem_runs_conclui_sem_cancelar_unidades() -> None:
    harness = Harness()
    result = harness.revoke()
    assert result == RevocationResult(4, (), ())
    assert harness.store.unit_cursors == []
    assert harness.store.progress.phase is RevocationPhase.COMPLETE


def test_queda_na_finalizacao_retoma_sem_refencear_nem_recancelar_executor() -> None:
    harness = _two_runs()
    harness.store.fail_on["cancel_run_units"] = RuntimeError("crash")
    with pytest.raises(RuntimeError):
        harness.revoke()
    assert harness.store.fence_requests == 2
    assert len(harness.executor.requests) == 2
    result = harness.revoke()
    assert harness.store.fence_requests == 2
    assert len(harness.executor.requests) == 2
    assert result == RevocationResult(4, (), ())
    assert all(r.state is RunState.CANCELED for r in harness.store.runs.values())


def test_queda_no_meio_do_fencing_retoma_do_cursor_salvo() -> None:
    harness = _two_runs(page=1)
    harness.store.fail_on["request_run_revocation"] = RuntimeError("crash")
    harness.store.states["run_01"] = _state("run_01", cancel_requested=True, fencing_token=8)
    with pytest.raises(RuntimeError):
        harness.revoke()
    assert harness.store.progress.run_cursor == "run_01"
    harness.revoke()
    assert harness.store.fence_requests == 1
    assert [r.run_id for r in harness.executor.requests] == ["run_01", "run_02"]


def test_validacoes_dos_comandos_e_resultados() -> None:
    with pytest.raises(ValueError):
        RevocationResult(0, (), ())
    with pytest.raises(ValueError):
        RevokeRunCommand(TENANT, "run_01", RunState.PUBLISHED, 1, "revoked", NOW)
    with pytest.raises(ValueError):
        CancelRunUnitsCommand(TENANT, "run_01", 1, 0, None, NOW)
    with pytest.raises(ValueError):
        CancelRunUnitsResult((), "u1", True)
    with pytest.raises(ValueError):
        RevocationProgress(ACCOUNT, 1, RevocationPhase.COMPLETE, "run_01", NOW)


@pytest.mark.parametrize("field", ["run_page_size", "unit_batch_size"])
def test_configuracao_exige_valores_positivos(field: str) -> None:
    with pytest.raises(ValueError):
        RevocationSettings(**{field: 0})


def test_metodos_do_port_sao_apenas_contrato() -> None:
    names = [n for n in vars(RevocationStorePort) if not n.startswith("_")]
    assert len(names) == 10
    for name in names:
        method = getattr(RevocationStorePort, name)
        arity = method.__code__.co_argcount
        assert method(*([None] * arity)) is None
