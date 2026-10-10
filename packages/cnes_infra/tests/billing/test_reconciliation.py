"""Testes do BillingReconciler: drift, CAS, enforcement de perda de acesso e cursor."""

import logging
from dataclasses import replace
from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock

import pytest

from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.inbox import ReconciliationRequest
from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.billing.revocation import (
    ImmediateRevocationService,
    RevocationDependencies,
)
from cnes_infra.billing.dynamodb_items import deterministic_id
from cnes_infra.billing.reconciliation import (
    COMPARED_FIELDS,
    CORRECTED_EVENT,
    DRIFT_EVENT,
    RECONCILER_ACTOR_ID,
    RECONCILIATION_REASON,
    AccessLossEnforcerPort,
    ReconciliationCursorPort,
)
from cnes_infra.billing.reconciliation_cursor import DynamoReconciliationCursor
from packages.cnes_infra.tests.billing.billing_factories import NOW, make_snapshot
from packages.cnes_infra.tests.billing.reconciliation_support import (
    Env,
    drifted,
    make_env,
    make_state,
)

REQUEST = ReconciliationRequest(limit=10, cursor=None)
RUNS_METRIC = "RunsCanceledByRevocation"
DRIFT_METRIC = "ReconciliationDrift"


def _run(env: Env, request: ReconciliationRequest = REQUEST):
    return env.reconciler().run(request)


def _single_drift_env() -> Env:
    env = make_env()
    env.projection.snapshots["ba_01"] = drifted()
    return env


def _admin_revoked_env() -> Env:
    env = make_env()
    env.projection.snapshots["ba_01"] = drifted(
        subscription_status=SubscriptionStatus.ADMIN_REVOKED
    )
    return env


def test_reconciliation_corrige_drift_com_conditional_write():
    env = _single_drift_env()
    result = _run(env)
    assert (result.examined, result.drift_found, result.corrected, result.failed) == (1, 1, 1, 0)
    assert result.next_cursor is None
    assert env.projection.cas_calls == 1
    write = env.projection.writes[0]
    assert write.expected_version == 1
    assert write.snapshot.entitlement_version == 2
    assert write.snapshot.features == frozenset({"create_run", "serving_access"})
    assert [event.event_type for event in write.audit_events] == [DRIFT_EVENT, CORRECTED_EVENT]
    assert env.audit.events == []
    assert env.enforcer.calls == []


def test_reconciliation_sem_drift_nao_incrementa_versao():
    env = make_env()
    env.projection.snapshots["ba_01"] = make_snapshot(
        valid_until=NOW + timedelta(days=60),
        updated_at=NOW - timedelta(days=1),
        source_event_id="evt_outro",
    )
    result = _run(env)
    assert (result.examined, result.drift_found, result.corrected) == (1, 0, 0)
    assert env.projection.cas_calls == 0
    assert env.projection.snapshots["ba_01"].entitlement_version == 1
    assert env.audit.events == []


def test_campos_comparados_sao_exatamente_os_financeiros_e_de_acesso():
    assert COMPARED_FIELDS == (
        "subscription_status",
        "stripe_subscription_id",
        "plan_version_id",
        "features",
        "quotas",
        "period_start",
        "period_end",
        "cancel_at_period_end",
        "grace_until",
    )


def test_admin_revoked_e_preservado_e_drift_auditado_sem_correcao():
    env = _admin_revoked_env()
    current = env.projection.snapshots["ba_01"]
    result = _run(env)
    assert (result.drift_found, result.corrected, result.failed) == (1, 0, 0)
    assert env.projection.cas_calls == 0
    [event] = env.audit.events
    assert event.event_type == DRIFT_EVENT
    assert event.attributes["corrected"] is False
    assert event.attributes["drift_fields"] == "subscription_status,features"
    assert event.attributes["subscription_status"] == "admin_revoked"
    assert env.projection.snapshots["ba_01"] == current
    assert env.enforcer.calls == []


@pytest.mark.parametrize(
    "status", [SubscriptionStatus.CANCELED, SubscriptionStatus.UNPAID], ids=["canceled", "unpaid"]
)
def test_perda_de_acesso_delega_a_revogacao_sem_admin_revoked(status):
    env = make_env()
    env.stripe.states = [make_state(subscription_status=status)]
    env.enforcer.fenced = ("run-1", "run-2")
    result = _run(env)
    written = env.projection.writes[0].snapshot
    assert (result.drift_found, result.corrected) == (1, 1)
    assert written.subscription_status is status
    assert env.enforcer.calls == [(written, "system:reconciler")]
    [metric] = env.metrics.named(RUNS_METRIC)
    assert metric.value == 2
    assert dict(metric.dimensions) == {"Reason": "stripe_access_loss"}
    assert metric.occurred_at == NOW


def test_perda_de_acesso_sem_runs_nao_emite_metrica_de_cancelamento():
    env = make_env()
    env.stripe.states = [make_state(subscription_status=SubscriptionStatus.CANCELED)]
    _run(env)
    assert len(env.enforcer.calls) == 1
    assert env.metrics.named(RUNS_METRIC) == []


def test_cancelamento_no_fim_do_periodo_nao_delega():
    env = make_env()
    env.stripe.states = [make_state(cancel_at_period_end=True)]
    result = _run(env)
    assert (result.drift_found, result.corrected) == (1, 1)
    assert env.projection.writes[0].snapshot.cancel_at_period_end is True
    assert env.enforcer.calls == []


def test_grace_expirado_sem_drift_delega_enforcement():
    env = make_env()
    start = NOW - timedelta(days=10)
    snapshot = make_snapshot(
        subscription_status=SubscriptionStatus.PAST_DUE,
        period_start=start,
        period_end=NOW + timedelta(days=20),
        grace_until=NOW - timedelta(days=1),
    )
    env.projection.snapshots["ba_01"] = snapshot
    env.stripe.states = [
        make_state(
            subscription_status=SubscriptionStatus.PAST_DUE,
            period_start=start,
            period_end=NOW + timedelta(days=20),
        )
    ]
    result = _run(env)
    assert (result.drift_found, result.corrected) == (0, 0)
    assert env.projection.cas_calls == 0
    assert env.enforcer.calls == [(snapshot, RECONCILER_ACTOR_ID)]


def test_valid_until_vencido_sem_drift_nao_delega():
    env = make_env()
    start = NOW - timedelta(days=40)
    env.projection.snapshots["ba_01"] = make_snapshot(
        period_start=start,
        period_end=NOW - timedelta(days=10),
        updated_at=NOW - timedelta(days=2),
        valid_until=NOW - timedelta(hours=1),
    )
    env.stripe.states = [make_state(period_start=start, period_end=NOW - timedelta(days=10))]
    result = _run(env)
    assert result.drift_found == 0
    assert env.projection.cas_calls == 0
    assert env.enforcer.calls == []


def test_retry_sem_drift_retoma_enforcement_pendente():
    env = make_env()
    corrected = make_snapshot(version=2, subscription_status=SubscriptionStatus.CANCELED)
    env.projection.snapshots["ba_01"] = corrected
    env.stripe.states = [make_state(subscription_status=SubscriptionStatus.CANCELED)]
    result = _run(env)
    assert (result.drift_found, result.corrected, result.failed) == (0, 0, 0)
    assert env.projection.cas_calls == 0
    assert env.enforcer.calls == [(corrected, RECONCILER_ACTOR_ID)]


def test_erro_do_enforcer_falha_a_conta_e_nao_avanca_cursor():
    env = make_env()
    env.stripe.states = [make_state(subscription_status=SubscriptionStatus.CANCELED)]
    env.enforcer.error = BillingDependencyError("dynamodb_unavailable")
    result = _run(env)
    assert (result.failed, result.next_cursor) == (1, None)
    assert env.cursor.saves == []


def test_cas_perdido_com_projector_corrigindo_audita_drift_sem_correcao():
    env = _single_drift_env()
    env.projection.racers = [make_snapshot(version=2)]
    result = _run(env)
    assert (result.drift_found, result.corrected, result.failed) == (1, 0, 0)
    assert env.projection.cas_calls == 1
    [event] = env.audit.events
    assert event.event_type == DRIFT_EVENT
    assert event.attributes["corrected"] is False
    assert event.attributes["entitlement_version"] == 1
    assert event.attributes["previous_version"] == 1


def test_cas_perdido_rele_e_rebusca_stripe():
    env = _single_drift_env()
    env.projection.racers = [drifted(version=2)]
    env.stripe.states = [
        make_state(cancel_at_period_end=True),
        make_state(cancel_at_period_end=False),
    ]
    result = _run(env)
    assert (result.drift_found, result.corrected) == (1, 1)
    assert env.projection.cas_calls == 2
    assert env.projection.writes[1].expected_version == 2
    assert env.projection.writes[1].snapshot.cancel_at_period_end is False
    assert env.events == ["snapshot", "stripe", "cas", "snapshot", "stripe", "cas"]


def test_cas_esgotado_falha_a_conta(caplog):
    env = _single_drift_env()
    env.projection.racers = [drifted(version=2), drifted(version=3), drifted(version=4)]
    with caplog.at_level(logging.WARNING):
        result = _run(env)
    assert (result.failed, result.drift_found, result.corrected) == (1, 0, 0)
    assert env.projection.cas_calls == 3
    assert "code=reconciliation_cas_exhausted" in caplog.text


def test_snapshot_removido_entre_tentativas_falha_a_conta(caplog):
    env = _single_drift_env()
    env.projection.racers = [None]
    with caplog.at_level(logging.WARNING):
        result = _run(env)
    assert result.failed == 1
    assert "code=entitlement_snapshot_missing" in caplog.text


def test_falha_retryable_para_o_lote_e_nao_avanca_cursor(caplog):
    env = make_env("ba_01", "ba_02", "ba_03")
    env.stripe.states = [make_state()]
    original = env.stripe.get_current_state

    def flaky(request):
        if len(env.stripe.requests) == 1:
            env.stripe.error = RetryableBillingError("stripe_unavailable")
        return original(request)

    env.stripe.get_current_state = flaky
    with caplog.at_level(logging.WARNING):
        result = _run(env)
    assert (result.examined, result.failed, result.next_cursor) == (2, 1, "ba_01")
    assert env.cursor.saves == ["ba_01"]
    assert len(env.stripe.requests) == 2
    assert "billing_account_id=ba_02 code=stripe_unavailable" in caplog.text


def test_falha_permanente_conta_em_failed_e_o_cursor_avanca():
    env = make_env("ba_01", "ba_02")
    env.stripe.error = PermanentBillingError("stripe_subscription_ambiguous")
    result = _run(env)
    assert (result.examined, result.failed, result.next_cursor) == (2, 2, None)
    assert env.cursor.saves == ["ba_01", "ba_02", None]
    assert len(env.metrics.named(DRIFT_METRIC)) == 1


def test_preco_nao_mapeado_no_meio_da_pagina_nao_para_o_lote(caplog):
    env = make_env("ba_01", "ba_02", "ba_03")
    env.stripe.states = [make_state(), make_state(stripe_price_id="price_legado"), make_state()]
    with caplog.at_level(logging.WARNING):
        result = _run(env)
    assert (result.examined, result.failed, result.drift_found) == (3, 1, 0)
    assert env.cursor.saves == ["ba_01", "ba_02", "ba_03", None]
    assert "billing_account_id=ba_02 code=stripe_price_unmapped" in caplog.text


def test_conta_sem_snapshot_e_ignorada(caplog):
    env = make_env()
    del env.projection.snapshots["ba_01"]
    with caplog.at_level(logging.INFO):
        result = _run(env)
    assert (result.examined, result.drift_found, result.failed) == (1, 0, 0)
    assert env.stripe.requests == []
    assert env.enforcer.calls == []
    assert env.cursor.saves == ["ba_01", None]
    assert "reason=snapshot_missing" in caplog.text


def test_cursor_avanca_apos_cada_conta_e_conclui_ciclo():
    env = make_env("ba_01", "ba_02", "ba_03")
    result = _run(env)
    assert env.cursor.saves == ["ba_01", "ba_02", "ba_03", None]
    assert (result.examined, result.next_cursor) == (3, None)
    assert env.cursor.stored.version == 4


def test_pagina_vazia_sem_proximo_cursor_conclui_ciclo():
    env = make_env("ba_01")
    env.cursor.stored = replace(env.cursor.stored, position="ba_01", version=3)
    result = _run(env)
    assert (result.examined, result.next_cursor) == (0, None)
    assert env.cursor.saves == [None]
    assert env.catalog.calls == [(10, "ba_01")]


def test_cursor_da_requisicao_tem_precedencia():
    env = make_env("ba_01", "ba_02", "ba_03")
    env.cursor.stored = replace(env.cursor.stored, position="ba_01", version=1)
    result = _run(env, ReconciliationRequest(limit=5, cursor="ba_02"))
    assert env.catalog.calls == [(5, "ba_02")]
    assert result.examined == 1


def test_cursor_persistido_e_usado_sem_cursor_na_requisicao():
    env = make_env("ba_01", "ba_02")
    env.cursor.stored = replace(env.cursor.stored, position="ba_01", version=1)
    result = _run(env)
    assert env.catalog.calls == [(10, "ba_01")]
    assert result.examined == 1


def test_pagina_com_next_cursor_persiste_cursor_da_listagem():
    env = make_env("ba_01")
    env.catalog.next_cursor = "ba_09"
    result = _run(env)
    assert env.cursor.saves == ["ba_01", "ba_09"]
    assert result.next_cursor == "ba_09"


def test_cursor_disputado_lanca_retryable():
    env = make_env("ba_01", "ba_02")
    env.cursor.contended_after = 1
    with pytest.raises(RetryableBillingError, match="reconciliation_cursor_contended"):
        _run(env)


def test_replay_de_drift_sem_correcao_usa_event_id_deterministico():
    first, second = _admin_revoked_env(), _admin_revoked_env()
    _run(first)
    _run(second)
    [one], [two] = first.audit.events, second.audit.events
    assert one.event_id == two.event_id
    expected = deterministic_id(
        DRIFT_EVENT, "ba_01", "1", str(one.attributes["new_snapshot_sha256"])
    )
    assert one.event_id == expected


@pytest.mark.parametrize("drift", [True, False], ids=["com_drift", "sem_drift"])
def test_metrica_reconciliation_drift_emitida_por_execucao(drift):
    env = _single_drift_env() if drift else make_env()
    _run(env)
    [metric] = env.metrics.named(DRIFT_METRIC)
    assert metric.value == (1 if drift else 0)
    assert metric.occurred_at == NOW
    assert dict(metric.dimensions) == {}


def test_dependencias_reais_satisfazem_os_ports():
    cursor = DynamoReconciliationCursor(MagicMock(), "tabela", lambda: NOW)
    service = ImmediateRevocationService(RevocationDependencies(*[MagicMock() for _ in range(5)]))
    assert isinstance(cursor, ReconciliationCursorPort)
    assert isinstance(service, AccessLossEnforcerPort)


def test_audit_nao_contem_payload_nem_segredo():
    env = _single_drift_env()
    _run(env)
    drift_event, corrected_event = env.projection.committed_audits
    for event in (drift_event, corrected_event):
        assert event.actor_id == RECONCILER_ACTOR_ID
        assert event.reason_code == RECONCILIATION_REASON
        assert event.aggregate_id == "ba_01"
        assert event.occurred_at == NOW
        assert set(event.attributes) == {
            "entitlement_version", "previous_version", "prior_snapshot_sha256",
            "new_snapshot_sha256", "drift_fields", "stripe_customer_id",
            "stripe_subscription_id", "latest_invoice_id", "previous_status",
            "subscription_status", "corrected",
        }
        assert all(isinstance(v, str | int | bool | None) for v in event.attributes.values())
        assert len(cast("str", event.attributes["new_snapshot_sha256"])) == 64
        assert event.attributes["stripe_customer_id"] == "cus_01"
        assert event.attributes["latest_invoice_id"] == "in_01"
        assert event.attributes["corrected"] is True
    assert drift_event.attributes["entitlement_version"] == 1
    assert corrected_event.attributes["entitlement_version"] == 2
    assert drift_event.event_id != corrected_event.event_id
