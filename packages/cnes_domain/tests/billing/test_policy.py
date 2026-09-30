"""Testes da política de entitlement por status e ação."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from itertools import product
from typing import Any

import pytest

from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.models import (
    AccessLevel,
    EntitlementAction,
    EntitlementDecision,
    EntitlementSnapshot,
    QuotaLimits,
    SubscriptionStatus,
)
from cnes_domain.billing.policy import (
    CRITICAL_ACTIONS,
    WILDCARD_FEATURE,
    EntitlementPolicy,
    require_allowed,
)
from cnes_domain.profiles import BillingMode

_NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
_HOUR = timedelta(hours=1)
_SECOND = timedelta(seconds=1)
_PERIOD_END = datetime(2026, 10, 1, tzinfo=UTC)
_QUOTAS = QuotaLimits(
    max_tenants=3,
    max_agents=5,
    max_runs_per_period=100,
    max_concurrency=4,
    retention_days=365,
    athena_scan_budget_bytes=1_000_000,
)
_BASE = EntitlementSnapshot(
    billing_account_id="ba-1",
    stripe_subscription_id="sub_1",
    subscription_status=SubscriptionStatus.ACTIVE,
    cancel_at_period_end=False,
    plan_version_id="plan-v1",
    features=frozenset({"analytics_query", "serving_history"}),
    quotas=_QUOTAS,
    period_start=datetime(2026, 9, 1, tzinfo=UTC),
    period_end=_PERIOD_END,
    grace_until=None,
    valid_until=_NOW + _HOUR,
    entitlement_version=7,
    updated_at=_NOW - _HOUR,
    source_event_id="evt_1",
)
_A = EntitlementAction
_S = SubscriptionStatus
_FULL = "full"
_READ_ONLY = "read_only"
_BLOCKED = "blocked"


def _snapshot(status: SubscriptionStatus, **overrides: Any) -> EntitlementSnapshot:
    if status is _S.PAST_DUE and "grace_until" not in overrides:
        overrides["grace_until"] = _NOW + timedelta(days=1)
    return replace(_BASE, subscription_status=status, **overrides)


def _evaluate(
    status: SubscriptionStatus,
    action: EntitlementAction,
    policy: EntitlementPolicy | None = None,
    now: datetime = _NOW,
    **overrides: Any,
) -> EntitlementDecision:
    snapshot = _snapshot(status, **overrides)
    return (policy or EntitlementPolicy()).evaluate(snapshot, action, now)


@pytest.mark.parametrize(
    ("status", "action", "allowed", "access"),
    [
        (_S.TRIALING, _A.CREATE_RUN, True, "full"),
        (_S.ACTIVE, _A.CREATE_RUN, True, "full"),
        (_S.PAST_DUE, _A.CREATE_RUN, True, "full"),
        (_S.INCOMPLETE, _A.CREATE_RUN, False, "blocked"),
        (_S.INCOMPLETE_EXPIRED, _A.SERVING_ACCESS, False, "blocked"),
        (_S.UNPAID, _A.SERVING_ACCESS, True, "read_only"),
        (_S.PAUSED, _A.CREATE_RUN, False, "read_only"),
        (_S.CANCELED, _A.SERVING_ACCESS, True, "read_only"),
        (_S.ADMIN_REVOKED, _A.SERVING_ACCESS, False, "blocked"),
    ],
)
def test_politica_status(
    status: SubscriptionStatus, action: EntitlementAction, allowed: bool, access: str
) -> None:
    decision = _evaluate(status, action)
    assert decision.allowed is allowed
    assert decision.access_level.value == access


def _expected(status: SubscriptionStatus, action: EntitlementAction) -> tuple[bool, str]:
    if status in (_S.TRIALING, _S.ACTIVE, _S.PAST_DUE):
        return True, _FULL
    if status in (_S.UNPAID, _S.PAUSED, _S.CANCELED):
        return action is _A.SERVING_ACCESS, _READ_ONLY
    return False, _BLOCKED


@pytest.mark.parametrize(("status", "action"), list(product(_S, _A)))
def test_matriz_completa_status_acao(
    status: SubscriptionStatus, action: EntitlementAction
) -> None:
    allowed, access = _expected(status, action)
    decision = _evaluate(status, action)
    assert (decision.allowed, decision.access_level.value) == (allowed, access)


def test_past_due_permite_no_limite_da_graca() -> None:
    grace = _NOW
    decision = _evaluate(_S.PAST_DUE, _A.CREATE_RUN, grace_until=grace)
    assert decision.allowed
    assert decision.access_level is AccessLevel.FULL


def test_past_due_nega_um_segundo_apos_a_graca() -> None:
    decision = _evaluate(_S.PAST_DUE, _A.CREATE_RUN, grace_until=_NOW - _SECOND)
    assert not decision.allowed
    assert decision.access_level is AccessLevel.READ_ONLY
    assert decision.reason == "grace_expired"


def test_past_due_sem_graca_nega_como_expirada() -> None:
    decision = _evaluate(_S.PAST_DUE, _A.CREATE_RUN, grace_until=None)
    assert not decision.allowed
    assert decision.reason == "grace_expired"


def test_past_due_apos_graca_ainda_permite_serving() -> None:
    decision = _evaluate(_S.PAST_DUE, _A.SERVING_ACCESS, grace_until=_NOW - _SECOND)
    assert decision.allowed
    assert decision.access_level is AccessLevel.READ_ONLY


def test_cancelamento_no_fim_do_periodo_mantem_full_ate_period_end() -> None:
    decision = _evaluate(
        _S.ACTIVE, _A.CREATE_RUN, now=_PERIOD_END, cancel_at_period_end=True,
        valid_until=_PERIOD_END + _HOUR,
    )
    assert decision.allowed
    assert decision.access_level is AccessLevel.FULL


def test_cancelamento_apos_period_end_nega_escrita_e_permite_serving() -> None:
    later = _PERIOD_END + _SECOND
    overrides = {"cancel_at_period_end": True, "valid_until": later + _HOUR}
    write = _evaluate(_S.ACTIVE, _A.CREATE_RUN, now=later, **overrides)
    read = _evaluate(_S.ACTIVE, _A.SERVING_ACCESS, now=later, **overrides)
    assert not write.allowed
    assert write.access_level is AccessLevel.READ_ONLY
    assert write.reason == "period_ended"
    assert read.allowed


def test_period_end_ultrapassado_sem_cancelamento_mantem_full() -> None:
    later = _PERIOD_END + _SECOND
    decision = _evaluate(_S.ACTIVE, _A.CREATE_RUN, now=later, valid_until=later + _HOUR)
    assert decision.allowed


@pytest.mark.parametrize("action", sorted(CRITICAL_ACTIONS))
def test_snapshot_expirado_bloqueia_acoes_criticas(action: EntitlementAction) -> None:
    decision = _evaluate(_S.ACTIVE, action, now=_NOW + _HOUR + _SECOND)
    assert not decision.allowed
    assert decision.access_level is AccessLevel.BLOCKED
    assert decision.reason == "snapshot_expired"


def test_acoes_criticas_conhecidas() -> None:
    expected = {
        _A.CREATE_RUN, _A.REGISTER_AGENT, _A.ANALYTICS_QUERY, _A.TENANT_CREATION,
        _A.PUBLISH_RUN,
    }
    assert set(CRITICAL_ACTIONS) == expected


def test_snapshot_expirado_nao_afeta_serving() -> None:
    decision = _evaluate(_S.ACTIVE, _A.SERVING_ACCESS, now=_NOW + _HOUR + _SECOND)
    assert decision.allowed


@pytest.mark.parametrize("action", sorted(CRITICAL_ACTIONS))
def test_snapshot_no_limite_da_validade_ainda_permite(action: EntitlementAction) -> None:
    decision = _evaluate(_S.ACTIVE, action, now=_NOW + _HOUR)
    assert decision.allowed


@pytest.mark.parametrize("action", [_A.ANALYTICS_QUERY, _A.SERVING_ACCESS])
def test_feature_ausente_nega(action: EntitlementAction) -> None:
    decision = _evaluate(_S.ACTIVE, action, features=frozenset())
    assert not decision.allowed
    assert decision.access_level is AccessLevel.FULL
    assert decision.reason == "feature_missing"
    assert decision.quota_limit is None


@pytest.mark.parametrize("action", [_A.ANALYTICS_QUERY, _A.SERVING_ACCESS])
def test_curinga_concede_feature_no_modo_desabilitado(action: EntitlementAction) -> None:
    policy = EntitlementPolicy(BillingMode.DISABLED)
    decision = _evaluate(_S.ACTIVE, action, policy, features=frozenset({WILDCARD_FEATURE}))
    assert decision.allowed


@pytest.mark.parametrize("action", [_A.ANALYTICS_QUERY, _A.SERVING_ACCESS])
def test_curinga_nao_concede_feature_no_modo_stripe(action: EntitlementAction) -> None:
    decision = _evaluate(_S.ACTIVE, action, features=frozenset({WILDCARD_FEATURE}))
    assert not decision.allowed
    assert decision.reason == "feature_missing"


@pytest.mark.parametrize("action", list(_A))
def test_admin_revogado_nega_toda_acao(action: EntitlementAction) -> None:
    decision = _evaluate(_S.ADMIN_REVOKED, action)
    assert not decision.allowed
    assert decision.access_level is AccessLevel.BLOCKED
    assert decision.reason == "admin_revoked"


def test_admin_revogado_prevalece_sobre_snapshot_expirado() -> None:
    decision = _evaluate(_S.ADMIN_REVOKED, _A.CREATE_RUN, now=_NOW + 2 * _HOUR)
    assert decision.reason == "admin_revoked"


def test_status_bloqueado_informa_motivo_do_status() -> None:
    decision = _evaluate(_S.INCOMPLETE, _A.CREATE_RUN)
    assert decision.reason == "status_incomplete"


def test_cota_zero_de_agentes_nega_registro() -> None:
    quotas = replace(_QUOTAS, max_agents=0)
    decision = _evaluate(_S.ACTIVE, _A.REGISTER_AGENT, quotas=quotas)
    assert not decision.allowed
    assert decision.reason == "quota_not_granted"
    assert decision.access_level is AccessLevel.FULL
    assert decision.quota_limit is None


def test_cota_zero_em_acao_nao_limitada_nao_nega() -> None:
    quotas = replace(_QUOTAS, athena_scan_budget_bytes=0)
    decision = _evaluate(_S.ACTIVE, _A.ANALYTICS_QUERY, quotas=quotas)
    assert decision.allowed
    assert decision.quota_limit == 0


def test_cotas_nulas_sao_ilimitadas() -> None:
    quotas = QuotaLimits(None, None, None, None, None, None)
    decision = _evaluate(_S.ACTIVE, _A.CREATE_RUN, quotas=quotas)
    assert decision.allowed
    assert decision.quota_limit is None


@pytest.mark.parametrize(
    ("action", "limit"),
    [
        (_A.CREATE_RUN, 100),
        (_A.REGISTER_AGENT, 5),
        (_A.TENANT_CREATION, 3),
        (_A.ANALYTICS_QUERY, 1_000_000),
        (_A.SERVING_ACCESS, 365),
        (_A.PUBLISH_RUN, None),
    ],
)
def test_decisao_permitida_informa_limite_do_campo_correto(
    action: EntitlementAction, limit: int | None
) -> None:
    decision = _evaluate(_S.ACTIVE, action)
    assert decision.allowed
    assert decision.reason == "allowed"
    assert decision.quota_limit == limit


@pytest.mark.parametrize(("status", "action"), list(product(_S, _A)))
def test_decisao_carrega_versao_e_acao_do_snapshot(
    status: SubscriptionStatus, action: EntitlementAction
) -> None:
    decision = _evaluate(status, action)
    assert decision.entitlement_version == 7
    assert decision.action is action


def test_require_allowed_devolve_decisao_permitida() -> None:
    decision = _evaluate(_S.ACTIVE, _A.CREATE_RUN)
    assert require_allowed(decision) is decision


def test_require_allowed_levanta_quando_negada() -> None:
    decision = _evaluate(_S.ACTIVE, _A.ANALYTICS_QUERY, features=frozenset())
    with pytest.raises(EntitlementDenied, match="reason=feature_missing action=analytics_query"):
        require_allowed(decision)
