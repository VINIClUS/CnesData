"""Testes do gate de entitlement para operações críticas e de serving."""

import inspect
from collections.abc import Callable
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from cnes_domain.billing.commands import (
    AnalyticsRequest,
    CreateRunRequest,
    GateRequest,
    PublishGateRequest,
    ReserveRunCommand,
)
from cnes_domain.billing.errors import EntitlementDenied
from cnes_domain.billing.gate import (
    EntitlementCacheReader,
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.models import (
    AccessLevel,
    EntitlementSnapshot,
    QuotaLimits,
    ReadConsistency,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.billing.ports import EntitlementProjectionPort, QuotaReservationPort
from cnes_domain.control_plane.entities import RunDependency
from cnes_domain.profiles import BillingMode

_NOW = datetime(2026, 9, 15, 12, tzinfo=UTC)
_HOUR = timedelta(hours=1)
_TTL = timedelta(minutes=5)
_ACCOUNT = "ba-1"
_S = SubscriptionStatus
_QUOTAS = QuotaLimits(
    max_tenants=3,
    max_agents=5,
    max_runs_per_period=100,
    max_concurrency=4,
    retention_days=365,
    athena_scan_budget_bytes=1_000_000,
)
_BASE = EntitlementSnapshot(
    billing_account_id=_ACCOUNT,
    stripe_subscription_id="sub_1",
    subscription_status=_S.ACTIVE,
    cancel_at_period_end=False,
    plan_version_id="plan-v1",
    features=frozenset({"analytics_query", "serving_history"}),
    quotas=_QUOTAS,
    period_start=datetime(2026, 9, 1, tzinfo=UTC),
    period_end=datetime(2026, 10, 1, tzinfo=UTC),
    grace_until=None,
    valid_until=_NOW + _HOUR,
    entitlement_version=7,
    updated_at=_NOW - _HOUR,
    source_event_id="evt_1",
)
_GATE_REQUEST = GateRequest(_ACCOUNT, "354130")
_RUN_REQUEST = CreateRunRequest(
    billing_account_id=_ACCOUNT,
    tenant_id="354130",
    run_id="run-1",
    competencia="2026-08",
    dataset_name="cnes",
    dependencies=(RunDependency(source_type="CNES", file_subtype="LOCAL", required=True),),
    idempotency_key="req-1",
    request_hash="a" * 64,
    requested_concurrency=2,
    estimated_scan_bytes=0,
)
_ANALYTICS_REQUEST = AnalyticsRequest(
    billing_account_id=_ACCOUNT,
    tenant_id="354130",
    query_id="q-1",
    idempotency_key="req-2",
    request_hash="b" * 64,
    estimated_scan_bytes=4096,
)


def _publish_request(expected_version: int = 7) -> PublishGateRequest:
    return PublishGateRequest(_ACCOUNT, "354130", "run-1", expected_version, 1)


def _snapshot(status: SubscriptionStatus = _S.ACTIVE, **overrides: Any) -> EntitlementSnapshot:
    return replace(_BASE, subscription_status=status, **overrides)


class _SpyProjection:
    def __init__(self, snapshot: EntitlementSnapshot | None) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[str, ReadConsistency]] = []

    def get_snapshot(
        self, billing_account_id: str, consistency: ReadConsistency,
    ) -> EntitlementSnapshot | None:
        self.calls.append((billing_account_id, consistency))
        return self.snapshot

    def compare_and_set_snapshot(self, command: Any) -> bool:
        raise AssertionError("write_called")

    def commit_claimed_snapshot(self, claim: Any, command: Any) -> bool:
        raise AssertionError("write_called")

    def complete_claim_unchanged(
        self, claim: Any, billing_account_id: str, expected_version: int,
    ) -> bool:
        raise AssertionError("write_called")


class _SpyQuotas:
    def __init__(self) -> None:
        self.commands: list[ReserveRunCommand] = []

    def reserve_and_create_run(self, command: ReserveRunCommand) -> RunAuthorization:
        self.commands.append(command)
        return RunAuthorization(
            billing_account_id=command.request.billing_account_id,
            plan_version_id=command.snapshot.plan_version_id,
            entitlement_version=command.snapshot.entitlement_version,
            max_concurrency=command.deployment_max_concurrency,
            budget_reservation_id="budget-1",
            authorized_at=_NOW,
        )

    def reserve_analytics(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")

    def reserve_capacity(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")

    def consume_capacity(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")

    def release_capacity(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")

    def consume(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")

    def release(self, command: Any) -> Any:
        raise AssertionError("unexpected_call")


class _SpyCache:
    def __init__(self, snapshot: EntitlementSnapshot | None) -> None:
        self.snapshot = snapshot
        self.calls: list[str] = []

    def get_latest(self, billing_account_id: str) -> EntitlementSnapshot | None:
        self.calls.append(billing_account_id)
        return self.snapshot


class _Factory:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> str:
        self.calls += 1
        return "res-1"


class _Harness:
    def __init__(
        self,
        snapshot: EntitlementSnapshot | None,
        cache: _SpyCache | None = None,
        policy: EntitlementPolicy | None = None,
    ) -> None:
        self.projection = _SpyProjection(snapshot)
        self.quotas = _SpyQuotas()
        self.factory = _Factory()
        self.cache = cache
        self.gate = EntitlementGate(EntitlementGateDependencies(
            projection=self.projection,
            quotas=self.quotas,
            clock=lambda: _NOW,
            run_settings=RunReservationSettings(4, self.factory, _TTL),
            policy=policy or EntitlementPolicy(),
            cache=cache,
        ))


_CRITICAL: dict[str, Callable[[EntitlementGate], object]] = {
    "create_run": lambda g: g.authorize_create_run(_RUN_REQUEST),
    "register_agent": lambda g: g.authorize_register_agent(_GATE_REQUEST),
    "analytics_query": lambda g: g.authorize_analytics_query(_ANALYTICS_REQUEST),
    "tenant_creation": lambda g: g.authorize_tenant_creation(_GATE_REQUEST),
    "publish_run": lambda g: g.authorize_publish_run(_publish_request()),
}
_CRITICAL_IDS = list(_CRITICAL)
_ALL: dict[str, Callable[[EntitlementGate], object]] = {
    **_CRITICAL,
    "serving_access": lambda g: g.authorize_serving_access(_GATE_REQUEST),
}
_STRONG_CALL = [(_ACCOUNT, ReadConsistency.STRONG)]


@pytest.mark.parametrize("operation", _CRITICAL_IDS)
def test_operacoes_criticas_leem_snapshot_com_consistencia_forte(operation: str) -> None:
    harness = _Harness(_snapshot())
    _CRITICAL[operation](harness.gate)
    assert harness.projection.calls == _STRONG_CALL


@pytest.mark.parametrize("operation", _CRITICAL_IDS)
def test_critical_gate_ignora_snapshot_em_cache(operation: str) -> None:
    cache = _SpyCache(_snapshot())
    harness = _Harness(_snapshot(_S.ADMIN_REVOKED), cache)
    with pytest.raises(EntitlementDenied, match="reason=admin_revoked"):
        _CRITICAL[operation](harness.gate)
    assert cache.calls == []
    assert harness.projection.calls == _STRONG_CALL


@pytest.mark.parametrize("operation", list(_ALL))
def test_rejeita_operacao_quando_snapshot_ausente(operation: str) -> None:
    harness = _Harness(None)
    with pytest.raises(EntitlementDenied, match="reason=snapshot_missing"):
        _ALL[operation](harness.gate)
    assert harness.projection.calls == _STRONG_CALL


@pytest.mark.parametrize("operation", list(_ALL))
def test_rejeita_snapshot_de_outra_conta_na_leitura_forte(operation: str) -> None:
    harness = _Harness(_snapshot(billing_account_id="ba-outra"))
    with pytest.raises(EntitlementDenied, match="reason=snapshot_account_mismatch"):
        _ALL[operation](harness.gate)
    assert harness.quotas.commands == []


def test_serving_rejeita_snapshot_em_cache_de_outra_conta() -> None:
    cache = _SpyCache(_snapshot(billing_account_id="ba-outra"))
    harness = _Harness(_snapshot(), cache)
    with pytest.raises(EntitlementDenied, match="reason=snapshot_account_mismatch"):
        harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=True)
    assert harness.projection.calls == []


def test_serving_nega_snapshot_revogado_vindo_do_cache() -> None:
    cache = _SpyCache(_snapshot(_S.ADMIN_REVOKED))
    harness = _Harness(_snapshot(), cache)
    with pytest.raises(EntitlementDenied, match="reason=admin_revoked"):
        harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=True)
    assert harness.projection.calls == []


def test_serving_com_cache_permitido_usa_snapshot_em_cache() -> None:
    cache = _SpyCache(_snapshot())
    harness = _Harness(None, cache)
    decision = harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=True)
    assert decision.allowed
    assert cache.calls == [_ACCOUNT]
    assert harness.projection.calls == []


def test_serving_com_cache_vazio_cai_para_leitura_forte() -> None:
    cache = _SpyCache(None)
    harness = _Harness(_snapshot(), cache)
    decision = harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=True)
    assert decision.allowed
    assert cache.calls == [_ACCOUNT]
    assert harness.projection.calls == _STRONG_CALL


def test_serving_sem_allow_cached_nao_consulta_cache() -> None:
    cache = _SpyCache(_snapshot(_S.ADMIN_REVOKED))
    harness = _Harness(_snapshot(), cache)
    decision = harness.gate.authorize_serving_access(_GATE_REQUEST)
    assert decision.allowed
    assert cache.calls == []
    assert harness.projection.calls == _STRONG_CALL


def test_serving_allow_cached_sem_cache_configurado_le_forte() -> None:
    harness = _Harness(_snapshot())
    decision = harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=True)
    assert decision.allowed
    assert harness.projection.calls == _STRONG_CALL


def test_serving_cancelado_retorna_decisao_somente_leitura() -> None:
    harness = _Harness(_snapshot(_S.CANCELED))
    decision = harness.gate.authorize_serving_access(_GATE_REQUEST)
    assert decision.allowed
    assert decision.access_level is AccessLevel.READ_ONLY


def test_serving_revogado_pela_projecao_e_negado() -> None:
    cache = _SpyCache(_snapshot())
    harness = _Harness(_snapshot(_S.ADMIN_REVOKED), cache)
    with pytest.raises(EntitlementDenied, match="reason=admin_revoked"):
        harness.gate.authorize_serving_access(_GATE_REQUEST, allow_cached=False)
    assert cache.calls == []


def test_create_run_reserva_quota_com_comando_completo() -> None:
    snapshot = _snapshot()
    harness = _Harness(snapshot)
    authorization = harness.gate.authorize_create_run(_RUN_REQUEST)
    assert authorization.budget_reservation_id == "budget-1"
    (command,) = harness.quotas.commands
    assert command.request == _RUN_REQUEST
    assert command.snapshot == snapshot
    assert command.deployment_max_concurrency == 4
    assert command.reservation_id == "res-1"
    assert command.expires_at == _NOW + _TTL


def test_create_run_negado_nao_reserva_nem_gera_id() -> None:
    harness = _Harness(_snapshot(_S.PAUSED))
    with pytest.raises(EntitlementDenied, match="reason=status_paused action=create_run"):
        harness.gate.authorize_create_run(_RUN_REQUEST)
    assert harness.quotas.commands == []
    assert harness.factory.calls == 0


def test_run_authorization_retornada_e_imutavel() -> None:
    authorization = _Harness(_snapshot()).gate.authorize_create_run(_RUN_REQUEST)
    with pytest.raises(FrozenInstanceError):
        authorization.max_concurrency = 99  # type: ignore[misc]


def test_analytics_autoriza_sem_reserva_com_limite_do_orcamento() -> None:
    harness = _Harness(_snapshot())
    result = harness.gate.authorize_analytics_query(_ANALYTICS_REQUEST)
    assert result.budget_reservation_id is None
    assert result.max_scan_bytes == 1_000_000
    assert result.entitlement_version == 7
    assert result.billing_account_id == _ACCOUNT
    assert result.authorized_at == _NOW
    assert harness.quotas.commands == []


def test_analytics_sem_orcamento_no_modo_desabilitado_usa_estimativa_da_requisicao() -> None:
    quotas = replace(_QUOTAS, athena_scan_budget_bytes=None)
    harness = _Harness(_snapshot(quotas=quotas), policy=EntitlementPolicy(BillingMode.DISABLED))
    result = harness.gate.authorize_analytics_query(_ANALYTICS_REQUEST)
    assert result.max_scan_bytes == 4096


def test_analytics_sem_orcamento_no_modo_stripe_e_negado() -> None:
    quotas = replace(_QUOTAS, athena_scan_budget_bytes=None)
    harness = _Harness(_snapshot(quotas=quotas))
    with pytest.raises(EntitlementDenied, match="reason=quota_missing action=analytics_query"):
        harness.gate.authorize_analytics_query(_ANALYTICS_REQUEST)


def test_analytics_sem_feature_e_negado() -> None:
    harness = _Harness(_snapshot(features=frozenset()))
    with pytest.raises(EntitlementDenied, match="reason=feature_missing"):
        harness.gate.authorize_analytics_query(_ANALYTICS_REQUEST)


def test_register_agent_retorna_limite_de_agentes() -> None:
    decision = _Harness(_snapshot()).gate.authorize_register_agent(_GATE_REQUEST)
    assert decision.allowed
    assert decision.quota_limit == 5


def test_tenant_creation_retorna_limite_de_tenants() -> None:
    decision = _Harness(_snapshot()).gate.authorize_tenant_creation(_GATE_REQUEST)
    assert decision.allowed
    assert decision.quota_limit == 3


def test_register_agent_com_cota_zero_e_negado() -> None:
    harness = _Harness(_snapshot(quotas=replace(_QUOTAS, max_agents=0)))
    with pytest.raises(EntitlementDenied, match="reason=quota_not_granted"):
        harness.gate.authorize_register_agent(_GATE_REQUEST)


def test_tenant_creation_com_cota_zero_e_negado() -> None:
    harness = _Harness(_snapshot(quotas=replace(_QUOTAS, max_tenants=0)))
    with pytest.raises(EntitlementDenied, match="reason=quota_not_granted"):
        harness.gate.authorize_tenant_creation(_GATE_REQUEST)


@pytest.mark.parametrize("expected_version", [7, 6])
def test_publish_permite_versao_esperada_menor_ou_igual(expected_version: int) -> None:
    harness = _Harness(_snapshot())
    decision = harness.gate.authorize_publish_run(_publish_request(expected_version))
    assert decision.allowed
    assert harness.projection.calls == _STRONG_CALL


def test_publish_nega_quando_versao_do_snapshot_regrediu() -> None:
    harness = _Harness(_snapshot())
    with pytest.raises(EntitlementDenied, match="reason=snapshot_version_regressed"):
        harness.gate.authorize_publish_run(_publish_request(8))
    assert harness.projection.calls == _STRONG_CALL


def test_rejeita_concorrencia_de_deployment_zero() -> None:
    with pytest.raises(ValueError, match="deployment_max_concurrency"):
        RunReservationSettings(0, _Factory(), _TTL)


@pytest.mark.parametrize("ttl", [timedelta(0), timedelta(seconds=-1)])
def test_rejeita_ttl_de_reserva_nao_positivo(ttl: timedelta) -> None:
    with pytest.raises(ValueError, match="reservation_ttl_not_positive"):
        RunReservationSettings(4, _Factory(), ttl)


def test_dependencias_usam_politica_stripe_por_padrao() -> None:
    dependencies = EntitlementGateDependencies(
        projection=_SpyProjection(_snapshot(features=frozenset({"*"}))),
        quotas=_SpyQuotas(),
        clock=lambda: _NOW,
        run_settings=RunReservationSettings(4, _Factory(), _TTL),
    )
    assert isinstance(dependencies.policy, EntitlementPolicy)
    with pytest.raises(EntitlementDenied, match="reason=feature_missing"):
        EntitlementGate(dependencies).authorize_analytics_query(_ANALYTICS_REQUEST)


def test_spies_satisfazem_os_protocolos_de_porta() -> None:
    assert isinstance(_SpyProjection(None), EntitlementProjectionPort)
    assert isinstance(_SpyQuotas(), QuotaReservationPort)


def test_protocolo_de_cache_expoe_somente_leitura_por_conta() -> None:
    declared = EntitlementCacheReader.get_latest
    assert tuple(inspect.signature(declared).parameters) == ("self", "billing_account_id")
    assert declared(_SpyCache(None), _ACCOUNT) is None
    assert isinstance(_SpyCache(None), EntitlementCacheReader)
    assert not isinstance(object(), EntitlementCacheReader)


def test_analytics_com_orcamento_zero_e_negado() -> None:
    quotas = replace(_QUOTAS, athena_scan_budget_bytes=0)
    harness = _Harness(_snapshot(quotas=quotas))
    with pytest.raises(EntitlementDenied, match="reason=quota_not_granted"):
        harness.gate.authorize_analytics_query(_ANALYTICS_REQUEST)

