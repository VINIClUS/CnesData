"""Gate de entitlement do serving: ordem, retenção, cache e auditoria sem ids."""
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import Mock

import pytest

from central_api.services.billing_gates import (
    ApiBillingGates,
    BillingAccountMissing,
    TenantAccountResolver,
)
from central_api.services.serving_access import ServingUnavailable
from central_api.services.serving_entitlement import (
    DatasetVersionReader,
    EntitledServingAccess,
)
from cnes_domain.billing.errors import (
    BillingDependencyError,
    EntitlementDenied,
    PermanentBillingError,
    RetryableBillingError,
)
from cnes_domain.billing.gate import (
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.models import (
    EntitlementSnapshot,
    QuotaLimits,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.control_plane.entities import DatasetVersion
from cnes_domain.ports.serving import ServingAccessPort, ServingGrant, ServingRequest
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.cache import LocalEntitlementCache
from cnes_infra.billing.disabled import DisabledQuotaReservations, disabled_snapshot

if TYPE_CHECKING:
    from cnes_domain.billing.ports import EntitlementProjectionPort

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
TENANT = "tenant-a"
USER = "user-secret-1"
ACCOUNT = "ba_secret_01"
RUN_ID = "run-01"
KEY = f"serving/{TENANT}/{RUN_ID}/overview.json"
RETENTION_DAYS = 30
LOGGER = "central_api.services.serving_entitlement"


def _snapshot(
    status: SubscriptionStatus = SubscriptionStatus.ACTIVE,
    retention_days: int | None = RETENTION_DAYS,
    features: frozenset[str] = frozenset({"serving_history"}),
) -> EntitlementSnapshot:
    return EntitlementSnapshot(
        ACCOUNT, "sub_1", status, False, "plan-1", features,
        QuotaLimits(1, 1, 1, 1, retention_days, 1),
        NOW, NOW, None, NOW + timedelta(days=1), 3, NOW, "evt_1",
    )


class FakeProjection:
    def __init__(self, snapshot=None, error: Exception | None = None) -> None:
        self.snapshot = snapshot
        self.error = error
        self.calls: list[tuple[str, ReadConsistency]] = []

    def get_snapshot(self, billing_account_id: str, consistency: ReadConsistency):
        self.calls.append((billing_account_id, consistency))
        if self.error is not None:
            raise self.error
        return self.snapshot


class FakeVersions:
    def __init__(self, version: DatasetVersion | None) -> None:
        self.version = version
        self.calls: list[tuple[str, str, str]] = []

    def get_dataset_version(self, tenant_id: str, dataset_name: str, version_id: str):
        self.calls.append((tenant_id, dataset_name, version_id))
        return self.version


def _gates(
    projection: FakeProjection,
    cache: LocalEntitlementCache | None = None,
    mode: BillingMode = BillingMode.STRIPE,
    resolver: Mock | TenantAccountResolver | None = None,
) -> ApiBillingGates:
    quotas = DisabledQuotaReservations(lambda: NOW)
    gate = EntitlementGate(EntitlementGateDependencies(
        projection=cast("EntitlementProjectionPort", projection),
        quotas=quotas,
        clock=lambda: NOW,
        run_settings=RunReservationSettings(1, lambda: "res-1", timedelta(minutes=5)),
        policy=EntitlementPolicy(mode),
        cache=cache,
    ))
    accounts = resolver or Mock(spec=TenantAccountResolver)
    if resolver is None:
        cast("Mock", accounts.resolve).return_value = ACCOUNT
    return ApiBillingGates(mode, gate, quotas, accounts)


def _version(created_at: datetime) -> DatasetVersion:
    return DatasetVersion(
        tenant_id=TENANT, dataset_name="cnes", version_id=RUN_ID, run_id=RUN_ID,
        run_manifest_key=f"reconciliation/{TENANT}/2026-09/{RUN_ID}/run-manifest.json",
        created_at=created_at,
    )


def _inner(error: ServingUnavailable | None = None) -> Mock:
    inner = Mock(spec=ServingAccessPort)
    if error is not None:
        inner.authorize.side_effect = error
    else:
        inner.authorize.return_value = ServingGrant(
            tenant_id=TENANT, run_id=RUN_ID, version_id=RUN_ID, object_keys=(KEY,),
        )
    return inner


def _request() -> ServingRequest:
    return ServingRequest(user_id=USER, tenant_id=TENANT, dataset_name="cnes")


def _access(gates: ApiBillingGates, versions=None, inner=None) -> EntitledServingAccess:
    return EntitledServingAccess(
        inner or _inner(), gates, versions or FakeVersions(None), lambda: NOW,
    )


def _audit_lines(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if "serving_denied" in r.getMessage()]


def _assert_sem_ids(caplog) -> None:
    for secret in (TENANT, USER, ACCOUNT, RUN_ID, "cnes"):
        assert secret not in caplog.text


def test_admin_revoked_nega_na_hora_mesmo_com_snapshot_anterior_em_cache(caplog) -> None:
    cache = LocalEntitlementCache(60, lambda: NOW)
    cache.put(_snapshot())
    projection = FakeProjection(_snapshot(SubscriptionStatus.ADMIN_REVOKED))
    access = _access(_gates(projection, cache))
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        access.authorize(_request())

    assert captured.value.code == "entitlement_denied"
    assert projection.calls == [(ACCOUNT, ReadConsistency.STRONG)]
    assert _audit_lines(caplog) == [
        "serving_denied reason=admin_revoked access_level=blocked"
    ]
    _assert_sem_ids(caplog)


def test_ativo_libera_grant() -> None:
    inner = _inner()
    access = _access(_gates(FakeProjection(_snapshot())), inner=inner)

    assert access.authorize(_request()) is inner.authorize.return_value


def test_nao_membro_nega_antes_do_gate(caplog) -> None:
    projection = FakeProjection(_snapshot())
    gates = _gates(projection)
    access = _access(gates, inner=_inner(ServingUnavailable("membership_denied")))
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        access.authorize(_request())

    assert captured.value.code == "membership_denied"
    cast("Mock", gates.accounts.resolve).assert_not_called()
    assert projection.calls == []
    assert _audit_lines(caplog) == []


def test_read_only_dentro_da_retencao_libera() -> None:
    versions = FakeVersions(_version(NOW - timedelta(days=RETENTION_DAYS)))
    gates = _gates(FakeProjection(_snapshot(SubscriptionStatus.CANCELED)))
    inner = _inner()

    grant = _access(gates, versions, inner).authorize(_request())

    assert grant is inner.authorize.return_value
    assert versions.calls == [(TENANT, "cnes", RUN_ID)]


def test_read_only_fora_da_retencao_nega(caplog) -> None:
    versions = FakeVersions(_version(NOW - timedelta(days=RETENTION_DAYS, seconds=1)))
    gates = _gates(FakeProjection(_snapshot(SubscriptionStatus.CANCELED)))
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates, versions).authorize(_request())

    assert captured.value.code == "retention_expired"
    assert _audit_lines(caplog) == [
        "serving_denied reason=retention_expired access_level=read_only"
    ]
    _assert_sem_ids(caplog)


def test_read_only_sem_versao_indisponivel(caplog) -> None:
    gates = _gates(FakeProjection(_snapshot(SubscriptionStatus.CANCELED)))
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        _access(gates, FakeVersions(None)).authorize(_request())

    assert captured.value.code == "serving_version_unavailable"
    assert _audit_lines(caplog) == []
    assert "serving_entitlement_unavailable code=serving_version_unavailable" in caplog.text


def test_full_nao_le_versao_mesmo_com_retencao() -> None:
    versions = FakeVersions(None)
    gates = _gates(FakeProjection(_snapshot()))

    _access(gates, versions).authorize(_request())

    assert versions.calls == []


def test_read_only_sem_limite_de_retencao_libera_sem_ler_versao() -> None:
    versions = FakeVersions(None)
    gates = _gates(
        FakeProjection(_snapshot(SubscriptionStatus.CANCELED, retention_days=None)),
        mode=BillingMode.DISABLED,
    )

    _access(gates, versions).authorize(_request())

    assert versions.calls == []


def test_conta_ausente_em_stripe_nega(caplog) -> None:
    resolver = Mock(spec=TenantAccountResolver)
    resolver.resolve.side_effect = BillingAccountMissing()
    projection = FakeProjection(_snapshot())
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        _access(_gates(projection, resolver=resolver)).authorize(_request())

    assert captured.value.code == "billing_account_missing"
    assert projection.calls == []
    assert _audit_lines(caplog) == [
        "serving_denied reason=billing_account_missing "
        "access_level=blocked"
    ]
    _assert_sem_ids(caplog)


def test_snapshot_ausente_nega_com_motivo_do_dominio(caplog) -> None:
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        _access(_gates(FakeProjection(None))).authorize(_request())

    assert captured.value.code == "entitlement_denied"
    assert _audit_lines(caplog) == [
        "serving_denied reason=snapshot_missing access_level=blocked"
    ]


def test_negacao_sem_reason_no_dominio_usa_codigo_padrao(caplog) -> None:
    gates = _gates(FakeProjection(_snapshot()))
    gates.gate.authorize_serving_access = Mock(side_effect=EntitlementDenied("opaque"))
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable):
        _access(gates).authorize(_request())

    assert "reason=entitlement_denied access_level=blocked" in caplog.text


@pytest.mark.parametrize("error", [
    RetryableBillingError("dynamodb_throttled"),
    BillingDependencyError("dynamodb_unavailable"),
    PermanentBillingError("billing_item_corrupt"),
])
def test_projecao_indisponivel_503(error, caplog) -> None:
    caplog.set_level("INFO", logger=LOGGER)

    with pytest.raises(ServingUnavailable) as captured:
        _access(_gates(FakeProjection(error=error))).authorize(_request())

    assert captured.value.code == "entitlement_unavailable"
    assert _audit_lines(caplog) == []
    assert "serving_entitlement_unavailable code=entitlement_unavailable" in caplog.text


def test_disabled_libera_sem_medicao() -> None:
    resolver = TenantAccountResolver(BillingMode.DISABLED)
    projection = FakeProjection(disabled_snapshot("local-tenant-a", NOW))
    gates = _gates(projection, mode=BillingMode.DISABLED, resolver=resolver)
    inner = _inner()

    grant = _access(gates, inner=inner).authorize(_request())

    assert grant is inner.authorize.return_value
    assert projection.calls == [("local-tenant-a", ReadConsistency.STRONG)]


def test_contrato_do_leitor_de_versao_nao_tem_implementacao() -> None:
    reader = cast("Any", DatasetVersionReader)
    assert reader.get_dataset_version(Mock(), TENANT, "cnes", RUN_ID) is None
