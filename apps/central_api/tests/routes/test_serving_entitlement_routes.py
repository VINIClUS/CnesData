"""Negação de entitlement ocorre antes de emitir stream local ou redirect AWS."""
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, cast
from unittest.mock import Mock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from central_api.routes.serving import (
    ServingPrincipal,
    get_serving_access,
    get_serving_delivery,
    get_serving_object_store,
    get_serving_principal,
    router,
    signed_serving_delivery,
)
from central_api.services.billing_gates import ApiBillingGates, TenantAccountResolver
from central_api.services.serving_entitlement import EntitledServingAccess
from central_api.serving.aws_signed import S3SignedServingAccess, SignedServingSettings
from cnes_domain.billing.errors import BillingDependencyError
from cnes_domain.billing.gate import (
    EntitlementGate,
    EntitlementGateDependencies,
    RunReservationSettings,
)
from cnes_domain.billing.models import SubscriptionStatus
from cnes_domain.billing.policy import EntitlementPolicy
from cnes_domain.ports.object_store import ObjectStorePort
from cnes_domain.ports.serving import ServingAccessPort, ServingGrant
from cnes_domain.profiles import BillingMode
from cnes_infra.billing.disabled import DisabledQuotaReservations

from .billing_fakes import QUOTAS, make_snapshot

if TYPE_CHECKING:
    from cnes_domain.billing.ports import EntitlementProjectionPort

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
TENANT = "tenant-a"
KEY = f"serving/{TENANT}/run-01/overview.json"
URL = "/api/v1/dashboard/serving/cnes/overview"


class Projection:
    def __init__(self, status: SubscriptionStatus, error: Exception | None = None) -> None:
        self.snapshot = replace(
            make_snapshot(status), features=frozenset({"serving_history"}),
            quotas=replace(QUOTAS, retention_days=30),
        )
        self.error = error

    def get_snapshot(self, billing_account_id, consistency):
        if self.error is not None:
            raise self.error
        return self.snapshot


def _entitled(projection: Projection) -> EntitledServingAccess:
    quotas = DisabledQuotaReservations(lambda: NOW)
    gate = EntitlementGate(EntitlementGateDependencies(
        projection=cast("EntitlementProjectionPort", projection), quotas=quotas, clock=lambda: NOW,
        run_settings=RunReservationSettings(1, lambda: "res-1", timedelta(minutes=5)),
        policy=EntitlementPolicy(BillingMode.STRIPE),
    ))
    accounts = Mock(spec=TenantAccountResolver)
    accounts.resolve.return_value = "ba_01"
    inner = Mock(spec=ServingAccessPort)
    inner.authorize.return_value = ServingGrant(
        tenant_id=TENANT, run_id="run-01", version_id="run-01", object_keys=(KEY,),
    )
    gates = ApiBillingGates(BillingMode.STRIPE, gate, quotas, accounts)
    return EntitledServingAccess(inner, gates, Mock(), lambda: NOW)


def _app() -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_serving_principal] = lambda: ServingPrincipal(TENANT, "user-1")
    return app


def _local_client(access: EntitledServingAccess, store: Mock) -> TestClient:
    app = _app()
    app.dependency_overrides[get_serving_access] = lambda: access
    app.dependency_overrides[get_serving_object_store] = lambda: store
    return TestClient(app)


def _aws_client(access: EntitledServingAccess, store: Mock, signer: Mock) -> TestClient:
    signed = S3SignedServingAccess(
        access, store, signer, SignedServingSettings(bucket="bucket", ttl_seconds=300),
    )
    app = _app()
    app.dependency_overrides[get_serving_delivery] = lambda: signed_serving_delivery(
        signed, lambda: NOW,
    )
    return TestClient(app)


def _store() -> Mock:
    return Mock(spec=ObjectStorePort)


@pytest.mark.parametrize(
    ("projection", "status", "detail"),
    [
        (Projection(SubscriptionStatus.ADMIN_REVOKED), 403, "serving_entitlement_denied"),
        (
            Projection(SubscriptionStatus.ACTIVE, BillingDependencyError("dynamodb_down")),
            503, "billing_dependency_unavailable",
        ),
    ],
)
def test_entrega_local_nega_antes_de_abrir_o_object_store(projection, status, detail) -> None:
    store = _store()

    response = _local_client(_entitled(projection), store).get(URL)

    assert (response.status_code, response.json()["detail"]) == (status, detail)
    store.stat.assert_not_called()
    store.open.assert_not_called()


@pytest.mark.parametrize(
    ("projection", "status", "detail"),
    [
        (Projection(SubscriptionStatus.ADMIN_REVOKED), 403, "serving_entitlement_denied"),
        (
            Projection(SubscriptionStatus.ACTIVE, BillingDependencyError("dynamodb_down")),
            503, "billing_dependency_unavailable",
        ),
    ],
)
def test_entrega_aws_nega_antes_de_assinar_a_url(projection, status, detail) -> None:
    store, signer = _store(), Mock()

    response = _aws_client(_entitled(projection), store, signer).get(
        URL, follow_redirects=False,
    )

    assert (response.status_code, response.json()["detail"]) == (status, detail)
    signer.generate_presigned_url.assert_not_called()
    store.stat.assert_not_called()
