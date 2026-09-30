"""Fakes e builders compartilhados dos testes das rotas de billing."""

from datetime import UTC, datetime
from unittest.mock import create_autospec

from fastapi import FastAPI

from central_api.auth.aws_oidc import AuthorizedTenant
from central_api.routes.billing import (
    TenantAuthorizer,
    get_billing_audit,
    get_billing_catalog,
    get_billing_clock,
    get_billing_mode,
    get_billing_principal,
    get_entitlement_projection,
    get_membership_authorizer,
    get_stripe_gateway,
    router,
)
from central_api.routes.raw_jobs import get_control_plane
from cnes_domain.billing.commands import HostedSession, PendingCheckout, StripeCustomer
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    EntitlementSnapshot,
    PlanVersion,
    QuotaLimits,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import (
    BillingAuditPort,
    BillingCatalogPort,
    EntitlementProjectionPort,
    StripeGatewayPort,
)
from cnes_domain.control_plane.entities import Membership
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_domain.profiles import BillingMode
from cnes_infra.auth.oidc import OidcPrincipal

NOW = datetime(2026, 9, 30, 12, tzinfo=UTC)
CLIENT_KEY = "client-key-0123456789"
PRINCIPAL = OidcPrincipal("https://issuer", "user-1", None, None)
HEADERS = {"X-Tenant-Id": "tenant-a"}
QUOTAS = QuotaLimits(1, 1, 1, 1, 1, 1)


def make_account(owner="user-1", customer="cus_1", status=BillingAccountStatus.ACTIVE):
    return BillingAccount("ba_01", customer, owner, status, NOW, NOW)


def make_link(tenant="tenant-b", account="ba_01"):
    return BillingAccountTenantLink(account, tenant, "user-9", "account_created", NOW)


def make_plan():
    return PlanVersion("plan-1", "pro", None, ("price_1",), frozenset({"a"}), QUOTAS, 3, NOW)


def make_snapshot(status=SubscriptionStatus.PAST_DUE):
    return EntitlementSnapshot(
        "ba_01", "sub_1", status, False, "plan-1", frozenset({"b", "a"}), QUOTAS,
        NOW, NOW, NOW, NOW, 3, NOW, "evt_1",
    )


def make_membership(tenant="tenant-a", user="user-2", role="gestor"):
    return Membership(tenant_id=tenant, user_id=user, role=role, created_at=NOW)


def _echo_reservation(command):
    return PendingCheckout(
        command.billing_account_id, command.request_key, NOW, command.expires_at,
    )


class Env:
    def __init__(self):
        self.catalog = create_autospec(BillingCatalogPort, instance=True)
        self.gateway = create_autospec(StripeGatewayPort, instance=True)
        self.projection = create_autospec(EntitlementProjectionPort, instance=True)
        self.audit = create_autospec(BillingAuditPort, instance=True)
        self.authorizer = create_autospec(TenantAuthorizer, instance=True)
        self.control_plane = create_autospec(ControlPlanePort, instance=True)
        self.role = "gestor"
        self.authorizer.authorize.side_effect = self._authorize
        self.catalog.get_account.return_value = make_account(owner="user-9")
        self.catalog.get_tenant_link.return_value = make_link("tenant-a")
        self.catalog.get_plan.return_value = make_plan()
        self.gateway.create_checkout.return_value = HostedSession("cs_01", "https://stripe.test/c")
        self.gateway.create_portal.return_value = HostedSession("bps_01", "https://stripe.test/p")
        self.gateway.create_customer.return_value = StripeCustomer("cus_new")
        self.projection.get_snapshot.return_value = None
        self.catalog.reserve_pending_checkout.side_effect = _echo_reservation

    def _authorize(self, principal, tenant_id):
        return AuthorizedTenant(tenant_id, "user-1", self.role)

    def app(self, mode=BillingMode.STRIPE):
        app = FastAPI()
        app.include_router(router)
        overrides = {
            get_billing_mode: lambda: mode,
            get_billing_principal: lambda: PRINCIPAL,
            get_membership_authorizer: lambda: self.authorizer,
            get_billing_catalog: lambda: self.catalog,
            get_stripe_gateway: lambda: self.gateway,
            get_entitlement_projection: lambda: self.projection,
            get_billing_audit: lambda: self.audit,
            get_billing_clock: lambda: (lambda: NOW),
            get_control_plane: lambda: self.control_plane,
        }
        app.dependency_overrides.update(overrides)
        return app


def checkout_body(**extra):
    return {
        "billing_account_id": "ba_01", "plan_version_id": "plan-1",
        "idempotency_key": CLIENT_KEY, **extra,
    }


def portal_body():
    return {"billing_account_id": "ba_01", "idempotency_key": CLIENT_KEY}


def create_body():
    return {"idempotency_key": CLIENT_KEY}


def transfer_body(new_owner="user-2"):
    return {"new_owner_user_id": new_owner, "reason_code": "handover"}


MUTATIONS = {
    "checkout": ("/api/v1/billing/checkout", checkout_body),
    "portal": ("/api/v1/billing/portal", portal_body),
    "accounts": ("/api/v1/billing/accounts", create_body),
    "transfer": ("/api/v1/billing/accounts/ba_01/transfer", transfer_body),
}


def post(client, name, headers=HEADERS):
    path, body = MUTATIONS[name]
    return client.post(path, json=body(), headers=headers)
