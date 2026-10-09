"""Rotas do Billing Owner: contas, checkout, portal e status de entitlement."""

import hashlib
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Protocol

from fastapi import APIRouter, Depends, Header, HTTPException, Path, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import BaseModel

from central_api.auth.aws_oidc import AuthorizedTenant, TenantAccessDenied
from central_api.routes.billing_checkout import (
    get_checkout_reservation_ttl,
    pending_checkout,
    reservation_expiry,
)
from central_api.routes.billing_errors import mapped_errors
from central_api.routes.billing_schemas import (
    BillingAccountCreate,
    BillingAccountOut,
    BillingAccountTransfer,
    BillingStatusOut,
    CheckoutCreate,
    HostedSessionOut,
    PortalCreate,
)
from central_api.routes.raw_jobs import get_control_plane
from cnes_domain.billing.commands import (
    AttachStripeCustomerCommand,
    CheckoutCommand,
    CreateBillingAccountCommand,
    CreateStripeCustomerCommand,
    HostedSession,
    PortalCommand,
    TransferOwnerCommand,
)
from cnes_domain.billing.errors import PermanentBillingError, RetryableBillingError
from cnes_domain.billing.models import (
    BILLING_ADMIN_ROLE,
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    PlanVersion,
    ReadConsistency,
    SubscriptionStatus,
)
from cnes_domain.billing.ports import (
    BillingAuditPort,
    BillingCatalogPort,
    ClockPort,
    EntitlementProjectionPort,
    StripeGatewayPort,
)
from cnes_domain.ports.control_plane import ControlPlanePort
from cnes_domain.profiles import BillingMode
from cnes_infra.auth.oidc import OidcPrincipal
from cnes_infra.billing.disabled import LOCAL_UNMETERED_PLAN_VERSION_ID

logger = logging.getLogger(__name__)

BILLING_ADMIN_ROLES = frozenset({BILLING_ADMIN_ROLE})

_OWNER_REQUIRED = "billing_owner_required"
_NOT_CONFIGURED = "billing_not_configured"
_ATTACH_CONFLICTS = frozenset({"stripe_customer_already_attached", "billing_account_stale"})


class _LocalUnmeteredStatus(Exception):
    pass


class _BillingRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def wrapped(request: Request) -> Response:
            try:
                return await handler(request)
            except _LocalUnmeteredStatus:
                return JSONResponse(
                    {"state": "disabled", "plan_version_id": LOCAL_UNMETERED_PLAN_VERSION_ID},
                )

        return wrapped


router = APIRouter(prefix="/api/v1/billing", tags=["billing"], route_class=_BillingRoute)


class TenantAuthorizer(Protocol):
    def authorize(self, principal: OidcPrincipal, tenant_id: str) -> AuthorizedTenant: ...


def _unavailable() -> HTTPException:
    return HTTPException(status_code=503, detail=_NOT_CONFIGURED)


def get_billing_mode() -> BillingMode:
    """Falha fechado até a composição fornecer o modo de billing."""
    raise _unavailable()


def get_billing_principal(request: Request) -> OidcPrincipal:
    """Entrega a identidade OIDC validada pelo middleware ou nega com 401."""
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, OidcPrincipal):
        raise HTTPException(status_code=401, detail="auth_required")
    return principal


def get_membership_authorizer() -> TenantAuthorizer:
    """Falha fechado até a composição fornecer o autorizador de membership."""
    raise _unavailable()


def get_billing_catalog() -> BillingCatalogPort:
    """Falha fechado até a composição fornecer o catálogo de billing."""
    raise _unavailable()


def get_stripe_gateway() -> StripeGatewayPort:
    """Falha fechado até a composição fornecer o gateway Stripe."""
    raise _unavailable()


def get_entitlement_projection() -> EntitlementProjectionPort:
    """Falha fechado até a composição fornecer a projeção de entitlement."""
    raise _unavailable()


def get_billing_audit() -> BillingAuditPort:
    """Falha fechado até a composição fornecer a auditoria de billing."""
    raise _unavailable()


def get_billing_clock() -> ClockPort:
    """Entrega o relógio UTC do billing."""
    return lambda: datetime.now(UTC)


def require_billing_enabled(mode: Annotated[BillingMode, Depends(get_billing_mode)]) -> None:
    """Responde 404 quando o billing está desabilitado."""
    if mode is BillingMode.DISABLED:
        raise HTTPException(status_code=404, detail="billing_disabled")


_ENABLED = [Depends(require_billing_enabled)]


def _gate_status_mode(mode: Annotated[BillingMode, Depends(get_billing_mode)]) -> None:
    if mode is BillingMode.DISABLED:
        raise _LocalUnmeteredStatus


def get_authorized_tenant(
    principal: Annotated[OidcPrincipal, Depends(get_billing_principal)],
    authorizer: Annotated[TenantAuthorizer, Depends(get_membership_authorizer)],
    x_tenant_id: Annotated[str | None, Header(alias="X-Tenant-Id")] = None,
) -> AuthorizedTenant | None:
    """Autoriza o tenant do cabeçalho X-Tenant-Id pela membership ou entrega None."""
    if x_tenant_id is None or not x_tenant_id.strip():
        return None
    try:
        return authorizer.authorize(principal, x_tenant_id)
    except TenantAccessDenied as error:
        logger.info("billing_tenant_denied code=%s", error.code)
        raise HTTPException(status_code=403, detail="tenant_not_allowed") from error


@dataclass(frozen=True, slots=True)
class BillingContext:
    principal: OidcPrincipal
    authorized_tenant: AuthorizedTenant | None
    catalog: BillingCatalogPort
    clock: ClockPort


def get_billing_context(
    principal: Annotated[OidcPrincipal, Depends(get_billing_principal)],
    authorized_tenant: Annotated[AuthorizedTenant | None, Depends(get_authorized_tenant)],
    catalog: Annotated[BillingCatalogPort, Depends(get_billing_catalog)],
    clock: Annotated[ClockPort, Depends(get_billing_clock)],
) -> BillingContext:
    """Agrupa identidade, tenant autorizado, catálogo e relógio da requisição."""
    return BillingContext(principal, authorized_tenant, catalog, clock)


def _dump[T: BaseModel](model: type[T], source: object) -> T:
    return model.model_validate(source, from_attributes=True)


def _scoped_key(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()


def _owner_denied() -> HTTPException:
    return HTTPException(status_code=403, detail=_OWNER_REQUIRED)


def _is_billing_admin(principal: OidcPrincipal, tenant: AuthorizedTenant | None) -> bool:
    return (
        tenant is not None
        and tenant.user_id == principal.subject
        and tenant.role in BILLING_ADMIN_ROLES
    )


def _read_link(
    account: BillingAccount, tenant: AuthorizedTenant, catalog: BillingCatalogPort,
) -> BillingAccountTenantLink:
    id_, tenant_id = account.billing_account_id, tenant.tenant_id
    link = catalog.get_tenant_link(id_, tenant_id, ReadConsistency.STRONG)
    if link is None or (link.billing_account_id, link.tenant_id) != (id_, tenant_id):
        raise _owner_denied()
    return link


def _check_owner(
    account: BillingAccount,
    principal: OidcPrincipal,
    tenant: AuthorizedTenant | None,
    catalog: BillingCatalogPort,
) -> BillingAccountTenantLink | None:
    if principal.subject == account.owner_user_id:
        return None
    if tenant is None or not _is_billing_admin(principal, tenant):
        raise _owner_denied()
    return _read_link(account, tenant, catalog)


def require_billing_owner(
    account: BillingAccount,
    principal: OidcPrincipal,
    authorized_tenant: AuthorizedTenant | None,
    catalog: BillingCatalogPort,
) -> None:
    """Exige o dono da conta ou um administrador do tenant vinculado.
    Raises: HTTPException: 403 billing_owner_required; erros de leitura propagam.
    """
    _check_owner(account, principal, authorized_tenant, catalog)


def _load_account(catalog: BillingCatalogPort, billing_account_id: str) -> BillingAccount:
    account = catalog.get_account(billing_account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="billing_account_not_found")
    return account


def _owned_account(ctx: BillingContext, billing_account_id: str) -> BillingAccount:
    account = _load_account(ctx.catalog, billing_account_id)
    require_billing_owner(account, ctx.principal, ctx.authorized_tenant, ctx.catalog)
    return account


def _hosted_account(ctx: BillingContext, billing_account_id: str) -> tuple[str, str]:
    account = _owned_account(ctx, billing_account_id)
    if account.status is not BillingAccountStatus.ACTIVE:
        raise HTTPException(status_code=409, detail="billing_account_not_active")
    if account.stripe_customer_id is None:
        raise HTTPException(status_code=409, detail="stripe_customer_missing")
    return account.billing_account_id, account.stripe_customer_id


def _create_account(ctx: BillingContext, tenant: AuthorizedTenant, id_: str) -> BillingAccount:
    owner = ctx.principal.subject
    now = ctx.clock()
    account = BillingAccount(id_, None, owner, BillingAccountStatus.ACTIVE, now, now)
    link = BillingAccountTenantLink(id_, tenant.tenant_id, owner, "account_created", now)
    return ctx.catalog.create_account(CreateBillingAccountCommand(account, link, id_))


def _attached_by_race(
    catalog: BillingCatalogPort, account_id: str, customer_id: str,
) -> BillingAccount:
    account = _load_account(catalog, account_id)
    if account.stripe_customer_id is None:
        raise RetryableBillingError("billing_account_stale")
    if account.stripe_customer_id != customer_id:
        logger.warning(
            "billing_customer_orphaned billing_account_id=%s stripe_customer_id=%s",
            account_id, customer_id,
        )
    return account


def _ensure_customer(
    ctx: BillingContext, gw: StripeGatewayPort, acc: BillingAccount,
) -> BillingAccount:
    if acc.stripe_customer_id is not None:
        return acc
    id_ = acc.billing_account_id
    customer = gw.create_customer(CreateStripeCustomerCommand(id_, id_)).stripe_customer_id
    try:
        return ctx.catalog.attach_customer(
            AttachStripeCustomerCommand(id_, customer, acc.updated_at),
        )
    except PermanentBillingError as error:
        if error.code not in _ATTACH_CONFLICTS:
            raise
    return _attached_by_race(ctx.catalog, id_, customer)


@router.post("/accounts", status_code=201, response_model=BillingAccountOut, dependencies=_ENABLED)
def create_billing_account(
    body: BillingAccountCreate,
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    gateway: Annotated[StripeGatewayPort, Depends(get_stripe_gateway)],
) -> BillingAccountOut:
    """Cria a conta de billing do administrador e o customer Stripe de forma idempotente."""
    tenant = ctx.authorized_tenant
    if tenant is None or not _is_billing_admin(ctx.principal, tenant):
        raise HTTPException(status_code=403, detail="billing_admin_required")
    owner = ctx.principal.subject
    digest = _scoped_key(owner, tenant.tenant_id, body.idempotency_key)
    account_id = f"ba_{digest[:32]}"
    with mapped_errors():
        account = ctx.catalog.get_account(account_id)
        if account is not None and account.owner_user_id != owner:
            raise HTTPException(status_code=409, detail="idempotency_conflict")
        if account is None:
            account = _create_account(ctx, tenant, account_id)
        account = _ensure_customer(ctx, gateway, account)
    return _dump(BillingAccountOut, account)


def _validate_transfer_target(cp: ControlPlanePort, tenant_id: str, new_owner: str) -> None:
    membership = cp.get_membership(tenant_id, new_owner)
    if (
        membership is None
        or membership.tenant_id != tenant_id
        or membership.user_id != new_owner
        or membership.role not in BILLING_ADMIN_ROLES
    ):
        raise HTTPException(status_code=422, detail="transfer_target_invalid")


_TRANSFER = "/accounts/{billing_account_id}/transfer"


@router.post(_TRANSFER, response_model=BillingAccountOut, dependencies=_ENABLED)
def transfer_billing_account(
    billing_account_id: Annotated[str, Path(min_length=1, max_length=128)],
    body: BillingAccountTransfer,
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    control_plane: Annotated[ControlPlanePort, Depends(get_control_plane)],
) -> BillingAccountOut:
    """Transfere a titularidade da conta a outro administrador do tenant vinculado."""
    tenant = ctx.authorized_tenant
    if tenant is None:
        raise _owner_denied()
    with mapped_errors():
        account = _load_account(ctx.catalog, billing_account_id)
        if _check_owner(account, ctx.principal, tenant, ctx.catalog) is None:
            _read_link(account, tenant, ctx.catalog)
        _validate_transfer_target(control_plane, tenant.tenant_id, body.new_owner_user_id)
        if body.new_owner_user_id == account.owner_user_id:
            raise HTTPException(status_code=409, detail="owner_unchanged")
        command = TransferOwnerCommand(
            account.billing_account_id, account.owner_user_id, body.new_owner_user_id,
            ctx.principal.subject, body.reason_code, ctx.clock(),
        )
        return _dump(BillingAccountOut, ctx.catalog.transfer_owner(command))


def _checkout_audit_event(
    ctx: BillingContext, account_id: str, session: HostedSession, attributes: dict[str, str],
) -> BillingAuditEvent:
    return BillingAuditEvent(
        event_id=f"checkout:{session.session_id}",
        event_type="checkout.session_created",
        aggregate_id=account_id,
        actor_id=ctx.principal.subject,
        reason_code="checkout_requested",
        occurred_at=ctx.clock(),
        attributes=attributes,
    )


@dataclass(frozen=True, slots=True)
class CheckoutPorts:
    gateway: StripeGatewayPort
    audit: BillingAuditPort
    projection: EntitlementProjectionPort


def get_checkout_ports(
    gateway: Annotated[StripeGatewayPort, Depends(get_stripe_gateway)],
    audit: Annotated[BillingAuditPort, Depends(get_billing_audit)],
    projection: Annotated[EntitlementProjectionPort, Depends(get_entitlement_projection)],
) -> CheckoutPorts:
    """Agrupa gateway Stripe, auditoria e projeção usados pelo checkout."""
    return CheckoutPorts(gateway, audit, projection)


_CHECKOUT_ALLOWED = frozenset({SubscriptionStatus.CANCELED, SubscriptionStatus.INCOMPLETE_EXPIRED})


def _checkout_plan(ctx: BillingContext, ports: CheckoutPorts, body: CheckoutCreate) -> PlanVersion:
    snapshot = ports.projection.get_snapshot(body.billing_account_id, ReadConsistency.STRONG)
    if snapshot is not None and snapshot.subscription_status not in _CHECKOUT_ALLOWED:
        raise HTTPException(status_code=409, detail="subscription_exists")
    plan = ctx.catalog.get_plan(body.plan_version_id)
    if plan is None:
        raise HTTPException(status_code=404, detail="plan_not_found")
    if plan.effective_from > ctx.clock():
        raise HTTPException(status_code=409, detail="plan_not_effective")
    return plan


@router.post("/checkout", status_code=201, response_model=HostedSessionOut, dependencies=_ENABLED)
def create_checkout_session(
    body: CheckoutCreate,
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    ports: Annotated[CheckoutPorts, Depends(get_checkout_ports)],
    ttl: Annotated[timedelta, Depends(get_checkout_reservation_ttl)],
) -> HostedSessionOut:
    """Abre checkout no Stripe; 409 subscription_exists ou checkout_in_progress."""
    with mapped_errors():
        account_id, customer_id = _hosted_account(ctx, body.billing_account_id)
        plan = _checkout_plan(ctx, ports, body)
        key = _scoped_key("checkout", account_id, body.plan_version_id, body.idempotency_key)
        command = CheckoutCommand(account_id, customer_id, plan, key)
        with pending_checkout(ctx.catalog, account_id, key, reservation_expiry(ctx.clock(), ttl)):
            session = ports.gateway.create_checkout(command)
        attributes = {
            "plan_version_id": plan.plan_version_id,
            "stripe_checkout_session_id": session.session_id,
            "idempotency_key_sha256": key,
        }
        ports.audit.append(_checkout_audit_event(ctx, account_id, session, attributes))
    return _dump(HostedSessionOut, session)


@router.post("/portal", status_code=201, response_model=HostedSessionOut, dependencies=_ENABLED)
def create_portal_session(
    body: PortalCreate,
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    gateway: Annotated[StripeGatewayPort, Depends(get_stripe_gateway)],
) -> HostedSessionOut:
    """Abre o portal de cobrança hospedado no Stripe para o dono da conta."""
    with mapped_errors():
        account_id, customer_id = _hosted_account(ctx, body.billing_account_id)
        key = _scoped_key("portal", account_id, body.idempotency_key)
        session = gateway.create_portal(PortalCommand(account_id, customer_id, key))
    return _dump(HostedSessionOut, session)


@router.get(
    "/status",
    response_model=BillingStatusOut,
    response_model_exclude_none=True,
    dependencies=[Depends(_gate_status_mode)],
)
def get_billing_status(
    billing_account_id: Annotated[str, Query(min_length=1, max_length=128)],
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    projection: Annotated[EntitlementProjectionPort, Depends(get_entitlement_projection)],
) -> BillingStatusOut:
    """Informa o estado de entitlement projetado; nunca consulta o Stripe."""
    with mapped_errors():
        account = _owned_account(ctx, billing_account_id)
        snapshot = projection.get_snapshot(account.billing_account_id, ReadConsistency.STRONG)
    if snapshot is None:
        return BillingStatusOut(state="pending", billing_account_id=billing_account_id)
    return BillingStatusOut(
        state=snapshot.subscription_status.value,
        billing_account_id=billing_account_id,
        plan_version_id=snapshot.plan_version_id,
        cancel_at_period_end=snapshot.cancel_at_period_end,
        period_end=snapshot.period_end,
        grace_until=snapshot.grace_until,
        entitlement_version=snapshot.entitlement_version,
        features=sorted(snapshot.features),
    )
