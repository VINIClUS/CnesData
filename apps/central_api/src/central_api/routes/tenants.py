"""Rota de criação de tenant cobrado pela conta de billing."""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from central_api.routes.billing import (
    BillingContext,
    _scoped_key,
    get_billing_context,
    require_billing_enabled,
    require_billing_owner,
)
from central_api.routes.billing_errors import mapped_errors
from central_api.routes.raw_jobs import get_control_plane
from central_api.services.billing_gates import ApiBillingGates
from central_api.validation_errors import validation_error
from cnes_domain.billing.commands import (
    CapacityReservationCommand,
    CreateBilledTenantCommand,
    GateRequest,
    ReleaseCapacityCommand,
)
from cnes_domain.billing.errors import (
    BillingError,
    EntitlementDenied,
    PermanentBillingError,
    QuotaExceeded,
)
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    CapacityKind,
    CapacityReservation,
    ReadConsistency,
)
from cnes_domain.control_plane.entities import Tenant
from cnes_domain.ports.control_plane import ControlPlanePort

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/billing", tags=["billing"])

_RESERVED = "tenant_id_reserved"


class TenantCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tenant_id: str = Field(pattern=r"^[a-z0-9][a-z0-9_-]{0,62}$")
    municipality_name: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200),
    ]
    idempotency_key: str = Field(min_length=1, max_length=128)

    @field_validator("tenant_id", mode="before")
    @classmethod
    def _reject_reserved(cls, value: object) -> object:
        if isinstance(value, str) and value.startswith("_"):
            raise ValueError(_RESERVED)
        return value


class TenantOut(BaseModel):
    tenant_id: str
    municipality_name: str
    created_at: datetime
    billing_account_id: str


@dataclass(frozen=True, slots=True)
class TenantCreationPorts:
    ctx: BillingContext
    gates: ApiBillingGates
    control_plane: ControlPlanePort


def get_tenant_gates() -> ApiBillingGates:
    """Falha fechado até a composição fornecer os gates de billing."""
    raise HTTPException(status_code=503, detail="billing_not_configured")


def get_tenant_creation_ports(
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    gates: Annotated[ApiBillingGates, Depends(get_tenant_gates)],
    control_plane: Annotated[ControlPlanePort, Depends(get_control_plane)],
) -> TenantCreationPorts:
    """Agrupa contexto de billing, gates e control plane da criação de tenant."""
    return TenantCreationPorts(ctx, gates, control_plane)


def _denial_to_http(error: BillingError) -> HTTPException:
    if isinstance(error, EntitlementDenied):
        return HTTPException(403, "tenant_entitlement_denied")
    if isinstance(error, QuotaExceeded):
        return HTTPException(403, "tenant_quota_exceeded")
    if error.code == _RESERVED:
        return validation_error(_RESERVED)
    return HTTPException(409, "tenant_creation_conflict")


@contextmanager
def _denials() -> Iterator[None]:
    try:
        yield
    except (EntitlementDenied, QuotaExceeded, PermanentBillingError) as error:
        mapped = _denial_to_http(error)
        logger.warning("tenant_creation_denied code=%s", error.code)
        raise mapped from error


def _active_account(ctx: BillingContext, billing_account_id: str) -> BillingAccount:
    account = ctx.catalog.get_account(billing_account_id)
    if account is None:
        raise HTTPException(status_code=404, detail="billing_account_not_found")
    require_billing_owner(account, ctx.principal, ctx.authorized_tenant, ctx.catalog)
    if account.status is not BillingAccountStatus.ACTIVE:
        raise HTTPException(status_code=409, detail="billing_account_not_active")
    return account


def _reserve(
    ports: TenantCreationPorts, body: TenantCreate, account_id: str, key: str,
) -> CapacityReservation:
    decision = ports.gates.gate.authorize_tenant_creation(GateRequest(account_id, body.tenant_id))
    request_hash = _scoped_key(account_id, body.tenant_id, body.municipality_name)
    return ports.gates.capacity.reserve_capacity(
        CapacityReservationCommand(
            account_id, body.tenant_id, body.tenant_id, CapacityKind.TENANT, key,
            request_hash, decision.entitlement_version, decision.quota_limit,
        ),
    )


def _build_command(
    ctx: BillingContext, body: TenantCreate, reservation: CapacityReservation, key: str,
) -> CreateBilledTenantCommand:
    account_id = reservation.billing_account_id
    now = ctx.clock()
    tenant = Tenant(
        tenant_id=body.tenant_id, municipality_name=body.municipality_name, created_at=now,
    )
    link = BillingAccountTenantLink(
        account_id, body.tenant_id, ctx.principal.subject, "tenant_created", now,
    )
    return CreateBilledTenantCommand(tenant, link, reservation.reservation_id, key)


def _release(ports: TenantCreationPorts, command: CreateBilledTenantCommand) -> None:
    release = ReleaseCapacityCommand(
        command.link.billing_account_id,
        command.reservation_id,
        ports.ctx.clock(),
        "tenant_creation_failed",
    )
    try:
        ports.gates.capacity.release_capacity(release)
    except Exception:
        logger.warning("tenant_creation_release_failed")


def _owns_link(link: BillingAccountTenantLink, command: CreateBilledTenantCommand) -> bool:
    expected = command.link
    return (link.billing_account_id, link.tenant_id) == (
        expected.billing_account_id, expected.tenant_id,
    )


def _recover(
    ports: TenantCreationPorts, command: CreateBilledTenantCommand, error: Exception,
) -> Tenant:
    tenant_id = command.tenant.tenant_id
    account_id = command.link.billing_account_id
    try:
        tenant = ports.control_plane.get_tenant(tenant_id)
        link = ports.ctx.catalog.get_tenant_link(account_id, tenant_id, ReadConsistency.STRONG)
    except Exception:
        logger.warning("tenant_creation_probe_failed")
        raise error from None
    if tenant is None and link is None:
        _release(ports, command)
    elif tenant is not None and link is not None and _owns_link(link, command):
        return tenant
    raise error


def _create(ports: TenantCreationPorts, command: CreateBilledTenantCommand) -> Tenant:
    try:
        return ports.control_plane.create_billed_tenant(command)
    except Exception as error:
        return _recover(ports, command, error)


@router.post(
    "/accounts/{billing_account_id}/tenants",
    status_code=201,
    response_model=TenantOut,
    dependencies=[Depends(require_billing_enabled)],
)
def create_billed_tenant(
    billing_account_id: Annotated[str, Path(min_length=1, max_length=128)],
    body: TenantCreate,
    ports: Annotated[TenantCreationPorts, Depends(get_tenant_creation_ports)],
) -> TenantOut:
    """Cria o tenant canônico cobrado pela conta com reserva de capacidade idempotente."""
    ctx = ports.ctx
    with mapped_errors(), _denials():
        account = _active_account(ctx, billing_account_id)
        account_id = account.billing_account_id
        key = _scoped_key("tenant", ctx.principal.subject, account_id, body.idempotency_key)
        reservation = _reserve(ports, body, account_id, key)
        command = _build_command(ctx, body, reservation, key)
        tenant = _create(ports, command)
    return TenantOut(
        tenant_id=tenant.tenant_id,
        municipality_name=tenant.municipality_name,
        created_at=tenant.created_at,
        billing_account_id=account_id,
    )
