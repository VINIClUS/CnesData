"""Rota administrativa de revogação imediata de entitlement de billing."""

import logging
from datetime import datetime
from typing import Annotated, Protocol

from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, ConfigDict, Field

from central_api.routes.billing import (
    BillingContext,
    get_billing_context,
    require_billing_enabled,
    require_billing_owner,
)
from central_api.routes.billing_errors import mapped_errors
from central_api.routes.stripe_webhook import get_billing_metrics
from cnes_domain.billing.errors import BillingDisabledError, PermanentBillingError
from cnes_domain.billing.ports import BillingMetricsPort
from cnes_domain.billing.revocation_models import (
    REASON_CODE_PATTERN,
    ImmediateRevocationCommand,
    RevocationResult,
)
from cnes_infra.billing.metrics import BillingMetricName, billing_metric

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/admin/billing", tags=["billing-admin"])


class RevocationService(Protocol):
    def revoke(self, command: ImmediateRevocationCommand) -> RevocationResult: ...


def get_revocation_service() -> RevocationService:
    """Falha fechado até a composição fornecer o serviço de revogação."""
    raise HTTPException(status_code=503, detail="billing_not_configured")


class RevocationCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason_code: str = Field(pattern=REASON_CODE_PATTERN)


class RevocationOut(BaseModel):
    billing_account_id: str
    entitlement_version: int
    fenced_run_count: int
    cancel_failure_count: int


def _revoke(
    service: RevocationService, command: ImmediateRevocationCommand,
) -> RevocationResult:
    try:
        return service.revoke(command)
    except PermanentBillingError as error:
        raise HTTPException(status_code=409, detail="revocation_conflict") from error
    except BillingDisabledError as error:
        raise HTTPException(status_code=404, detail="billing_disabled") from error


def _emit_revoked_runs(
    metrics: BillingMetricsPort, result: RevocationResult, now: datetime,
) -> None:
    if result.fenced_run_ids:
        metrics.emit(billing_metric(
            BillingMetricName.RUNS_CANCELED_BY_REVOCATION,
            len(result.fenced_run_ids),
            now,
            {"Reason": "admin_revoked"},
        ))


@router.post(
    "/{billing_account_id}/revoke",
    status_code=200,
    response_model=RevocationOut,
    dependencies=[Depends(require_billing_enabled)],
)
def revoke_billing_account(
    billing_account_id: Annotated[str, Path(min_length=1, max_length=128)],
    body: RevocationCreate,
    ctx: Annotated[BillingContext, Depends(get_billing_context)],
    service: Annotated[RevocationService, Depends(get_revocation_service)],
    metrics: Annotated[BillingMetricsPort, Depends(get_billing_metrics)],
) -> RevocationOut:
    """Revoga de imediato o entitlement da conta para o dono ou gestor do tenant vinculado."""
    with mapped_errors():
        account = ctx.catalog.get_account(billing_account_id)
        if account is None:
            raise HTTPException(status_code=404, detail="billing_account_not_found")
        require_billing_owner(account, ctx.principal, ctx.authorized_tenant, ctx.catalog)
        command = ImmediateRevocationCommand(
            account.billing_account_id, ctx.principal.subject, body.reason_code, ctx.clock(),
        )
        result = _revoke(service, command)
    _emit_revoked_runs(metrics, result, ctx.clock())
    logger.info(
        "billing_admin_revoked entitlement_version=%d fenced=%d failures=%d",
        result.entitlement_version, len(result.fenced_run_ids), len(result.cancel_failures),
    )
    return RevocationOut(
        billing_account_id=account.billing_account_id,
        entitlement_version=result.entitlement_version,
        fenced_run_count=len(result.fenced_run_ids),
        cancel_failure_count=len(result.cancel_failures),
    )
