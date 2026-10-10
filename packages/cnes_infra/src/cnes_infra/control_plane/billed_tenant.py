"""Regras compartilhadas da criação de tenant faturado."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from cnes_domain.billing.errors import PermanentBillingError
from cnes_domain.billing.models import BillingAuditEvent
from cnes_domain.control_plane.entities import IdempotencyRecord, Membership

if TYPE_CHECKING:
    from datetime import datetime

    from cnes_domain.billing.commands import CreateBilledTenantCommand

TENANT_SCOPE = "tenant.create_billed"
RESERVED_TENANT_PREFIX = "_"
IDEMPOTENCY_TTL = timedelta(days=1)
TENANT_CREATOR_ROLE = "gestor"


def require_creatable_tenant_id(tenant_id: str) -> None:
    """Rejeita identificadores reservados ao sistema.

    Args: tenant_id: Identificador do tenant a criar.
    Raises: PermanentBillingError: Identificador começa com o prefixo reservado.
    """
    if tenant_id.startswith(RESERVED_TENANT_PREFIX):
        raise PermanentBillingError("tenant_id_reserved")


def billed_tenant_digest(command: CreateBilledTenantCommand) -> str:
    """Calcula o digest do pedido sem os instantes gerados pelo cliente.

    Args: command: Comando de criação de tenant faturado.
    Returns: SHA-256 hexadecimal do pedido canônico.
    """
    from cnes_infra.billing.dynamodb_items import request_hash

    link = command.link
    identity = {
        "tenant_id": command.tenant.tenant_id,
        "municipality_name": command.tenant.municipality_name,
        "billing_account_id": link.billing_account_id,
        "linked_by_user_id": link.linked_by_user_id,
        "reason_code": link.reason_code,
        "reservation_id": command.reservation_id,
    }
    return request_hash(identity)


def tenant_created_event(command: CreateBilledTenantCommand) -> BillingAuditEvent:
    """Cria o evento de auditoria determinístico do tenant criado.

    Args: command: Comando de criação de tenant faturado.
    Returns: Evento `tenant.created` agregado à conta de billing.
    """
    from cnes_infra.billing.dynamodb_items import deterministic_id

    tenant_id = command.tenant.tenant_id
    return BillingAuditEvent(
        event_id=deterministic_id("tenant.created", tenant_id),
        event_type="tenant.created",
        aggregate_id=command.link.billing_account_id,
        actor_id=command.link.linked_by_user_id,
        reason_code=command.link.reason_code,
        occurred_at=command.tenant.created_at,
        attributes={"tenant_id": tenant_id},
    )


def completed_record(command: CreateBilledTenantCommand, now: datetime) -> IdempotencyRecord:
    """Cria o registro de idempotência concluído do comando.

    Args: command: Comando de criação; now: Instante da gravação.
    Returns: Registro COMPLETED com validade de um dia.
    """
    return IdempotencyRecord(
        tenant_id=command.tenant.tenant_id,
        scope=TENANT_SCOPE,
        key=command.idempotency_key,
        request_hash=billed_tenant_digest(command),
        status="COMPLETED",
        resource_id=command.tenant.tenant_id,
        created_at=now,
        expires_at=now + IDEMPOTENCY_TTL,
    )


def creator_membership(command: CreateBilledTenantCommand) -> Membership:
    """Cria a membership de gestor de quem criou o tenant.

    Args: command: Comando de criação de tenant faturado.
    Returns: Membership do autor do link no tenant novo.
    """
    return Membership(
        tenant_id=command.tenant.tenant_id,
        user_id=command.link.linked_by_user_id,
        role=TENANT_CREATOR_ROLE,
        created_at=command.tenant.created_at,
    )
