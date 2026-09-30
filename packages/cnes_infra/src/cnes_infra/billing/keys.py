"""Pure DynamoDB billing key builders."""

from datetime import UTC, datetime, timedelta

from cnes_infra.control_plane.dynamodb_keys import (
    entity_key,
    key_component,
    tenant_partition,
    timestamp,
)

type Key = tuple[str, str]

BILLING_ACCOUNT_LIST_PARTITION = "BILLING_ACCOUNTS"
BILLING_ACCOUNT_LIST_PREFIX = "ACCOUNT#"
BILLING_AUDIT_TENANT_ID = "_billing"
STRIPE_RECOVERY_DUE_INDEX = "gsi1"
STRIPE_RECOVERY_DUE_PARTITION = "STRIPE_RECOVERY#DUE"
_SYSTEM_PARTITION = "BILLING#SYSTEM"
_REVOCATION_WIDTH = 20


def _utc_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("reason=non_utc_datetime")
    return timestamp(value.astimezone(UTC))


def billing_partition(billing_account_id: str) -> str:
    """Cria a partição base da conta de billing."""
    return f"BILLING#{key_component(billing_account_id)}"


def entitlement_snapshot_key(billing_account_id: str) -> Key:
    """Cria a chave do snapshot de entitlement."""
    return billing_partition(billing_account_id), "ENTITLEMENT"


def billing_account_key(billing_account_id: str) -> Key:
    """Cria a chave base da conta."""
    return billing_partition(billing_account_id), "ACCOUNT"


def capacity_usage_key(billing_account_id: str) -> Key:
    """Cria a chave do uso de capacidade."""
    return billing_partition(billing_account_id), "CAPACITY"


def billing_account_list_key(billing_account_id: str) -> Key:
    """Cria a chave da lista global de contas."""
    return BILLING_ACCOUNT_LIST_PARTITION, BILLING_ACCOUNT_LIST_PREFIX + key_component(
        billing_account_id
    )


def account_tenant_key(billing_account_id: str, tenant_id: str) -> Key:
    """Cria a chave do link conta-tenant."""
    return billing_partition(billing_account_id), f"TENANT#{key_component(tenant_id)}"


def tenant_account_key(tenant_id: str) -> Key:
    """Cria a chave reversa tenant-conta."""
    return tenant_partition(tenant_id), "BILLING_ACCOUNT"


def tenant_entity_key(tenant_id: str) -> Key:
    """Cria a chave canônica do tenant no control plane."""
    return entity_key(tenant_id, "TENANT", tenant_id)


def stripe_customer_key(stripe_customer_id: str) -> Key:
    """Cria a chave do mapa Customer para conta."""
    return f"STRIPE_CUSTOMER#{key_component(stripe_customer_id)}", "BILLING_ACCOUNT"


def plan_version_key(plan_version_id: str) -> Key:
    """Cria a chave da PlanVersion."""
    return f"PLAN_VERSION#{key_component(plan_version_id)}", "META"


def stripe_price_key(stripe_price_id: str) -> Key:
    """Cria a chave do mapa Price para PlanVersion."""
    return f"STRIPE_PRICE#{key_component(stripe_price_id)}", "PLAN_VERSION"


def billing_period_partition(billing_account_id: str, period_start: datetime) -> str:
    """Cria a partição do período da conta."""
    stamp = _utc_timestamp(period_start)
    return f"{billing_partition(billing_account_id)}#PERIOD#{stamp}"


def usage_key(billing_account_id: str, period_start: datetime) -> Key:
    """Cria a chave do uso do período."""
    return billing_period_partition(billing_account_id, period_start), "USAGE"


def reservation_key(billing_account_id: str, period_start: datetime, reservation_id: str) -> Key:
    """Cria a chave da reserva de quota do período."""
    partition = billing_period_partition(billing_account_id, period_start)
    return partition, f"RESERVATION#{key_component(reservation_id)}"


def capacity_reservation_key(billing_account_id: str, reservation_id: str) -> Key:
    """Cria a chave da reserva de capacidade."""
    partition = billing_partition(billing_account_id)
    return partition, f"CAPACITY_RESERVATION#{key_component(reservation_id)}"


def run_billing_key(tenant_id: str, run_id: str) -> Key:
    """Cria a chave do companion de billing do run."""
    return entity_key(tenant_id, "RUN_BILLING", run_id)


def run_lookup_partition(billing_account_id: str) -> str:
    """Cria a partição de lookup de runs da conta."""
    return f"{billing_partition(billing_account_id)}#RUNS"


def run_lookup_key(billing_account_id: str, tenant_id: str, run_id: str) -> Key:
    """Cria a chave de lookup de um run da conta."""
    identity = f"{key_component(tenant_id)}#{key_component(run_id)}"
    return run_lookup_partition(billing_account_id), f"RUN#{identity}"


def billing_idempotency_key(billing_account_id: str, scope: str, key: str) -> Key:
    """Cria a chave de idempotência da conta."""
    partition = f"{billing_partition(billing_account_id)}#IDEMPOTENCY#{key_component(scope)}"
    return partition, f"KEY#{key_component(key)}"


def stripe_event_key(event_id: str) -> Key:
    """Cria a chave do item de inbox do evento Stripe."""
    return f"STRIPE_EVENT#{key_component(event_id)}", "EVENT"


def stripe_recovery_due_sort_key(due_at: datetime, event_id: str) -> str:
    """Cria a sort key do índice de recovery por vencimento."""
    return f"{_utc_timestamp(due_at)}#{key_component(event_id)}"


def stripe_recovery_cursor_key() -> Key:
    """Cria a chave do cursor de recovery Stripe."""
    return _SYSTEM_PARTITION, "RECOVERY#STRIPE"


def revocation_progress_key(billing_account_id: str, entitlement_version: int) -> Key:
    """Cria a chave do progresso de revogação de uma versão."""
    if (
        isinstance(entitlement_version, bool)
        or not isinstance(entitlement_version, int)
        or entitlement_version < 1
    ):
        raise ValueError("reason=invalid_entitlement_version")
    version = str(entitlement_version).zfill(_REVOCATION_WIDTH)
    return billing_partition(billing_account_id), f"REVOCATION#{version}"
