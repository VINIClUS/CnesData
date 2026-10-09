"""Contadores de capacidade: semente da conta e contagem fora do enforce."""

import logging

from cnes_infra.billing.dynamodb_items import decode_tenant_account, put_new
from cnes_infra.billing.dynamodb_quota_items import (
    USAGE_ENTITY,
    settle_usage_update,
    usage_counter,
)
from cnes_infra.billing.keys import capacity_usage_key, pending_capacity_key, tenant_account_key
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    absent_check_action,
    check_action,
)
from cnes_infra.control_plane.dynamodb_keys import item_key

PENDING_CAPACITY_ENTITY = "BILLINGPENDINGCAPACITY"
CAPACITY_NOT_SEEDED = "capacity_not_seeded"
AGENT_COUNTER = "agent_count"
TENANT_COUNTER = "tenant_count"
INITIAL_TENANTS = 1

logger = logging.getLogger(__name__)


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def log_not_seeded(billing_account_id: str, kind: str) -> None:
    """Registra a conta sem contador de capacidade semeado."""
    logger.warning(
        "capacity_counter_missing reason=%s billing_account_id=%s kind=%s",
        CAPACITY_NOT_SEEDED, billing_account_id, kind,
    )


def seed_capacity_actions(
    table: str, billing_account_id: str, tenant_id: str, pending: Item | None
) -> tuple[Action, Action]:
    """Cria a semente CAPACITY da conta e o CAS do contador pendente do tenant inicial.

    Args: tabela, conta nova, tenant inicial e item pendente lido (ou None).
    Returns: Put da semente e Delete/ConditionCheck do pendente.
    """
    agents = usage_counter(pending, AGENT_COUNTER)
    pk, sk = capacity_usage_key(billing_account_id)
    seed: Item = {
        "pk": {"S": pk},
        "sk": {"S": sk},
        "entity": {"S": USAGE_ENTITY},
        TENANT_COUNTER: _number(INITIAL_TENANTS),
        AGENT_COUNTER: _number(agents),
    }
    return put_new(table, seed), _pending_cas(table, tenant_id, pending, agents)


def _pending_cas(table: str, tenant_id: str, pending: Item | None, agents: int) -> Action:
    key = pending_capacity_key(tenant_id)
    if pending is None:
        return absent_check_action(table, key)
    return {"Delete": {
        "TableName": table,
        "Key": item_key(*key),
        "ConditionExpression": "#agents = :agents",
        "ExpressionAttributeNames": {"#agents": AGENT_COUNTER},
        "ExpressionAttributeValues": {":agents": _number(agents)},
    }}


def linked_agent_actions(table: str, tenant_id: str, link: Item) -> tuple[Action, Action]:
    """Cria a contagem do agente novo na conta do link reverso lido.

    Args: tabela, tenant e item do link reverso TENANT#t/BILLING_ACCOUNT.
    Returns: ConditionCheck do link inalterado e ADD agent_count no CAPACITY.
    """
    account = decode_tenant_account(link, tenant_id)
    key = capacity_usage_key(account)
    return check_action(table, link), settle_usage_update(table, key, {AGENT_COUNTER: 1})


def unlinked_agent_actions(table: str, tenant_id: str) -> tuple[Action, Action]:
    """Cria a contagem do agente novo no contador pendente do tenant sem conta.

    Args: tabela e tenant sem link reverso.
    Returns: ConditionCheck da ausência do link e ADD agent_count no pendente.
    """
    update: Action = {"Update": {
        "TableName": table,
        "Key": item_key(*pending_capacity_key(tenant_id)),
        "UpdateExpression": "SET #entity = :entity ADD #agents :one",
        "ExpressionAttributeNames": {"#entity": "entity", "#agents": AGENT_COUNTER},
        "ExpressionAttributeValues": {
            ":entity": {"S": PENDING_CAPACITY_ENTITY}, ":one": _number(1),
        },
    }}
    return absent_check_action(table, tenant_account_key(tenant_id)), update
