"""DynamoDB item codec shared by the billing adapters."""

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import fields
from datetime import UTC, datetime
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from cnes_domain.billing.commands import CreateBillingAccountCommand, LinkBillingTenantCommand
from cnes_domain.billing.errors import (
    BillingDependencyError,
    PermanentBillingError,
)
from cnes_domain.billing.models import (
    BillingAccount,
    BillingAccountStatus,
    BillingAccountTenantLink,
    BillingAuditEvent,
    EntitlementSnapshot,
    PlanVersion,
    QuotaLimits,
    SubscriptionStatus,
)
from cnes_domain.billing.validation import require_utc
from cnes_domain.control_plane.entities import IdempotencyRecord, OutboxEvent
from cnes_domain.control_plane.errors import Conflict
from cnes_domain.control_plane.errors import ControlPlaneErrorCode as ErrorCode
from cnes_infra.billing.keys import (
    BILLING_ACCOUNT_LIST_PREFIX,
    BILLING_AUDIT_TENANT_ID,
    Key,
    account_tenant_key,
    billing_account_key,
    billing_account_list_key,
    entitlement_snapshot_key,
    plan_version_key,
    stripe_customer_key,
    stripe_price_key,
    tenant_account_key,
)
from cnes_infra.control_plane.dynamodb_codec import (
    Action,
    Item,
    encode_model,
    execute_transaction,
    put_action,
)
from cnes_infra.control_plane.dynamodb_keys import (
    idempotency_key,
    item_key,
    key_component,
    outbox_key,
    timestamp,
)

SNAPSHOT_ENTITY = "ENTITLEMENTSNAPSHOT"
ACCOUNT_ENTITY = "BILLINGACCOUNT"
ACCOUNT_LIST_ENTITY = "BILLINGACCOUNTLIST"
ACCOUNT_TENANT_ENTITY = "BILLINGACCOUNTTENANTLINK"
TENANT_ACCOUNT_ENTITY = "TENANTBILLINGACCOUNT"
CUSTOMER_MAP_ENTITY = "STRIPECUSTOMERMAP"
PLAN_ENTITY = "PLANVERSION"
PRICE_MAP_ENTITY = "STRIPEPRICEMAP"
IDEMPOTENCY_ENTITY = "IDEMPOTENCYRECORD"
CORRUPT_CODE = "billing_item_corrupt"
UNAVAILABLE_CODE = "dynamodb_unavailable"
_TRANSACTION_ERRORS = (KeyError, TypeError, ValueError, AttributeError)
_LIMIT_CODES = {
    ErrorCode.TRANSACTION_LIMIT: "billing_transaction_too_large",
    ErrorCode.DUPLICATE_TRANSACTION_KEY: "billing_duplicate_action",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, frozenset):
        return sorted(value)
    return {field.name: getattr(value, field.name) for field in fields(value)}


def canonical_json(value: Any) -> str:
    """Serializa o valor em JSON canônico e determinístico."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=_json_default)


def request_hash(value: Any) -> str:
    """Calcula o SHA-256 hexadecimal do JSON canônico."""
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def deterministic_id(*parts: str) -> str:
    """Deriva um identificador estável de 32 hex a partir das partes."""
    return request_hash(list(parts))[:32]


def utc_attribute(value: datetime) -> str:
    """Codifica um instante UTC como atributo ordenável de largura fixa."""
    require_utc(value, "value")
    return timestamp(value.astimezone(UTC))


def corrupt_item(entity: str) -> PermanentBillingError:
    """Cria o erro permanente de item corrompido da entidade."""
    return PermanentBillingError(CORRUPT_CODE, detail=f"entity={entity}")


def _expect(condition: bool, entity: str) -> None:
    if not condition:
        raise corrupt_item(entity)


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _item(entity: str, key: Key, value: Any, attributes: Mapping[str, Any] | None = None) -> Item:
    item: Item = {
        "pk": _text(key[0]),
        "sk": _text(key[1]),
        "entity": _text(entity),
        "payload": _text(canonical_json(value)),
    }
    item.update(attributes or {})
    return item


def _decode[T](item: Item, entity: str, key: Key, build: Callable[[Any], T]) -> T:
    try:
        if (item["entity"], item["pk"], item["sk"]) == (_text(entity), *map(_text, key)):
            return build(json.loads(item["payload"]["S"]))
    except _TRANSACTION_ERRORS as error:
        raise corrupt_item(entity) from error
    raise corrupt_item(entity)


def _when(data: Mapping[str, Any], name: str) -> datetime:
    return datetime.fromisoformat(data[name])


def _optional_when(data: Mapping[str, Any], name: str) -> datetime | None:
    return None if data[name] is None else _when(data, name)


def _pair(first: str, second: str) -> Callable[[Any], tuple[Any, Any]]:
    return lambda data: (data[first], data[second])


def _snapshot(data: dict[str, Any]) -> EntitlementSnapshot:
    return EntitlementSnapshot(
        **{
            **data,
            "subscription_status": SubscriptionStatus(data["subscription_status"]),
            "features": frozenset(data["features"]),
            "quotas": QuotaLimits(**data["quotas"]),
            "period_start": _when(data, "period_start"),
            "period_end": _when(data, "period_end"),
            "grace_until": _optional_when(data, "grace_until"),
            "valid_until": _when(data, "valid_until"),
            "updated_at": _when(data, "updated_at"),
        }
    )


def _account(data: dict[str, Any]) -> BillingAccount:
    return BillingAccount(
        **{
            **data,
            "status": BillingAccountStatus(data["status"]),
            "created_at": _when(data, "created_at"),
            "updated_at": _when(data, "updated_at"),
        }
    )


def _link(data: dict[str, Any]) -> BillingAccountTenantLink:
    return BillingAccountTenantLink(**{**data, "linked_at": _when(data, "linked_at")})


def _plan(data: dict[str, Any]) -> PlanVersion:
    return PlanVersion(
        **{
            **data,
            "stripe_price_ids": tuple(data["stripe_price_ids"]),
            "features": frozenset(data["features"]),
            "quotas": QuotaLimits(**data["quotas"]),
            "effective_from": _when(data, "effective_from"),
        }
    )


def _idempotency_record(data: Any) -> IdempotencyRecord:
    return IdempotencyRecord.model_validate(data, strict=False)


def _customer_attribute(account: BillingAccount) -> Item:
    if account.stripe_customer_id is None:
        return {}
    return {"stripe_customer_id": _text(account.stripe_customer_id)}


def encode_snapshot(snapshot: EntitlementSnapshot) -> Item:
    """Codifica o snapshot com atributos numéricos e de status para condições."""
    key = entitlement_snapshot_key(snapshot.billing_account_id)
    attributes = {
        "entitlement_version": {"N": str(snapshot.entitlement_version)},
        "subscription_status": _text(snapshot.subscription_status.value),
        "valid_until": _text(utc_attribute(snapshot.valid_until)),
    }
    return _item(SNAPSHOT_ENTITY, key, snapshot, attributes)


def decode_snapshot(item: Item, billing_account_id: str) -> EntitlementSnapshot:
    """Decodifica o snapshot validando identidade e versão do atributo."""
    key = entitlement_snapshot_key(billing_account_id)
    snapshot = _decode(item, SNAPSHOT_ENTITY, key, _snapshot)
    version = _decode(item, SNAPSHOT_ENTITY, key, lambda _: item["entitlement_version"]["N"])
    consistent = snapshot.billing_account_id == billing_account_id
    _expect(consistent and version == str(snapshot.entitlement_version), SNAPSHOT_ENTITY)
    return snapshot


def encode_account(account: BillingAccount) -> Item:
    """Codifica a conta com atributos de status, owner e updated_at."""
    attributes = {
        "status": _text(account.status.value),
        "owner_user_id": _text(account.owner_user_id),
        "updated_at": _text(utc_attribute(account.updated_at)),
        **_customer_attribute(account),
    }
    key = billing_account_key(account.billing_account_id)
    return _item(ACCOUNT_ENTITY, key, account, attributes)


def decode_account(item: Item, billing_account_id: str) -> BillingAccount:
    """Decodifica a conta validando o identificador solicitado."""
    key = billing_account_key(billing_account_id)
    account = _decode(item, ACCOUNT_ENTITY, key, _account)
    _expect(account.billing_account_id == billing_account_id, ACCOUNT_ENTITY)
    return account


def encode_account_list_row(account: BillingAccount) -> Item:
    """Codifica a linha da lista global de contas."""
    key = billing_account_list_key(account.billing_account_id)
    payload = {"billing_account_id": account.billing_account_id}
    return _item(ACCOUNT_LIST_ENTITY, key, payload, _customer_attribute(account))


def _list_row_account_id(item: Item) -> str:
    try:
        sort_key = item["sk"]["S"]
        _expect(sort_key.startswith(BILLING_ACCOUNT_LIST_PREFIX), ACCOUNT_LIST_ENTITY)
        return bytes.fromhex(sort_key.removeprefix(BILLING_ACCOUNT_LIST_PREFIX)).decode()
    except _TRANSACTION_ERRORS as error:
        raise corrupt_item(ACCOUNT_LIST_ENTITY) from error


def decode_account_list_row(item: Item) -> str:
    """Decodifica o identificador da conta de uma linha da lista global."""
    account_id = _list_row_account_id(item)
    key = billing_account_list_key(account_id)
    stored = _decode(item, ACCOUNT_LIST_ENTITY, key, lambda data: data["billing_account_id"])
    _expect(stored == account_id, ACCOUNT_LIST_ENTITY)
    return account_id


def encode_link(link: BillingAccountTenantLink) -> Item:
    """Codifica o link conta-tenant."""
    key = account_tenant_key(link.billing_account_id, link.tenant_id)
    return _item(ACCOUNT_TENANT_ENTITY, key, link)


def decode_link(item: Item, billing_account_id: str, tenant_id: str) -> BillingAccountTenantLink:
    """Decodifica o link validando conta e tenant solicitados."""
    key = account_tenant_key(billing_account_id, tenant_id)
    link = _decode(item, ACCOUNT_TENANT_ENTITY, key, _link)
    matches = (link.billing_account_id, link.tenant_id) == (billing_account_id, tenant_id)
    _expect(matches, ACCOUNT_TENANT_ENTITY)
    return link


def encode_tenant_account(link: BillingAccountTenantLink) -> Item:
    """Codifica o índice reverso tenant-conta."""
    payload = {"billing_account_id": link.billing_account_id, "tenant_id": link.tenant_id}
    return _item(TENANT_ACCOUNT_ENTITY, tenant_account_key(link.tenant_id), payload)


def decode_tenant_account(item: Item, tenant_id: str) -> str:
    """Decodifica a conta associada ao tenant."""
    key = tenant_account_key(tenant_id)
    account_id, stored = _decode(
        item, TENANT_ACCOUNT_ENTITY, key, _pair("billing_account_id", "tenant_id")
    )
    _expect(stored == tenant_id and isinstance(account_id, str), TENANT_ACCOUNT_ENTITY)
    return account_id


def encode_customer_map(billing_account_id: str, stripe_customer_id: str) -> Item:
    """Codifica o mapa Customer para conta."""
    payload = {"billing_account_id": billing_account_id, "stripe_customer_id": stripe_customer_id}
    return _item(CUSTOMER_MAP_ENTITY, stripe_customer_key(stripe_customer_id), payload)


def decode_customer_map(item: Item, stripe_customer_id: str) -> str:
    """Decodifica a conta mapeada ao Customer."""
    key = stripe_customer_key(stripe_customer_id)
    account_id, stored = _decode(
        item, CUSTOMER_MAP_ENTITY, key, _pair("billing_account_id", "stripe_customer_id")
    )
    _expect(stored == stripe_customer_id and isinstance(account_id, str), CUSTOMER_MAP_ENTITY)
    return account_id


def encode_plan(plan: PlanVersion) -> Item:
    """Codifica a PlanVersion imutável."""
    return _item(PLAN_ENTITY, plan_version_key(plan.plan_version_id), plan)


def decode_plan(item: Item, plan_version_id: str) -> PlanVersion:
    """Decodifica a PlanVersion validando o identificador solicitado."""
    plan = _decode(item, PLAN_ENTITY, plan_version_key(plan_version_id), _plan)
    _expect(plan.plan_version_id == plan_version_id, PLAN_ENTITY)
    return plan


def encode_price_map(stripe_price_id: str, plan_version_id: str) -> Item:
    """Codifica o mapa Price para PlanVersion."""
    payload = {"plan_version_id": plan_version_id, "stripe_price_id": stripe_price_id}
    return _item(PRICE_MAP_ENTITY, stripe_price_key(stripe_price_id), payload)


def decode_price_map(item: Item, stripe_price_id: str) -> str:
    """Decodifica a PlanVersion mapeada ao Price."""
    key = stripe_price_key(stripe_price_id)
    plan_id, stored = _decode(
        item, PRICE_MAP_ENTITY, key, _pair("plan_version_id", "stripe_price_id")
    )
    _expect(stored == stripe_price_id and isinstance(plan_id, str), PRICE_MAP_ENTITY)
    return plan_id


def decode_idempotency_record(item: Item, identity: tuple[str, str, str]) -> IdempotencyRecord:
    """Decodifica o registro de idempotência validando tenant, escopo e chave."""
    key = idempotency_key(*identity)
    record = _decode(item, IDEMPOTENCY_ENTITY, key, _idempotency_record)
    _expect((record.tenant_id, record.scope, record.key) == identity, IDEMPOTENCY_ENTITY)
    return record


def _link_identity(link: BillingAccountTenantLink) -> dict[str, str]:
    return {
        "billing_account_id": link.billing_account_id,
        "tenant_id": link.tenant_id,
        "linked_by_user_id": link.linked_by_user_id,
        "reason_code": link.reason_code,
    }


def idempotency_digest(command: CreateBillingAccountCommand | LinkBillingTenantCommand) -> str:
    """Calcula o digest do comando sem os instantes gerados pelo servidor."""
    if isinstance(command, LinkBillingTenantCommand):
        expected = utc_attribute(command.expected_account_updated_at)
        identity = {"link": _link_identity(command.link), "expected_updated_at": expected}
    else:
        account = command.account
        identity = {
            "link": _link_identity(command.initial_tenant_link),
            "account": [
                account.billing_account_id,
                account.stripe_customer_id,
                account.owner_user_id,
                account.status,
            ],
        }
    return request_hash([type(command).__name__, command.idempotency_key, identity])


def audit_outbox_event(audit: BillingAuditEvent) -> OutboxEvent:
    """Converte a auditoria de billing em evento de outbox."""
    return OutboxEvent(
        tenant_id=BILLING_AUDIT_TENANT_ID,
        event_id=audit.event_id,
        event_type=audit.event_type,
        aggregate_id=audit.aggregate_id,
        payload={
            "actor_id": audit.actor_id,
            "reason_code": audit.reason_code,
            "attributes": dict(audit.attributes),
        },
        created_at=audit.occurred_at,
        delivered_at=None,
    )


def outbox_item(event: OutboxEvent) -> Item:
    """Codifica o evento igual ao encoder de outbox do control plane."""
    due = f"{timestamp(event.created_at)}#{key_component(event.event_id)}"
    pending = {"gsi6pk": "OUTBOX#PENDING", "gsi6sk": due}
    attributes = pending if event.delivered_at is None else {}
    return encode_model(event, "OUTBOXEVENT", outbox_key(event.event_id), attributes)


def idempotency_item(record: IdempotencyRecord) -> Item:
    """Codifica o registro igual ao encoder de idempotência do control plane."""
    key = idempotency_key(record.tenant_id, record.scope, record.key)
    item = encode_model(record, IDEMPOTENCY_ENTITY, key)
    item["expires_at"] = {"N": str(int(record.expires_at.timestamp()))}
    return item


def put_new(table_name: str, item: Item) -> Action:
    """Cria um Put que exige ausência da chave."""
    return put_action(table_name, item, None)


def get_item(client: Any, table_name: str, key: Key, strong: bool) -> Item | None:
    """Lê um item pela chave base, convertendo falhas de storage."""
    try:
        response = client.get_item(TableName=table_name, Key=item_key(*key), ConsistentRead=strong)
    except (ClientError, BotoCoreError) as error:
        raise BillingDependencyError(UNAVAILABLE_CODE) from error
    return response.get("Item")


def transact(client: Any, actions: tuple[Action, ...]) -> bool:
    """Executa a transação; False em cancelamento condicional.

    Args: Cliente DynamoDB e ações de chaves únicas.
    Returns: True se gravou; False se uma condição falhou.
    Raises: PermanentBillingError, BillingDependencyError.
    """
    try:
        execute_transaction(client, actions)
    except Conflict as error:
        if error.code in _LIMIT_CODES:
            raise PermanentBillingError(_LIMIT_CODES[error.code]) from error
        return False
    except (ClientError, BotoCoreError) as error:
        raise BillingDependencyError(UNAVAILABLE_CODE) from error
    return True
