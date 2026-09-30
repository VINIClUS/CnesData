"""DynamoDB item codec shared by the billing adapters."""

import hashlib
import json
from collections.abc import Callable, Mapping
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from botocore.exceptions import ClientError

from cnes_domain.billing.commands import CreateBillingAccountCommand, LinkBillingTenantCommand
from cnes_domain.billing.errors import BillingDependencyError, PermanentBillingError
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
    decode_model,
    encode_model,
    execute_transaction,
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
_INVALID_TRANSACTION_CODES = {
    ErrorCode.TRANSACTION_LIMIT: "billing_transaction_too_large",
    ErrorCode.DUPLICATE_TRANSACTION_KEY: "billing_duplicate_action",
}


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, frozenset | tuple):
        ordered = sorted(value) if isinstance(value, frozenset) else value
        return [_jsonable(item) for item in ordered]
    if is_dataclass(value):
        value = {field.name: getattr(value, field.name) for field in fields(value)}
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    return value


def canonical_json(value: Any) -> str:
    """Serializa valores de billing em JSON canônico e ordenado."""
    return json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":"))


def request_hash(value: Any) -> str:
    """Calcula o sha256 do JSON canônico de um comando."""
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def deterministic_id(*parts: str) -> str:
    """Deriva um identificador determinístico a partir das partes."""
    return hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:32]


def utc_attribute(value: datetime) -> str:
    """Codifica um instante UTC em largura fixa para condições."""
    require_utc(value, "value")
    return timestamp(value.astimezone(UTC))


def corrupt_item(entity: str) -> PermanentBillingError:
    """Cria o erro estável de item de billing corrompido."""
    return PermanentBillingError("billing_item_corrupt", detail=f"entity={entity}")


def _item(entity: str, key: tuple[str, str], value: Any) -> Item:
    return {
        "pk": {"S": key[0]},
        "sk": {"S": key[1]},
        "entity": {"S": entity},
        "payload": {"S": canonical_json(value)},
    }


def _decode[T](
    item: Item, entity: str, key: tuple[str, str], build: Callable[[dict[str, Any]], T]
) -> T:
    try:
        if item["entity"]["S"] != entity or (item["pk"]["S"], item["sk"]["S"]) != key:
            raise ValueError("reason=item_identity_mismatch")
        return build(json.loads(item["payload"]["S"]))
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(entity) from error


def _require_equal(actual: object, expected: object) -> None:
    if actual != expected:
        raise ValueError("reason=decoded_id_mismatch")


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _optional_dt(value: str | None) -> datetime | None:
    return None if value is None else _dt(value)


def encode_snapshot(snapshot: EntitlementSnapshot) -> Item:
    """Codifica o snapshot com a versão como atributo numérico."""
    item = _item(SNAPSHOT_ENTITY, entitlement_snapshot_key(snapshot.billing_account_id), snapshot)
    item["entitlement_version"] = {"N": str(snapshot.entitlement_version)}
    item["subscription_status"] = {"S": snapshot.subscription_status.value}
    item["valid_until"] = {"S": utc_attribute(snapshot.valid_until)}
    return item


def _build_snapshot(data: dict[str, Any]) -> EntitlementSnapshot:
    return EntitlementSnapshot(
        **{
            **data,
            "subscription_status": SubscriptionStatus(data["subscription_status"]),
            "features": frozenset(data["features"]),
            "quotas": QuotaLimits(**data["quotas"]),
            "period_start": _dt(data["period_start"]),
            "period_end": _dt(data["period_end"]),
            "grace_until": _optional_dt(data["grace_until"]),
            "valid_until": _dt(data["valid_until"]),
            "updated_at": _dt(data["updated_at"]),
        }
    )


def decode_snapshot(item: Item, billing_account_id: str) -> EntitlementSnapshot:
    """Decodifica o snapshot e rejeita identidade ou versão divergente."""

    def build(data: dict[str, Any]) -> EntitlementSnapshot:
        snapshot = _build_snapshot(data)
        _require_equal(snapshot.billing_account_id, billing_account_id)
        _require_equal(str(snapshot.entitlement_version), item["entitlement_version"]["N"])
        return snapshot

    key = entitlement_snapshot_key(billing_account_id)
    return _decode(item, SNAPSHOT_ENTITY, key, build)


def encode_account(account: BillingAccount) -> Item:
    """Codifica a conta com atributos usados em condições."""
    item = _item(ACCOUNT_ENTITY, billing_account_key(account.billing_account_id), account)
    item["status"] = {"S": account.status.value}
    item["owner_user_id"] = {"S": account.owner_user_id}
    item["updated_at"] = {"S": utc_attribute(account.updated_at)}
    if account.stripe_customer_id is not None:
        item["stripe_customer_id"] = {"S": account.stripe_customer_id}
    return item


def decode_account(item: Item, billing_account_id: str) -> BillingAccount:
    """Decodifica a conta e rejeita identidade divergente."""

    def build(data: dict[str, Any]) -> BillingAccount:
        account = BillingAccount(
            **{
                **data,
                "status": BillingAccountStatus(data["status"]),
                "created_at": _dt(data["created_at"]),
                "updated_at": _dt(data["updated_at"]),
            }
        )
        _require_equal(account.billing_account_id, billing_account_id)
        return account

    return _decode(item, ACCOUNT_ENTITY, billing_account_key(billing_account_id), build)


def encode_account_list_row(account: BillingAccount) -> Item:
    """Codifica a linha da lista global de contas."""
    key = billing_account_list_key(account.billing_account_id)
    item = _item(ACCOUNT_LIST_ENTITY, key, {"billing_account_id": account.billing_account_id})
    if account.stripe_customer_id is not None:
        item["stripe_customer_id"] = {"S": account.stripe_customer_id}
    return item


def decode_account_list_row(item: Item) -> str:
    """Decodifica o ID de conta de uma linha da lista global."""
    try:
        sort_key = item["sk"]["S"]
        if not sort_key.startswith(BILLING_ACCOUNT_LIST_PREFIX):
            raise ValueError("reason=account_list_prefix")
        account_id = bytes.fromhex(sort_key.removeprefix(BILLING_ACCOUNT_LIST_PREFIX)).decode()
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(ACCOUNT_LIST_ENTITY) from error

    def build(data: dict[str, Any]) -> str:
        _require_equal(data["billing_account_id"], account_id)
        return account_id

    key = billing_account_list_key(account_id)
    return _decode(item, ACCOUNT_LIST_ENTITY, key, build)


def encode_link(link: BillingAccountTenantLink) -> Item:
    """Codifica o link conta para tenant."""
    key = account_tenant_key(link.billing_account_id, link.tenant_id)
    return _item(ACCOUNT_TENANT_ENTITY, key, link)


def decode_link(item: Item, billing_account_id: str, tenant_id: str) -> BillingAccountTenantLink:
    """Decodifica o link e rejeita IDs divergentes dos solicitados."""

    def build(data: dict[str, Any]) -> BillingAccountTenantLink:
        link = BillingAccountTenantLink(**{**data, "linked_at": _dt(data["linked_at"])})
        _require_equal((link.billing_account_id, link.tenant_id), (billing_account_id, tenant_id))
        return link

    key = account_tenant_key(billing_account_id, tenant_id)
    return _decode(item, ACCOUNT_TENANT_ENTITY, key, build)


def encode_tenant_account(link: BillingAccountTenantLink) -> Item:
    """Codifica o link reverso único tenant para conta."""
    value = {"billing_account_id": link.billing_account_id, "tenant_id": link.tenant_id}
    return _item(TENANT_ACCOUNT_ENTITY, tenant_account_key(link.tenant_id), value)


def decode_tenant_account(item: Item, tenant_id: str) -> str:
    """Decodifica o ID da conta dona do tenant."""

    def build(data: dict[str, Any]) -> str:
        _require_equal(data["tenant_id"], tenant_id)
        return str(data["billing_account_id"])

    return _decode(item, TENANT_ACCOUNT_ENTITY, tenant_account_key(tenant_id), build)


def encode_customer_map(billing_account_id: str, stripe_customer_id: str) -> Item:
    """Codifica o mapa único Customer para conta."""
    value = {"billing_account_id": billing_account_id, "stripe_customer_id": stripe_customer_id}
    return _item(CUSTOMER_MAP_ENTITY, stripe_customer_key(stripe_customer_id), value)


def decode_customer_map(item: Item, stripe_customer_id: str) -> str:
    """Decodifica o ID da conta mapeada ao Customer."""

    def build(data: dict[str, Any]) -> str:
        _require_equal(data["stripe_customer_id"], stripe_customer_id)
        return str(data["billing_account_id"])

    key = stripe_customer_key(stripe_customer_id)
    return _decode(item, CUSTOMER_MAP_ENTITY, key, build)


def encode_plan(plan: PlanVersion) -> Item:
    """Codifica a PlanVersion imutável."""
    return _item(PLAN_ENTITY, plan_version_key(plan.plan_version_id), plan)


def decode_plan(item: Item, plan_version_id: str) -> PlanVersion:
    """Decodifica a PlanVersion e rejeita identidade divergente."""

    def build(data: dict[str, Any]) -> PlanVersion:
        plan = PlanVersion(
            **{
                **data,
                "stripe_price_ids": tuple(data["stripe_price_ids"]),
                "features": frozenset(data["features"]),
                "quotas": QuotaLimits(**data["quotas"]),
                "effective_from": _dt(data["effective_from"]),
            }
        )
        _require_equal(plan.plan_version_id, plan_version_id)
        return plan

    return _decode(item, PLAN_ENTITY, plan_version_key(plan_version_id), build)


def encode_price_map(stripe_price_id: str, plan_version_id: str) -> Item:
    """Codifica o mapa Price para PlanVersion."""
    value = {"plan_version_id": plan_version_id, "stripe_price_id": stripe_price_id}
    return _item(PRICE_MAP_ENTITY, stripe_price_key(stripe_price_id), value)


def decode_price_map(item: Item, stripe_price_id: str) -> str:
    """Decodifica o ID da PlanVersion mapeada ao Price."""

    def build(data: dict[str, Any]) -> str:
        _require_equal(data["stripe_price_id"], stripe_price_id)
        return str(data["plan_version_id"])

    return _decode(item, PRICE_MAP_ENTITY, stripe_price_key(stripe_price_id), build)


def audit_outbox_event(event: BillingAuditEvent) -> OutboxEvent:
    """Converte um audit de billing no OutboxEvent de escopo de conta."""
    return OutboxEvent(
        tenant_id=BILLING_AUDIT_TENANT_ID,
        event_id=event.event_id,
        event_type=event.event_type,
        aggregate_id=event.aggregate_id,
        payload={
            "actor_id": event.actor_id,
            "reason_code": event.reason_code,
            "attributes": dict(event.attributes),
        },
        created_at=event.occurred_at,
        delivered_at=None,
    )


def outbox_item(event: OutboxEvent) -> Item:
    """Codifica um OutboxEvent pendente no formato canônico CND."""
    attributes = {
        "gsi6pk": "OUTBOX#PENDING",
        "gsi6sk": f"{timestamp(event.created_at)}#{key_component(event.event_id)}",
    }
    return encode_model(event, "OUTBOXEVENT", outbox_key(event.event_id), attributes)


def idempotency_item(record: IdempotencyRecord) -> Item:
    """Codifica um IdempotencyRecord no formato canônico CND."""
    key = idempotency_key(record.tenant_id, record.scope, record.key)
    item = encode_model(record, "IDEMPOTENCYRECORD", key)
    item["expires_at"] = {"N": str(int(record.expires_at.timestamp()))}
    return item


def idempotency_digest(command: CreateBillingAccountCommand | LinkBillingTenantCommand) -> str:
    """Calcula o hash de idempotência sem timestamps definidos pelo servidor."""
    if isinstance(command, CreateBillingAccountCommand):
        account, link = command.account, command.initial_tenant_link
        return request_hash(
            {
                "billing_account_id": account.billing_account_id,
                "owner_user_id": account.owner_user_id,
                "status": account.status,
                "tenant_id": link.tenant_id,
                "linked_by_user_id": link.linked_by_user_id,
                "reason_code": link.reason_code,
            }
        )
    link = command.link
    return request_hash(
        {
            "billing_account_id": link.billing_account_id,
            "tenant_id": link.tenant_id,
            "linked_by_user_id": link.linked_by_user_id,
            "reason_code": link.reason_code,
            "expected_account_updated_at": utc_attribute(command.expected_account_updated_at),
        }
    )


def decode_idempotency_record(item: Item, identity: tuple[str, str, str]) -> IdempotencyRecord:
    """Decodifica o registro e rejeita identidade divergente da solicitada."""
    try:
        record = decode_model(item, IdempotencyRecord)
    except (KeyError, TypeError, ValueError) as error:
        raise corrupt_item(IDEMPOTENCY_ENTITY) from error
    if item.get("entity", {}).get("S") != IDEMPOTENCY_ENTITY or (
        (record.tenant_id, record.scope, record.key) != identity
    ):
        raise corrupt_item(IDEMPOTENCY_ENTITY)
    return record


def put_new(table_name: str, item: Item) -> Action:
    """Cria um Put condicionado à ausência da chave."""
    return {
        "Put": {
            "TableName": table_name,
            "Item": item,
            "ConditionExpression": "attribute_not_exists(pk)",
        }
    }


def get_item(client: Any, table_name: str, key: tuple[str, str], strong: bool) -> Item | None:
    """Lê um item por base key; falha de storage vira erro retryable."""
    try:
        response = client.get_item(TableName=table_name, Key=item_key(*key), ConsistentRead=strong)
    except ClientError as error:
        raise BillingDependencyError("dynamodb_unavailable") from error
    return response.get("Item")


def transact(client: Any, actions: tuple[Action, ...]) -> bool:
    """Envia a transação; retorna False só em cancelamento condicional."""
    try:
        execute_transaction(client, actions)
    except Conflict as error:
        invalid = _INVALID_TRANSACTION_CODES.get(error.code)
        if invalid is not None:
            raise PermanentBillingError(invalid) from error
        return False
    except ClientError as error:
        raise BillingDependencyError("dynamodb_unavailable") from error
    return True
