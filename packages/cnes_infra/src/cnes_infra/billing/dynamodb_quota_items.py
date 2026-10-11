"""DynamoDB item codec for quota usage, reservations and run billing companions."""

import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from cnes_domain.billing.errors import EntitlementDenied, IdempotencyConflict
from cnes_domain.billing.execution import RunBillingState
from cnes_domain.billing.models import (
    AnalyticsAuthorization,
    CapacityKind,
    CapacityReservation,
    EntitlementSnapshot,
    QuotaReservation,
    ReservationKind,
    ReservationStatus,
    RunAuthorization,
    SubscriptionStatus,
)
from cnes_domain.control_plane.entities import IdempotencyRecord, OutboxEvent
from cnes_domain.control_plane.enums import DispatchOutcome, DispatchState
from cnes_infra.billing.dynamodb_items import (
    IDEMPOTENCY_ENTITY,
    canonical_json,
    corrupt_item,
    decode_idempotency_record,
    deterministic_id,
    get_item,
    idempotency_item,
    utc_attribute,
)
from cnes_infra.billing.keys import (
    QUOTA_RESERVATION_DUE_PARTITION,
    QUOTA_RESERVATION_LOCATOR_SORT_KEY,
    Key,
    capacity_reservation_key,
    entitlement_snapshot_key,
    quota_reservation_due_sort_key,
    quota_reservation_locator,
    reservation_key,
    run_billing_key,
    run_lookup_key,
)
from cnes_infra.control_plane.dynamodb_codec import Action, Item, payload, put_action
from cnes_infra.control_plane.dynamodb_keys import idempotency_key

RESERVATION_ENTITY = "QUOTARESERVATION"
CAPACITY_RESERVATION_ENTITY = "CAPACITYRESERVATION"
USAGE_ENTITY = "BILLINGUSAGE"
RUN_BILLING_ENTITY = "RUNBILLINGSTATE"
RUN_LOOKUP_ENTITY = "BILLINGRUNLOOKUP"
RESULT_ATTRIBUTE = "result"
RUN_SCOPE = "billing.quota.run"
ANALYTICS_SCOPE = "billing.quota.analytics"
CAPACITY_SCOPE = "billing.quota.capacity"
IDEMPOTENCY_TTL = timedelta(days=1)
CONSUMED_RUNS = "consumed_runs"
CAPACITY_COUNTERS = {CapacityKind.TENANT: "tenant_count", CapacityKind.AGENT: "agent_count"}
_DECODE_ERRORS = (KeyError, TypeError, ValueError, AttributeError)


@dataclass(frozen=True, slots=True)
class ScanAttributes:
    reserved: str
    consumed: str
    committed: str


@dataclass(frozen=True, slots=True)
class UsageGuard:
    attribute: str
    ceiling: int


@dataclass(frozen=True, slots=True)
class SnapshotExpectation:
    billing_account_id: str
    entitlement_version: int
    subscription_status: SubscriptionStatus | None


def scan_attributes(kind: ReservationKind) -> ScanAttributes:
    """Retorna os nomes dos contadores de scan/compute do tipo de reserva."""
    prefix = kind.value
    return ScanAttributes(
        f"{prefix}_reserved_scan_bytes",
        f"{prefix}_consumed_scan_bytes",
        f"{prefix}_committed_scan_bytes",
    )


def _text(value: str) -> dict[str, str]:
    return {"S": value}


def _number(value: int) -> dict[str, str]:
    return {"N": str(value)}


def _when(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _base_item(entity: str, key: Key, value: Any) -> Item:
    return {
        "pk": _text(key[0]),
        "sk": _text(key[1]),
        "entity": _text(entity),
        "payload": _text(canonical_json(value)),
    }


def _decode_payload[T](item: Item, entity: str, build: Callable[[Any], T]) -> T:
    try:
        if item["entity"] != _text(entity):
            raise corrupt_item(entity)
        return build(json.loads(item["payload"]["S"]))
    except _DECODE_ERRORS as error:
        raise corrupt_item(entity) from error


def _require_key(item: Item, key: Key, entity: str) -> None:
    if (item["pk"], item["sk"]) != (_text(key[0]), _text(key[1])):
        raise corrupt_item(entity)


def _due_attributes(expires_at: datetime, account_id: str, reservation_id: str) -> Item:
    sort_key = quota_reservation_due_sort_key(expires_at, account_id, reservation_id)
    return {"gsi1pk": _text(QUOTA_RESERVATION_DUE_PARTITION), "gsi1sk": _text(sort_key)}


def _tenant_of(item: Item, entity: str) -> str:
    tenant = item.get("tenant_id", {}).get("S")
    if not isinstance(tenant, str) or not tenant:
        raise corrupt_item(entity)
    return tenant


def _quota_reservation(data: dict[str, Any]) -> QuotaReservation:
    values: dict[str, Any] = {
        **data,
        "kind": ReservationKind(data["kind"]),
        "status": ReservationStatus(data["status"]),
        "period_start": _when(data["period_start"]),
        "created_at": _when(data["created_at"]),
        "expires_at": _when(data["expires_at"]),
    }
    return QuotaReservation(**values)


def reservation_item_key(reservation: QuotaReservation) -> Key:
    """Retorna a chave base da reserva de quota."""
    return reservation_key(
        reservation.billing_account_id, reservation.period_start, reservation.reservation_id
    )


def encode_reservation(reservation: QuotaReservation, tenant_id: str) -> Item:
    """Codifica a reserva com localizador e índice de vencimento enquanto reservada."""
    account, reservation_id = reservation.billing_account_id, reservation.reservation_id
    item = _base_item(RESERVATION_ENTITY, reservation_item_key(reservation), reservation)
    item["tenant_id"] = _text(tenant_id)
    item["status"] = _text(reservation.status.value)
    item["gsi2pk"] = _text(quota_reservation_locator(account, reservation_id))
    item["gsi2sk"] = _text(QUOTA_RESERVATION_LOCATOR_SORT_KEY)
    if reservation.status is ReservationStatus.RESERVED:
        item.update(_due_attributes(reservation.expires_at, account, reservation_id))
    return item


def decode_reservation(item: Item) -> tuple[QuotaReservation, str]:
    """Decodifica a reserva e o tenant, validando entidade e chave."""
    reservation = _decode_payload(item, RESERVATION_ENTITY, _quota_reservation)
    _require_key(item, reservation_item_key(reservation), RESERVATION_ENTITY)
    return reservation, _tenant_of(item, RESERVATION_ENTITY)


def _capacity_reservation(data: dict[str, Any]) -> CapacityReservation:
    values: dict[str, Any] = {
        **data,
        "kind": CapacityKind(data["kind"]),
        "status": ReservationStatus(data["status"]),
        "created_at": _when(data["created_at"]),
        "expires_at": _when(data["expires_at"]),
    }
    return CapacityReservation(**values)


def encode_capacity_reservation(reservation: CapacityReservation, tenant_id: str) -> Item:
    """Codifica a reserva de capacidade com índice de vencimento enquanto reservada."""
    account, reservation_id = reservation.billing_account_id, reservation.reservation_id
    key = capacity_reservation_key(account, reservation_id)
    item = _base_item(CAPACITY_RESERVATION_ENTITY, key, reservation)
    item["tenant_id"] = _text(tenant_id)
    item["status"] = _text(reservation.status.value)
    if reservation.status is ReservationStatus.RESERVED:
        item.update(_due_attributes(reservation.expires_at, account, reservation_id))
    return item


def decode_capacity_reservation(item: Item) -> tuple[CapacityReservation, str]:
    """Decodifica a reserva de capacidade e o tenant, validando entidade e chave."""
    reservation = _decode_payload(item, CAPACITY_RESERVATION_ENTITY, _capacity_reservation)
    key = capacity_reservation_key(reservation.billing_account_id, reservation.reservation_id)
    _require_key(item, key, CAPACITY_RESERVATION_ENTITY)
    return reservation, _tenant_of(item, CAPACITY_RESERVATION_ENTITY)


def _run_authorization(data: dict[str, Any]) -> RunAuthorization:
    values: dict[str, Any] = {**data, "authorized_at": _when(data["authorized_at"])}
    return RunAuthorization(**values)


def _analytics_authorization(data: dict[str, Any]) -> AnalyticsAuthorization:
    values: dict[str, Any] = {**data, "authorized_at": _when(data["authorized_at"])}
    return AnalyticsAuthorization(**values)


def _run_billing_state(data: dict[str, Any]) -> RunBillingState:
    status = data["execution_status"]
    outcome = data["execution_terminal_outcome"]
    values: dict[str, Any] = {
        **data,
        "authorization": _run_authorization(data["authorization"]),
        "execution_unit_ids": tuple(data["execution_unit_ids"]),
        "execution_status": None if status is None else DispatchState(status),
        "execution_terminal_outcome": None if outcome is None else DispatchOutcome(outcome),
        "updated_at": _when(data["updated_at"]),
    }
    return RunBillingState(**values)


def encode_run_billing_state(state: RunBillingState) -> Item:
    """Codifica o companion de billing do Run."""
    key = run_billing_key(state.tenant_id, state.run_id)
    return _base_item(RUN_BILLING_ENTITY, key, state)


def decode_run_billing_state(item: Item) -> RunBillingState:
    """Decodifica o companion de billing do Run, validando entidade e chave."""
    state = _decode_payload(item, RUN_BILLING_ENTITY, _run_billing_state)
    _require_key(item, run_billing_key(state.tenant_id, state.run_id), RUN_BILLING_ENTITY)
    return state


def encode_run_lookup(state: RunBillingState, reservation: QuotaReservation) -> Item:
    """Codifica o lookup do Run na conta de billing."""
    key = run_lookup_key(state.billing_account_id, state.tenant_id, state.run_id)
    value = {
        "billing_account_id": state.billing_account_id,
        "tenant_id": state.tenant_id,
        "run_id": state.run_id,
        "reservation_id": reservation.reservation_id,
        "period_start": reservation.period_start,
    }
    return _base_item(RUN_LOOKUP_ENTITY, key, value)


def with_result(item: Item, result: Any) -> Item:
    """Anexa o resultado gravado ao item de idempotência."""
    return {**item, RESULT_ATTRIBUTE: _text(canonical_json(result))}


def _result[T](item: Item, build: Callable[[Any], T]) -> T:
    try:
        return build(json.loads(item[RESULT_ATTRIBUTE]["S"]))
    except _DECODE_ERRORS as error:
        raise corrupt_item(IDEMPOTENCY_ENTITY) from error


def decode_run_result(item: Item) -> RunAuthorization:
    """Decodifica a autorização de Run gravada na idempotência."""
    return _result(item, _run_authorization)


def decode_analytics_result(item: Item) -> AnalyticsAuthorization:
    """Decodifica a autorização analítica gravada na idempotência."""
    return _result(item, _analytics_authorization)


def decode_capacity_result(item: Item) -> CapacityReservation:
    """Decodifica a reserva de capacidade gravada na idempotência."""
    return _result(item, _capacity_reservation)


def snapshot_check(
    table_name: str, expected: SnapshotExpectation, now: datetime
) -> Action:
    """Cria o ConditionCheck do snapshot: versão, status opcional e validade."""
    condition = "entitlement_version = :version AND valid_until > :now"
    values = {
        ":version": _number(expected.entitlement_version),
        ":now": _text(utc_attribute(now)),
    }
    if expected.subscription_status is not None:
        condition += " AND subscription_status = :status"
        values[":status"] = _text(expected.subscription_status.value)
    pk, sk = entitlement_snapshot_key(expected.billing_account_id)
    return {"ConditionCheck": {
        "TableName": table_name,
        "Key": {"pk": _text(pk), "sk": _text(sk)},
        "ConditionExpression": condition,
        "ExpressionAttributeValues": values,
    }}


def _add_expression(deltas: Mapping[str, int]) -> tuple[str, dict[str, str], Item]:
    names = {"#entity": "entity"}
    values: Item = {":entity": _text(USAGE_ENTITY)}
    clauses: list[str] = []
    for index, (attribute, delta) in enumerate(sorted(deltas.items())):
        names[f"#a{index}"] = attribute
        values[f":a{index}"] = _number(delta)
        clauses.append(f"#a{index} :a{index}")
    expression = "SET #entity = :entity ADD " + ", ".join(clauses)
    return expression, names, values


def usage_update(
    table_name: str, key: Key, deltas: Mapping[str, int], guard: UsageGuard | None
) -> Action:
    """Cria o Update de uso; com guarda, exige contador ausente ou <= teto."""
    expression, names, values = _add_expression(deltas)
    request: dict[str, Any] = {
        "TableName": table_name,
        "Key": {"pk": _text(key[0]), "sk": _text(key[1])},
        "UpdateExpression": expression,
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }
    if guard is not None:
        names["#guard"] = guard.attribute
        values[":ceiling"] = _number(guard.ceiling)
        request["ConditionExpression"] = "attribute_not_exists(#guard) OR #guard <= :ceiling"
    return {"Update": request}


def capacity_update(table_name: str, key: Key, attribute: str, ceiling: int | None) -> Action:
    """Cria o ADD de capacidade; exige contador semeado e, com teto, <= teto."""
    action = usage_update(table_name, key, {attribute: 1}, None)
    request = action["Update"]
    request["ExpressionAttributeNames"]["#guard"] = attribute
    condition = "attribute_exists(#guard)"
    if ceiling is not None:
        request["ExpressionAttributeValues"][":ceiling"] = _number(ceiling)
        condition += " AND #guard <= :ceiling"
    request["ConditionExpression"] = condition
    return action


def settle_usage_update(table_name: str, key: Key, deltas: Mapping[str, int]) -> Action:
    """Cria o Update de liquidação, que exige o item de uso existente."""
    action = usage_update(table_name, key, deltas, None)
    action["Update"]["ConditionExpression"] = "attribute_exists(pk)"
    return action


def usage_counter(item: Item | None, attribute: str) -> int:
    """Lê um contador numérico do item de uso; ausente vale zero."""
    if item is None or attribute not in item:
        return 0
    try:
        return int(item[attribute]["N"])
    except _DECODE_ERRORS as error:
        raise corrupt_item(USAGE_ENTITY) from error


def quota_event(
    event_type: str, tenant_id: str, payload: Mapping[str, str | int], created_at: datetime
) -> OutboxEvent:
    """Cria o evento de outbox determinístico da mutação de quota."""
    account_id = str(payload["billing_account_id"])
    reservation_id = str(payload["reservation_id"])
    return OutboxEvent(
        tenant_id=tenant_id,
        event_id=deterministic_id(event_type, account_id, reservation_id),
        event_type=event_type,
        aggregate_id=reservation_id,
        payload=dict(payload),
        created_at=created_at,
        delivered_at=None,
    )


@dataclass(frozen=True, slots=True)
class ReplayQuery:
    identity: tuple[str, str, str]
    request_hash: str
    now: datetime


@dataclass(frozen=True, slots=True)
class Replay:
    stored: Item | None
    expired: Item | None


def idempotency_put(
    table_name: str, record: IdempotencyRecord, result: Any, expired: Item | None
) -> Action:
    """Cria o Put da idempotência CND com o resultado; sobrescreve por CAS um expirado."""
    item = with_result(idempotency_item(record), result)
    return put_action(table_name, item, None if expired is None else payload(expired))


def read_replay(client: Any, table_name: str, query: ReplayQuery) -> Replay:
    """Lê fortemente a idempotência; registro expirado conta como ausente (semântica CND).

    Returns: Replay com o item vigente de mesmo hash ou o item expirado a sobrescrever.
    Raises: IdempotencyConflict: chave vigente com outro hash.
    """
    item = get_item(client, table_name, idempotency_key(*query.identity), True)
    if item is None:
        return Replay(None, None)
    record = decode_idempotency_record(item, query.identity)
    if record.expires_at <= query.now:
        return Replay(None, item)
    if record.request_hash != query.request_hash:
        raise IdempotencyConflict(f"key={query.identity[2]}")
    return Replay(item, None)


def collision_keys(actions: tuple[Action, ...], excluded: Key) -> tuple[Key, ...]:
    """Retorna as chaves dos Puts da transação, exceto a informada."""
    keys = (
        (action["Put"]["Item"]["pk"]["S"], action["Put"]["Item"]["sk"]["S"])
        for action in actions
        if "Put" in action
    )
    return tuple(key for key in keys if key != excluded)


def any_present(client: Any, table_name: str, keys: tuple[Key, ...]) -> bool:
    """Prova colisão: alguma chave já existe numa leitura forte."""
    return any(get_item(client, table_name, key, True) is not None for key in keys)


def require_commit_access(snapshot: EntitlementSnapshot, now: datetime) -> None:
    """Reaplica em now as regras temporais da política (carência, fim de período).

    Raises: EntitlementDenied: carência vencida ou período cancelado encerrado.
    """
    grace = snapshot.grace_until
    past_due = snapshot.subscription_status is SubscriptionStatus.PAST_DUE
    if past_due and (grace is None or now > grace):
        raise EntitlementDenied("reason=grace_expired")
    if snapshot.cancel_at_period_end and now > snapshot.period_end:
        raise EntitlementDenied("reason=period_ended")
