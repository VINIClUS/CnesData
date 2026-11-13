"""Contexto sintético de produção: record/control fortes, ledger de tickets e facades."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Final, Protocol, cast

from botocore.exceptions import ClientError

from cnes_domain.orchestration.mode_canary import (
    ModeCanaryBinding,
    ModeCanaryControl,
    ModeCanaryGrant,
    ModeCanaryRecord,
    SyntheticContextRequired,
    SyntheticEffectTicket,
    is_canonical_mode_canary_tenant,
)
from cnes_infra.control_plane.dynamodb_keys import run_partition, tenant_partition

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from cnes_domain.control_plane.environment_gate import PermitHolder
    from cnes_domain.ports.processing import StartRunExecution

_TENANT_PREFIX: Final = "prod-canary-"
GENESIS_LEDGER_SHA256: Final = sha256(b"[]\n").hexdigest()
_CEILINGS: Final = {
    "promotion": {"unit_tasks": 8, "recovery_resumes": 8, "recovery_hangs": 1},
    "readiness": {"unit_tasks": 8, "recovery_resumes": 8, "recovery_hangs": 2},
}
_COUNTERS: Final = {
    "start-execution": "unit_tasks", "run-recovery": "recovery_resumes",
    "run-hang": "recovery_hangs",
}
_KINDS: Final = frozenset({*_COUNTERS, "native-mutation", "settle-permit"})
_ATTEMPTS: Final = 3
_SEAL_ATTEMPTS: Final = 64


def canonical_text(mapping: dict[str, Any]) -> str:
    """Returns: JSON ordenado e compacto em UTF-8 com LF terminal único."""
    return json.dumps(mapping, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"


def _digest(text: str) -> str:
    return sha256(text.encode()).hexdigest()


def _stamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")


def _refuse(reason: str) -> SyntheticContextRequired:
    return SyntheticContextRequired(f"reason={reason} field=tenant_id")


def start_effect_id(
    record_sha256: str, holder: PermitHolder | StartRunExecution, unit_ids: Sequence[str],
) -> str:
    """Returns: effect_id do start nativo ligado ao record e ao request do dispatch."""
    return _effect_id(record_sha256, "start-execution", _start_request(holder, unit_ids))


def _start_request(holder: PermitHolder | StartRunExecution, unit_ids: Sequence[str]) -> str:
    return _digest(canonical_text({
        "dispatch_id": holder.dispatch_id, "run_id": holder.run_id,
        "tenant_id": holder.tenant_id, "unit_ids": list(unit_ids),
    }))


def _effect_id(record_sha256: str, kind: str, request_sha256: str) -> str:
    return _digest(canonical_text({
        "effect_kind": kind, "phase_record_sha256": record_sha256,
        "request_sha256": request_sha256,
    }))


class DispatchReader(Protocol):
    """Leitura forte do dispatch nativo reservado para o run."""

    def get_run_dispatch(self, tenant_id: str, run_id: str) -> Any:
        raise NotImplementedError


@dataclass(frozen=True, slots=True)
class RuntimeBounds:
    """Teto absoluto e falha autenticados do contexto ativo."""

    deadline: datetime
    fault: str


@dataclass(frozen=True, slots=True)
class _Effect:
    kind: str
    request_sha256: str
    target_sha256: str
    amount: int
    binding_item: dict[str, Any] | None


def _put(table: str, item: dict[str, str], condition: str, values: dict[str, Any] | None):
    put: dict[str, Any] = {
        "TableName": table, "Item": {name: {"S": value} for name, value in item.items()},
        "ConditionExpression": condition,
    }
    if values:
        put["ExpressionAttributeValues"] = values
    return {"Put": put}


def _cancelled(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code")
    return code in {"TransactionCanceledException", "ConditionalCheckFailedException"}


class _Store:
    def __init__(self, client: Any, table_name: str, tenant_id: str) -> None:
        self.client = client
        self.table = table_name
        self.pk = tenant_partition(tenant_id)

    def payload(self, sort_key: str, pk: str | None = None) -> str | None:
        key = {"pk": {"S": pk or self.pk}, "sk": {"S": sort_key}}
        item = self.client.get_item(TableName=self.table, Key=key, ConsistentRead=True).get("Item")
        return None if item is None else cast("str", item["payload"]["S"])

    def item(self, sort_key: str, entity: str, text: str) -> dict[str, str]:
        return {"pk": self.pk, "sk": sort_key, "entity": entity, "payload": text}

    def cas(self, item: dict[str, str], old: str | None) -> bool:
        condition = "attribute_not_exists(pk)" if old is None else "payload = :old"
        values = None if old is None else {":old": {"S": old}}
        try:
            self.client.put_item(**_put(self.table, item, condition, values)["Put"])
        except ClientError as error:
            if not _cancelled(error):
                raise
            return False
        return True


def _mapping(text: str, kind: str) -> dict[str, Any]:
    try:
        mapping = json.loads(text)
    except json.JSONDecodeError as error:
        raise _refuse(f"{kind}_invalid") from error
    if type(mapping) is not dict:
        raise _refuse(f"{kind}_invalid")
    if canonical_text(cast("dict[str, Any]", mapping)) != text:
        raise _refuse(f"{kind}_not_canonical")
    return cast("dict[str, Any]", mapping)


def _parse_control(text: str) -> ModeCanaryControl:
    try:
        return ModeCanaryControl.from_mapping(_mapping(text, "control"))
    except ValueError as error:
        if isinstance(error, SyntheticContextRequired):
            raise
        raise _refuse("control_invalid") from error


def initialize_control(client: Any, table_name: str, tenant_id: str, control: dict[str, Any]):
    """Grava o control ACTIVE inicial, insert-only; replay só com bytes idênticos.

    Raises: SyntheticContextRequired: Tenant, control inválido ou divergente do gravado.
    """
    if not is_canonical_mode_canary_tenant(tenant_id):
        raise _refuse("tenant_invalid")
    try:
        parsed = ModeCanaryControl.from_mapping(control)
    except ValueError as error:
        raise _refuse("control_invalid") from error
    initial = (parsed.admission_state == "ACTIVE" and not any(parsed.admitted_counts.values())
               and parsed.ledger_sha256 == GENESIS_LEDGER_SHA256)
    if not initial:
        raise _refuse("control_invalid")
    store = _Store(client, table_name, tenant_id)
    sort_key = f"PROD_CANARY_CONTROL#{parsed.owner_sha256}"
    text = canonical_text(control)
    if store.cas(store.item(sort_key, "MODECANARYCONTROL", text), None):
        return
    if store.payload(sort_key) != text:
        raise _refuse("control_conflict")


class ModeCanaryContext:
    """`ModeCanaryAdmissionProvider` e ledger de efeitos sobre record e control do owner."""

    def __init__(
        self, client: Any, table_name: str, binding: ModeCanaryBinding, dispatches: DispatchReader,
    ) -> None:
        self._store = _Store(client, table_name, binding.tenant_id)
        self._binding = binding
        self._dispatches = dispatches

    @property
    def binding(self) -> ModeCanaryBinding:
        """Returns: Locator validado do contexto."""
        return self._binding

    def binding_for(self, holder: PermitHolder, now: datetime) -> ModeCanaryGrant:
        """Returns: Grant do holder depois de admitir o ticket do start do seu dispatch.

        Raises:
            SyntheticContextRequired: Holder fora do binding, record ou control ausente,
                adulterado, divergente, selado, vencido, dispatch divergente ou orçamento esgotado.
        """
        if not self._binding.permits(holder.tenant_id, holder.run_id):
            raise _refuse("run_not_bound")
        record = self._record()
        if holder.run_id not in record.run_ids:
            raise _refuse("run_not_bound")
        dispatch = self._dispatches.get_run_dispatch(holder.tenant_id, holder.run_id)
        if dispatch is None or dispatch.dispatch_id != holder.dispatch_id:
            raise _refuse("dispatch_mismatch")
        units = tuple(cast("Sequence[str]", dispatch.unit_ids))
        effect = _Effect(
            "start-execution",
            _start_request(holder, units),
            _digest(record.machine_arn), len(units), self._binding_item(holder),
        )
        self._admit(record, effect, now)
        return ModeCanaryGrant(record.promotion_id)

    def admit_effect(
        self, kind: str, request_sha256: str, target_sha256: str, now: datetime,
    ) -> str:
        """Returns: effect_id do ticket admitido (ou reutilizado) antes do efeito sintético.

        Raises: SyntheticContextRequired: Tipo fora do enum, contexto inválido, selado, vencido
            ou orçamento esgotado.
        """
        if kind not in _KINDS:
            raise _refuse("effect_kind_invalid")
        record = self._record()
        self._admit(record, _Effect(kind, request_sha256, target_sha256, 1, None), now)
        return _effect_id(self._binding.record_sha256, kind, request_sha256)

    def runtime_bounds(self) -> RuntimeBounds:
        """Returns: Menor prazo entre record e control e o fault do record.

        Raises: SyntheticContextRequired: Record ou control ausente, divergente ou selado.
        """
        record = self._record()
        _, control = self._control(record)
        deadline = min(record.expires_at, record.canary_deadline, control.deadline)
        return RuntimeBounds(deadline, record.fault)

    def require_live(self, effect_id: str, now: datetime) -> None:
        """Raises: SyntheticContextRequired: Control selado/vencido ou ticket não pendente."""
        record = self._record()
        _, control = self._control(record)
        ticket = self._ticket(effect_id)
        if ticket is None:
            raise _refuse("ticket_missing")
        if ticket.state != "PENDING":
            raise _refuse(f"ticket_{ticket.state.lower()}")
        if now >= min(ticket.deadline, control.deadline, record.canary_deadline):
            raise _refuse("context_expired")

    def settle(self, effect_id: str, receipt_sha256: str) -> None:
        """Liquida o ticket com o recibo original; permitido depois do seal.

        Raises: SyntheticContextRequired: Ticket ausente ou já liquidado com outro recibo.
        """
        self._transition(effect_id, "SETTLED", receipt_sha256)

    def mark_unknown(self, effect_id: str) -> None:
        """Raises: SyntheticContextRequired: Ticket ausente ou já liquidado."""
        self._transition(effect_id, "UNKNOWN", None)

    def seal(self, now: datetime) -> None:
        """Sela a admissão ACTIVE→SEALED por CAS, insistindo contra admissões concorrentes.

        Raises: SyntheticContextRequired: Control ausente, inválido ou de outro owner.
        """
        sort_key = f"PROD_CANARY_CONTROL#{self._binding.owner_sha256}"
        for _ in range(_SEAL_ATTEMPTS):
            text = self._store.payload(sort_key)
            if text is None:
                raise _refuse("control_missing")
            control = _parse_control(text)
            if control.owner_sha256 != self._binding.owner_sha256:
                raise _refuse("control_mismatch")
            if control.admission_state == "SEALED":
                return
            sealed = _mapping(text, "control") | {"admission_state": "SEALED",
                                                  "sealed_at": _stamp(now)}
            item = self._store.item(sort_key, "MODECANARYCONTROL", canonical_text(sealed))
            if self._store.cas(item, text):
                return
        raise _refuse("ledger_contended")

    def _binding_item(self, holder: PermitHolder) -> dict[str, Any]:
        return {
            "schema": "cnesdata.mode-canary-binding/v1", "tenant_id": holder.tenant_id,
            "run_id": holder.run_id, "dispatch_id": holder.dispatch_id,
            "owner_sha256": self._binding.owner_sha256,
            "record_sha256": self._binding.record_sha256, "phase": self._binding.phase,
            "context_kind": self._binding.context_kind,
        }

    def _ticket_key(self, effect_id: str) -> str:
        return f"PROD_CANARY_EFFECT#{self._binding.owner_sha256}#{effect_id}"

    def _ticket_text(self, effect_id: str) -> str | None:
        return self._store.payload(self._ticket_key(effect_id))

    def _ticket(self, effect_id: str) -> SyntheticEffectTicket | None:
        text = self._ticket_text(effect_id)
        if text is None:
            return None
        try:
            return SyntheticEffectTicket.from_mapping(_mapping(text, "ticket"))
        except ValueError as error:
            if isinstance(error, SyntheticContextRequired):
                raise
            raise _refuse("ticket_invalid") from error

    def _transition(self, effect_id: str, state: str, receipt: str | None) -> None:
        for _ in range(_ATTEMPTS):
            text = self._ticket_text(effect_id)
            ticket = self._ticket(effect_id)
            if text is None or ticket is None:
                raise _refuse("ticket_missing")
            if ticket.state == state and ticket.receipt_sha256 == receipt:
                return
            if ticket.state == "SETTLED":
                raise _refuse("ticket_settled")
            changed = _mapping(text, "ticket") | {"state": state, "receipt_sha256": receipt}
            item = self._store.item(self._ticket_key(effect_id), "MODECANARYEFFECT",
                                    canonical_text(changed))
            if self._store.cas(item, text):
                return
        raise _refuse("ledger_contended")

    def _admit(self, record: ModeCanaryRecord, effect: _Effect, now: datetime) -> None:
        effect_id = _effect_id(self._binding.record_sha256, effect.kind, effect.request_sha256)
        for _ in range(_ATTEMPTS):
            control_text, control = self._control(record)
            existing = self._ticket(effect_id)
            if existing is not None:
                self._check_reentry(existing, effect)
                return
            deadline = min(record.expires_at, record.canary_deadline, control.deadline)
            if now >= deadline:
                raise _refuse("context_expired")
            ticket = canonical_text(self._new_ticket(effect_id, effect, control, (now, deadline)))
            if self._commit(control_text, ticket, effect, effect_id):
                return
        raise _refuse("ledger_contended")

    @staticmethod
    def _check_reentry(ticket: SyntheticEffectTicket, effect: _Effect) -> None:
        if (ticket.effect_kind, ticket.request_sha256) != (effect.kind, effect.request_sha256):
            raise _refuse("ticket_conflict")

    def _new_ticket(
        self, effect_id: str, effect: _Effect, control: ModeCanaryControl,
        window: tuple[datetime, datetime],
    ) -> dict[str, Any]:
        return {
            "schema": "cnesdata.mode-canary-effect/v1", "effect_id": effect_id,
            "owner_sha256": control.owner_sha256, "generation": control.generation,
            "context_manifest_sha256": control.context_manifest_sha256,
            "phase_record_sha256": self._binding.record_sha256, "effect_kind": effect.kind,
            "request_sha256": effect.request_sha256, "target_sha256": effect.target_sha256,
            "admitted_at": _stamp(window[0]), "deadline": _stamp(window[1]),
            "state": "PENDING", "receipt_sha256": None,
        }

    def _commit(self, control_text: str, ticket: str, effect: _Effect, effect_id: str) -> bool:
        control = _mapping(control_text, "control")
        counter = _COUNTERS.get(effect.kind)
        if counter is not None:
            total = control["admitted_counts"][counter] + effect.amount
            if total > _CEILINGS[control["context_kind"]][counter]:
                raise _refuse("budget_exhausted")
            control["admitted_counts"] = control["admitted_counts"] | {counter: total}
        control["ledger_sha256"] = _digest(canonical_text({
            "previous_ledger_sha256": control["ledger_sha256"], "ticket_sha256": _digest(ticket),
        }))
        store = self._store
        control_key = f"PROD_CANARY_CONTROL#{self._binding.owner_sha256}"
        items = [
            _put(store.table, store.item(self._ticket_key(effect_id), "MODECANARYEFFECT", ticket),
                 "attribute_not_exists(pk)", None),
            _put(store.table, store.item(control_key, "MODECANARYCONTROL",
                                         canonical_text(control)),
                 "payload = :old", {":old": {"S": control_text}}),
        ]
        if effect.binding_item is not None:
            items.append(_put(store.table, self._binding_put(effect.binding_item),
                              "attribute_not_exists(pk)", None))
        try:
            store.client.transact_write_items(TransactItems=items)
        except ClientError as error:
            if not _cancelled(error):
                raise
            return False
        return True

    def _binding_put(self, binding: dict[str, Any]) -> dict[str, str]:
        return {
            "pk": run_partition(binding["tenant_id"], binding["run_id"]),
            "sk": f"PROD_CANARY#{binding['dispatch_id']}", "entity": "MODECANARYBINDING",
            "payload": canonical_text(binding),
        }

    def _record(self) -> ModeCanaryRecord:
        binding = self._binding
        text = self._store.payload(f"PROD_CANARY#{binding.owner_sha256}#{binding.phase}")
        if text is None:
            raise _refuse("record_missing")
        if _digest(text) != binding.record_sha256:
            raise _refuse("record_hash_mismatch")
        mapping = _mapping(text, "record")
        try:
            record = ModeCanaryRecord.from_mapping(mapping)
        except ValueError as error:
            raise _refuse("record_invalid") from error
        if (
            record.owner_sha256 != binding.owner_sha256
            or record.phase != binding.phase
            or record.context_kind != binding.context_kind
            or binding.tenant_id != f"{_TENANT_PREFIX}{record.canary_id}"
        ):
            raise _refuse("record_mismatch")
        return record

    def _control(self, record: ModeCanaryRecord) -> tuple[str, ModeCanaryControl]:
        text = self._store.payload(f"PROD_CANARY_CONTROL#{self._binding.owner_sha256}")
        if text is None:
            raise _refuse("control_missing")
        control = _parse_control(text)
        generation = record.promotion_generation
        if (
            control.owner_sha256 != record.owner_sha256
            or control.context_kind != record.context_kind
            or control.context_manifest_sha256 != record.context_manifest_sha256
            or (generation is not None and control.generation != generation)
        ):
            raise _refuse("control_mismatch")
        if control.admission_state != "ACTIVE":
            raise _refuse("control_sealed")
        return text, control


__all__ = [
    "GENESIS_LEDGER_SHA256",
    "DispatchReader",
    "ModeCanaryContext",
    "RuntimeBounds",
    "canonical_text",
    "initialize_control",
    "start_effect_id",
]
